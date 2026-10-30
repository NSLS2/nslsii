"""JSON-lines log files for log collectors such as Grafana Alloy (Loki)."""

from __future__ import annotations

import getpass
import json
import logging
import os
import warnings
from datetime import UTC, datetime
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Mapping

    from bluesky import RunEngine
    from IPython.core.interactiveshell import InteractiveShell

LOGGER_NAMES = ("bluesky", "ophyd", "ophyd_async", "nslsii")
DEFAULT_LOG_DIR = "/var/log/bluesky"
# Rotation of each file: (bytes per file, number of backups). Alloy reads only
# the live file, so its size bounds how long Alloy can be down without losing
# lines; backups are kept only for reading on disk.
LOG_FILE_ROTATION = (10 * 1024 * 1024, 5)
DOCUMENT_FILE_ROTATION = (100 * 1024 * 1024, 1)

# Logger of the Bluesky document lines. Each record carries the line's fields
# as its ``document_fields`` attribute.
DOCUMENT_LOGGER_NAME = "nslsii.documents"
# The pointer fields of each document type: uids, links to other documents,
# positions within streams and resources, and a few identifiers. Data,
# configuration, metadata, file paths, and the document's own timestamp are
# *not* included.
POINTER_FIELDS: dict[str, tuple[str, ...]] = {
    "start": ("uid", "scan_id"),
    "descriptor": ("uid", "run_start", "name"),
    "event": ("uid", "descriptor", "seq_num"),
    "event_page": ("descriptor",),
    "stop": ("uid", "run_start", "exit_status"),
    "resource": ("uid", "run_start", "spec"),
    "datum": ("datum_id", "resource"),
    "datum_page": ("datum_id", "resource"),
    "stream_resource": ("uid", "run_start", "data_key", "mimetype"),
    "stream_datum": ("uid", "descriptor", "stream_resource", "seq_nums", "indices"),
}


def document_pointers(name: str, doc: Mapping[str, Any]) -> dict[str, Any]:
    """Return the ``POINTER_FIELDS`` that ``doc`` has; for event pages, also
    the entry count and the seq_num range covered (``stop`` exclusive).

    This runs in the RunEngine thread, where an exception aborts the run, so
    optional fields are copied only if present and empty pages are handled.
    """
    pointers = {key: doc[key] for key in POINTER_FIELDS.get(name, ()) if key in doc}
    if name == "event_page":
        # Special handling of the potentially long list of sequence numbers
        seq_nums = doc.get("seq_num", ())
        pointers["count"] = len(seq_nums)
        if len(seq_nums):
            pointers["seq_nums"] = {"start": int(min(seq_nums)), "stop": int(max(seq_nums)) + 1}
    return pointers


class OpenRuns:
    """The uids of the open RunEngine runs, in the order they were opened.

    ``uids`` is one space-separated string ("" if no run is open). Updates
    replace it rather than modify it, so other threads can read it while the
    RunEngine thread updates it.
    """

    def __init__(self) -> None:
        self.uids = ""

    def add(self, uid: str) -> None:
        self.uids = " ".join([*self.uids.split(), uid])

    def remove(self, uid: str) -> None:
        self.uids = " ".join(other for other in self.uids.split() if other != uid)


class DocumentPointerLoggingCallback:
    """RunEngine callback that logs the pointers of every document and keeps
    ``open_runs`` current.

    Each document is logged to ``logger`` at INFO with the document name as the
    message and ``{"document": name, **pointers}`` as the record's
    ``document_fields`` attribute.
    """

    def __init__(self, logger: logging.Logger, open_runs: OpenRuns) -> None:
        self._logger = logger
        self._open_runs = open_runs

    def __call__(self, name: str, doc: Mapping[str, Any]) -> None:
        # Add a run before logging its start and remove it after logging its
        # stop, so both lines list their own run.
        if name == "start":
            self._open_runs.add(doc["uid"])
        fields = {"document": name, **document_pointers(name, doc)}
        self._logger.info("%s", name, extra={"document_fields": fields})
        if name == "stop":
            self._open_runs.remove(doc["run_start"])


def _utc_time(record: logging.LogRecord) -> str:
    """When ``record`` was created, as ISO 8601 in UTC."""
    return datetime.fromtimestamp(record.created, tz=UTC).isoformat(timespec="microseconds")


class BlueskyJSONLinesFormatter(logging.Formatter):
    """Format a log record as one line of JSON.

    ``traceback`` holds the exception and stack text that ``logging.Formatter``
    would append to the message; ``json.dumps`` escapes its newlines.
    ``run_uids`` is read from ``open_runs`` when the record is formatted, so
    the handler must format in the thread that logs (not behind a queue).
    """

    def __init__(self, open_runs: OpenRuns) -> None:
        super().__init__()
        self._open_runs = open_runs

    def format(self, record: logging.LogRecord) -> str:
        return json.dumps(
            {
                "time": _utc_time(record),
                "level": record.levelname,
                "logger": record.name,
                "message": record.getMessage(),
                "traceback": self._traceback(record),
                "module": record.module,
                "lineno": record.lineno,
                "process": record.process,
                "thread": record.threadName,
                "run_uids": self._open_runs.uids,
            }
        )

    def _traceback(self, record: logging.LogRecord) -> str:
        # Cache the exception text on the record, as logging.Formatter.format
        # does, so other handlers reuse it.
        if record.exc_info and not record.exc_text:
            record.exc_text = self.formatException(record.exc_info)
        parts = [record.exc_text] if record.exc_text else []
        if record.stack_info:
            parts.append(self.formatStack(record.stack_info))
        return "\n".join(parts)


class BlueskyDocumentJSONLinesFormatter(logging.Formatter):
    """Format a record of ``DocumentPointerLoggingCallback`` as one line of JSON.

    ``run_uids`` is read from ``open_runs`` when the record is formatted, so
    the handler must format in the thread that logs (not behind a queue).
    """

    def __init__(self, open_runs: OpenRuns) -> None:
        super().__init__()
        self._open_runs = open_runs

    def format(self, record: logging.LogRecord) -> str:
        # "document_fields" is passed as "extra" in logger.info
        document_fields = getattr(record, "document_fields", {})
        return json.dumps({"time": _utc_time(record), **document_fields, "run_uids": self._open_runs.uids})


def _open_json_log_file(path: Path, rotation: tuple[int, int]) -> RotatingFileHandler | None:
    """Return a handler that writes ``path`` and rotates it as ``rotation``
    ``(max_bytes, backup_count)`` says, or None, with a ``UserWarning`` for the
    caller of ``configure_bluesky_json_logging``, if the file cannot be opened."""
    max_bytes, backup_count = rotation
    try:
        return RotatingFileHandler(path, maxBytes=max_bytes, backupCount=backup_count, encoding="utf-8")
    except OSError as exc:
        warnings.warn(f"cannot open bluesky JSON log file ({exc}), not writing it", stacklevel=3)
        return None


def configure_bluesky_json_logging(
    ipython: InteractiveShell | None = None, RE: RunEngine | None = None
) -> list[Path]:
    """
    Write the log records at level INFO and above of the bluesky, ophyd,
    ophyd_async, and nslsii loggers, and of ``ipython.log`` if ``ipython`` is
    given, to a JSON-lines file for log collectors such as Grafana Alloy. If
    ``RE`` is given, also write the pointers of every document it emits to a
    second JSON-lines file.

    This is opt-in: ``configure_base`` does not call it. It is independent of
    ``configure_bluesky_logging`` and can be used with or without it.

    The files are ``<user>-<pid>.log.jsonl`` and, if ``RE`` is given,
    ``<user>-<pid>.documents.jsonl`` in the directory named by the
    BLUESKY_JSON_LOG_DIR environment variable (default ``/var/log/bluesky``).
    The log file is rotated at 10 MB with 5 backups (``.jsonl.1`` …
    ``.jsonl.5``). The document file is rotated at 100 MB with 1 backup
    (``.jsonl.1``), so a burst of documents never rotates log lines away.
    Log collectors read only the live files, so their size bounds how long a 
    collector can be down without losing lines.

    Parameters
    ----------
    ipython: InteractiveShell, optional
        IPython InteractiveShell whose logger is also written to the JSON log
    RE: RunEngine, optional
        RunEngine whose documents are logged and determine ``run_uids``

    Returns
    -------
    paths: list of Path
        The files being written: the log file and, if ``RE`` is given, the
        document file, each only if it could be opened
    """
    log_dir = Path(os.environ.get("BLUESKY_JSON_LOG_DIR", DEFAULT_LOG_DIR))
    stem = f"{getpass.getuser()}-{os.getpid()}"
    open_runs = OpenRuns()
    paths: list[Path] = []

    log_path = log_dir / f"{stem}.log.jsonl"
    log_handler = _open_json_log_file(log_path, LOG_FILE_ROTATION)
    if log_handler is not None:
        log_handler.setLevel(logging.INFO)
        log_handler.setFormatter(BlueskyJSONLinesFormatter(open_runs))
        loggers = [logging.getLogger(name) for name in LOGGER_NAMES]
        if ipython is not None:
            loggers.append(ipython.log)
        for logger in loggers:
            logger.addHandler(log_handler)
            logger.setLevel(logging.INFO)
        paths.append(log_path)

    if RE is not None:
        documents_path = log_dir / f"{stem}.documents.jsonl"
        document_handler = _open_json_log_file(documents_path, DOCUMENT_FILE_ROTATION)
        if document_handler is not None:
            document_handler.setFormatter(BlueskyDocumentJSONLinesFormatter(open_runs))
            document_logger = logging.getLogger(DOCUMENT_LOGGER_NAME)
            document_logger.propagate = False
            document_logger.addHandler(document_handler)
            document_logger.setLevel(logging.INFO)
            RE.subscribe(DocumentPointerLoggingCallback(document_logger, open_runs))
            paths.append(documents_path)
    return paths
