"""Opt-in JSON-lines log file for log collectors such as Grafana Alloy (Loki)."""

import getpass
import json
import logging
import os
import warnings
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path

LOGGER_NAMES = ("bluesky", "ophyd", "ophyd_async", "nslsii")
DEFAULT_LOG_DIR = "/var/log/bluesky"
MAX_BYTES = 10 * 1024 * 1024  # per file, before rotation
BACKUP_COUNT = 5

# Logger of the Bluesky document lines. Each record carries the line's fields
# as its ``document_fields`` attribute.
DOCUMENT_LOGGER_NAME = "nslsii.documents"
# The pointer fields of each document type: uids, links to other documents,
# positions within streams and resources, and a few identifiers. Data,
# configuration, metadata, file paths, and the document's own timestamp are
# *not* included.
POINTER_FIELDS = {
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


def document_pointers(name, doc):
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
        if seq_nums:
            pointers["seq_nums"] = {"start": min(seq_nums), "stop": max(seq_nums) + 1}
    return pointers


class OpenRuns:
    """The uids of the open RunEngine runs, in the order they were opened.

    ``uids`` is one space-separated string ("" if no run is open). Updates
    replace it rather than modify it, so other threads can read it while the
    RunEngine thread updates it.
    """

    def __init__(self):
        self.uids = ""

    def add(self, uid):
        self.uids = " ".join([*self.uids.split(), uid])

    def remove(self, uid):
        self.uids = " ".join(other for other in self.uids.split() if other != uid)


class DocumentPointerLoggingCallback:
    """RunEngine callback that logs the pointers of every document and keeps
    ``open_runs`` current.

    Each document is logged to ``logger`` at INFO with the document name as the
    message and ``{"document": name, **pointers}`` as the record's
    ``document_fields`` attribute.
    """

    def __init__(self, logger, open_runs):
        self._logger = logger
        self._open_runs = open_runs

    def __call__(self, name, doc):
        # Add a run before logging its start and remove it after logging its
        # stop, so both lines list their own run.
        if name == "start":
            self._open_runs.add(doc["uid"])
        fields = {"document": name, **document_pointers(name, doc)}
        self._logger.info("%s", name, extra={"document_fields": fields})
        if name == "stop":
            self._open_runs.remove(doc["run_start"])


class BlueskyJSONLinesFormatter(logging.Formatter):
    """Format a record as one line of JSON.

    Every line starts with ``time``, when the record was created, and ends with
    ``run_uids``, read from ``open_runs`` when the record is formatted; the
    handler must therefore format in the thread that logs (not behind a
    queue). Between them, a record with a ``document_fields`` attribute gets
    those fields, and any other record gets the log fields.
    ``traceback`` holds the exception and stack text that ``logging.Formatter``
    would append to the message; ``json.dumps`` escapes its newlines.
    """

    def __init__(self, open_runs):
        super().__init__()
        self._open_runs = open_runs

    def format(self, record):
        time = datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(timespec="microseconds")
        if record.name == DOCUMENT_LOGGER_NAME:
            # "document_fields" is passed as "extra" in logger.info
            document_fields = getattr(record, "document_fields", {})
            fields = {"time": time, **document_fields}
        else:
            fields = {
                "time": time,
                "level": record.levelname,
                "logger": record.name,
                "message": record.getMessage(),
                "traceback": self._traceback(record),
                "module": record.module,
                "lineno": record.lineno,
                "process": record.process,
                "thread": record.threadName,
            }
        fields["run_uids"] = self._open_runs.uids
        return json.dumps(fields)

    def _traceback(self, record):
        # Cache the exception text on the record, as logging.Formatter.format
        # does, so other handlers reuse it.
        if record.exc_info and not record.exc_text:
            record.exc_text = self.formatException(record.exc_info)
        parts = [record.exc_text] if record.exc_text else []
        if record.stack_info:
            parts.append(self.formatStack(record.stack_info))
        return "\n".join(parts)


def configure_bluesky_json_logging(ipython=None, RE=None):
    """
    Write the log records at level INFO and above of the bluesky, ophyd,
    ophyd_async, and nslsii loggers, and of ``ipython.log`` if ``ipython`` is
    given, to a JSON-lines file for log collectors such as Grafana Alloy. If
    ``RE`` is given, also write the pointers of every document it emits.

    This is opt-in: ``configure_base`` does not call it. It is independent of
    ``configure_bluesky_logging`` and can be used with or without it.

    The file is ``<user>-<pid>.jsonl`` in the directory named by the
    BLUESKY_JSON_LOG_DIR environment variable (default ``/var/log/bluesky``),
    rotated at 10 MB with 5 backups (``.jsonl.1`` … ``.jsonl.5``). Records are
    written by the thread that logs them, so the directory should be on local
    disk. The directory is never created: if it does not exist or the file
    cannot be opened, a ``UserWarning`` is issued, nothing is configured, and
    ``None`` is returned.

    Each line is one JSON object. Every line has ``time``, when the line was
    written (ISO 8601 in UTC), and ``run_uids``, the uids of the runs ``RE``
    has open, in the order they were opened and separated by spaces ("" if
    none is open or ``RE`` is not given).

    A log line also has ``level``, ``logger``, ``message``, ``traceback``,
    ``module``, ``lineno``, ``process``, and ``thread``. ``traceback`` is the
    exception and stack text (``logger.exception(...)``, ``exc_info=True``,
    ``stack_info=True``), or "". Its newlines are escaped like any JSON
    string, so a line with a traceback is still one line.

    A document line, one per document ``RE`` emits, also has ``document``,
    the document name, and the document's pointer fields. Data,
    configuration, metadata, file paths, and the document's own timestamp are
    never included:

    - ``start``: ``uid``, ``scan_id``
    - ``descriptor``: ``uid``, ``run_start``, ``name``
    - ``event``: ``uid``, ``descriptor``, ``seq_num``
    - ``event_page``: ``descriptor``, ``count``, ``seq_nums``
    - ``stop``: ``uid``, ``run_start``, ``exit_status``
    - ``resource``: ``uid``, ``run_start``, ``spec``
    - ``datum``: ``datum_id``, ``resource``
    - ``datum_page``: ``datum_id`` (a list), ``resource``
    - ``stream_resource``: ``uid``, ``run_start``, ``data_key``, ``mimetype``
    - ``stream_datum``: ``uid``, ``descriptor``, ``stream_resource``,
      ``seq_nums``, ``indices``

    ``seq_nums`` and ``indices`` are ``{"start": ..., "stop": ...}`` ranges
    with ``stop`` exclusive, and ``count`` is the number of events in a page.
    Fields a document lacks are omitted. The ``run_uids`` of a start or stop
    line includes its own run. Document lines come from the
    ``nslsii.documents`` logger, which does not propagate, so they reach only
    this file.

    The level of each logger is set to INFO; the ``propagate`` field of the
    other loggers is not changed. Call this function once per process: each
    call adds another handler and RunEngine subscription.

    Parameters
    ----------
    ipython: InteractiveShell, optional
        IPython InteractiveShell whose logger is also written to the JSON log
    RE: RunEngine, optional
        RunEngine whose documents are logged and determine ``run_uids``

    Returns
    -------
    path: Path or None
        JSON log file path, or None if the file could not be opened

    """
    log_dir = Path(os.environ.get("BLUESKY_JSON_LOG_DIR", DEFAULT_LOG_DIR))
    path = log_dir / f"{getpass.getuser()}-{os.getpid()}.jsonl"
    try:
        handler = RotatingFileHandler(path, maxBytes=MAX_BYTES, backupCount=BACKUP_COUNT, encoding="utf-8")
    except OSError as exc:
        warnings.warn(f"cannot open bluesky JSON log file '{path}' ({exc}), not writing a JSON log file", stacklevel=2)
        return None
    open_runs = OpenRuns()
    handler.setLevel(logging.INFO)
    handler.setFormatter(BlueskyJSONLinesFormatter(open_runs))

    loggers = [logging.getLogger(name) for name in LOGGER_NAMES]
    if ipython is not None:
        loggers.append(ipython.log)
    for logger in loggers:
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)

    if RE is not None:
        document_logger = logging.getLogger(DOCUMENT_LOGGER_NAME)
        document_logger.propagate = False
        document_logger.addHandler(handler)
        document_logger.setLevel(logging.INFO)
        RE.subscribe(DocumentPointerLoggingCallback(document_logger, open_runs))
    return path
