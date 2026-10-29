"""Opt-in JSON-lines log file for log collectors such as Grafana Alloy (Loki)."""

import functools
import getpass
import json
import logging
import os
import sys
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path

_LOGGER_NAMES = ("bluesky", "caproto", "ophyd", "ophyd_async", "nslsii")
_DEFAULT_JSON_LOG_DIR = "/var/log/bluesky"
JSON_LOG_MAX_BYTES = 10 * 1024 * 1024  # bytes per JSON log file before rotation
JSON_LOG_BACKUP_COUNT = 5
JSON_FIELDS = (
    "time",
    "level",
    "logger",
    "message",
    "traceback",
    "module",
    "lineno",
    "process",
    "thread",
    "run_uids",
)

# (loggers, handler, RunEngine unsubscribe callable or None) from the last
# configure_bluesky_json_logging call
_json_log_config = None


class RunTracker:
    """Track the uids of the open RunEngine runs.

    Subscribe ``on_document`` to the RunEngine. ``run_uids`` is the uids of
    the open runs in the order they were opened, separated by spaces, or ""
    if no run is open.
    """

    def __init__(self):
        self.run_uids = ""

    def on_document(self, name, doc):
        if name == "start":
            self.run_uids = " ".join([*self.run_uids.split(), doc["uid"]])
        elif name == "stop":
            self.run_uids = " ".join(uid for uid in self.run_uids.split() if uid != doc["run_start"])


class JSONLinesFormatter(logging.Formatter):
    """Format a record as one JSON object with exactly the keys in ``JSON_FIELDS``.

    ``traceback`` holds the exception and stack text that ``logging.Formatter``
    would append to the message; ``json.dumps`` escapes its newlines, so every
    record is one line. ``run_uids`` is read from ``run_tracker`` when the
    record is formatted, so the handler must format in the thread that logs
    (not behind a queue).
    """

    def __init__(self, run_tracker):
        super().__init__()
        self._run_tracker = run_tracker

    def format(self, record):
        payload = {
            "time": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(timespec="microseconds"),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "traceback": self._traceback(record),
            "module": record.module,
            "lineno": record.lineno,
            "process": record.process,
            "thread": record.threadName,
            "run_uids": self._run_tracker.run_uids,
        }
        return json.dumps(payload)

    def _traceback(self, record):
        # Cache the exception text on the record, as logging.Formatter.format
        # does, so other handlers reuse it.
        if record.exc_info and not record.exc_text:
            record.exc_text = self.formatException(record.exc_info)
        parts = [record.exc_text] if record.exc_text else []
        if record.stack_info:
            parts.append(self.formatStack(record.stack_info))
        return "\n".join(parts)


def _remove_json_logging():
    """Undo the last ``configure_bluesky_json_logging`` call: detach and close its handler and stop run tracking."""
    global _json_log_config
    config, _json_log_config = _json_log_config, None
    if config is None:
        return
    loggers, handler, unsubscribe = config
    for logger in loggers:
        logger.removeHandler(handler)
    handler.close()
    if unsubscribe is not None:
        unsubscribe()


def configure_bluesky_json_logging(ipython=None, RE=None):
    """
    Write records at level INFO and above from the bluesky, caproto, ophyd,
    ophyd_async, and nslsii loggers, and from the IPython logger if ``ipython``
    is given, to a JSON-lines file for log collectors such as Grafana Alloy.

    This is opt-in: ``configure_base`` does not call it. It is independent of
    ``configure_bluesky_logging`` and can be used with or without it.

    The file is ``<user>-<pid>.jsonl`` in the directory named by the
    BLUESKY_JSON_LOG_DIR environment variable (default ``/var/log/bluesky``).
    Each line is one JSON object with the keys ``time``, ``level``, ``logger``,
    ``message``, ``traceback``, ``module``, ``lineno``, ``process``,
    ``thread``, and ``run_uids``. The file is rotated at 10 MB with 5 backups
    (``.jsonl.1`` … ``.jsonl.5``). It is written by the thread that logs, so
    the directory should be on local disk.

    ``traceback`` is the exception and stack text of the record (from
    ``logger.exception(...)``, ``exc_info=True``, or ``stack_info=True``), or
    "". Its newlines are escaped like any other JSON string, so a record with
    a traceback is still one line and one log collector entry.

    The directory is never created. If it does not exist or the file cannot be
    opened, a message is printed to stderr, the current configuration is left
    in place, and ``None`` is returned.

    If ``RE`` is given, ``run_uids`` is the uids of the runs that RunEngine has
    open when the message is logged, in the order they were opened and
    separated by spaces; it is "" when no run is open or ``RE`` is not given.

    The level of each logger is set to INFO; the ``propagate`` field is not
    changed. Calling this function again replaces the handler and RunEngine
    subscription from the previous call.

    Parameters
    ----------
    ipython: InteractiveShell, optional
        IPython InteractiveShell whose logger is also written to the JSON log
    RE: RunEngine, optional
        RunEngine whose documents determine the ``run_uids`` field

    Returns
    -------
    json_log_file_path: Path or None
        JSON log file path, or None if the file could not be opened

    """
    global _json_log_config

    json_log_dir = Path(os.environ.get("BLUESKY_JSON_LOG_DIR", _DEFAULT_JSON_LOG_DIR))
    json_log_file_path = json_log_dir / f"{getpass.getuser()}-{os.getpid()}.jsonl"
    try:
        json_file_handler = RotatingFileHandler(
            filename=str(json_log_file_path),
            maxBytes=JSON_LOG_MAX_BYTES,
            backupCount=JSON_LOG_BACKUP_COUNT,
            encoding="utf-8",
        )
    except OSError as exc:
        print(
            f"cannot open bluesky JSON log file '{json_log_file_path}' ({exc}), not writing a JSON log file",
            file=sys.stderr,
        )
        return None
    run_tracker = RunTracker()
    json_file_handler.setLevel(logging.INFO)
    json_file_handler.setFormatter(JSONLinesFormatter(run_tracker))
    print(f"bluesky JSON log file path: '{json_log_file_path}'", file=sys.stderr)

    unsubscribe = None
    if RE is not None:
        # The formatter keeps the tracker alive; bluesky holds bound-method
        # callbacks weakly.
        token = RE.subscribe(run_tracker.on_document)
        unsubscribe = functools.partial(RE.unsubscribe, token)

    loggers = [logging.getLogger(name) for name in _LOGGER_NAMES]
    if ipython:
        loggers.append(ipython.log)

    _remove_json_logging()
    for logger in loggers:
        logger.addHandler(json_file_handler)
        logger.setLevel("INFO")
    _json_log_config = (loggers, json_file_handler, unsubscribe)
    return json_log_file_path
