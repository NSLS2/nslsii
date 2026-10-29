import getpass
import io
import json
import logging
import os
import stat

import bluesky.plan_stubs as bps
import bluesky.preprocessors as bpp
import IPython.core.interactiveshell
import pytest
from bluesky.plans import count

from nslsii import configure_bluesky_json_logging
from nslsii._json_log import JSON_FIELDS, _remove_json_logging


@pytest.fixture
def json_log_dir(tmp_path, monkeypatch):
    """A JSON log directory in place of /var/log/bluesky; the JSON handler is closed afterwards."""
    json_log_dir = tmp_path / "json-logs"
    json_log_dir.mkdir()
    monkeypatch.setenv("BLUESKY_JSON_LOG_DIR", str(json_log_dir))
    yield json_log_dir
    _remove_json_logging()


def _json_log_file(json_log_dir):
    return json_log_dir / f"{getpass.getuser()}-{os.getpid()}.jsonl"


def _read_payloads(json_log_dir):
    return [json.loads(line) for line in _json_log_file(json_log_dir).read_text().splitlines()]


def test_json_log_records_from_all_loggers(json_log_dir):
    ip = IPython.core.interactiveshell.InteractiveShell()
    assert configure_bluesky_json_logging(ipython=ip) == _json_log_file(json_log_dir)

    loggers = [
        logging.getLogger(name) for name in ("bluesky", "caproto", "nslsii", "ophyd", "ophyd_async")
    ] + [ip.log]
    for logger in loggers:
        logger.info("%s log message", logger.name)

    payloads = _read_payloads(json_log_dir)
    for logger in loggers:
        assert any(
            payload["logger"] == logger.name
            and payload["message"] == f"{logger.name} log message"
            and payload["level"] == "INFO"
            for payload in payloads
        )


def test_json_log_records_carry_run_uids(RE, hw, json_log_dir):
    configure_bluesky_json_logging(RE=RE)

    start_uids = []
    RE.subscribe(lambda name, doc: start_uids.append(doc["uid"]), "start")
    RE.subscribe(lambda name, doc: logging.getLogger("nslsii").info("inside run"), "event")

    logging.getLogger("nslsii").info("before run")
    RE(count([hw.det], num=1))
    logging.getLogger("nslsii").info("after run")

    payloads = _read_payloads(json_log_dir)
    for payload in payloads:
        assert list(payload) == list(JSON_FIELDS)
        assert payload["process"] == os.getpid()

    run_uids = {payload["message"]: payload["run_uids"] for payload in payloads}
    assert run_uids["inside run"] == start_uids[0]
    assert run_uids["before run"] == ""
    assert run_uids["after run"] == ""


def test_json_log_run_uids_with_overlapping_runs(RE, json_log_dir):
    """While several runs are open, run_uids lists them in the order they were opened."""
    configure_bluesky_json_logging(RE=RE)
    start_uids = {}
    RE.subscribe(lambda name, doc: start_uids.setdefault(doc["purpose"], doc["uid"]), "start")
    logger = logging.getLogger("nslsii")

    def overlapping_runs():
        yield from bpp.set_run_key_wrapper(bps.open_run(md={"purpose": "a"}), "a")
        yield from bpp.set_run_key_wrapper(bps.open_run(md={"purpose": "b"}), "b")
        logger.info("a and b open")
        yield from bpp.set_run_key_wrapper(bps.close_run(), "a")
        logger.info("only b open")
        yield from bpp.set_run_key_wrapper(bps.close_run(), "b")
        logger.info("no run open")

    RE(overlapping_runs())

    run_uids = {payload["message"]: payload["run_uids"] for payload in _read_payloads(json_log_dir)}
    assert run_uids["a and b open"] == f"{start_uids['a']} {start_uids['b']}"
    assert run_uids["only b open"] == start_uids["b"]
    assert run_uids["no run open"] == ""


def test_json_log_traceback_field(json_log_dir):
    configure_bluesky_json_logging()
    logger = logging.getLogger("nslsii")
    logger.info("no traceback")
    try:
        raise ValueError("boom")
    except ValueError:
        logger.exception("failed")
    logger.info("where am I", stack_info=True)

    # one line per record, even with multi-line tracebacks
    payloads = _read_payloads(json_log_dir)
    assert [payload["message"] for payload in payloads] == ["no traceback", "failed", "where am I"]
    plain, failed, stack = payloads
    assert plain["traceback"] == ""
    assert failed["traceback"].startswith("Traceback (most recent call last):\n")
    assert failed["traceback"].endswith("ValueError: boom")
    assert stack["traceback"].startswith("Stack (most recent call last):\n")


@pytest.mark.parametrize("problem", ["missing", "read-only"])
def test_json_log_skipped_when_dir_unusable(problem, json_log_dir, capsys):
    if problem == "missing":
        json_log_dir.rmdir()
    else:
        json_log_dir.chmod(stat.S_IREAD | stat.S_IEXEC)

    assert configure_bluesky_json_logging() is None
    assert "not writing a JSON log file" in capsys.readouterr().err
    assert not _json_log_file(json_log_dir).exists()


def test_reconfigure_replaces_only_json_handler(json_log_dir):
    other_stream = io.StringIO()
    other_handler = logging.StreamHandler(other_stream)
    bluesky_logger = logging.getLogger("bluesky")
    bluesky_logger.addHandler(other_handler)
    try:
        configure_bluesky_json_logging()
        configure_bluesky_json_logging()
        bluesky_logger.info("logged once")
    finally:
        bluesky_logger.removeHandler(other_handler)

    assert other_stream.getvalue().count("logged once") == 1
    assert sum(payload["message"] == "logged once" for payload in _read_payloads(json_log_dir)) == 1
