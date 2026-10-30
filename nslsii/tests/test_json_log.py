import getpass
import io
import json
import logging
import os
import stat
from datetime import datetime, timezone

import bluesky.plan_stubs as bps
import bluesky.preprocessors as bpp
import IPython.core.interactiveshell
import pytest
from bluesky.plans import count
from event_model import DocumentNames

from nslsii import configure_bluesky_json_logging


@pytest.fixture
def json_log_dir(tmp_path, monkeypatch):
    """A JSON log directory in place of /var/log/bluesky.

    configure_bluesky_json_logging has no undo, so afterwards the handlers
    writing to this directory are detached from every logger and closed.
    """
    json_log_dir = tmp_path / "json-logs"
    json_log_dir.mkdir()
    monkeypatch.setenv("BLUESKY_JSON_LOG_DIR", str(json_log_dir))
    yield json_log_dir
    for logger in [logging.getLogger(), *logging.Logger.manager.loggerDict.values()]:
        for handler in list(getattr(logger, "handlers", [])):
            if getattr(handler, "baseFilename", "").startswith(str(json_log_dir)):
                logger.removeHandler(handler)
                handler.close()


def _log_file(json_log_dir):
    return json_log_dir / f"{getpass.getuser()}-{os.getpid()}.log.jsonl"


def _documents_file(json_log_dir):
    return json_log_dir / f"{getpass.getuser()}-{os.getpid()}.documents.jsonl"


def _lines(path):
    return [json.loads(line) for line in path.read_text().splitlines()]


def _log_lines(json_log_dir):
    return _lines(_log_file(json_log_dir))


def _document_lines(json_log_dir):
    return _lines(_documents_file(json_log_dir))


def _without_time(line):
    return {key: value for key, value in line.items() if key != "time"}


def test_log_lines_from_all_loggers(json_log_dir):
    ip = IPython.core.interactiveshell.InteractiveShell()
    assert configure_bluesky_json_logging(ipython=ip, extra_loggers=["my_beamline"]) == [_log_file(json_log_dir)]

    loggers = [
        logging.getLogger(name) for name in ("bluesky", "nslsii", "ophyd", "ophyd_async", "my_beamline")
    ] + [ip.log]
    for logger in loggers:
        logger.info("%s log message", logger.name)

    lines = _log_lines(json_log_dir)
    assert [(line["logger"], line["level"], line["message"]) for line in lines] == [
        (logger.name, "INFO", f"{logger.name} log message") for logger in loggers
    ]
    for line in lines:
        assert list(line) == [
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
        ]
        assert line["process"] == os.getpid()
    # without RE there are no documents to write
    assert not _documents_file(json_log_dir).exists()


def test_run_uids(RE, hw, json_log_dir):
    configure_bluesky_json_logging(RE=RE)
    start_uids = []
    RE.subscribe(lambda name, doc: start_uids.append(doc["uid"]), "start")
    RE.subscribe(lambda name, doc: logging.getLogger("nslsii").info("inside run"), "event")

    logging.getLogger("nslsii").info("before run")
    RE(count([hw.det], num=1))
    logging.getLogger("nslsii").info("after run")

    run_uids = {line["message"]: line["run_uids"] for line in _log_lines(json_log_dir)}
    assert run_uids["inside run"] == start_uids[0]
    assert run_uids["before run"] == ""
    assert run_uids["after run"] == ""


def test_run_uids_with_overlapping_runs(RE, json_log_dir):
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

    run_uids = {line["message"]: line["run_uids"] for line in _log_lines(json_log_dir)}
    assert run_uids["a and b open"] == f"{start_uids['a']} {start_uids['b']}"
    assert run_uids["only b open"] == start_uids["b"]
    assert run_uids["no run open"] == ""


def test_document_lines(RE, hw, json_log_dir):
    assert configure_bluesky_json_logging(RE=RE) == [_log_file(json_log_dir), _documents_file(json_log_dir)]
    docs = []
    RE.subscribe(lambda name, doc: docs.append(doc))
    nslsii_stream = io.StringIO()
    nslsii_handler = logging.StreamHandler(nslsii_stream)
    logging.getLogger("nslsii").addHandler(nslsii_handler)
    try:
        RE(count([hw.det], num=2))
    finally:
        logging.getLogger("nslsii").removeHandler(nslsii_handler)

    start, descriptor, event1, event2, stop = docs
    run = start["uid"]
    assert [_without_time(line) for line in _document_lines(json_log_dir)] == [
        {"document": "start", "uid": run, "scan_id": start["scan_id"], "run_uids": run},
        {"document": "descriptor", "uid": descriptor["uid"], "run_start": run, "name": "primary", "run_uids": run},
        {"document": "event", "uid": event1["uid"], "descriptor": descriptor["uid"], "seq_num": 1, "run_uids": run},
        {"document": "event", "uid": event2["uid"], "descriptor": descriptor["uid"], "seq_num": 2, "run_uids": run},
        {
            "document": "stop",
            "uid": stop["uid"],
            "run_start": run,
            "exit_status": "success",
            "reason": "",
            "run_uids": run,
        },
    ]
    # every line, log or document, is timestamped the same way: when it was written
    log_lines = _log_lines(json_log_dir)
    for line in log_lines + _document_lines(json_log_dir):
        assert datetime.fromisoformat(line["time"]).tzinfo == timezone.utc
    # document lines reach only the document file
    assert not any("document" in line for line in log_lines)
    assert nslsii_stream.getvalue() == ""


def test_failed_run_stop_line_has_reason(RE, json_log_dir):
    configure_bluesky_json_logging(RE=RE)

    def failing_plan():
        yield from bps.open_run()
        raise RuntimeError("shutter did not open")

    with pytest.raises(RuntimeError):
        RE(failing_plan())

    stop = _document_lines(json_log_dir)[-1]
    assert (stop["document"], stop["exit_status"], stop["reason"]) == ("stop", "fail", "shutter did not open")


def test_asset_and_page_document_summaries(RE, json_log_dir):
    configure_bluesky_json_logging(RE=RE)
    proposal_dir = "/nsls2/data/tst/proposals/2026-3/pass-123456"
    # (name, document, expected summary)
    documents = [
        (
            "resource",
            {
                "uid": "r1",
                "run_start": "s1",
                "spec": "AD_HDF5",
                "root": proposal_dir,
                "resource_path": "assets/det.h5",
                "resource_kwargs": {"frame_per_point": 1},
                "path_semantics": "posix",
            },
            {"uid": "r1", "run_start": "s1", "spec": "AD_HDF5"},
        ),
        (
            "datum",
            {"datum_id": "r1/0", "resource": "r1", "datum_kwargs": {"point_number": 0}},
            {"datum_id": "r1/0", "resource": "r1"},
        ),
        (
            "datum_page",
            {"datum_id": ["r1/1", "r1/2"], "resource": "r1", "datum_kwargs": {"point_number": [1, 2]}},
            {"datum_id": ["r1/1", "r1/2"], "resource": "r1"},
        ),
        (
            "event_page",
            {
                "descriptor": "d1",
                "uid": ["e3", "e4", "e5"],
                "seq_num": [3, 4, 5],
                "time": [1.0, 2.0, 3.0],
                "data": {"det": [0.1, 0.2, 0.3]},
                "timestamps": {"det": [1.0, 2.0, 3.0]},
                "filled": {},
            },
            {"descriptor": "d1", "count": 3, "seq_nums": {"start": 3, "stop": 6}},
        ),
        (
            "event_page",
            {"descriptor": "d1", "uid": [], "seq_num": [], "time": [], "data": {}, "timestamps": {}, "filled": {}},
            {"descriptor": "d1", "count": 0},
        ),
        (
            "stream_resource",
            {
                "uid": "sr1",
                "run_start": "s1",
                "data_key": "det_image",
                "mimetype": "application/x-hdf5",
                "uri": f"file://localhost{proposal_dir}/assets/det.h5",
                "parameters": {"dataset": "/entry/data"},
            },
            {"uid": "sr1", "run_start": "s1", "data_key": "det_image", "mimetype": "application/x-hdf5"},
        ),
        (
            "stream_datum",
            {
                "uid": "sr1/0",
                "stream_resource": "sr1",
                "descriptor": "d1",
                "seq_nums": {"start": 1, "stop": 11},
                "indices": {"start": 0, "stop": 10},
            },
            {
                "uid": "sr1/0",
                "stream_resource": "sr1",
                "descriptor": "d1",
                "seq_nums": {"start": 1, "stop": 11},
                "indices": {"start": 0, "stop": 10},
            },
        ),
    ]
    for name, doc, _ in documents:
        RE.dispatcher.process(DocumentNames[name], doc)

    assert [_without_time(line) for line in _document_lines(json_log_dir)] == [
        {"document": name, **summary, "run_uids": ""} for name, _, summary in documents
    ]
    for path in (_log_file(json_log_dir), _documents_file(json_log_dir)):
        assert "pass-123456" not in path.read_text()


def test_traceback(json_log_dir):
    configure_bluesky_json_logging()
    logger = logging.getLogger("nslsii")
    logger.info("no traceback")
    try:
        raise ValueError("boom")
    except ValueError:
        logger.exception("failed")
    logger.info("where am I", stack_info=True)

    # one line per record, even with multi-line tracebacks
    lines = _log_lines(json_log_dir)
    assert [line["message"] for line in lines] == ["no traceback", "failed", "where am I"]
    plain, failed, stack = lines
    assert plain["traceback"] == ""
    assert failed["traceback"].startswith("Traceback (most recent call last):\n")
    assert failed["traceback"].endswith("ValueError: boom")
    assert stack["traceback"].startswith("Stack (most recent call last):\n")


@pytest.mark.parametrize("problem", ["missing", "read-only"])
def test_skipped_when_dir_unusable(problem, RE, json_log_dir):
    if problem == "missing":
        json_log_dir.rmdir()
    else:
        json_log_dir.chmod(stat.S_IREAD | stat.S_IEXEC)

    with pytest.warns(UserWarning, match="cannot open bluesky JSON log file") as caught:
        assert configure_bluesky_json_logging(RE=RE) == []
    # one warning per file, each attributed to the caller
    assert [warning.filename for warning in caught] == [__file__, __file__]
    assert not _log_file(json_log_dir).exists()
    assert not _documents_file(json_log_dir).exists()


def test_files_are_skipped_separately(RE, json_log_dir):
    # a directory where the document file should be makes only that file unusable
    _documents_file(json_log_dir).mkdir()

    with pytest.warns(UserWarning, match="cannot open bluesky JSON log file") as caught:
        assert configure_bluesky_json_logging(RE=RE) == [_log_file(json_log_dir)]
    assert len(caught) == 1
    logging.getLogger("bluesky").info("still logged")
    assert [line["message"] for line in _log_lines(json_log_dir)] == ["still logged"]
