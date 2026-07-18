import datetime
import io
import os
import logging
import subprocess
from logging.handlers import SysLogHandler
from pathlib import Path
import shutil
import stat
from unittest.mock import MagicMock

import appdirs
import IPython.core.interactiveshell
import pytest

from nslsii import configure_bluesky_logging, configure_ipython_logging
from nslsii.common.ipynb.logutils import log_exception


@pytest.fixture(autouse=True)
def remove_logging_handlers():
    """
    Logging handlers from each test will accumulate. This fixture
    removes handlers added by a previous test before running the
    current test.
    """
    ip = IPython.core.interactiveshell.InteractiveShell()
    for logger_name in ("bluesky", "caproto", "nslsii", "ophyd", ip.log.name):
        logger = logging.getLogger(name=logger_name)
        for handler in logger.handlers.copy():
            logger.removeHandler(handler)


def test_configure_bluesky_logging(tmpdir):
    """
    Set environment variable BLUESKY_LOG_FILE and assert the log
    file is created.
    """
    log_file_path = Path(tmpdir) / Path("bluesky.log")

    ip = IPython.core.interactiveshell.InteractiveShell()
    os.environ["BLUESKY_LOG_FILE"] = str(log_file_path)
    bluesky_log_file_path = configure_bluesky_logging(
        ipython=ip,
    )
    assert bluesky_log_file_path == log_file_path
    assert log_file_path.exists()


def test_configure_bluesky_logging_with_nonexisting_dir(tmpdir):
    """
    Set environment variable BLUESKY_LOG_FILE to include a directory
    that does not exist. Assert an exception is raised.
    """
    log_dir = Path(tmpdir) / Path("does_not_exist")
    log_file_path = log_dir / Path("bluesky.log")

    ip = IPython.core.interactiveshell.InteractiveShell()
    os.environ["BLUESKY_LOG_FILE"] = str(log_file_path)
    with pytest.raises(FileNotFoundError):
        configure_bluesky_logging(
            ipython=ip,
        )


def test_configure_bluesky_logging_with_unwriteable_dir(tmpdir):
    """
    Set environment variable BLUESKY_LOG_FILE to include a directory
    that is not writeable. Assert an exception is raised.
    """

    log_dir = Path(tmpdir)
    log_file_path = log_dir / Path("bluesky.log")

    # make the log_dir read-only to force an exception
    log_dir.chmod(mode=stat.S_IREAD)

    ip = IPython.core.interactiveshell.InteractiveShell()
    os.environ["BLUESKY_LOG_FILE"] = str(log_file_path)
    with pytest.raises(PermissionError):
        configure_bluesky_logging(
            ipython=ip,
        )


def test_configure_bluesky_logging_creates_default_dir():
    """
    Remove environment variable BLUESKY_LOG_FILE and test that
    the default log file path is created. This test creates a
    directory rather than using pytest's tmp_path so the test
    must clean up at the end.
    """
    test_appname = "bluesky-test"
    log_dir = Path(appdirs.user_log_dir(appname=test_appname))
    # remove log_dir if it exists to test that it will be created
    if log_dir.exists():
        shutil.rmtree(path=log_dir)
    log_file_path = log_dir / Path("bluesky.log")

    ip = IPython.core.interactiveshell.InteractiveShell()
    os.environ.pop("BLUESKY_LOG_FILE", default=None)

    bluesky_log_file_path = configure_bluesky_logging(ipython=ip, appdirs_appname=test_appname)
    assert bluesky_log_file_path == log_file_path
    assert log_file_path.exists()

    # clean up the file and directory this test creates
    bluesky_log_file_path.unlink()
    bluesky_log_file_path.parent.rmdir()


def test_configure_bluesky_logging_existing_default_dir():
    """
    Remove environment variable BLUESKY_LOG_FILE and test that
    the default log file path is used. This test creates a
    directory rather than using pytest's tmp_path so the test
    must clean up at the end.
    """
    test_appname = "bluesky-test"
    log_dir = Path(appdirs.user_log_dir(appname=test_appname))
    # create the default log directory
    log_dir.mkdir(parents=True, exist_ok=True)

    log_file_path = log_dir / Path("bluesky.log")
    ip = IPython.core.interactiveshell.InteractiveShell()
    os.environ.pop("BLUESKY_LOG_FILE", default=None)

    bluesky_log_file_path = configure_bluesky_logging(ipython=ip, appdirs_appname=test_appname)
    assert bluesky_log_file_path == log_file_path
    assert log_file_path.exists()

    # clean up the file and directory this test creates
    bluesky_log_file_path.unlink()
    bluesky_log_file_path.parent.rmdir()


def test_configure_bluesky_logging_propagate_false(tmpdir):
    """
    Configure a root logger. Assert that a log message does
    not propagate from the bluesky logger to the root logger.
    """
    root_logger_stream = io.StringIO()
    logging.getLogger().addHandler(logging.StreamHandler(stream=root_logger_stream))

    log_file_path = Path(tmpdir) / Path("bluesky.log")

    ip = IPython.core.interactiveshell.InteractiveShell()
    os.environ["BLUESKY_LOG_FILE"] = str(log_file_path)
    bluesky_log_file_path = configure_bluesky_logging(
        ipython=ip,
    )

    logging.getLogger("bluesky").info("bluesky log message")
    logging.getLogger("caproto").info("caproto log message")
    logging.getLogger("nslsii").info("nslsii log message")
    logging.getLogger("ophyd").info("ophyd log message")
    ip.log.info("ipython log message")

    assert bluesky_log_file_path == log_file_path
    assert log_file_path.exists()

    # the log messages sent above should not
    # propagate to the root logger
    assert len(root_logger_stream.getvalue()) == 0


def test_configure_bluesky_logging_propagate_true(tmpdir):
    """
    Configure a root logger and set ``propagate=True`` on
    the bluesky loggers. Assert that a log message propagates
    from the bluesky loggers to the root logger.
    """
    root_logger_stream = io.StringIO()
    logging.getLogger().addHandler(logging.StreamHandler(stream=root_logger_stream))

    log_file_path = Path(tmpdir) / Path("bluesky.log")

    ip = IPython.core.interactiveshell.InteractiveShell()
    os.environ["BLUESKY_LOG_FILE"] = str(log_file_path)
    bluesky_log_file_path = configure_bluesky_logging(ipython=ip, propagate_log_messages=True)

    logging.getLogger("bluesky").info("bluesky log message")
    logging.getLogger("caproto").info("caproto log message")
    logging.getLogger("nslsii").info("nslsii log message")
    logging.getLogger("ophyd").info("ophyd log message")
    ip.log.info("ipython log message")

    assert bluesky_log_file_path == log_file_path
    assert log_file_path.exists()

    # the log message sent to the bluesky logger should
    # propagate to the root logger
    root_logger_output = root_logger_stream.getvalue()
    assert "bluesky log message" in root_logger_output
    assert "caproto log message" in root_logger_output
    assert "nslsii log message" in root_logger_output
    assert "ophyd log message" in root_logger_output
    assert "ipython log message" in root_logger_output


def test_configure_bluesky_logging_syslog_logging(tmpdir):
    """
    Verify SysLogHandler is configured correctly by checking
    for log messages in journalctl output.
    """
    log_file_path = Path(tmpdir) / Path("bluesky.log")
    os.environ["BLUESKY_LOG_FILE"] = str(log_file_path)
    ip = IPython.core.interactiveshell.InteractiveShell()
    configure_bluesky_logging(ipython=ip)
    for logger_name in ("bluesky", "caproto", "nslsii", "ophyd", ip.log.name):
        assert any([isinstance(handler, SysLogHandler) for handler in logging.getLogger(logger_name).handlers])

    # remember the time so we can ask journalctl for only the most recent log messages
    time_before_logging = datetime.datetime.now().time().isoformat(timespec="seconds")
    for logger_name in ("bluesky", "caproto", "nslsii", "ophyd", ip.log.name):
        logging.getLogger(logger_name).info(f"{logger_name} log message %s", time_before_logging)

    for logger_name in ("bluesky", "caproto", "nslsii", "ophyd", ip.log.name):
        journalctl_output = subprocess.run(
            ["journalctl", f"SYSLOG_IDENTIFIER={logger_name}", f"--since={time_before_logging}", "--no-pager"],
            capture_output=True,
        )
        assert f"{logger_name} log message {time_before_logging}" in journalctl_output.stdout.decode()


def test_ipython_log_exception():
    ip = IPython.core.interactiveshell.InteractiveShell()
    ip.logger = MagicMock()
    ip.set_custom_exc((BaseException,), log_exception)
    ip.run_cell("raise Exception")
    ip.logger.log_write.assert_called_with("Exception\n", kind="output")


def test_ipython_exc_logging_creates_default_dir():
    """
    Remove environment variable BLUESKY_IPYTHON_LOG_FILE and
    test that the default log file path is created. This test creates
    a directory rather than using pytest's tmp_path so the test
    must clean up at the end.
    """
    test_appname = "bluesky-test"
    log_dir = Path(appdirs.user_log_dir(appname=test_appname))
    # remove log_dir if it exists to test that it will be created
    if log_dir.exists():
        shutil.rmtree(path=log_dir)
    log_file_path = log_dir / Path("bluesky_ipython.log")

    ip = IPython.core.interactiveshell.InteractiveShell()
    os.environ.pop("BLUESKY_IPYTHON_LOG_FILE", default=None)
    bluesky_ipython_log_file_path = configure_ipython_logging(
        exception_logger=log_exception, ipython=ip, appdirs_appname=test_appname
    )

    assert bluesky_ipython_log_file_path == log_file_path
    assert log_file_path.exists()

    bluesky_ipython_log_file_path.unlink()
    bluesky_ipython_log_file_path.parent.rmdir()


def test_ipython_exc_logging_existing_default_dir():
    """
    Remove environment variable BLUESKY_IPYTHON_LOG_FILE and
    test that the default log file path is used. This test creates
    a directory rather than using pytest's tmp_path so the test
    must clean up at the end.
    """
    test_appname = "bluesky-test"
    log_dir = Path(appdirs.user_log_dir(appname=test_appname))
    # create the default log directory
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file_path = log_dir / Path("bluesky_ipython.log")

    ip = IPython.core.interactiveshell.InteractiveShell()
    os.environ.pop("BLUESKY_IPYTHON_LOG_FILE", default=None)
    bluesky_ipython_log_file_path = configure_ipython_logging(
        exception_logger=log_exception, ipython=ip, appdirs_appname=test_appname
    )

    assert bluesky_ipython_log_file_path == log_file_path
    assert log_file_path.exists()

    bluesky_ipython_log_file_path.unlink()
    bluesky_ipython_log_file_path.parent.rmdir()


def test_configure_ipython_exc_logging(tmpdir):
    log_file_path = Path(tmpdir) / Path("bluesky_ipython.log")

    ip = IPython.core.interactiveshell.InteractiveShell()
    os.environ["BLUESKY_IPYTHON_LOG_FILE"] = str(log_file_path)
    bluesky_ipython_log_file_path = configure_ipython_logging(
        exception_logger=log_exception,
        ipython=ip,
    )
    assert bluesky_ipython_log_file_path == log_file_path
    assert log_file_path.exists()


def test_configure_ipython_exc_logging_with_nonexisting_dir(tmpdir):
    log_dir = Path(tmpdir) / Path("does_not_exist")
    log_file_path = log_dir / Path("bluesky_ipython.log")

    ip = IPython.core.interactiveshell.InteractiveShell()
    os.environ["BLUESKY_IPYTHON_LOG_FILE"] = str(log_file_path)
    with pytest.raises(UserWarning):
        configure_ipython_logging(
            exception_logger=log_exception,
            ipython=ip,
        )


def test_configure_ipython_exc_logging_with_unwriteable_dir(tmpdir):
    log_dir = Path(tmpdir)
    log_file_path = log_dir / Path("bluesky_ipython.log")

    log_dir.chmod(stat.S_IREAD)

    ip = IPython.core.interactiveshell.InteractiveShell()
    os.environ["BLUESKY_IPYTHON_LOG_FILE"] = str(log_file_path)
    with pytest.raises(PermissionError):
        configure_ipython_logging(
            exception_logger=log_exception,
            ipython=ip,
        )


def test_configure_ipython_exc_logging_file_exists(tmpdir):
    log_file_path = Path(tmpdir) / Path("bluesky_ipython.log")

    with open(log_file_path, "w") as f:
        f.write("log log log")

    ip = IPython.core.interactiveshell.InteractiveShell()
    os.environ["BLUESKY_IPYTHON_LOG_FILE"] = str(log_file_path)
    bluesky_ipython_log_file_path = configure_ipython_logging(
        exception_logger=log_exception,
        ipython=ip,
    )
    assert bluesky_ipython_log_file_path == log_file_path
    assert log_file_path.exists()


def test_configure_ipython_exc_logging_rotate(tmpdir):
    log_file_path = Path(tmpdir) / Path("bluesky_ipython.log")

    with open(log_file_path, "w") as f:
        f.write("log log log")

    ip = IPython.core.interactiveshell.InteractiveShell()
    os.environ["BLUESKY_IPYTHON_LOG_FILE"] = str(log_file_path)
    bluesky_ipython_log_file_path = configure_ipython_logging(
        exception_logger=log_exception, ipython=ip, rotate_file_size=0
    )
    assert bluesky_ipython_log_file_path == log_file_path
    assert log_file_path.exists()

    old_log_file_path = log_file_path.parent / Path(log_file_path.name + ".old")
    assert old_log_file_path.exists()
