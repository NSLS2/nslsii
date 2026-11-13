"""Tests for redis-related parameter changes in utils, __init__, and sync_experiment."""

import logging
from unittest.mock import MagicMock, patch, mock_open

import pytest

import sys

import nslsii
from nslsii.utils.redis import open_redis_client
from nslsii.sync_experiment.sync_experiment import switch_redis_proposal

# The __init__.py of nslsii.sync_experiment re-exports a function called
# sync_experiment, which shadows the module of the same name. We need the
# actual module object for patch.object, so grab it from sys.modules.
_sync_mod = sys.modules["nslsii.sync_experiment.sync_experiment"]


# ---------------------------------------------------------------------------
# open_redis_client tests
# ---------------------------------------------------------------------------


@patch("nslsii.utils.redis.os.getenv", return_value=None)
@patch("nslsii.utils.redis.socket.gethostname", return_value="xf12id1-ws1")
@patch("nslsii.utils.redis.Redis")
def test_open_redis_client_uses_redis_location_for_ssl(
    mock_redis, mock_hostname, mock_getenv
):
    """redis_location should override hostname-based lookup when using SSL."""
    # "opls" matches "xf12id1-opls-redis1.nsls2.bnl.gov" in redis_hosts
    with patch("builtins.open", mock_open(read_data="secret")):
        open_redis_client(redis_ssl=True, redis_location="opls")

    call_kwargs = mock_redis.call_args[1]
    assert "opls" in call_kwargs["host"]
    assert call_kwargs["ssl"] is True


@patch("nslsii.utils.redis.os.getenv", return_value=None)
@patch("nslsii.utils.redis.Redis")
def test_open_redis_client_passes_redis_db(mock_redis, mock_getenv):
    """redis_db should be forwarded to the Redis constructor."""
    open_redis_client(redis_url="localhost", redis_db=3)

    call_kwargs = mock_redis.call_args[1]
    assert call_kwargs["db"] == 3


# ---------------------------------------------------------------------------
# configure_base tests
# ---------------------------------------------------------------------------


@patch("nslsii.open_redis_client")
def test_configure_base_raises_on_redis_prefix_and_ssl(mock_open_rc):
    """ValueError should be raised when redis_prefix and redis_ssl are both set."""
    ns = {}

    with patch("redis_json_dict.RedisJSONDict", return_value={}):
        with pytest.raises(ValueError, match="Incompatible arguments"):
            nslsii.configure_base(
                user_ns=ns,
                broker_name=MagicMock(),
                redis_url="localhost",
                redis_prefix="arpes-",
                redis_ssl=True,
                bec=False,
                epics_context=False,
                magics=False,
                mpl=False,
                configure_logging=False,
                pbar=False,
                ipython_logging=False,
            )


@patch("nslsii.open_redis_client")
def test_configure_base_passes_redis_db(mock_open_rc):
    """redis_db should be forwarded to open_redis_client."""
    ns = {}

    with patch("redis_json_dict.RedisJSONDict", return_value={}):
        nslsii.configure_base(
            user_ns=ns,
            broker_name=MagicMock(),
            redis_url="localhost",
            redis_db=5,
            bec=False,
            epics_context=False,
            magics=False,
            mpl=False,
            configure_logging=False,
            pbar=False,
            ipython_logging=False,
        )

    call_kwargs = mock_open_rc.call_args[1]
    assert call_kwargs["redis_db"] == 5


# ---------------------------------------------------------------------------
# switch_redis_proposal tests
# ---------------------------------------------------------------------------


@pytest.fixture()
def switch_mocks():
    """Patch all external dependencies of switch_redis_proposal and yield a dict of mocks."""
    with (
        patch.object(_sync_mod, "open_redis_client") as mock_open_rc,
        patch.object(_sync_mod, "RedisJSONDict", return_value={}) as mock_rjd,
        patch.object(_sync_mod, "should_they_be_here", return_value=True),
        patch.object(
            _sync_mod,
            "validate_proposal",
            return_value={
                "proposal_id": "123",
                "title": "t",
                "type": "GU",
                "users": [],
            },
        ),
        patch.object(_sync_mod, "is_commissioning_proposal", return_value=False),
        patch.object(_sync_mod, "get_current_cycle", return_value="2026-1"),
    ):
        yield {"open_redis_client": mock_open_rc, "RedisJSONDict": mock_rjd}


def test_switch_redis_proposal_ssl_no_prefix(switch_mocks):
    """With redis_ssl=True the RedisJSONDict prefix should be empty."""
    switch_redis_proposal(123456, beamline="SMI", username="testuser", redis_ssl=True)

    mock_rjd = switch_mocks["RedisJSONDict"]
    mock_rjd.assert_called_once()
    assert mock_rjd.call_args[1]["prefix"] == ""


def test_switch_redis_proposal_endstation_no_ssl(switch_mocks):
    """With endstation set and redis_ssl=False the prefix should be '{endstation}-'."""
    switch_redis_proposal(
        123456, beamline="SMI", username="testuser", endstation="opls", redis_ssl=False
    )

    mock_rjd = switch_mocks["RedisJSONDict"]
    mock_rjd.assert_called_once()
    assert mock_rjd.call_args[1]["prefix"] == "opls-"


def test_switch_redis_proposal_passes_redis_db(switch_mocks):
    """redis_db should be forwarded to open_redis_client."""
    switch_redis_proposal(
        123456, beamline="SMI", username="testuser", redis_db=7, redis_ssl=False
    )

    call_kwargs = switch_mocks["open_redis_client"].call_args[1]
    assert call_kwargs["redis_db"] == 7


def test_switch_redis_proposal_logs_instead_of_printing(switch_mocks, caplog, capsys):
    """Status messages should go through the module logger, not stdout."""
    with caplog.at_level(logging.DEBUG, logger=_sync_mod.logger.name):
        switch_redis_proposal(123456, beamline="SMI", username="testuser")

    assert capsys.readouterr().out == ""
    assert "Redis connection info" in caplog.text
    assert "Started experiment pass-123456 by testuser." in caplog.text


def test_switch_redis_proposal_rejects_verbose_kwarg(switch_mocks):
    """The verbose kwarg was removed in favor of logging."""
    with pytest.raises(TypeError):
        switch_redis_proposal(123456, beamline="SMI", username="testuser", verbose=True)


# ---------------------------------------------------------------------------
# sync_experiment tests
# ---------------------------------------------------------------------------


@pytest.fixture()
def sync_mocks():
    """Patch authentication, prompts, and switch_redis_proposal for sync_experiment."""
    with (
        patch.object(_sync_mod, "authenticate") as mock_auth,
        patch.object(_sync_mod, "switch_redis_proposal", return_value={"md": 1}) as mock_switch,
        patch("builtins.input", return_value="prompted_user") as mock_input,
        patch.object(_sync_mod, "getpass", return_value="prompted_pw") as mock_getpass,
    ):
        yield {
            "authenticate": mock_auth,
            "switch_redis_proposal": mock_switch,
            "input": mock_input,
            "getpass": mock_getpass,
        }


def test_sync_experiment_uses_provided_credentials(sync_mocks):
    """Provided username/password should be used without prompting."""
    md = _sync_mod.sync_experiment(
        123456, beamline="SMI", username="testuser", password="secret"
    )

    sync_mocks["input"].assert_not_called()
    sync_mocks["getpass"].assert_not_called()
    sync_mocks["authenticate"].assert_called_once_with("testuser", "secret")
    assert md == {"md": 1}


def test_sync_experiment_prompts_for_missing_credentials(sync_mocks):
    """Missing username/password should fall back to interactive prompts."""
    _sync_mod.sync_experiment(123456, beamline="SMI")

    sync_mocks["input"].assert_called_once()
    sync_mocks["getpass"].assert_called_once()
    sync_mocks["authenticate"].assert_called_once_with("prompted_user", "prompted_pw")
    assert sync_mocks["switch_redis_proposal"].call_args[1]["username"] == "prompted_user"


def test_sync_experiment_forwards_redis_args(sync_mocks):
    """Redis-related args should be forwarded, and verbose should not be."""
    _sync_mod.sync_experiment(
        123456,
        beamline="SMI",
        username="testuser",
        password="secret",
        endstation="opls",
        redis_db=4,
        redis_ssl=False,
    )

    args, kwargs = sync_mocks["switch_redis_proposal"].call_args
    assert args == (123456,)
    assert kwargs == {
        "beamline": "SMI",
        "username": "testuser",
        "endstation": "opls",
        "redis_db": 4,
        "redis_ssl": False,
    }


@pytest.mark.parametrize("verbose_flag, expected_level", [("--verbose", logging.DEBUG), ("--no-verbose", logging.INFO)])
def test_main_sets_log_level_and_prompts(monkeypatch, verbose_flag, expected_level):
    """main() should set the logger level from --verbose and leave credentials to prompts."""
    monkeypatch.setattr(
        sys, "argv", ["sync-experiment", "-b", "SMI", "-p", "123456", verbose_flag]
    )
    original_level = _sync_mod.logger.level
    try:
        with patch.object(_sync_mod, "sync_experiment") as mock_sync:
            _sync_mod.main()
        assert _sync_mod.logger.level == expected_level
    finally:
        _sync_mod.logger.setLevel(original_level)

    kwargs = mock_sync.call_args[1]
    assert kwargs["username"] is None
    assert kwargs["password"] is None
    assert kwargs["proposal_number"] == 123456
    assert kwargs["beamline"] == "SMI"


# ---------------------------------------------------------------------------
# authenticate tests
# ---------------------------------------------------------------------------


@pytest.fixture()
def auth_mocks():
    with (
        patch.object(_sync_mod, "open", mock_open(read_data="common:\n  server: ldap.example\n"), create=True),
        patch.object(_sync_mod, "Server") as mock_server,
        patch.object(_sync_mod, "Connection") as mock_conn,
        patch.object(_sync_mod, "getpass") as mock_getpass,
    ):
        yield {"Server": mock_server, "Connection": mock_conn, "getpass": mock_getpass}


def test_authenticate_uses_given_password(auth_mocks):
    """authenticate() should use the supplied password and never prompt."""
    _sync_mod.authenticate("testuser", "secret")

    auth_mocks["getpass"].assert_not_called()
    kwargs = auth_mocks["Connection"].call_args[1]
    assert kwargs["user"] == "BNL\\testuser"
    assert kwargs["password"] == "secret"


def test_authenticate_invalid_credentials(auth_mocks):
    auth_mocks["Connection"].side_effect = _sync_mod.LDAPInvalidCredentialsResult
    with pytest.raises(RuntimeError, match="Invalid credentials for user 'testuser'"):
        _sync_mod.authenticate("testuser", "bad")
