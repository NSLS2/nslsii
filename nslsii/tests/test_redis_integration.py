"""Tests for redis-related parameter changes in utils, __init__, and sync_experiment."""

from unittest.mock import MagicMock, patch, mock_open

import httpx
import pytest

import sys

import nslsii
from nslsii.utils import open_redis_client
from nslsii.sync_experiment.sync_experiment import switch_proposal

# The __init__.py of nslsii.sync_experiment re-exports a function called
# sync_experiment, which shadows the module of the same name. We need the
# actual module object for patch.object, so grab it from sys.modules.
_sync_mod = sys.modules["nslsii.sync_experiment.sync_experiment"]


# ---------------------------------------------------------------------------
# open_redis_client tests
# ---------------------------------------------------------------------------


@patch("nslsii.utils.os.getenv", return_value=None)
@patch("nslsii.utils.socket.gethostname", return_value="xf12id1-ws1")
@patch("nslsii.utils.Redis")
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


@patch("nslsii.utils.os.getenv", return_value=None)
@patch("nslsii.utils.Redis")
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
# switch_proposal tests
# ---------------------------------------------------------------------------


@pytest.fixture()
def switch_mocks():
    """Patch all external dependencies of switch_proposal and yield a dict of mocks."""
    md = {
        "data_sessions_authorized": ["pass-123456"],
        "username": "testuser",
    }
    with (
        patch.object(_sync_mod, "open_redis_client") as mock_open_rc,
        patch.object(_sync_mod, "RedisJSONDict", return_value=md) as mock_rjd,
        patch.object(
            _sync_mod,
            "retrieve_proposals",
            return_value={
                "123456": {
                    "proposal_id": "123456",
                    "title": "t",
                    "type": "GU",
                    "users": [],
                }
            },
        ),
        patch.object(_sync_mod, "get_commissioning_proposals", return_value=[]),
        patch.object(_sync_mod, "get_current_cycle", return_value="2026-1"),
    ):
        yield {"open_redis_client": mock_open_rc, "RedisJSONDict": mock_rjd}


def test_switch_proposal_ssl_no_prefix(switch_mocks):
    """With redis_ssl=True the RedisJSONDict prefix should be empty."""
    switch_proposal(123456, beamline="SMI", username="testuser", redis_ssl=True)

    mock_rjd = switch_mocks["RedisJSONDict"]
    mock_rjd.assert_called_once()
    assert mock_rjd.call_args[1]["prefix"] == ""


def test_switch_proposal_endstation_no_ssl(switch_mocks):
    """With an endstation and no SSL, prefix should identify beamline and endstation."""
    switch_proposal(
        123456, beamline="SMI", username="testuser", endstation="opls", redis_ssl=False
    )

    mock_rjd = switch_mocks["RedisJSONDict"]
    mock_rjd.assert_called_once()
    assert mock_rjd.call_args[1]["prefix"] == "smi-opls-"


def test_switch_proposal_passes_redis_db(switch_mocks):
    """redis_db should be forwarded to open_redis_client."""
    switch_proposal(
        123456, beamline="SMI", username="testuser", redis_db=7, redis_ssl=False
    )

    call_kwargs = switch_mocks["open_redis_client"].call_args[1]
    assert call_kwargs["redis_db"] == 7


def test_revoke_active_api_key_clears_unusable_key():
    redis_client = MagicMock()
    tiled_context = MagicMock()
    request = httpx.Request("GET", "https://tiled.example/auth/apikey")
    tiled_context.which_api_key.side_effect = httpx.HTTPStatusError(
        "API key is no longer valid",
        request=request,
        response=httpx.Response(401, request=request),
    )

    with (
        patch.object(_sync_mod, "get_api_key", return_value="expired-key"),
        patch.object(
            _sync_mod, "create_tiled_context", return_value=(tiled_context, None)
        ),
        patch.object(_sync_mod, "revoke_api_key") as mock_revoke_api_key,
        patch.object(_sync_mod, "set_api_key") as mock_set_api_key,
    ):
        _sync_mod.revoke_active_api_key(redis_client, "smi", None)

    mock_set_api_key.assert_called_once_with(redis_client, "smi", None, "")
    mock_revoke_api_key.assert_not_called()
    tiled_context.logout.assert_called_once_with()
    tiled_context.close.assert_called_once_with()


def test_revoke_active_api_key_retains_key_without_revoke_scope():
    redis_client = MagicMock()
    tiled_context = MagicMock()
    request = httpx.Request("DELETE", "https://tiled.example/auth/apikey")
    response = httpx.Response(401, request=request)
    error = httpx.HTTPStatusError(
        "Not enough permissions", request=request, response=response
    )

    with (
        patch.object(_sync_mod, "get_api_key", return_value="under-scoped-key"),
        patch.object(
            _sync_mod, "create_tiled_context", return_value=(tiled_context, None)
        ),
        patch.object(_sync_mod, "revoke_api_key", side_effect=error),
        patch.object(_sync_mod, "set_api_key") as mock_set_api_key,
        pytest.raises(httpx.HTTPStatusError),
    ):
        _sync_mod.revoke_active_api_key(redis_client, "smi", None)

    tiled_context.which_api_key.assert_called_once_with()
    mock_set_api_key.assert_not_called()
    tiled_context.logout.assert_called_once_with()
    tiled_context.close.assert_called_once_with()


def test_revoke_active_api_key_retains_key_on_server_error():
    redis_client = MagicMock()
    tiled_context = MagicMock()
    request = httpx.Request("DELETE", "https://tiled.example/auth/apikey")
    response = httpx.Response(503, request=request)
    error = httpx.HTTPStatusError(
        "Tiled is unavailable", request=request, response=response
    )

    with (
        patch.object(_sync_mod, "get_api_key", return_value="active-key"),
        patch.object(
            _sync_mod, "create_tiled_context", return_value=(tiled_context, None)
        ),
        patch.object(_sync_mod, "revoke_api_key", side_effect=error),
        patch.object(_sync_mod, "set_api_key") as mock_set_api_key,
        pytest.raises(httpx.HTTPStatusError),
    ):
        _sync_mod.revoke_active_api_key(redis_client, "smi", None)

    mock_set_api_key.assert_not_called()
    tiled_context.logout.assert_called_once_with()
    tiled_context.close.assert_called_once_with()
