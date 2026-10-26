"""Tests for redis-related parameter changes in utils, __init__, and sync_experiment."""

from unittest.mock import MagicMock, patch, mock_open

import httpx
import pytest

import sys

import nslsii
from nslsii.utils.redis import open_redis_client
from nslsii.sync_experiment.sync_experiment import switch_proposal

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
# switch_proposal tests
# ---------------------------------------------------------------------------


@pytest.fixture()
def switch_mocks(monkeypatch):
    """Patch all external dependencies of switch_proposal and yield a dict of mocks."""
    # Beamline workstations set these; they would override the test arguments.
    monkeypatch.delenv("BEAMLINE_ACRONYM", raising=False)
    monkeypatch.delenv("ENDSTATION_ACRONYM", raising=False)
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


@pytest.mark.parametrize(
    "beamline, endstation, expected_prefix",
    [
        ("SMI", None, "smi-md-"),
        ("SMI", "opls", "smi-opls-md-"),
        ("sst1", None, "sst-md-"),
    ],
)
def test_switch_proposal_redis_prefix(
    switch_mocks, beamline, endstation, expected_prefix
):
    """The metadata prefix is built from the normalized beamline and endstation."""
    switch_proposal(
        123456, beamline=beamline, username="testuser", endstation=endstation
    )

    mock_rjd = switch_mocks["RedisJSONDict"]
    mock_rjd.assert_called_once()
    assert mock_rjd.call_args[1]["prefix"] == expected_prefix


def test_switch_proposal_opens_default_redis_client(switch_mocks):
    """Redis connection settings are no longer passed through switch_proposal."""
    switch_proposal(123456, beamline="SMI", username="testuser")

    switch_mocks["open_redis_client"].assert_called_once_with()


@pytest.mark.parametrize("stored", [b"abcd1234", "abcd1234"])
def test_get_api_key_returns_str(stored):
    """Redis returns bytes unless decoding is on; the key must always be a str."""
    redis_client = MagicMock()
    redis_client.get.return_value = stored

    assert _sync_mod.get_api_key(redis_client, "smi", None) == "abcd1234"


def test_get_api_key_returns_none_when_unset():
    redis_client = MagicMock()
    redis_client.get.return_value = None

    assert _sync_mod.get_api_key(redis_client, "smi", None) is None


@pytest.mark.parametrize(
    "proposal, match",
    [
        ({"instruments": None, "cycles": ["2026-3"]}, "not at this beamline"),
        ({"instruments": ["SMI"], "cycles": None}, "not allocated for the current"),
    ],
)
def test_retrieve_proposals_null_lists_raise_value_error(proposal, match):
    """nsls2api may return null lists; they must fail validation, not TypeError."""
    response = MagicMock()
    response.json.return_value = {"proposal": {"proposal_id": "123456", **proposal}}

    with (
        patch.object(_sync_mod, "get_current_cycle", return_value="2026-3"),
        patch.object(_sync_mod, "get_commissioning_proposals", return_value=[]),
        patch.object(_sync_mod, "facility_api_client") as mock_api,
    ):
        mock_api.get.return_value = response
        with pytest.raises(ValueError, match=match):
            _sync_mod.retrieve_proposals("nsls2", "smi", ["123456"])


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
