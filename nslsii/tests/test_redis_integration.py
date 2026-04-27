"""Tests for redis-related parameter changes in utils, __init__, and sync_experiment."""

from unittest.mock import MagicMock, patch, mock_open

import pytest

import nslsii
from nslsii.utils import open_redis_client
from nslsii.sync_experiment.sync_experiment import switch_redis_proposal


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
# switch_redis_proposal tests
# ---------------------------------------------------------------------------


@pytest.fixture()
def switch_mocks():
    """Patch all external dependencies of switch_redis_proposal and yield a dict of mocks."""
    sync_experiment_module = "nslsii.sync_experiment.sync_experiment"
    with (
        patch(f"{sync_experiment_module}.open_redis_client") as mock_open_rc,
        patch(f"{sync_experiment_module}.RedisJSONDict", return_value={}) as mock_rjd,
        patch(f"{sync_experiment_module}.should_they_be_here", return_value=True),
        patch(
            f"{sync_experiment_module}.validate_proposal",
            return_value={
                "proposal_id": "123",
                "title": "t",
                "type": "GU",
                "users": [],
            },
        ),
        patch(
            f"{sync_experiment_module}.is_commissioning_proposal", return_value=False
        ),
        patch(f"{sync_experiment_module}.get_current_cycle", return_value="2026-1"),
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
