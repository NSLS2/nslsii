import asyncio
import time

import bluesky.plan_stubs as bps
import pytest
from bluesky.protocols import Reading
from bluesky.run_engine import RunEngine
from bluesky.utils import FailedStatus
from ophyd_async.core import (
    DeviceMock,
    StrictEnum,
    callback_on_mock_execute,
    callback_on_mock_put,
    get_mock_put,
    init_devices,
    set_mock_value,
)

from nslsii.ophyd_async.devices.shutter import (
    InterlockAcknowledge,
    Shutter,
    ShutterStatus,
)


@pytest.fixture
def shutter() -> Shutter:
    with init_devices(mock=True):
        ps = Shutter("TEST:SHUTTER:", name="test_shutter")
        ps._mock_class = DeviceMock  # bypass MockShutter's actuation behavior
    return ps


@pytest.fixture
def mock_shutter() -> Shutter:
    with init_devices(mock=True):
        ms = Shutter("TEST:MOCKSHUTTER:", name="mock_shutter")
    return ms


async def _delayed_readback(ps: Shutter, value: bool, delay: float):
    await asyncio.sleep(delay)
    set_mock_value(ps.status, ShutterStatus.OPEN if value else ShutterStatus.CLOSED)


@pytest.mark.asyncio
@pytest.mark.parametrize("delay", [0.0, 0.05, 0.1, 0.15])
async def test_shutter_open_close_behavior(
    RE: RunEngine, shutter: Shutter, delay: float
):
    set_mock_value(shutter.status, ShutterStatus.CLOSED)

    callback_on_mock_execute(
        shutter.open_cmd,
        lambda *_: asyncio.ensure_future(_delayed_readback(shutter, True, delay)),
    )
    callback_on_mock_execute(
        shutter.close_cmd,
        lambda *_: asyncio.ensure_future(_delayed_readback(shutter, False, delay)),
    )
    # The first plan in a process pays a one-off startup cost; keep it out of the timing
    RE(bps.null())

    t0 = time.monotonic()
    RE(bps.mv(shutter, True))
    open_duration = time.monotonic() - t0
    assert await shutter.status.get_value() is ShutterStatus.OPEN
    assert open_duration >= delay
    assert open_duration < delay + 0.1

    t0 = time.monotonic()
    RE(bps.mv(shutter, False))
    close_duration = time.monotonic() - t0
    assert await shutter.status.get_value() is ShutterStatus.CLOSED
    assert close_duration >= delay
    assert close_duration < delay + 0.1


@pytest.mark.asyncio
async def test_mock_shutter_default_behavior(
    RE: RunEngine, mock_shutter: Shutter, monkeypatch: pytest.MonkeyPatch
):
    uniform_calls: list[tuple[float, float]] = []

    def fake_uniform(a: float, b: float) -> float:
        uniform_calls.append((a, b))
        return 0.1

    monkeypatch.setattr(
        "nslsii.ophyd_async.devices.shutter.random.uniform", fake_uniform
    )

    assert await mock_shutter.status.get_value() is ShutterStatus.CLOSED

    t0 = time.monotonic()
    RE(bps.mv(mock_shutter, True))
    open_duration = time.monotonic() - t0
    assert await mock_shutter.status.get_value() is ShutterStatus.OPEN
    assert open_duration >= 0.1

    t0 = time.monotonic()
    RE(bps.mv(mock_shutter, False))
    close_duration = time.monotonic() - t0
    assert await mock_shutter.status.get_value() is ShutterStatus.CLOSED
    assert close_duration >= 0.1

    assert uniform_calls == [(0.0, 3.0), (0.0, 3.0)]


@pytest.mark.asyncio
async def test_subscribe_reading_translates_status_to_bool(
    RE: RunEngine, shutter: Shutter
):
    set_mock_value(shutter.status, ShutterStatus.CLOSED)

    received: list[dict[str, Reading[bool]]] = []
    shutter.subscribe_reading(received.append)

    # Subscribing delivers an immediate reading for the current CLOSED status.
    assert received[-1].keys() == {shutter.name}
    assert received[-1][shutter.name]["value"] is False
    assert "timestamp" in received[-1][shutter.name]

    set_mock_value(shutter.status, ShutterStatus.OPEN)
    assert received[-1][shutter.name]["value"] is True

    set_mock_value(shutter.status, ShutterStatus.CLOSED)
    assert received[-1][shutter.name]["value"] is False

    # After clearing the subscription no further updates are delivered.
    shutter.clear_sub(received.append)
    count = len(received)
    set_mock_value(shutter.status, ShutterStatus.OPEN)
    assert len(received) == count


def _interlock() -> InterlockAcknowledge:
    with init_devices(mock=True):
        interlock = InterlockAcknowledge("TEST:FE:")
    return interlock


@pytest.mark.asyncio
async def test_interlock_acknowledge_writes_and_waits_for_the_pps(RE: RunEngine):
    interlock = _interlock()
    set_mock_value(interlock.acknowledged, False)

    RE(bps.mv(interlock, True))

    get_mock_put(interlock.request).assert_called_once()
    assert await interlock.acknowledged.get_value()


@pytest.mark.asyncio
async def test_interlock_already_acknowledged_is_not_written(RE: RunEngine):
    interlock = _interlock()

    RE(bps.mv(interlock, True))

    get_mock_put(interlock.request).assert_not_called()


@pytest.mark.asyncio
async def test_interlock_acknowledge_fails_if_the_pps_never_reports_it(
    RE: RunEngine, monkeypatch
):
    monkeypatch.setattr("nslsii.ophyd_async.devices.shutter.DEFAULT_TIMEOUT", 0.1)
    interlock = _interlock()
    set_mock_value(interlock.acknowledged, False)
    callback_on_mock_put(interlock.request, lambda *_, **__: None)

    with pytest.raises(FailedStatus):
        RE(bps.mv(interlock, True))


def _bare_shutter(**kwargs) -> Shutter:
    with init_devices(mock=True):
        sh = Shutter("TEST:SH:", name="sh", **kwargs)
        sh._mock_class = DeviceMock
    set_mock_value(sh.status, ShutterStatus.CLOSED)
    return sh


def _open_on_nth_execute(sh: Shutter, n: int) -> list[int]:
    """Make the shutter open only on the n-th open command, like a PPS dropping commands."""
    sends: list[int] = []

    def _execute(*_):
        sends.append(1)
        if len(sends) == n:
            set_mock_value(sh.status, ShutterStatus.OPEN)

    callback_on_mock_execute(sh.open_cmd, _execute)
    return sends


def test_retries_resend_dropped_commands(RE: RunEngine):
    sh = _bare_shutter(retries=3, retry_period=0.05)
    sends = _open_on_nth_execute(sh, 3)

    RE(bps.mv(sh, True))

    assert len(sends) == 3


def test_no_retries_by_default(RE: RunEngine, monkeypatch):
    monkeypatch.setattr("nslsii.ophyd_async.devices.shutter.DEFAULT_TIMEOUT", 0.1)
    sh = _bare_shutter()
    sends = _open_on_nth_execute(sh, 2)

    with pytest.raises(FailedStatus):
        RE(bps.mv(sh, True))
    assert len(sends) == 1


def test_retries_give_up_after_the_last_attempt(RE: RunEngine, monkeypatch):
    monkeypatch.setattr("nslsii.ophyd_async.devices.shutter.DEFAULT_TIMEOUT", 0.1)
    sh = _bare_shutter(retries=2, retry_period=0.05)
    sends = _open_on_nth_execute(sh, 99)

    with pytest.raises(FailedStatus):
        RE(bps.mv(sh, True))
    assert len(sends) == 3


@pytest.mark.parametrize("cmd_value", [0, 1])
def test_cmd_value_is_written_to_the_command_pvs(cmd_value):
    sh = Shutter("TEST:SH:", cmd_value=cmd_value)
    for cmd in (sh.open_cmd, sh.close_cmd):
        assert cmd._connector.backend._execute_value == cmd_value


@pytest.mark.parametrize("kwargs", [{"cmd_value": 2}, {"retries": -1}, {"retry_period": 0}, {"settle_time": -1}])
def test_invalid_arguments(kwargs):
    with pytest.raises(ValueError):
        Shutter("TEST:SH:", **kwargs)


@pytest.mark.asyncio
async def test_shutter_is_readable(RE: RunEngine):
    sh = _bare_shutter()
    set_mock_value(sh.enabled, True)
    assert (await sh.read())["sh-status"]["value"] == ShutterStatus.CLOSED
    assert (await sh.read_configuration())["sh-enabled"]["value"] is True
    assert sh.hints == {"fields": ["sh-status"]}


class _ValveStatus(StrictEnum):
    OPEN = "Not Closed"
    CLOSED = "Closed"


@pytest.mark.asyncio
async def test_custom_status_enum(RE: RunEngine):
    with init_devices(mock=True):
        valve = Shutter("TEST:GV:", name="valve", status_enum=_ValveStatus)
        valve._mock_class = DeviceMock
    set_mock_value(valve.status, _ValveStatus.CLOSED)
    callback_on_mock_execute(
        valve.open_cmd, lambda *_: set_mock_value(valve.status, _ValveStatus.OPEN)
    )
    readings: list = []
    valve.subscribe_reading(readings.append)

    RE(bps.mv(valve, True))

    assert await valve.status.get_value() is _ValveStatus.OPEN
    assert readings[-1]["valve"]["value"] is True


def test_status_enum_needs_open_and_closed():
    class Incomplete(StrictEnum):
        OPEN = "Open"
        SHUT = "Shut"

    with pytest.raises(TypeError, match="no CLOSED member"):
        Shutter("TEST:GV:", status_enum=Incomplete)


@pytest.mark.parametrize("value, status", [(True, ShutterStatus.OPEN), (False, ShutterStatus.CLOSED)])
def test_no_command_sent_when_already_in_state(RE: RunEngine, value, status):
    sh = _bare_shutter(settle_time=10)
    set_mock_value(sh.status, status)
    sends: list[bool] = []
    callback_on_mock_execute(sh.open_cmd, lambda *_: sends.append(True))
    callback_on_mock_execute(sh.close_cmd, lambda *_: sends.append(False))

    t0 = time.monotonic()
    RE(bps.mv(sh, value))

    assert sends == []
    assert time.monotonic() - t0 < 1  # nothing moved, so no settling either


@pytest.mark.asyncio
async def test_settle_time_waits_after_the_status_confirms(RE: RunEngine):
    sh = _bare_shutter(settle_time=0.2)
    callback_on_mock_execute(
        sh.open_cmd, lambda *_: set_mock_value(sh.status, ShutterStatus.OPEN)
    )
    callback_on_mock_execute(
        sh.close_cmd, lambda *_: set_mock_value(sh.status, ShutterStatus.CLOSED)
    )
    config = await sh.read_configuration()
    assert config["sh-settle_time"]["value"] == 0.2

    t0 = time.monotonic()
    RE(bps.mv(sh, True))
    assert time.monotonic() - t0 >= 0.2

    # It is a signal, so it can be changed at runtime
    RE(bps.mv(sh.settle_time, 0))
    t0 = time.monotonic()
    RE(bps.mv(sh, False))
    assert time.monotonic() - t0 < 0.2


@pytest.mark.parametrize(
    "value, expected",
    [
        (True, ShutterStatus.OPEN),
        (1, ShutterStatus.OPEN),
        ("Open", ShutterStatus.OPEN),
        (ShutterStatus.OPEN, ShutterStatus.OPEN),
        (False, ShutterStatus.CLOSED),
        (0, ShutterStatus.CLOSED),
        ("Close", ShutterStatus.CLOSED),
        ("Closed", ShutterStatus.CLOSED),
        (ShutterStatus.CLOSED, ShutterStatus.CLOSED),
    ],
)
@pytest.mark.asyncio
async def test_set_accepts_bools_ints_strings_and_enum_members(RE: RunEngine, value, expected):
    sh = _bare_shutter()
    set_mock_value(sh.status, ShutterStatus.OPEN if expected is ShutterStatus.CLOSED else ShutterStatus.CLOSED)
    callback_on_mock_execute(sh.open_cmd, lambda *_: set_mock_value(sh.status, ShutterStatus.OPEN))
    callback_on_mock_execute(sh.close_cmd, lambda *_: set_mock_value(sh.status, ShutterStatus.CLOSED))

    RE(bps.mv(sh, value))

    assert await sh.status.get_value() is expected


@pytest.mark.parametrize("value", [2, "Opened", "close", "Not Open", "open", 1.0, None, _ValveStatus.OPEN])
@pytest.mark.asyncio
async def test_set_rejects_other_values(RE: RunEngine, value):
    sh = _bare_shutter()
    with pytest.raises(ValueError, match="Can't set sh"):
        await sh.set(value)


@pytest.mark.asyncio
async def test_set_rejected_while_an_open_is_pending(RE: RunEngine):
    sh = _bare_shutter()
    opened = asyncio.Event()
    closes: list[bool] = []
    callback_on_mock_execute(sh.close_cmd, lambda *_: closes.append(True))

    async def _slow_open(*_):
        await opened.wait()
        set_mock_value(sh.status, ShutterStatus.OPEN)

    callback_on_mock_execute(sh.open_cmd, _slow_open)

    opening = sh.set(True)
    await asyncio.sleep(0.01)  # the open command is out, the status still reads CLOSED
    assert await sh.status.get_value() is ShutterStatus.CLOSED
    with pytest.raises(RuntimeError, match="in progress"):
        await sh.set(False)
    assert closes == []

    opened.set()
    await opening
    assert await sh.status.get_value() is ShutterStatus.OPEN


@pytest.mark.asyncio
async def test_set_rejected_while_settling(RE: RunEngine):
    sh = _bare_shutter(settle_time=0.2)
    callback_on_mock_execute(
        sh.open_cmd, lambda *_: set_mock_value(sh.status, ShutterStatus.OPEN)
    )

    opening = sh.set(True)
    await asyncio.sleep(0.05)  # status already OPEN, still settling
    assert await sh.status.get_value() is ShutterStatus.OPEN
    with pytest.raises(RuntimeError, match="in progress"):
        await sh.set(False)
    await opening


@pytest.mark.asyncio
async def test_failed_or_invalid_set_releases_the_guard(RE: RunEngine, monkeypatch):
    monkeypatch.setattr("nslsii.ophyd_async.devices.shutter.DEFAULT_TIMEOUT", 0.05)
    sh = _bare_shutter()

    with pytest.raises(TimeoutError):
        await sh.set(True)  # the open command is never confirmed
    with pytest.raises(ValueError):
        await sh.set("Shut")

    callback_on_mock_execute(
        sh.open_cmd, lambda *_: set_mock_value(sh.status, ShutterStatus.OPEN)
    )
    await sh.set(True)
    assert await sh.status.get_value() is ShutterStatus.OPEN


@pytest.mark.asyncio
async def test_subscribing_twice_is_cleared_by_one_clear_sub(RE: RunEngine):
    sh = _bare_shutter()
    received: list[dict[str, Reading[bool]]] = []

    sh.subscribe_reading(received.append)
    sh.subscribe_reading(received.append)
    set_mock_value(sh.status, ShutterStatus.OPEN)
    assert [r["sh"]["value"] for r in received].count(True) == 1  # delivered once

    sh.clear_sub(received.append)
    count = len(received)
    set_mock_value(sh.status, ShutterStatus.CLOSED)
    assert len(received) == count
