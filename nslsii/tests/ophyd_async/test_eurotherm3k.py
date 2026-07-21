import pytest
from ophyd_async.core import get_mock_put, init_devices, set_mock_value

from nslsii.ophyd_async.devices import Eurotherm3k, Eurotherm3kControlMode

PREFIX = "XF:28ID1-ES{ET:05}"


@pytest.mark.asyncio
async def test_pvs_are_addressed():
    async with init_devices(mock=True):
        dev = Eurotherm3k(PREFIX, name="et")
    # child loop signals compose under <prefix>LOOPn:
    assert dev.loop1.setpoint.source == f"mock+ca://{PREFIX}LOOP1:SP:RBV"
    assert dev.loop1.readback.source == f"mock+ca://{PREFIX}LOOP1:PV:RBV"
    assert dev.loop2.setpoint.source == f"mock+ca://{PREFIX}LOOP2:SP:RBV"
    # global signal sits directly under the prefix
    assert dev.programmer_status.source == f"mock+ca://{PREFIX}PROGSTAT:RBV"


@pytest.mark.asyncio
async def test_format_tags_route_signals():
    async with init_devices(mock=True):
        dev = Eurotherm3k(PREFIX, name="et")
    reading = await dev.read()
    config = await dev.read_configuration()
    # HINTED readbacks (merged from both loop CHILDren) appear in read()
    assert "et-loop1-readback" in reading
    assert "et-loop2-readback" in reading
    # CONFIG signals appear in read_configuration()
    assert "et-loop1-ramp_rate" in config


@pytest.mark.asyncio
async def test_control_mode_enum_roundtrip():
    async with init_devices(mock=True):
        dev = Eurotherm3k(PREFIX, name="et")
    await dev.loop1.control_mode.set(Eurotherm3kControlMode.MANUAL)
    assert await dev.loop1.control_mode.get_value() is Eurotherm3kControlMode.MANUAL


@pytest.mark.asyncio
async def test_set_commands_setpoint_and_settles():
    async with init_devices(mock=True):
        dev = Eurotherm3k(PREFIX, name="et")
    dev.loop1.tolerance = 1.0
    dev.loop1.settle_time = 0.0
    # pretend the controller is already at temperature
    set_mock_value(dev.loop1.readback, 300.0)
    await dev.loop1.set(300.0)  # returns => it settled
    get_mock_put(dev.loop1.setpoint).assert_called_once()  # and it wrote the setpoint


@pytest.mark.asyncio
async def test_set_times_out_if_never_in_band():
    async with init_devices(mock=True):
        dev = Eurotherm3k(PREFIX, name="et")
    dev.loop1.tolerance = 0.5
    dev.loop1.move_timeout = 0.3  # keep the test fast
    set_mock_value(dev.loop1.readback, 0.0)  # stays far from 300
    with pytest.raises(TimeoutError):
        await dev.loop1.set(300.0)


# Legacy PDF eurotherm3k (pdf-profile-collection/startup/16-eurotherm_HAB.py):
# each component -> the ophyd-async signal that must address the same PV.
_LEGACY_PV_MAP = {
    "setpoint": ("loop1", "setpoint", "LOOP1:SP:RBV"),
    "readback": ("loop1", "readback", "LOOP1:PV:RBV"),
    "working": ("loop1", "working_setpoint", "LOOP1:WSP:RBV"),
    "output": ("loop1", "output", "LOOP1:O:RBV"),
    "ramprate": ("loop1", "ramp_rate", "LOOP1:RR:RBV"),
    "manual_mode": ("loop1", "control_mode", "LOOP1:MAN:RBV"),
    "autotune": ("loop1", "autotune", "LOOP1:AUTOTUNE:RBV"),
}


@pytest.mark.asyncio
async def test_legacy_pv_surface_equivalence():
    """Every PV of the legacy PDF eurotherm3k is addressed identically here."""
    async with init_devices(mock=True):
        dev = Eurotherm3k(PREFIX, name="et")
    for _legacy, (loop, attr, suffix) in _LEGACY_PV_MAP.items():
        signal = getattr(getattr(dev, loop), attr)
        assert signal.source == f"mock+ca://{PREFIX}{suffix}"
