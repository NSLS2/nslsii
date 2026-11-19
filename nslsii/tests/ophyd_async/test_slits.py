import asyncio
import threading

import bluesky.plan_stubs as bps
import pytest
import pytest_asyncio
from bluesky.protocols import Subscribable
from bluesky.run_engine import RunEngine, call_in_bluesky_event_loop
from bluesky.utils import RunEngineInterrupted
from ophyd_async.epics.motor import Motor as AsyncEpicsMotor
from ophyd_async.core import (
    WatchableAsyncStatus,
    callback_on_mock_put,
    init_devices,
    set_mock_value,
)

from nslsii.ophyd_async.devices import SlitCenter, SlitGap, Slits, move_slits


@pytest_asyncio.fixture
async def slits() -> Slits:
    async with init_devices(mock=True):
        device = Slits("XF:TEST:")
    # Mirror each motor setpoint to its readback so moves complete in mock mode.
    for motor in (device.inboard, device.outboard, device.bottom, device.top):
        callback_on_mock_put(
            motor.user_setpoint,
            lambda value, motor=motor, **_: set_mock_value(motor.user_readback, value),
        )
    return device


# axis label, low motor, high motor, gap signal, center signal
_AXES = [
    ("horizontal", "inboard", "outboard", "horizontal_gap", "horizontal_center"),
    ("vertical", "bottom", "top", "vertical_gap", "vertical_center"),
]


def _park(motor, value: float) -> None:
    """Put a mock motor at rest at ``value``: setpoint and readback agree."""
    set_mock_value(motor.user_setpoint, value)
    set_mock_value(motor.user_readback, value)


@pytest.mark.asyncio
@pytest.mark.parametrize("axis, low, high, gap, center", _AXES)
async def test_slits_gap_and_center_readback(
    slits: Slits, axis: str, low: str, high: str, gap: str, center: str
):
    set_mock_value(getattr(slits, low).user_readback, -2.0)
    set_mock_value(getattr(slits, high).user_readback, 4.0)

    assert await getattr(slits, gap).readback.get_value() == pytest.approx(6.0)
    assert await getattr(slits, center).readback.get_value() == pytest.approx(1.0)


@pytest.mark.asyncio
@pytest.mark.parametrize("axis, low, high, gap, center", _AXES)
async def test_slits_set_gap_keeps_center(
    slits: Slits, axis: str, low: str, high: str, gap: str, center: str
):
    low_motor, high_motor = getattr(slits, low), getattr(slits, high)
    _park(low_motor, -1.0)
    _park(high_motor, 3.0)  # center 1.0, gap 4.0

    await getattr(slits, gap).set(10.0)

    # Blades move symmetrically about the unchanged center.
    assert await low_motor.user_readback.get_value() == pytest.approx(-4.0)
    assert await high_motor.user_readback.get_value() == pytest.approx(6.0)
    assert await getattr(slits, center).readback.get_value() == pytest.approx(1.0)
    assert await getattr(slits, gap).readback.get_value() == pytest.approx(10.0)


@pytest.mark.asyncio
@pytest.mark.parametrize("axis, low, high, gap, center", _AXES)
async def test_slits_set_center_keeps_gap(
    slits: Slits, axis: str, low: str, high: str, gap: str, center: str
):
    low_motor, high_motor = getattr(slits, low), getattr(slits, high)
    _park(low_motor, -1.0)
    _park(high_motor, 3.0)  # center 1.0, gap 4.0

    await getattr(slits, center).set(5.0)

    # Both blades shift by the same amount, preserving the gap.
    assert await low_motor.user_readback.get_value() == pytest.approx(3.0)
    assert await high_motor.user_readback.get_value() == pytest.approx(7.0)
    assert await getattr(slits, gap).readback.get_value() == pytest.approx(4.0)
    assert await getattr(slits, center).readback.get_value() == pytest.approx(5.0)


@pytest.mark.asyncio
async def test_slits_set_all_gaps_and_centers_via_mv(RE: RunEngine):
    # Build under the RE loop so bps.mv can drive the device.
    with init_devices(mock=True):
        slits = Slits("XF:TEST:")
    for motor in (slits.inboard, slits.outboard, slits.bottom, slits.top):
        callback_on_mock_put(
            motor.user_setpoint,
            lambda value, motor=motor, **_: set_mock_value(motor.user_readback, value),
        )
        set_mock_value(motor.user_readback, 0.0)

    # Flat (h_gap, h_center, v_gap, v_center).
    RE(bps.mv(slits, (10, 10, 4, 2)))

    # Horizontal: gap 10, center 10 -> inboard 5, outboard 15.
    assert await slits.inboard.user_readback.get_value() == pytest.approx(5.0)
    assert await slits.outboard.user_readback.get_value() == pytest.approx(15.0)
    # Vertical: gap 4, center 2 -> bottom 0, top 4.
    assert await slits.bottom.user_readback.get_value() == pytest.approx(0.0)
    assert await slits.top.user_readback.get_value() == pytest.approx(4.0)

    assert await slits.horizontal_gap.readback.get_value() == pytest.approx(10.0)
    assert await slits.horizontal_center.readback.get_value() == pytest.approx(10.0)
    assert await slits.vertical_gap.readback.get_value() == pytest.approx(4.0)
    assert await slits.vertical_center.readback.get_value() == pytest.approx(2.0)


@pytest.mark.asyncio
async def test_slits_set_reports_gap_and_center_progress_to_watchers(slits: Slits):
    for motor in (slits.inboard, slits.outboard, slits.bottom, slits.top):
        set_mock_value(motor.user_readback, 0.0)
    updates = []

    status = slits.set((10, 10, 4, 2))
    status.watch(lambda **kw: updates.append(kw["name"]))  # type: ignore
    await status

    assert {
        slits.horizontal_gap.name,
        slits.horizontal_center.name,
        slits.vertical_gap.name,
        slits.vertical_center.name,
    } <= set(updates)


@pytest.mark.asyncio
@pytest.mark.parametrize("axis, low, high, gap, center", _AXES)
async def test_slits_gap_and_center_report_their_own_progress(
    slits: Slits, axis: str, low: str, high: str, gap: str, center: str
):
    for motor in _motors(slits):
        set_mock_value(motor.user_readback, 0.0)
    for name in (gap, center):
        updates = []
        device = getattr(slits, name)
        status = device.set(2.0)
        status.watch(lambda updates=updates, **kw: updates.append(kw))  # type: ignore
        await status
        assert updates
        assert {u["name"] for u in updates} == {device.name}
        assert updates[-1]["target"] == 2.0
        assert updates[-1]["unit"] == "mm"


@pytest.mark.asyncio
async def test_slits_gap_and_center_read_under_their_own_names(slits: Slits):
    reading = await slits.read()
    for name in ("horizontal_gap", "horizontal_center", "vertical_gap"):
        assert getattr(slits, name).name in reading


@pytest.mark.asyncio
@pytest.mark.parametrize("axis, low, high, gap, center", _AXES)
async def test_slits_gap_and_center_are_subscribable(
    slits: Slits, axis: str, low: str, high: str, gap: str, center: str
):
    set_mock_value(getattr(slits, low).user_readback, -2.0)
    set_mock_value(getattr(slits, high).user_readback, 4.0)
    for name, expected in ((gap, 6.0), (center, 1.0)):
        device = getattr(slits, name)
        assert isinstance(device, Subscribable)
        readings = []
        device.subscribe_reading(readings.append)
        try:
            await asyncio.sleep(0.01)
            assert readings[-1][device.name]["value"] == pytest.approx(expected)
        finally:
            device.clear_sub(readings.append)


def _re_slits(blades: tuple[float, float, float, float]) -> Slits:
    """Mock slits under the RE loop; blades at (inboard, outboard, bottom, top)."""
    with init_devices(mock=True):
        slits = Slits("XF:TEST:")
    for motor, value in zip(_motors(slits), blades, strict=True):
        callback_on_mock_put(
            motor.user_setpoint,
            lambda value, motor=motor, **_: set_mock_value(motor.user_readback, value),
        )
        # A motor at rest: bps.mvr moves relative to the setpoint, not the readback
        set_mock_value(motor.user_setpoint, value)
        set_mock_value(motor.user_readback, value)
    return slits


def _motors(slits: Slits):
    return (slits.inboard, slits.outboard, slits.bottom, slits.top)


async def _blades(slits: Slits) -> list[float]:
    return [await m.user_readback.get_value() for m in _motors(slits)]


@pytest.mark.asyncio
async def test_move_slits_moves_only_the_given_values(RE: RunEngine):
    slits = _re_slits((-1.0, 3.0, -2.0, 2.0))

    RE(move_slits(slits, horizontal_center=2.0))

    # Both horizontal blades shift by +1 (gap 4 kept); the vertical pair stays put.
    assert await _blades(slits) == pytest.approx([0.0, 4.0, -2.0, 2.0])


@pytest.mark.asyncio
async def test_move_slits_sets_gap_before_center(RE: RunEngine):
    slits = _re_slits((-1.0, 1.0, 0.0, 0.0))

    RE(move_slits(slits, horizontal_gap=4.0, horizontal_center=5.0))

    assert (await _blades(slits))[:2] == pytest.approx([3.0, 7.0])


@pytest.mark.asyncio
async def test_move_slits_relative_shifts_from_current(RE: RunEngine):
    slits = _re_slits((-1.0, 3.0, -2.0, 2.0))

    RE(move_slits(slits, vertical_center=0.5, horizontal_gap=-1.0, relative=True))

    # Horizontal gap 4 -> 3 about center 1; vertical pair shifted up by 0.5.
    assert await _blades(slits) == pytest.approx([-0.5, 2.5, -1.5, 2.5])


def test_move_slits_needs_a_value(RE: RunEngine):
    slits = _re_slits((0.0, 0.0, 0.0, 0.0))
    with pytest.raises(ValueError, match="at least one"):
        RE(move_slits(slits))


def test_move_slits_checkpoints_before_each_stage(RE: RunEngine):
    slits = _re_slits((-1.0, 1.0, 0.0, 0.0))
    commands: list[str] = []
    RE.msg_hook = lambda msg: commands.append(msg.command)  # type: ignore[assignment]

    RE(move_slits(slits, horizontal_gap=4.0, horizontal_center=5.0))

    moves = [c for c in commands if c in ("checkpoint", "set")]
    assert moves == ["checkpoint", "set", "checkpoint", "set"]


@pytest.mark.asyncio
async def test_stopping_slits_with_success_stops_blades_and_skips_centers(
    RE: RunEngine, monkeypatch
):
    slits = _re_slits((-1.0, 1.0, -1.0, 1.0))
    moving = asyncio.Event()
    stopped = asyncio.Event()
    stops: list[bool] = []
    centers: list[float] = []

    @WatchableAsyncStatus.wrap
    async def slow_gap(axis, value):
        moving.set()
        await stopped.wait()
        if False:
            yield

    @WatchableAsyncStatus.wrap
    async def record_center(axis, value):
        centers.append(value)
        if False:
            yield

    async def stop_motor(motor, success: bool = False):
        stops.append(success)
        stopped.set()

    monkeypatch.setattr(SlitGap, "set", slow_gap)
    monkeypatch.setattr(SlitCenter, "set", record_center)
    monkeypatch.setattr(type(slits.inboard), "stop", stop_motor)

    status = slits.set(((4.0, 0.0), (4.0, 0.0)))
    await moving.wait()
    await slits.stop(success=True)
    await asyncio.wait_for(status, timeout=1)

    assert status.success
    assert stops == [True] * 4
    assert centers == []


@pytest.mark.asyncio
@pytest.mark.parametrize("axis, low, high, gap, center", _AXES)
async def test_slits_gap_and_center_work_with_locating_plan_stubs(
    RE: RunEngine, axis: str, low: str, high: str, gap: str, center: str
):
    # bps.rd and bps.mvr locate() a movable, so the gap/center must be locatable.
    slits = _re_slits((-1.0, 3.0, -1.0, 3.0))
    readings = {}

    def plan():
        readings["gap"] = yield from bps.rd(getattr(slits, gap))
        readings["center"] = yield from bps.rd(getattr(slits, center))
        yield from bps.mvr(getattr(slits, center), 0.5)

    RE(plan())

    assert readings == {"gap": pytest.approx(4.0), "center": pytest.approx(1.0)}
    assert await getattr(slits, center).readback.get_value() == pytest.approx(1.5)
    assert await getattr(slits, gap).readback.get_value() == pytest.approx(4.0)


def test_slits_pairs_can_have_their_own_prefixes():
    slits = Slits("XF:TEST{Slt:1-Ax:", vertical_prefix="XF:TEST{Slt:2-Ax:")
    assert slits.inboard.user_setpoint.source == "ca://XF:TEST{Slt:1-Ax:I}Mtr.VAL"
    assert slits.outboard.user_setpoint.source == "ca://XF:TEST{Slt:1-Ax:O}Mtr.VAL"
    assert slits.bottom.user_setpoint.source == "ca://XF:TEST{Slt:2-Ax:B}Mtr.VAL"
    assert slits.top.user_setpoint.source == "ca://XF:TEST{Slt:2-Ax:T}Mtr.VAL"

    slits = Slits(horizontal_prefix="XF:TEST{Slt:H-Ax:", vertical_prefix="XF:TEST{Slt:V-Ax:")
    assert slits.inboard.user_setpoint.source == "ca://XF:TEST{Slt:H-Ax:I}Mtr.VAL"
    assert slits.top.user_setpoint.source == "ca://XF:TEST{Slt:V-Ax:T}Mtr.VAL"

    with pytest.raises(ValueError, match="horizontal_prefix and vertical_prefix"):
        Slits(horizontal_prefix="XF:TEST{Slt:H-Ax:")



@pytest.mark.parametrize("axis, low, high, gap, center", _AXES)
@pytest.mark.asyncio
async def test_repeated_moves_keep_the_commanded_value_despite_positioning_error(
    RE: RunEngine, axis: str, low: str, high: str, gap: str, center: str
):
    # Each blade lands a little off its setpoint, as a motor within its deadband might;
    # different errors per blade, so neither the gap nor the center error cancels out
    slits = _re_slits((-1.0, 1.0, -1.0, 1.0))
    for motor in _motors(slits):
        set_mock_value(motor.deadband, 0.005)
    for motor, error in zip(_motors(slits), (0.001, 0.002, 0.001, 0.002), strict=True):
        callback_on_mock_put(
            motor.user_setpoint,
            lambda value, motor=motor, error=error, **_: set_mock_value(
                motor.user_readback, value + error
            ),
        )

    def scan(axis_device, values):
        for value in values:
            yield from bps.mv(axis_device, value)

    # A gap scan keeps the commanded center (0), and a center scan the commanded gap (2)
    RE(scan(getattr(slits, gap), [2.0 + 0.1 * i for i in range(50)]))
    low_sp = await getattr(slits, low).user_setpoint.get_value()
    high_sp = await getattr(slits, high).user_setpoint.get_value()
    assert (low_sp + high_sp) / 2 == pytest.approx(0.0, abs=1e-9)

    RE(bps.mv(getattr(slits, gap), 2.0))
    RE(scan(getattr(slits, center), [0.1 * i for i in range(50)]))
    low_sp = await getattr(slits, low).user_setpoint.get_value()
    high_sp = await getattr(slits, high).user_setpoint.get_value()
    assert high_sp - low_sp == pytest.approx(2.0, abs=1e-9)



@pytest.mark.parametrize("axis, low, high, gap, center", _AXES)
@pytest.mark.asyncio
async def test_stale_setpoints_fall_back_to_readbacks(
    RE: RunEngine, caplog, axis: str, low: str, high: str, gap: str, center: str
):
    # As after an IOC reboot: blades really at -1 and 3 (center 1, gap 4), setpoints stale
    slits = _re_slits((0.0, 0.0, 0.0, 0.0))
    low_motor, high_motor = getattr(slits, low), getattr(slits, high)
    for motor in (low_motor, high_motor):
        set_mock_value(motor.deadband, 0.005)
    set_mock_value(low_motor.user_readback, -1.0)
    set_mock_value(high_motor.user_readback, 3.0)

    location = await getattr(slits, gap).locate()
    assert location["setpoint"] == pytest.approx(4.0)

    with caplog.at_level("WARNING", logger="nslsii.ophyd_async.devices.slits"):
        RE(bps.mv(getattr(slits, gap), 10.0))

    # The real center (1) is kept, not the stale one (0)
    assert await low_motor.user_readback.get_value() == pytest.approx(-4.0)
    assert await high_motor.user_readback.get_value() == pytest.approx(6.0)
    assert "using its readback" in caplog.text


@pytest.mark.asyncio
async def test_moving_blade_uses_its_setpoint(RE: RunEngine):
    # The inboard blade is on its way to 3, so far from it but not at rest
    slits = _re_slits((-1.0, 1.0, 0.0, 0.0))
    set_mock_value(slits.inboard.user_setpoint, 3.0)
    set_mock_value(slits.inboard.motor_done_move, 0)

    location = await slits.horizontal_center.locate()

    assert location["setpoint"] == pytest.approx(2.0)  # (3 + 1) / 2, the commanded center
    assert location["readback"] == pytest.approx(0.0)


def _stall_next_move(
    blades: dict[AsyncEpicsMotor, float], RE: RunEngine | None = None
) -> None:
    """Make the next move of each of ``blades`` hang until stopped, then rest where given.

    Stopped, each ends up at rest with setpoint and readback where it stopped, as the
    motor record leaves it. With ``RE``, a pause is requested once all are moving.
    """
    moving: list[AsyncEpicsMotor] = []

    def _setup(blade: AsyncEpicsMotor, stop_at: float):
        released = asyncio.Event()
        calls = 0

        async def _move(value, **_):
            nonlocal calls
            calls += 1
            if calls > 1:
                set_mock_value(blade.user_readback, value)
                return
            set_mock_value(blade.motor_done_move, 0)
            moving.append(blade)
            if RE is not None and len(moving) == len(blades):
                # From another thread, as a Ctrl-C pause is; it blocks on the RE loop
                threading.Thread(target=RE.request_pause).start()
            await released.wait()  # still moving until stopped
            _park(blade, stop_at)
            set_mock_value(blade.motor_done_move, 1)

        callback_on_mock_put(blade.user_setpoint, _move)
        callback_on_mock_put(blade.motor_stop, lambda *_, **__: released.set())

    for blade, stop_at in blades.items():
        _setup(blade, stop_at)


def _pause_partway_through_first_move(
    RE: RunEngine, blades: dict[AsyncEpicsMotor, float]
) -> None:
    """Make the first move of each of ``blades`` stop where given, on a RunEngine pause."""
    _stall_next_move(blades, RE)


@pytest.mark.parametrize("via", ["gap", "slits"])
def test_replay_after_pause_finishes_the_commanded_move(RE: RunEngine, via: str):
    slits = _re_slits((-1.0, 1.0, -1.0, 1.0))
    for motor in _motors(slits):
        set_mock_value(motor.deadband, 0.005)
    # Gap 4 from (-1, 1) heads for (-2, 2); stopped at (-2, 1), centered on -0.5
    _pause_partway_through_first_move(RE, {slits.inboard: -2.0, slits.outboard: 1.0})

    def plan():
        yield from bps.checkpoint()
        if via == "gap":
            yield from bps.mv(slits.horizontal_gap, 4.0)
        else:
            yield from bps.mv(slits, (4.0, 0.0, 2.0, 0.0))

    with pytest.raises(RunEngineInterrupted):
        RE(plan())
    RE.resume()

    # Gap 4 about the commanded center 0, not about -0.5 where the blades stopped
    assert call_in_bluesky_event_loop(_blades(slits))[:2] == pytest.approx([-2.0, 2.0])


def test_another_move_forgets_the_interrupted_one(RE: RunEngine):
    slits = _re_slits((-1.0, 1.0, -1.0, 1.0))
    for motor in _motors(slits):
        set_mock_value(motor.deadband, 0.005)
    # Gap 4 from (-1, 1) heads for (-2, 2); stopped at (-2, 1), centered on -0.5
    _pause_partway_through_first_move(RE, {slits.inboard: -2.0, slits.outboard: 1.0})

    with pytest.raises(RunEngineInterrupted):
        RE(bps.mv(slits.horizontal_gap, 4.0))
    RE.abort()

    RE(bps.mv(slits.horizontal_center, 1.0))  # gap 3 from (-2, 1), now about center 1
    RE(bps.mv(slits.horizontal_gap, 4.0))  # recomputed: about center 1, not the old 0

    assert call_in_bluesky_event_loop(_blades(slits))[:2] == pytest.approx([-1.0, 3.0])


@pytest.mark.parametrize(
    "inboard_moved_to, expected",
    [
        (-3.0, [-3.0, 1.0]),  # moved: gap 4 computed afresh about the new center, -1
        (-2.001, [-2.0, 2.0]),  # within the 5 um deadband: the commanded move resumes
    ],
)
def test_blade_moved_directly_while_paused(
    RE: RunEngine, inboard_moved_to: float, expected: list[float]
):
    slits = _re_slits((-1.0, 1.0, -1.0, 1.0))
    for motor in _motors(slits):
        set_mock_value(motor.deadband, 0.005)
    _pause_partway_through_first_move(RE, {slits.inboard: -2.0, slits.outboard: 1.0})

    def plan():
        yield from bps.checkpoint()
        yield from bps.mv(slits.horizontal_gap, 4.0)

    with pytest.raises(RunEngineInterrupted):
        RE(plan())

    async def move_inboard_directly():
        await slits.inboard.set(inboard_moved_to)

    call_in_bluesky_event_loop(move_inboard_directly())
    RE.resume()

    assert call_in_bluesky_event_loop(_blades(slits))[:2] == pytest.approx(expected, abs=1e-6)


@pytest.mark.parametrize("end", ["abort", "halt", "stop"])
def test_ending_the_run_after_a_pause_forgets_the_interrupted_move(RE: RunEngine, end: str):
    slits = _re_slits((-1.0, 1.0, -1.0, 1.0))
    for motor in _motors(slits):
        set_mock_value(motor.deadband, 0.005)
    # Gap 4 from (-1, 1) heads for (-2, 2); stopped at (-2, 1), centered on -0.5
    _pause_partway_through_first_move(RE, {slits.inboard: -2.0, slits.outboard: 1.0})

    def plan():
        yield from bps.checkpoint()
        yield from bps.mv(slits.horizontal_gap, 4.0)

    with pytest.raises(RunEngineInterrupted):
        RE(plan())
    try:
        getattr(RE, end)()
    except Exception:
        pass  # abort and halt re-raise the interruption; stop doesn't

    # A new plan's gap 4 is computed from where the blades are, not resumed
    RE(bps.mv(slits.horizontal_gap, 4.0))
    assert call_in_bluesky_event_loop(_blades(slits))[:2] == pytest.approx([-2.5, 1.5])



@pytest.mark.parametrize("stopped_first", ["gap", "center"])
@pytest.mark.parametrize("paused_before_blades_stopped", [False, True])
def test_stopping_both_axes_in_a_pause_keeps_the_interrupted_move(
    RE: RunEngine, stopped_first: str, paused_before_blades_stopped: bool
):
    # The RunEngine stops each moved device in turn, then pauses them. Whichever axis
    # it stops first, and whether the blades finish stopping before the pause or after,
    # the center's stop must not lose the gap's interrupted move.
    slits = _re_slits((-1.0, 1.0, -1.0, 1.0))
    for motor in _motors(slits):
        set_mock_value(motor.deadband, 0.005)
    gap, center = slits.horizontal_gap, slits.horizontal_center
    axes = {"gap": gap, "center": center}
    order = [stopped_first, "center" if stopped_first == "gap" else "gap"]

    async def scenario():
        await center.set(0.0)  # both axes have been used
        # Gap 4 from (-1, 1) heads for (-2, 2); stopped at (-2, 1), centered on -0.5
        _stall_next_move({slits.inboard: -2.0, slits.outboard: 1.0})
        moving = gap.set(4.0)
        await asyncio.sleep(0.01)
        for name in order:
            await axes[name].stop(success=True)
            if name == "gap" and not paused_before_blades_stopped:
                await moving
        for axis in axes.values():
            axis.pause()
        if paused_before_blades_stopped:
            await moving
        for axis in axes.values():
            axis.resume()
        await gap.set(4.0)  # the replay

    call_in_bluesky_event_loop(scenario())

    assert call_in_bluesky_event_loop(_blades(slits))[:2] == pytest.approx([-2.0, 2.0])


def test_replay_after_pause_with_both_axes_used(RE: RunEngine):
    slits = _re_slits((-1.0, 1.0, -1.0, 1.0))
    for motor in _motors(slits):
        set_mock_value(motor.deadband, 0.005)

    def plan():
        yield from bps.mv(slits.horizontal_center, 0.0)
        # Gap 4 from (-1, 1) heads for (-2, 2); stopped at (-2, 1), centered on -0.5
        _pause_partway_through_first_move(RE, {slits.inboard: -2.0, slits.outboard: 1.0})
        yield from bps.checkpoint()
        yield from bps.mv(slits.horizontal_gap, 4.0)

    with pytest.raises(RunEngineInterrupted):
        RE(plan())
    RE.resume()

    assert call_in_bluesky_event_loop(_blades(slits))[:2] == pytest.approx([-2.0, 2.0])


def test_move_stopped_without_a_pause_is_not_replayed(RE: RunEngine):
    slits = _re_slits((-1.0, 1.0, -1.0, 1.0))
    for motor in _motors(slits):
        set_mock_value(motor.deadband, 0.005)
    gap = slits.horizontal_gap

    async def scenario():
        _stall_next_move({slits.inboard: -2.0, slits.outboard: 1.0})
        moving = gap.set(4.0)
        await asyncio.sleep(0.01)
        await gap.stop(success=True)  # e.g. an error ending the run, no pause
        await moving
        await gap.set(4.0)

    call_in_bluesky_event_loop(scenario())

    # Computed afresh from where the blades are: about -0.5
    assert call_in_bluesky_event_loop(_blades(slits))[:2] == pytest.approx([-2.5, 1.5])
