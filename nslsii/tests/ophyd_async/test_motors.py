import bluesky.plan_stubs as bps
import pytest
from bluesky.run_engine import call_in_bluesky_event_loop
from ophyd_async.core import init_devices
from ophyd_async.epics.motor import Motor

from nslsii.ophyd_async.devices import MotorizedStage, XYStage, XYZStage, XZStage, YZStage

PREFIX = "XF:31ID1-ES{Stg:1-Ax:"


@pytest.mark.parametrize(
    "stage_cls, axes",
    [(XYStage, ("x", "y")), (XZStage, ("x", "z")), (YZStage, ("y", "z")), (XYZStage, ("x", "y", "z"))],
)
def test_stage_axes_and_pv_names(stage_cls, axes):
    stage = stage_cls(PREFIX, name="stage")
    assert tuple(stage.axes) == axes
    for axis, motor in stage.axes.items():
        assert motor.user_readback.source == f"ca://{PREFIX}{axis.upper()}}}Mtr.RBV"
        assert motor.name == f"stage-{axis}"


@pytest.mark.parametrize("prefix", ["XF:31ID1-ES{Stg:1", "XF:31ID1-ES{Stg:1-Ax:"])
def test_prefix_with_or_without_ax(prefix):
    stage = XYStage(prefix)
    assert stage.x.user_setpoint.source == "ca://XF:31ID1-ES{Stg:1-Ax:X}Mtr.VAL"


def test_append_ax_disabled():
    stage = XYStage("XF:31ID1-ES{Stg:1-", append_ax=False)
    assert stage.x.user_setpoint.source == "ca://XF:31ID1-ES{Stg:1-X}Mtr.VAL"


def test_axis_suffix_overrides():
    stage = XYStage("XF:TEST{Stg:2-Ax:", axis_suffixes={"x": "X1"}, motor_suffix="}Mtr2")
    assert stage.x.user_setpoint.source == "ca://XF:TEST{Stg:2-Ax:X1}Mtr2.VAL"
    assert stage.y.user_setpoint.source == "ca://XF:TEST{Stg:2-Ax:Y}Mtr2.VAL"
    with pytest.raises(ValueError, match="Unknown axes"):
        XYStage(PREFIX, axis_suffixes={"z": "Z"})


def test_axes_inferred_from_motor_annotations():
    class SampleStage(MotorizedStage):
        x: Motor
        theta: Motor
        label: str

    stage = SampleStage(PREFIX, axis_suffixes={"theta": "Th"})
    assert tuple(stage.axes) == ("x", "theta")
    assert stage.x.user_setpoint.source == f"ca://{PREFIX}X}}Mtr.VAL"
    assert stage.theta.user_setpoint.source == f"ca://{PREFIX}Th}}Mtr.VAL"


@pytest.mark.asyncio
async def test_set_moves_axes():
    async with init_devices(mock=True):
        stage = XYZStage(PREFIX)

    await stage.set((1.0, 2.0, 3.0))
    assert [await m.user_readback.get_value() for m in stage.axes.values()] == [1.0, 2.0, 3.0]

    await stage.set((None, 5.0, None))
    await stage.set({"z": -1.0})
    assert [await m.user_readback.get_value() for m in stage.axes.values()] == [1.0, 5.0, -1.0]

    with pytest.raises(ValueError, match="expects 3 positions"):
        await stage.set((1.0, 2.0))
    with pytest.raises(ValueError, match="Unknown axes"):
        await stage.set({"q": 1.0})


def test_stage_in_plans(RE):
    async def connect():
        async with init_devices(mock=True):
            stage = XYStage(PREFIX)
        return stage

    stage = call_in_bluesky_event_loop(connect())
    docs = []
    RE(bps.mv(stage, (1.5, -2.5)))
    RE(bps.mv(stage.x, 4.0))

    def read_plan():
        yield from bps.open_run()
        yield from bps.trigger_and_read([stage])
        yield from bps.close_run()

    RE(read_plan(), lambda name, doc: docs.append((name, doc)))
    event = next(doc for name, doc in docs if name == "event")
    assert event["data"]["stage-x"] == 4.0
    assert event["data"]["stage-y"] == -2.5
