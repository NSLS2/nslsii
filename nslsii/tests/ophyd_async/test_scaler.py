import pytest
import pytest_asyncio

from ophyd_async.core import (
    callback_on_mock_put,
    get_mock_put,
    init_devices,
    set_mock_value,
)
from ophyd_async.testing import (
    assert_configuration,
    assert_reading,
    assert_value,
    partial_reading,
)

from nslsii.ophyd_async.devices import (
    Scaler,
    ScalerCountMode,
)


@pytest_asyncio.fixture
async def scaler():
    async with init_devices(mock=True):
        scaler = Scaler(
            "XF:99ID-ES{Sclr:1}",
            num_channels=4,
            hinted_channels=(2,),
            num_calculations=2,
            hinted_calculations=(1,),
        )
    return scaler


@pytest.mark.asyncio
async def test_channel_readback_and_hints(scaler):
    set_mock_value(scaler.channels[2].value, 123.0)
    await assert_reading(
        scaler,
        {f"{scaler.name}-channels-2-value": partial_reading(123.0)},
        full_match=False,
    )
    assert set(scaler.hints["fields"]) == {
        f"{scaler.name}-channels-2-value",
        f"{scaler.name}-calculations-1-value",
    }


@pytest.mark.asyncio
async def test_channel_configuration(scaler):
    set_mock_value(scaler.channels[1].channel_name, "I0")
    set_mock_value(scaler.channels[1].preset, 0.0)
    set_mock_value(scaler.channels[1].gate, "Y")
    await assert_configuration(
        scaler,
        {
            f"{scaler.name}-channels-1-channel_name": partial_reading("I0"),
            f"{scaler.name}-channels-1-preset": partial_reading(0.0),
            f"{scaler.name}-channels-1-gate": partial_reading("Y"),
        },
        full_match=False,
    )


@pytest.mark.asyncio
async def test_stage_forces_one_shot_count_mode(scaler):
    await scaler.stage()
    assert get_mock_put(scaler.count_mode).call_args.args[0] == ScalerCountMode.ONE_SHOT


@pytest.mark.asyncio
async def test_trigger_waits_for_counting_to_finish(scaler):
    set_mock_value(scaler.preset_time, 0.0)
    # Start from a non-idle count value so the wait genuinely depends on the
    # callback-driven transition below, not an already-matching initial read.
    set_mock_value(scaler.count, 1)

    def _finish_counting(value):
        # Returning a value from a mock put callback overrides what gets
        # stored, simulating the record resetting .CNT to 0 once counting
        # completes.
        return 0

    callback_on_mock_put(scaler.count, _finish_counting)
    await scaler.trigger()
    assert get_mock_put(scaler.count).call_args.args[0] == 1
    await assert_value(scaler.count, 0)


@pytest.mark.asyncio
async def test_calculations_wired_when_requested(scaler):
    assert set(scaler.calculations.keys()) == {1, 2}
    set_mock_value(scaler.calculations[1].value, 1.5)
    await assert_reading(
        scaler,
        {f"{scaler.name}-calculations-1-value": partial_reading(1.5)},
        full_match=False,
    )
