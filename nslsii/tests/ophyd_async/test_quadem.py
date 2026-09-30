import asyncio
from unittest.mock import call

from bluesky import RunEngine
import bluesky.plans as bp
from bluesky_tiled_plugins import TiledWriter
import pytest
from ophyd_async.core import (
    DetectorTrigger,
    EnableDisable,
    TriggerInfo,
    callback_on_mock_put,
    get_mock_put,
    set_mock_put_proceeds,
    set_mock_value,
    wait_for_value,
)
from ophyd_async.epics.adcore import NDArrayBaseIO, NDPluginBaseIO
from tiled.client import from_uri
from tiled.server import SimpleTiledServer


from nslsii.ophyd_async.devices import (
    NDTimeSeriesIO,
    NDTimeSeriesNIO,
    QUADEM_TIME_SERIES_CHANNELS,
    QuadEM,
    QuadEMAcquireMode,
    QuadEMDriverIO,
    QuadEMStatsIO,
)


def _primary_stats(device: QuadEM) -> tuple[QuadEMStatsIO, ...]:
    return (
        *device.current.values(),
        device.sum_x,
        device.sum_y,
        device.sum_all,
        device.diff_x,
        device.diff_y,
        device.pos_x,
        device.pos_y,
    )


def _source_without_mock_prefix(signal) -> str:
    return signal.source.removeprefix("mock+")


async def _complete_trigger(device: QuadEM) -> None:
    acquisition_started = asyncio.Event()

    def record_acquire(value: bool) -> None:
        if value:
            acquisition_started.set()

    with callback_on_mock_put(device.driver.acquire, record_acquire):
        set_mock_put_proceeds(device.driver.acquire, False)
        try:
            status = device.trigger()
            await asyncio.wait_for(acquisition_started.wait(), timeout=1)
            await wait_for_value(device.driver.acquire, True, timeout=1)
            set_mock_value(device.driver.acquire, False)
            set_mock_put_proceeds(device.driver.acquire, True)
            await status
        finally:
            set_mock_put_proceeds(device.driver.acquire, True)


@pytest.mark.asyncio
async def test_quadem_topology_reuses_adcore_plugins() -> None:
    device = QuadEM("TEST:QEM:", name="qem")
    await device.connect(mock=True)

    assert isinstance(device.driver, QuadEMDriverIO)
    assert isinstance(device.current[1], QuadEMStatsIO)
    assert (
        _source_without_mock_prefix(device.current[1].enable_callbacks)
        == "ca://TEST:QEM:Current1:EnableCallbacks_RBV"
    )
    assert _source_without_mock_prefix(device.current[1].mean_value) == "ca://TEST:QEM:Current1:MeanValue_RBV"
    assert _source_without_mock_prefix(device.current[1].sigma) == "ca://TEST:QEM:Current1:Sigma_RBV"


@pytest.mark.asyncio
async def test_quadem_reads_scalar_statistics_from_standard_detector() -> None:
    device = QuadEM("TEST:QEM:", name="qem")
    await device.connect(mock=True)
    stats = _primary_stats(device)
    expected_values = {stats_plugin.mean_value.name: float(index) for index, stats_plugin in enumerate(stats)}
    for stats_plugin, value in zip(stats, expected_values.values(), strict=True):
        set_mock_value(stats_plugin.mean_value, value)
    set_mock_value(device.driver.fast_current_averages[1], -1.0)

    await device.stage()
    await _complete_trigger(device)

    readings = await device.read()
    assert {name: reading["value"] for name, reading in readings.items()} == expected_values
    assert device.hints["fields"] == [device.current[index].mean_value.name for index in range(1, 5)]
    assert await device.driver.fast_current_averages[1].get_value() == -1.0
    assert device.driver.fast_current_averages[1].name not in readings

    configuration = await device.read_configuration()
    assert {
        device.driver.acquire_mode.name,
        device.driver.integration_time.name,
        device.driver.averaging_time.name,
        device.driver.fast_averaging_time.name,
        device.driver.values_per_read.name,
        device.driver.num_acquire.name,
    } <= configuration.keys()


@pytest.mark.asyncio
async def test_quadem_preparation_and_lifecycle() -> None:
    device = QuadEM("TEST:QEM:", name="qem")
    await device.connect(mock=True)
    stats = _primary_stats(device)

    set_mock_value(device.driver.acquire, True)
    await device.stage()
    assert await device.driver.acquire.get_value() is False

    await device.prepare(TriggerInfo(livetime=0.02))
    assert await device.driver.averaging_time.get_value() == 0.02
    assert await device.driver.acquire_mode.get_value() == QuadEMAcquireMode.SINGLE
    assert await device.driver.num_acquire.get_value() == 1
    assert await device.driver.wait_for_plugins.get_value() is True
    for stats_plugin in stats:
        assert await stats_plugin.enable_callbacks.get_value() == EnableDisable.ENABLE
        assert get_mock_put(stats_plugin.enable_callbacks).await_args_list == [call(EnableDisable.ENABLE)]

    with pytest.raises(ValueError, match="nonzero deadtime"):
        await device.prepare(TriggerInfo(deadtime=0.01))
    with pytest.raises(ValueError, match="one exposure"):
        await device.prepare(TriggerInfo(number_of_events=2))
    for trigger in (DetectorTrigger.EXTERNAL_EDGE, DetectorTrigger.EXTERNAL_LEVEL):
        with pytest.raises(ValueError, match="not supported"):
            await device.prepare(TriggerInfo(trigger=trigger))

    await device.unstage()
    assert await device.driver.acquire.get_value() is False


@pytest.mark.asyncio
async def test_quadem_trigger_waits_for_acquire_to_clear() -> None:
    device = QuadEM("TEST:QEM:", name="qem")
    await device.connect(mock=True)
    await device.stage()

    acquisition_started = asyncio.Event()

    def record_acquire(value: bool) -> None:
        if value:
            acquisition_started.set()

    with callback_on_mock_put(device.driver.acquire, record_acquire):
        set_mock_put_proceeds(device.driver.acquire, False)
        try:
            status = device.trigger()
            await asyncio.wait_for(acquisition_started.wait(), timeout=1)
            await wait_for_value(device.driver.acquire, True, timeout=1)
            assert not status.done

            set_mock_value(device.driver.acquire, False)
            set_mock_put_proceeds(device.driver.acquire, True)
            await status
            assert status.success
        finally:
            set_mock_put_proceeds(device.driver.acquire, True)


@pytest.mark.asyncio
async def test_quadem_attaches_time_series_plugin() -> None:
    device = QuadEM("TEST:QEM:", name="qem")
    time_series = NDTimeSeriesIO("TEST:QEM:TS:", channels=QUADEM_TIME_SERIES_CHANNELS)
    with_time_series = QuadEM("TEST:QEM:", plugins={"time_series": time_series}, name="qem_ts")
    await device.connect(mock=True)
    await with_time_series.connect(mock=True)

    assert not hasattr(device, "time_series")
    assert with_time_series.time_series is time_series
    assert isinstance(time_series, NDPluginBaseIO)
    assert isinstance(time_series, NDArrayBaseIO)
    assert set(time_series.channels) == set(QUADEM_TIME_SERIES_CHANNELS)
    assert isinstance(time_series.channels["current_1"], NDTimeSeriesNIO)
    assert _source_without_mock_prefix(time_series.enable_callbacks) == "ca://TEST:QEM:TS:EnableCallbacks_RBV"
    assert (
        _source_without_mock_prefix(time_series.channels["current_1"].time_series)
        == "ca://TEST:QEM:TS:Current1:TimeSeries"
    )
    assert (
        _source_without_mock_prefix(time_series.channels["sum_all"].time_series)
        == "ca://TEST:QEM:TS:SumAll:TimeSeries"
    )


@pytest.mark.asyncio
async def test_quadem_calibration_controls_are_opt_in() -> None:
    device = QuadEM("TEST:QEM:", name="qem")
    with_calibration = QuadEM("TEST:QEM:", with_calibration_controls=True, name="qem_calibration")
    await device.connect(mock=True)
    await with_calibration.connect(mock=True)

    assert not hasattr(device.driver, "calibration_mode")
    assert not hasattr(device.driver, "adc_offsets")
    assert not hasattr(device.driver, "copy_adc_offsets")
    assert (
        _source_without_mock_prefix(with_calibration.driver.calibration_mode)
        == "ca://TEST:QEM:CalibrationMode_RBV"
    )
    assert _source_without_mock_prefix(with_calibration.driver.adc_offsets[4]) == "ca://TEST:QEM:ADCOffset4"
    assert hasattr(with_calibration.driver, "copy_adc_offsets")


def test_quadem_writes_scalar_statistics_to_tiled() -> None:
    device = QuadEM("TEST:QEM:", name="qem")
    asyncio.run(device.connect(mock=True))
    stats = _primary_stats(device)
    for expected, stats_plugin in enumerate(stats, start=1):
        set_mock_value(stats_plugin.mean_value, float(expected))

    def clear_after_start(value: bool) -> None:
        if value:
            asyncio.get_running_loop().call_soon(set_mock_value, device.driver.acquire, False)

    with SimpleTiledServer() as server:
        client = from_uri(server.uri)
        writer = TiledWriter(client)
        run_engine = RunEngine()
        with callback_on_mock_put(device.driver.acquire, clear_after_start):
            run_engine(bp.count([device]), writer)

        primary = client.values().last()["primary"]
        for expected, stats_plugin in enumerate(stats, start=1):
            assert primary[stats_plugin.mean_value.name].read().item() == float(expected)
