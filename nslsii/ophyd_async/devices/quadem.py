"""Ophyd-async components for current standard quadEM IOCs.

``QuadEM`` is an internal-step detector that reads scalar statistics from the
normal ADCore plugins loaded by the quadEM IOC.
"""

import asyncio
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Annotated as A

import numpy as np

from ophyd_async.core import (
    AsyncStatus,
    DetectorAcquireLogic,
    DetectorDataLogic,
    DetectorTriggerLogic,
    DeviceVector,
    EnableDisable,
    ReadableDataProvider,
    SignalR,
    SignalRW,
    StandardDetector,
    StrictEnum,
    TriggerInfo,
    merge_gathered_dicts,
    non_zero,
    set_and_wait_for_value,
)
from ophyd_async.epics.adcore import NDArrayBaseIO, NDPluginBaseIO, NDStatsIO
from ophyd_async.epics.core import (
    EpicsOptions,
    PvSuffix,
    epics_signal_r,
    epics_signal_rw,
    epics_triggerable_command,
    stop_busy_record,
)

__all__ = [
    "QuadEMAcquireMode",
    "QuadEMGeometry",
    "QuadEMDriverIO",
    "QuadEMStatsIO",
    "QUADEM_TIME_SERIES_CHANNELS",
    "QuadEMInternalTriggerLogic",
    "QuadEMAcquireLogic",
    "QuadEMStatisticsDataLogic",
    "QuadEM",
]

QUADEM_TIME_SERIES_CHANNELS: Mapping[str, str] = {
    "current_1": "Current1:",
    "current_2": "Current2:",
    "current_3": "Current3:",
    "current_4": "Current4:",
    "sum_x": "SumX:",
    "sum_y": "SumY:",
    "sum_all": "SumAll:",
    "diff_x": "DiffX:",
    "diff_y": "DiffY:",
    "pos_x": "PosX:",
    "pos_y": "PosY:",
}


class QuadEMAcquireMode(StrictEnum):
    """Stable acquisition mode values exposed by quadEM controllers.

    Attributes
    ----------
    CONTINUOUS : str
        Acquire continuously.
    MULTIPLE : str
        Acquire a configured number of samples.
    SINGLE : str
        Acquire one sample.
    """

    CONTINUOUS = "Continuous"
    MULTIPLE = "Multiple"
    SINGLE = "Single"


class QuadEMGeometry(StrictEnum):
    """Stable position-geometry values exposed by quadEM controllers.

    Attributes
    ----------
    DIAMOND : str
        Diamond geometry.
    SQUARE : str
        Square geometry.
    SQUARE_CC : str
        Corner-corrected square geometry.
    CUSTOM : str
        User-defined geometry.
    """

    DIAMOND = "Diamond"
    SQUARE = "Square"
    SQUARE_CC = "SquareCC"
    CUSTOM = "Custom"


class QuadEMDriverIO(NDArrayBaseIO):
    """EPICS IO map for a standard quadEM driver.

    Parameters
    ----------
    prefix : str
        PV prefix of the quadEM controller.
    name : str, optional
        Ophyd device name.
    with_calibration_controls : bool, optional
        Attach ``CalibrationMode``, ``ADCOffset1`` through ``ADCOffset4``,
        and ``CopyADCOffsets.PROC``. Enable this only for IOCs that expose
        those model-specific records.

    Notes
    -----
    The map inherits the NDArray driver records from ``NDArrayBaseIO``. The
    generic acquisition lifecycle never changes calibration, bias, offset, or
    scale controls.
    """

    acquire: A[SignalRW[bool], PvSuffix("Acquire"), EpicsOptions(wait=non_zero)]
    wait_for_plugins: A[SignalRW[bool], PvSuffix("WaitForPlugins")]
    acquire_mode: A[SignalRW[QuadEMAcquireMode], PvSuffix.rbv("AcquireMode")]
    integration_time: A[SignalRW[float], PvSuffix.rbv("IntegrationTime")]
    averaging_time: A[SignalRW[float], PvSuffix.rbv("AveragingTime")]
    fast_averaging_time: A[SignalRW[float], PvSuffix.rbv("FastAveragingTime")]
    values_per_read: A[SignalRW[int], PvSuffix.rbv("ValuesPerRead")]
    num_acquire: A[SignalRW[int], PvSuffix.rbv("NumAcquire")]
    num_acquired: A[SignalR[int], PvSuffix("NumAcquired")]
    num_average: A[SignalR[int], PvSuffix("NumAverage_RBV")]
    num_averaged: A[SignalR[int], PvSuffix("NumAveraged_RBV")]
    num_fast_average: A[SignalR[int], PvSuffix("NumFastAverage")]
    sample_time: A[SignalR[float], PvSuffix("SampleTime_RBV")]
    ring_overflows: A[SignalR[int], PvSuffix("RingOverflows")]
    geometry: A[SignalRW[QuadEMGeometry], PvSuffix.rbv("Geometry")]

    # The menu choices vary by controller model.
    range: A[SignalRW[str], PvSuffix.rbv("Range")]
    read_format: A[SignalRW[str], PvSuffix.rbv("ReadFormat")]
    trigger_mode: A[SignalRW[str], PvSuffix.rbv("TriggerMode")]
    num_channels: A[SignalRW[str], PvSuffix.rbv("NumChannels")]
    model: A[SignalR[int], PvSuffix("Model")]
    firmware: A[SignalR[np.ndarray], PvSuffix("Firmware")]

    # These model-specific controls are never set by the generic acquisition
    # lifecycle.
    bias_state: A[SignalRW[bool], PvSuffix.rbv("BiasState")]
    bias_voltage: A[SignalRW[float], PvSuffix.rbv("BiasVoltage")]
    bias_interlock: A[SignalRW[bool], PvSuffix.rbv("BiasInterlock")]
    hvs_readback: A[SignalR[bool], PvSuffix("HVSReadback")]
    hvv_readback: A[SignalR[float], PvSuffix("HVVReadback")]
    hvi_readback: A[SignalR[float], PvSuffix("HVIReadback")]
    position_offset_x: A[SignalRW[float], PvSuffix("PositionOffsetX")]
    position_offset_y: A[SignalRW[float], PvSuffix("PositionOffsetY")]
    position_scale_x: A[SignalRW[float], PvSuffix("PositionScaleX")]
    position_scale_y: A[SignalRW[float], PvSuffix("PositionScaleY")]

    def __init__(self, prefix: str, name: str = "", *, with_calibration_controls: bool = False) -> None:
        self.current_names = DeviceVector({i: epics_signal_r(str, f"{prefix}CurrentName{i}") for i in range(1, 5)})
        self.current_offsets = DeviceVector(
            {i: epics_signal_rw(float, f"{prefix}CurrentOffset{i}") for i in range(1, 5)}
        )
        self.compute_current_offsets = DeviceVector(
            {i: epics_triggerable_command(f"{prefix}ComputeCurrentOffset{i}.PROC") for i in range(1, 5)}
        )
        self.current_scales = DeviceVector(
            {i: epics_signal_rw(float, f"{prefix}CurrentScale{i}") for i in range(1, 5)}
        )
        self.fast_current_averages = DeviceVector(
            {i: epics_signal_r(float, f"{prefix}Current{i}Ave") for i in range(1, 5)}
        )
        self.fast_sum_x_average = epics_signal_r(float, f"{prefix}SumXAve")
        self.fast_sum_y_average = epics_signal_r(float, f"{prefix}SumYAve")
        self.fast_sum_all_average = epics_signal_r(float, f"{prefix}SumAllAve")
        self.fast_diff_x_average = epics_signal_r(float, f"{prefix}DiffXAve")
        self.fast_diff_y_average = epics_signal_r(float, f"{prefix}DiffYAve")
        self.fast_position_x_average = epics_signal_r(float, f"{prefix}PositionXAve")
        self.fast_position_y_average = epics_signal_r(float, f"{prefix}PositionYAve")
        if with_calibration_controls:
            self.calibration_mode = epics_signal_rw(
                bool, f"{prefix}CalibrationMode_RBV", f"{prefix}CalibrationMode"
            )
            self.adc_offsets = DeviceVector(
                {i: epics_signal_rw(int, f"{prefix}ADCOffset{i}") for i in range(1, 5)}
            )
            self.copy_adc_offsets = epics_triggerable_command(f"{prefix}CopyADCOffsets.PROC")
        self.compute_position_offset_x = epics_triggerable_command(f"{prefix}ComputePosOffsetX.PROC")
        self.compute_position_offset_y = epics_triggerable_command(f"{prefix}ComputePosOffsetY.PROC")
        super().__init__(prefix=prefix, name=name)


class QuadEMStatsIO(NDStatsIO):
    """EPICS IO map for one standard quadEM ``NDStats`` plugin.

    Parameters
    ----------
    prefix : str
        PV prefix of the statistics plugin, such as ``"PREFIX:Current1:"``.
    name : str, optional
        Ophyd device name.
    """

    mean_value: A[SignalR[float], PvSuffix("MeanValue_RBV")]
    sigma: A[SignalR[float], PvSuffix("Sigma_RBV")]
    min_value: A[SignalR[float], PvSuffix("MinValue_RBV")]
    max_value: A[SignalR[float], PvSuffix("MaxValue_RBV")]
    net: A[SignalR[float], PvSuffix("Net_RBV")]


@dataclass
class QuadEMInternalTriggerLogic(DetectorTriggerLogic):
    """Configure a single internally triggered quadEM acquisition.

    Parameters
    ----------
    driver : QuadEMDriverIO
        Controller signals to configure.
    stats : Sequence[QuadEMStatsIO]
        Statistics plugins that must receive callbacks for each acquisition.

    Notes
    -----
    Internal preparation accepts one exposure and zero deadtime only. It sets
    single-acquisition mode, waits for plugins, and enables the supplied
    statistics callbacks.
    """

    driver: QuadEMDriverIO
    stats: Sequence[QuadEMStatsIO]

    def config_sigs(self) -> set[SignalR]:
        return {
            self.driver.acquire_mode,
            self.driver.integration_time,
            self.driver.averaging_time,
            self.driver.fast_averaging_time,
            self.driver.values_per_read,
            self.driver.num_acquire,
            self.driver.wait_for_plugins,
        }

    async def prepare_internal(self, num: int, livetime: float, deadtime: float) -> None:
        if num != 1:
            raise ValueError("QuadEM only supports one exposure per trigger")
        if deadtime != 0:
            raise ValueError("QuadEM does not support a nonzero deadtime")

        operations = [
            self.driver.acquire_mode.set(QuadEMAcquireMode.SINGLE),
            self.driver.num_acquire.set(1),
            self.driver.wait_for_plugins.set(True),
            *(stats.enable_callbacks.set(EnableDisable.ENABLE) for stats in self.stats),
        ]
        if livetime > 0:
            operations.append(self.driver.averaging_time.set(livetime))
        await asyncio.gather(*operations)

    async def default_trigger_info(self) -> TriggerInfo:
        return TriggerInfo()


class QuadEMAcquireLogic(DetectorAcquireLogic):
    """Start a quadEM acquisition and wait for its busy record to clear.

    Parameters
    ----------
    driver : QuadEMDriverIO
        Controller whose ``Acquire`` record represents acquisition state.

    Notes
    -----
    Both ``stage`` and ``unstage`` stop the IOC. A previous continuous
    acquisition is not restored automatically.
    """

    def __init__(self, driver: QuadEMDriverIO) -> None:
        self.driver = driver
        self.acquire_status: AsyncStatus | None = None

    async def start_acquiring(self) -> None:
        self.acquire_status = await set_and_wait_for_value(
            self.driver.acquire,
            True,
            wait_for_set_completion=False,
        )

    async def wait_for_idle(self) -> None:
        if self.acquire_status is not None:
            await self.acquire_status

    async def ensure_ready(self) -> None:
        await stop_busy_record(self.driver.acquire)

    async def ensure_stopped(self) -> None:
        await stop_busy_record(self.driver.acquire)


@dataclass
class _QuadEMSignalsDataProvider(ReadableDataProvider):
    """Read all selected scalar statistics concurrently."""

    signals: Sequence[SignalR]

    async def make_datakeys(self):
        return await merge_gathered_dicts(signal.describe() for signal in self.signals)

    async def make_readings(self):
        return await merge_gathered_dicts(signal.read(cached=False) for signal in self.signals)


@dataclass
class QuadEMStatisticsDataLogic(DetectorDataLogic):
    """Expose selected quadEM statistics as one scalar event.

    Parameters
    ----------
    driver : QuadEMDriverIO
        Controller used to wait for statistics plugins.
    signals : Sequence[SignalR]
        Primary statistic signals included in each event.
    hinted_signals : Sequence[SignalR]
        Subset of ``signals`` advertised as Bluesky hinted fields.
    """

    driver: QuadEMDriverIO
    signals: Sequence[SignalR]
    hinted_signals: Sequence[SignalR]

    async def prepare_single(self, datakey_name: str) -> ReadableDataProvider:
        await self.driver.wait_for_plugins.set(True)
        return _QuadEMSignalsDataProvider(self.signals)

    def get_hinted_fields(self, datakey_name: str) -> Sequence[str]:
        return [signal.name for signal in self.hinted_signals]


class QuadEM(StandardDetector):
    """Internal-step QuadEM detector with configurable event signals.

    Parameters
    ----------
    prefix : str
        PV prefix of the quadEM controller and its standard statistics plugins.
    plugins : Mapping[str, NDPluginBaseIO], optional
        Traditional ADCore plugins keyed by the child attribute name to expose
        on this detector. Construct each plugin with its own PV prefix. Plugins
        are connected but are not automatically scan data or flyer providers.
        For the standard quadEM time-series layout, use ``NDTimeSeriesIO``
        with ``QUADEM_TIME_SERIES_CHANNELS``.
    with_calibration_controls : bool, optional
        Attach model-specific calibration and ADC-offset controls to
        ``driver``. Leave disabled for ordinary IOCs that lack those records.
    name : str, optional
        Ophyd device name.

    Notes
    -----
    The IOC must provide ``WaitForPlugins`` and eleven standard ``NDStats``
    plugins: four currents, sum X/Y/all, difference X/Y, and position X/Y.
    This is an internal single-acquisition step detector, not a flyer. It
    always emits all eleven means and hints the four current means.
    """

    def __init__(
        self,
        prefix: str,
        *,
        plugins: Mapping[str, NDPluginBaseIO] | None = None,
        with_calibration_controls: bool = False,
        name: str = "",
    ) -> None:
        self.driver = QuadEMDriverIO(prefix, with_calibration_controls=with_calibration_controls)
        self.current = DeviceVector({i: QuadEMStatsIO(f"{prefix}Current{i}:") for i in range(1, 5)})
        self.sum_x = QuadEMStatsIO(f"{prefix}SumX:")
        self.sum_y = QuadEMStatsIO(f"{prefix}SumY:")
        self.sum_all = QuadEMStatsIO(f"{prefix}SumAll:")
        self.diff_x = QuadEMStatsIO(f"{prefix}DiffX:")
        self.diff_y = QuadEMStatsIO(f"{prefix}DiffY:")
        self.pos_x = QuadEMStatsIO(f"{prefix}PosX:")
        self.pos_y = QuadEMStatsIO(f"{prefix}PosY:")
        if plugins is not None:
            for plugin_name, plugin in plugins.items():
                setattr(self, plugin_name, plugin)

        stats = (
            *self.current.values(),
            self.sum_x,
            self.sum_y,
            self.sum_all,
            self.diff_x,
            self.diff_y,
            self.pos_x,
            self.pos_y,
        )
        primary_signals = tuple(stats_plugin.mean_value for stats_plugin in stats)
        hinted_signals = tuple(self.current[index].mean_value for index in self.current)
        self._statistics_data_logic = QuadEMStatisticsDataLogic(self.driver, primary_signals, hinted_signals)
        self.add_detector_logics(
            QuadEMInternalTriggerLogic(self.driver, stats),
            QuadEMAcquireLogic(self.driver),
            self._statistics_data_logic,
        )
        super().__init__(name=name)
