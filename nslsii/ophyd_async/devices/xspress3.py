"""Ophyd-async support for the community Xspress3 IOC."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass

from collections.abc import Awaitable, Mapping, Sequence
from typing import Annotated as A, cast

import numpy as np

from ophyd_async.core import (
    Array1D,
    AsyncStatus,
    DetectorDataLogic,
    DetectorTriggerLogic,
    DeviceVector,
    PathProvider,
    SignalDict,
    SignalR,
    SignalRW,
    StreamResourceDataProvider,
    StreamResourceInfo,
    StrictEnum,
    SupersetEnum,
    TriggerableCommand,
    TriggerInfo,
    soft_signal_rw,
)
from ophyd_async.epics.adcore import (
    ADAcquireLogic,
    ADBaseColorMode,
    ADBaseDataType,
    ADBaseIO,
    ADHDFDataLogic,
    ADWriterFactory,
    AreaDetector,
    NDArrayDescription,
    NDFileHDF5IO,
    NDPluginBaseIO,
    trigger_info_from_num_images,
)
from ophyd_async.epics.core import (
    EpicsDevice,
    PvSuffix,
    epics_signal_r,
    epics_triggerable_command,
)


async def _gather_and_raise(*awaitables: Awaitable[object]) -> None:
    results = await asyncio.gather(*awaitables, return_exceptions=True)
    for result in results:
        if isinstance(result, BaseException):
            raise result


class Xspress3TriggerMode(SupersetEnum):
    """Trigger modes exposed by the canonical Xspress3 IOC."""

    SOFTWARE = "Software"
    INTERNAL = "Internal"
    IDC = "IDC"
    TTL_VETO_ONLY = "TTL Veto Only"
    TTL_BOTH = "TTL Both"
    LVDS_VETO_ONLY = "LVDS Veto Only"
    LVDS_BOTH = "LVDS Both"
    SOFTWARE_INTERNAL = "Software + Internal"
    TTL_INTERNAL = "TTL + Internal"


class Xspress3RunFlags(StrictEnum):
    """Data sources selected by the Xspress3 run-flags records."""

    SCALERS_AND_HISTOGRAMS = "SCALERS & HIST"
    PLAYBACK_SCALERS_AND_HISTOGRAMS = "PLAYB, SCALERS & HIST"


class Xspress3LevelTriggerMode(StrictEnum):
    """IOC modes valid for externally level-triggered acquisition."""

    TTL_VETO_ONLY = "TTL Veto Only"
    TTL_BOTH = "TTL Both"
    LVDS_VETO_ONLY = "LVDS Veto Only"
    LVDS_BOTH = "LVDS Both"


class Xspress3RoiTimeSeriesControl(StrictEnum):
    """Commands exposed by the ROIStat time-series control record."""

    ERASE_AND_START = "Erase/Start"
    START = "Start"
    STOP = "Stop"
    READ = "Read"
    ERASE = "Erase"


@dataclass
class Xspress3TriggerLogic(DetectorTriggerLogic):
    """Configure finite Xspress3 acquisitions.

    Parameters
    ----------
    driver : Xspress3DriverIO
        Xspress3 driver signals.
    level_trigger_mode : SignalR[Xspress3LevelTriggerMode]
        Runtime selection for externally level-triggered acquisitions.
    minimum_deadtime : float, optional
        Minimum supported deadtime in seconds.
    """

    driver: Xspress3DriverIO
    level_trigger_mode: SignalR[Xspress3LevelTriggerMode]
    # Unknown what the true minimum deadtime is
    minimum_deadtime: float = 0.0

    def get_deadtime(self, config_values: SignalDict) -> float:
        """Return the configured minimum deadtime.

        Parameters
        ----------
        config_values : SignalDict
            Unused configuration values.

        Returns
        -------
        float
            Minimum deadtime in seconds.
        """
        return self.minimum_deadtime

    @staticmethod
    def _require_finite(num: int) -> None:
        if num == 0:
            raise ValueError("Xspress3 does not support unbounded acquisition")

    async def _prepare(self, mode: Xspress3TriggerMode, num: int, livetime: float = 0.0) -> None:
        self._require_finite(num)
        coros = [
            self.driver.trigger_mode.set(mode),
            self.driver.num_images.set(num),
        ]
        if livetime:
            coros.append(self.driver.acquire_time.set(livetime))
        await asyncio.gather(*coros)

    async def prepare_internal(self, num: int, livetime: float, deadtime: float):
        """Configure a finite internally triggered acquisition.

        Parameters
        ----------
        num : int
            Number of frames.
        livetime : float
            Exposure time in seconds, or zero to preserve the current value.
        deadtime : float
            Requested deadtime; nonzero values are unsupported since the ``AcquirePeriod``
            record is disabled in the IOC.
        """
        if deadtime:
            raise ValueError("Xspress3 internal triggering does not support deadtime")
        await self._prepare(Xspress3TriggerMode.INTERNAL, num, livetime)

    async def prepare_edge(self, num: int, livetime: float):
        """Configure finite, externally edge-triggered frames.

        Parameters
        ----------
        num : int
            Number of frames.
        livetime : float
            Exposure time in seconds, or zero to preserve the current value.
        """
        await self._prepare(Xspress3TriggerMode.TTL_INTERNAL, num, livetime)

    async def prepare_level(self, num: int):
        """Configure finite, externally level-triggered frames.

        Parameters
        ----------
        num : int
            Number of frames.
        """
        mode = Xspress3TriggerMode((await self.level_trigger_mode.get_value()).value)
        await self._prepare(mode, num)

    async def default_trigger_info(self) -> TriggerInfo:
        """Return internal trigger information preserving ``NumImages``."""
        return await trigger_info_from_num_images(self.driver)


class Xspress3AcquireLogic(ADAcquireLogic):
    """Erase before acquisition without forwarding the IOC's blank frame."""

    driver: Xspress3DriverIO

    async def start_acquiring(self) -> None:
        """Erase with callbacks disabled, then begin acquisition."""
        # Older IOCs forward a blank NDArray when they erase. Suppress that
        # callback so one requested exposure produces exactly one HDF frame.
        array_callbacks = await self.driver.array_callbacks.get_value()
        await self.driver.erase_on_start.set(False)
        if array_callbacks:
            await self.driver.array_callbacks.set(False)
        try:
            await self.driver.erase.trigger()
        finally:
            if array_callbacks:
                await self.driver.array_callbacks.set(True)
        await super().start_acquiring()


class Xspress3DriverIO(ADBaseIO):
    """Signals exposed by the community Xspress3 driver database."""

    def __init__(
        self,
        prefix: str,
        *,
        channel_numbers: Sequence[int] = (),
        mca_roi_numbers: Sequence[int] = (),
        name: str = "",
    ) -> None:
        self.channel_numbers = _validate_numbers(channel_numbers, "channel", 1, 24)
        self.mca_roi_numbers = _validate_numbers(mca_roi_numbers, "ROI", 1, 48)
        super().__init__(prefix, name=name)

    trigger_mode: A[SignalRW[Xspress3TriggerMode], PvSuffix.rbv("TriggerMode")]
    erase: A[TriggerableCommand, PvSuffix("ERASE")]
    reset: A[TriggerableCommand, PvSuffix("RESET")]
    erase_on_start: A[SignalRW[bool], PvSuffix("EraseOnStart")]
    soft_trigger: A[SignalRW[bool], PvSuffix.rbv("SoftTrigger")]
    array_callbacks: A[SignalRW[bool], PvSuffix.rbv("ArrayCallbacks")]
    frame_count: A[SignalR[int], PvSuffix("FRAME_COUNT_RBV")]
    num_channels: A[SignalRW[int], PvSuffix.rbv("NUM_CHANNELS")]
    max_num_channels: A[SignalR[int], PvSuffix("MAX_NUM_CHANNELS_RBV")]
    num_frames_config: A[SignalRW[int], PvSuffix.rbv("NUM_FRAMES_CONFIG")]
    max_frames: A[SignalR[int], PvSuffix("MAX_FRAMES_RBV")]
    max_frames_driver: A[SignalR[int], PvSuffix("MAX_FRAMES_DRIVER_RBV")]
    max_spectra: A[SignalRW[int], PvSuffix.rbv("MAX_SPECTRA")]
    invert_f0: A[SignalRW[int], PvSuffix.rbv("INVERT_F0")]
    invert_veto: A[SignalRW[int], PvSuffix.rbv("INVERT_VETO")]
    debounce: A[SignalRW[int], PvSuffix.rbv("DEBOUNCE")]
    ctrl_dtc: A[SignalRW[bool], PvSuffix.rbv("CTRL_DTC")]
    run_flags: A[SignalRW[Xspress3RunFlags], PvSuffix.rbv("RUN_FLAGS")]
    config_path: A[SignalRW[str], PvSuffix.rbv("CONFIG_PATH")]
    config_save_path: A[SignalRW[str], PvSuffix.rbv("CONFIG_SAVE_PATH")]


class Xspress3HDFIO(NDFileHDF5IO):
    """HDF plugin signals including the Xspress3 capture calculation switch."""

    num_capture_calc_disable: A[SignalRW[int], PvSuffix("NumCapture_CALC.DISA")]


class Xspress3Sca(EpicsDevice):
    """Eleven scaler and deadtime values for one detector channel."""

    clock_ticks: A[SignalR[float], PvSuffix("0:Value_RBV")]
    reset_ticks: A[SignalR[float], PvSuffix("1:Value_RBV")]
    reset_counts: A[SignalR[float], PvSuffix("2:Value_RBV")]
    all_event: A[SignalR[float], PvSuffix("3:Value_RBV")]
    all_good: A[SignalR[float], PvSuffix("4:Value_RBV")]
    window_1: A[SignalR[float], PvSuffix("5:Value_RBV")]
    window_2: A[SignalR[float], PvSuffix("6:Value_RBV")]
    pileup: A[SignalR[float], PvSuffix("7:Value_RBV")]
    event_width: A[SignalR[float], PvSuffix("8:Value_RBV")]
    dt_factor: A[SignalR[float], PvSuffix("9:Value_RBV")]
    dt_percent: A[SignalR[float], PvSuffix("10:Value_RBV")]


class _Xspress3RoiTimeSeriesIO(EpicsDevice):
    ts_acquiring: A[SignalRW[bool], PvSuffix("TSAcquiring")]
    ts_read: A[SignalRW[int], PvSuffix("TSRead")]
    ts_num_points: A[SignalRW[int], PvSuffix("TSNumPoints")]
    ts_current_point: A[SignalR[int], PvSuffix("TSCurrentPoint")]
    ts_control: A[SignalRW[Xspress3RoiTimeSeriesControl], PvSuffix("TSControl")]
    ts_scan_rate: A[SignalRW[str], PvSuffix("TSRead.SCAN")]


class Xspress3McaRoi(EpicsDevice):
    """Configure and read one MCA region of interest.

    Parameters
    ----------
    prefix : str
        EPICS prefix ending in the ROI number.
    reset_prefix : str, optional
        EPICS command PV used to reset this ROI.
    name : str, optional
        Ophyd device name.
    """

    label: A[SignalRW[str], PvSuffix("Name")]
    min_x: A[SignalRW[int], PvSuffix.rbv("MinX")]
    size_x: A[SignalRW[int], PvSuffix.rbv("SizeX")]
    total: A[SignalR[float], PvSuffix("Total_RBV")]
    enabled: A[SignalRW[bool], PvSuffix.rbv("Use")]
    time_series_total: A[SignalR[Array1D[np.float64]], PvSuffix("TSTotal")]

    def __init__(
        self,
        prefix: str,
        *,
        reset_prefix: str | None = None,
        name: str = "",
    ) -> None:
        super().__init__(prefix, name=name)
        if reset_prefix is not None:
            self.reset = epics_triggerable_command(reset_prefix)

    @AsyncStatus.wrap
    async def set_roi_bins(self, low_bin: int, high_bin: int, *, enabled: bool = True) -> None:
        """Set a half-open interval of MCA bins.

        Parameters
        ----------
        low_bin : int
            First included bin.
        high_bin : int
            First excluded bin.
        enabled : bool, optional
            Whether to enable the ROI after updating its bounds.

        Raises
        ------
        TypeError
            If either bound is not an integer.
        ValueError
            If bounds are negative, empty, or reversed.
        """
        if type(low_bin) is not int or type(high_bin) is not int:
            raise TypeError("ROI bin bounds must be integers")
        if low_bin < 0 or high_bin < 0:
            raise ValueError("ROI bin bounds must be non-negative")
        if high_bin <= low_bin:
            raise ValueError("high_bin must be greater than low_bin")

        await self.enabled.set(False)
        await self.min_x.set(low_bin)
        await self.size_x.set(high_bin - low_bin)
        await self.enabled.set(enabled)


class Xspress3Channel(EpicsDevice):
    """Spectrum, scalers, and ROIs for one detector channel.

    Parameters
    ----------
    prefix : str
        Root EPICS prefix for the detector.
    channel_number : int
        One-based channel number in the range 1 through 24.
    roi_numbers : sequence of int
        ROI numbers in the range 1 through 48.
    include_roi_reset : bool, optional
        Create reset commands for ROI numbers 1 through 48.
    name : str, optional
        Ophyd device name.
    """

    def __init__(
        self,
        prefix: str,
        channel_number: int,
        roi_numbers: Sequence[int],
        *,
        include_roi_reset: bool = False,
        name: str = "",
    ) -> None:
        _validate_number(channel_number, "channel", 1, 24)
        roi_numbers = _validate_numbers(roi_numbers, "ROI", 1, 48)
        self.channel_number = channel_number
        self.spectrum = epics_signal_r(Array1D[np.float64], f"{prefix}MCA{channel_number}:ArrayData")
        self.spectrum_sum = epics_signal_r(Array1D[np.float64], f"{prefix}MCASUM{channel_number}:ArrayData")
        self.scalers = Xspress3Sca(f"{prefix}C{channel_number}SCA:")
        self.roi_time_series = _Xspress3RoiTimeSeriesIO(f"{prefix}MCA{channel_number}ROI:")
        self.rois = DeviceVector(
            {
                roi_number: Xspress3McaRoi(
                    f"{prefix}MCA{channel_number}ROI:{roi_number}:",
                    reset_prefix=(
                        f"{prefix}C{channel_number}_ROI{roi_number}:Reset" if include_roi_reset else None
                    ),
                )
                for roi_number in roi_numbers
            }
        )
        super().__init__(prefix, name=name)


_SPECTRUM_DATASET = "/entry/data/data"
_NDATTRIBUTES_GROUP = "/entry/instrument/NDAttributes"


_SCA_NAMES = (
    "clock_ticks",
    "reset_ticks",
    "reset_counts",
    "all_event",
    "all_good",
    "window_1",
    "window_2",
    "pileup",
    "event_width",
    "dt_factor",
    "dt_percent",
)
_SCA_DATASETS = (
    "SCA0",
    "SCA1",
    "SCA2",
    "SCA3",
    "SCA4",
    "SCA5",
    "SCA6",
    "SCA7",
    "EventWidth",
    "DTFactor",
    "DTPercent",
)


class Xspress3HDFDataLogic(DetectorDataLogic):
    """Describe fixed Xspress3 datasets from one HDF capture.

    Parameters
    ----------
    array_description : NDArrayDescription
        Signals describing the bulk spectrum array.
    path_provider : PathProvider
        Provider for HDF write and read paths.
    writer : Xspress3HDFIO
        HDF plugin signals.
    driver : Xspress3DriverIO
        Xspress3 driver signals.
    channel_numbers : sequence of int
        Channels exposed as per-channel spectrum streams.
    mca_roi_numbers : sequence of int
        ROIs exposed when ROI streams are enabled.
    include_roi_streams : bool, optional
        Emit ROI NDAttribute stream resources.
    include_sca_streams : bool, optional
        Emit SCA NDAttribute stream resources.
    hinted_streams : sequence of str, optional
        Detector-name-relative stream suffixes to hint. ``None`` hints the bulk
        spectrum.
    """

    datakey_suffix = ""

    def __init__(
        self,
        array_description: NDArrayDescription,
        path_provider: PathProvider,
        writer: Xspress3HDFIO,
        driver: Xspress3DriverIO,
        *,
        channel_numbers: Sequence[int],
        mca_roi_numbers: Sequence[int],
        include_roi_streams: bool = False,
        include_sca_streams: bool = False,
        hinted_streams: Sequence[str] | None = None,
    ) -> None:
        self.driver = driver
        self.channel_numbers = tuple(channel_numbers)
        self.mca_roi_numbers = tuple(mca_roi_numbers)
        self.include_roi_streams = include_roi_streams
        self.include_sca_streams = include_sca_streams
        self.hinted_streams = None if hinted_streams is None else tuple(hinted_streams)
        self._delegate = ADHDFDataLogic(
            array_description=array_description,
            path_provider=path_provider,
            writer=writer,
        )

    async def _read_array_metadata(self) -> tuple[tuple[int, int], str]:
        size_z, size_y, size_x, data_type, color_mode = await asyncio.gather(
            self.driver.array_size_z.get_value(),
            self.driver.array_size_y.get_value(),
            self.driver.array_size_x.get_value(),
            self.driver.data_type.get_value(),
            self.driver.color_mode.get_value(),
        )
        shape = tuple(size for size in (size_z, size_y, size_x) if size > 0)
        highest_channel = max(self.channel_numbers, default=0)
        metadata_error = (
            len(shape) != 2
            or shape[0] < highest_channel
            or data_type is ADBaseDataType.UNDEFINED
            or color_mode is not ADBaseColorMode.MONO
        )
        if metadata_error:
            raise ValueError(
                "Xspress3 array metadata must describe a nonempty two-dimensional "
                "Mono (channels, bins) frame containing every requested channel; "
                "call warmup() first"
            )
        return (shape[0], shape[1]), np.dtype(data_type.value.lower()).str

    @staticmethod
    def _ndattribute_resource(data_key: str, attribute_name: str) -> StreamResourceInfo:
        return StreamResourceInfo(
            data_key=data_key,
            shape=(),
            chunk_shape=(16384,),
            dtype_numpy=np.dtype("float64").str,
            parameters={"dataset": f"{_NDATTRIBUTES_GROUP}/{attribute_name}"},
        )

    async def prepare_unbounded(self, datakey_name: str) -> StreamResourceDataProvider:
        """Prepare HDF capture and construct stream resources.

        Parameters
        ----------
        datakey_name : str
            Base Bluesky data key.

        Returns
        -------
        StreamResourceDataProvider
            Provider for bulk, channel, and optional attribute streams.
        """
        frame_shape, frame_dtype = await self._read_array_metadata()
        delegate = await self._delegate.prepare_unbounded(datakey_name)

        frames_per_chunk = delegate.resources[0].chunk_shape[0]
        resources = [
            StreamResourceInfo(
                data_key=datakey_name,
                shape=frame_shape,
                chunk_shape=(frames_per_chunk, *frame_shape),
                dtype_numpy=frame_dtype,
                parameters={"dataset": _SPECTRUM_DATASET},
            )
        ]
        for channel in self.channel_numbers:
            channel_key = f"{datakey_name}-channel{channel}"
            resources.append(
                StreamResourceInfo(
                    data_key=channel_key,
                    shape=(frame_shape[1],),
                    chunk_shape=(frames_per_chunk, frame_shape[1]),
                    dtype_numpy=frame_dtype,
                    parameters={
                        "dataset": _SPECTRUM_DATASET,
                        "slice": f":,{channel - 1},:",
                    },
                )
            )
            if self.include_roi_streams:
                for roi in self.mca_roi_numbers:
                    resources.append(
                        self._ndattribute_resource(
                            f"{channel_key}-roi{roi}",
                            f"CHAN{channel}ROI{roi}",
                        )
                    )
            if self.include_sca_streams:
                for sca_name, dataset_suffix in zip(_SCA_NAMES, _SCA_DATASETS, strict=True):
                    resources.append(
                        self._ndattribute_resource(
                            f"{channel_key}-{sca_name}",
                            f"CHAN{channel}{dataset_suffix}",
                        )
                    )

        return StreamResourceDataProvider(
            uri=delegate.uri,
            resources=resources,
            mimetype="application/x-hdf5",
            collections_written_signal=delegate.collections_written_signal,
            flush_signal=delegate.flush_signal,
        )

    async def stop(self) -> None:
        """Stop HDF capture."""
        await self._delegate.stop()

    def get_hinted_fields(self, datakey_name: str) -> Sequence[str]:
        """Return configured hints with the detector data-key prefix."""
        if self.hinted_streams is None:
            return [datakey_name]
        return [datakey_name if not suffix else f"{datakey_name}-{suffix}" for suffix in self.hinted_streams]


class Xspress3HDFWriterFactory(ADWriterFactory[Xspress3HDFIO]):
    """Create the Xspress3 HDF plugin and its stream data logic.

    Parameters
    ----------
    path_provider : PathProvider
        Provider for HDF write and read paths.
    writer_suffix : str, optional
        HDF plugin suffix appended to the detector prefix.
    include_roi_streams : bool, optional
        Emit configured ROI NDAttribute streams.
    include_sca_streams : bool, optional
        Emit all canonical SCA NDAttribute streams.
    hinted_streams : sequence of str, optional
        Detector-name-relative stream suffixes to hint, such as
        ``"channel1-roi1"``. ``None`` hints the bulk spectrum; ``""`` selects
        the bulk spectrum explicitly.
    """

    path_provider: PathProvider
    include_roi_streams: bool
    include_sca_streams: bool
    hinted_streams: tuple[str, ...] | None

    def __init__(
        self,
        path_provider: PathProvider,
        *,
        writer_suffix: str = "HDF1:",
        include_roi_streams: bool = False,
        include_sca_streams: bool = False,
        hinted_streams: Sequence[str] | None = None,
    ) -> None:
        normalized_hints = None if hinted_streams is None else tuple(hinted_streams)
        if normalized_hints is not None and len(normalized_hints) != len(set(normalized_hints)):
            raise ValueError("hinted_streams must be unique")

        self.path_provider = path_provider
        self.include_roi_streams = include_roi_streams
        self.include_sca_streams = include_sca_streams
        self.hinted_streams = normalized_hints
        super().__init__(
            writer_cls=Xspress3HDFIO,
            writer_suffix=writer_suffix,
            writer_name="hdf",
            datakey_suffix="_image",
            array_description=None,
            data_logic_factory=self._make_data_logic,
        )

    def _validate_hinted_streams(
        self,
        channel_numbers: Sequence[int],
        mca_roi_numbers: Sequence[int],
    ) -> None:
        if self.hinted_streams is None:
            return
        emitted_streams = {""}
        for channel_number in channel_numbers:
            channel_stream = f"channel{channel_number}"
            emitted_streams.add(channel_stream)
            if self.include_roi_streams:
                emitted_streams.update(f"{channel_stream}-roi{roi_number}" for roi_number in mca_roi_numbers)
            if self.include_sca_streams:
                emitted_streams.update(f"{channel_stream}-{sca_name}" for sca_name in _SCA_NAMES)
        missing = tuple(stream for stream in self.hinted_streams if stream not in emitted_streams)
        if missing:
            raise ValueError(
                f"hinted_streams must refer to streams emitted by this writer; not emitted: {missing!r}"
            )

    def _make_data_logic(
        self,
        writer: Xspress3HDFIO,
        array_description: NDArrayDescription,
        driver: ADBaseIO,
        _plugins: Sequence[NDPluginBaseIO],
    ) -> Xspress3HDFDataLogic:
        xspress3_driver = cast(Xspress3DriverIO, driver)
        self._validate_hinted_streams(
            xspress3_driver.channel_numbers,
            xspress3_driver.mca_roi_numbers,
        )
        return Xspress3HDFDataLogic(
            array_description,
            self.path_provider,
            writer,
            xspress3_driver,
            channel_numbers=xspress3_driver.channel_numbers,
            mca_roi_numbers=xspress3_driver.mca_roi_numbers,
            include_roi_streams=self.include_roi_streams,
            include_sca_streams=self.include_sca_streams,
            hinted_streams=self.hinted_streams,
        )


class Xspress3Detector(AreaDetector[Xspress3DriverIO]):
    """Control an Xspress3 detector using the community IOC.

    Parameters
    ----------
    prefix : str
        Root EPICS prefix.
    writer_factory : Xspress3HDFWriterFactory, optional
        Factory defining the optional HDF writer and stream configuration.
    channel_numbers : sequence of int, optional
        One-based detector channels to expose.
    mca_roi_numbers : sequence of int, optional
        ROI numbers created for each channel.
    driver_suffix : str, optional
        Driver suffix appended to ``prefix``.
    minimum_deadtime : float, optional
        Minimum external-trigger deadtime in seconds.
    include_roi_reset : bool, optional
        Create optional ROI reset commands.
    plugins : mapping of str to NDPluginBaseIO, optional
        Additional AreaDetector plugins.
    config_sigs : sequence of SignalR, optional
        Additional configuration signals.
    name : str, optional
        Ophyd device name.

    Notes
    -----
    When a writer is configured, bulk and per-channel spectra are always emitted.
    External edge triggering uses ``TTL + Internal``; level mode is selected through
    ``level_trigger_mode``. The community IOC's ``NumCapture_CALC`` record is
    permanently disabled because ophyd-async owns ``NumCapture``.
    """

    def __init__(
        self,
        prefix: str,
        writer_factory: Xspress3HDFWriterFactory | None = None,
        *,
        channel_numbers: Sequence[int] = (1,),
        mca_roi_numbers: Sequence[int] = (1, 2, 3, 4),
        driver_suffix: str = "det1:",
        minimum_deadtime: float = 0.0,
        include_roi_reset: bool = False,
        plugins: Mapping[str, NDPluginBaseIO] | None = None,
        config_sigs: Sequence[SignalR] = (),
        name: str = "",
    ) -> None:
        if minimum_deadtime < 0:
            raise ValueError("minimum_deadtime must be non-negative")

        driver = Xspress3DriverIO(
            prefix + driver_suffix,
            channel_numbers=channel_numbers,
            mca_roi_numbers=mca_roi_numbers,
        )
        writer_factories = () if writer_factory is None else (writer_factory,)
        self._has_hdf_writer = writer_factory is not None
        self.channels = DeviceVector(
            {
                channel_number: Xspress3Channel(
                    prefix,
                    channel_number,
                    driver.mca_roi_numbers,
                    include_roi_reset=include_roi_reset,
                )
                for channel_number in driver.channel_numbers
            }
        )
        self.level_trigger_mode = soft_signal_rw(Xspress3LevelTriggerMode, Xspress3LevelTriggerMode.TTL_VETO_ONLY)
        trigger_logic = Xspress3TriggerLogic(
            driver,
            level_trigger_mode=self.level_trigger_mode,
            minimum_deadtime=minimum_deadtime,
        )
        acquire_logic = Xspress3AcquireLogic(driver)
        super().__init__(
            driver,
            prefix,
            *writer_factories,
            acquire_logic=acquire_logic,
            trigger_logic=trigger_logic,
            plugins=plugins,
            config_sigs=(
                driver.trigger_mode,
                driver.num_images,
                driver.num_channels,
                self.level_trigger_mode,
                driver.ctrl_dtc,
                driver.erase_on_start,
                *config_sigs,
            ),
            name=name,
        )

    @AsyncStatus.wrap
    async def stage(self) -> None:
        """Disable the capture calculation and make the detector ready."""
        if self._has_hdf_writer:
            await self.hdf.num_capture_calc_disable.set(1)
        await super().stage()

    @AsyncStatus.wrap
    async def prepare(self, value: TriggerInfo) -> None:
        """Configure a finite acquisition.

        Parameters
        ----------
        value : TriggerInfo
            Trigger type, timing, and frame counts.
        """
        if self._has_hdf_writer:
            await self.hdf.num_capture_calc_disable.set(1)
            await self.hdf.num_capture.set(0)
        await super().prepare(value)

    async def warmup(self, exposure: float = 0.1) -> None:
        """Acquire one internal frame to initialize array metadata.

        Parameters
        ----------
        exposure : float, optional
            Warmup exposure time in seconds.

        Notes
        -----
        Saved acquisition settings are restored even if warmup fails.
        """
        saved = await asyncio.gather(
            self.driver.array_callbacks.get_value(),
            self.driver.trigger_mode.get_value(),
            self.driver.num_images.get_value(),
            self.driver.acquire_time.get_value(),
            self.driver.erase_on_start.get_value(),
        )
        warmup_acquire = ADAcquireLogic(self.driver)
        try:
            await warmup_acquire.ensure_stopped()
            if self._has_hdf_writer:
                await self.hdf.num_capture_calc_disable.set(1)
            await _gather_and_raise(
                self.driver.array_callbacks.set(True),
                self.driver.trigger_mode.set(Xspress3TriggerMode.INTERNAL),
                self.driver.num_images.set(1),
                self.driver.acquire_time.set(exposure),
                self.driver.erase_on_start.set(True),
            )
            await warmup_acquire.start_acquiring()
            await warmup_acquire.wait_for_idle()
        finally:
            try:
                await warmup_acquire.ensure_stopped()
            finally:
                await _gather_and_raise(
                    self.driver.array_callbacks.set(saved[0]),
                    self.driver.trigger_mode.set(saved[1]),
                    self.driver.num_images.set(saved[2]),
                    self.driver.acquire_time.set(saved[3]),
                    self.driver.erase_on_start.set(saved[4]),
                )


def _validate_number(value: int, label: str, minimum: int, maximum: int) -> None:
    if type(value) is not int:
        raise ValueError(f"{label} number {value!r} is not an integer")
    if not minimum <= value <= maximum:
        raise ValueError(f"{label} number {value!r} is outside the allowed interval [{minimum}, {maximum}]")


def _validate_numbers(values: Sequence[int], label: str, minimum: int, maximum: int) -> tuple[int, ...]:
    numbers = tuple(values)
    for value in numbers:
        _validate_number(value, label, minimum, maximum)
    if len(numbers) != len(set(numbers)):
        raise ValueError(f"{label} numbers must be unique")
    return tuple(sorted(numbers))
