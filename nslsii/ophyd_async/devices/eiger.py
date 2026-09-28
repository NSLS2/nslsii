"""ophyd-async support for the areaDetector ADEiger IOC."""

from __future__ import annotations

import asyncio
from collections import deque
from collections.abc import AsyncIterator, Callable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from pathlib import PureWindowsPath
from typing import Annotated as A

import numpy as np
from bluesky.protocols import StreamAsset
from event_model import ComposeStreamResource, ComposeStreamResourceBundle, DataKey
from ophyd_async.core import (
    DEFAULT_TIMEOUT,
    AsyncStatus,
    DetectorAcquireLogic,
    DetectorDataLogic,
    DetectorTriggerLogic,
    DeviceConnector,
    PathProvider,
    SignalDict,
    SignalR,
    SignalRW,
    StreamableDataProvider,
    StreamResourceInfo,
    StrictEnum,
    SupersetEnum,
    derived_signal_r,
    error_if_none,
    set_and_wait_for_other_value,
    soft_signal_r_and_setter,
    wait_for_value,
)
from ophyd_async.epics.adcore import (
    ADBaseColorMode,
    ADBaseDataType,
    ADBaseIO,
    ADState,
    ADWriterFactory,
    AreaDetector,
    NDArrayDescription,
    NDFileIO,
    NDPluginBaseIO,
    trigger_info_from_num_images,
)
from ophyd_async.epics.core import PvSuffix, stop_busy_record, wait_for_good_state

# NumImages for unbounded requests; the driver clamps to the DCU limit and the readback
# reflects the clamped value.
_MAX_NUM_IMAGES = 999_999
# NumTriggers for internally triggered series. It bounds the software triggers a series
# accepts, so it must exceed the number of trigger() calls in a scan (unknowable from an
# implicit-prepare trigger()). Deliberately below the DCU maximum: at arm the driver allocates
# one file slot per expected data file, and in FileWriter-counting mode that is one per
# trigger. Exhausting it is still correct: the series ends and the next point arms a new one.
_MAX_NUM_TRIGGERS = 10_000


class EigerTriggerMode(SupersetEnum):
    """``TriggerMode`` choices; ``External Gate`` exists on Eiger2 only."""

    INTERNAL_SERIES = "Internal Series"
    INTERNAL_ENABLE = "Internal Enable"
    EXTERNAL_SERIES = "External Series"
    EXTERNAL_ENABLE = "External Enable"
    CONTINUOUS = "Continuous"
    EXTERNAL_GATE = "External Gate"


class EigerExtGateMode(StrictEnum):
    """``ExtGateMode`` choices (Eiger2)."""

    HDR = "HDR"
    PUMP_AND_PROBE = "Pump & Probe"


class EigerCountingMode(StrictEnum):
    """``CountingMode`` choices (Eiger2)."""

    NORMAL = "Normal"
    RETRIGGER = "Retrigger"


class EigerROIMode(StrictEnum):
    """``ROIMode`` choices."""

    DISABLED = "Disable"
    _4M = "4M"


class EigerCompressionAlgo(SupersetEnum):
    """``CompressionAlgo`` choices; ``None`` exists on Eiger2 only."""

    LZ4 = "LZ4"
    BSLZ4 = "BS LZ4"
    NONE = "None"


class EigerDataSource(StrictEnum):
    """``DataSource`` choices: where the IOC gets NDArrays from."""

    NONE = "None"
    FILE_WRITER = "FileWriter"
    STREAM = "Stream"


class EigerHDF5Format(StrictEnum):
    """``FWHDF5Format`` choices (Eiger2)."""

    LEGACY = "Legacy"
    V2024_2 = "v2024.2"


class EigerStreamVersion(StrictEnum):
    """``StreamVersion`` choices (Eiger2)."""

    STREAM1 = "Stream"
    STREAM2 = "Stream2"


class EigerStreamHdrDetail(StrictEnum):
    """``StreamHdrDetail`` choices."""

    ALL = "All"
    BASIC = "Basic"
    NONE = "None"


def _image_data_type(bit_depth: int, signed: bool) -> ADBaseDataType:
    # Same switch as eigerDetector.cpp when it builds NDArrays from the FileWriter/Stream data
    if bit_depth not in (8, 16, 32):
        raise ValueError(f"Unexpected Eiger bit depth {bit_depth}")
    return ADBaseDataType(f"{'' if signed else 'U'}Int{bit_depth}")


class EigerDriverIO(ADBaseIO, NDFileIO):
    """Records common to Eiger1 and Eiger2 (``eigerBase.template``).

    ``DataType_RBV`` and ``ColorMode_RBV`` are disabled in the template and never update, so
    ``image_data_type`` / ``image_color_mode`` provide the real NDArray description for plugin
    writers (see `eiger_array_description`).
    """

    # Standard Driver Parameters
    trigger_mode: A[SignalRW[EigerTriggerMode], PvSuffix.rbv("TriggerMode")]
    num_images_counter: A[SignalR[int], PvSuffix("NumImagesCounter_RBV")]
    num_exposures: A[SignalRW[int], PvSuffix.rbv("NumExposures")]
    temperature_actual: A[SignalR[float], PvSuffix("TemperatureActual")]
    max_size_x: A[SignalR[int], PvSuffix("MaxSizeX_RBV")]
    max_size_y: A[SignalR[int], PvSuffix("MaxSizeY_RBV")]

    # Detector Information
    description: A[SignalR[str], PvSuffix("Description_RBV")]
    x_pixel_size: A[SignalR[float], PvSuffix("XPixelSize_RBV")]
    y_pixel_size: A[SignalR[float], PvSuffix("YPixelSize_RBV")]
    sensor_material: A[SignalR[str], PvSuffix("SensorMaterial_RBV")]
    sensor_thickness: A[SignalR[float], PvSuffix("SensorThickness_RBV")]
    dead_time: A[SignalR[float], PvSuffix("DeadTime_RBV")]

    # Detector Status
    restart: A[SignalRW[bool], PvSuffix("Restart")]
    initialize: A[SignalRW[bool], PvSuffix("Initialize")]
    state: A[SignalR[str], PvSuffix("State_RBV")]
    error: A[SignalR[str], PvSuffix("Error_RBV")]
    temp0: A[SignalR[float], PvSuffix("Temp0_RBV")]
    humid0: A[SignalR[float], PvSuffix("Humid0_RBV")]

    # Acquisition Setup
    photon_energy: A[SignalRW[float], PvSuffix.rbv("PhotonEnergy")]
    threshold_energy: A[SignalRW[float], PvSuffix.rbv("ThresholdEnergy")]

    # Trigger Setup
    trigger_: A[SignalRW[float], PvSuffix("Trigger")]
    num_triggers: A[SignalRW[int], PvSuffix.rbv("NumTriggers")]
    manual_trigger: A[SignalRW[bool], PvSuffix.rbv("ManualTrigger")]

    # Readout Setup
    roi_mode: A[SignalRW[EigerROIMode], PvSuffix.rbv("ROIMode")]
    flatfield_applied: A[SignalRW[bool], PvSuffix.rbv("FlatfieldApplied")]
    countrate_corr_applied: A[SignalRW[bool], PvSuffix.rbv("CountrateCorrApplied")]
    pixel_mask_applied: A[SignalRW[bool], PvSuffix.rbv("PixelMaskApplied")]
    auto_summation: A[SignalRW[bool], PvSuffix.rbv("AutoSummation")]
    compression_algo: A[SignalRW[EigerCompressionAlgo], PvSuffix.rbv("CompressionAlgo")]
    data_source: A[SignalRW[EigerDataSource], PvSuffix.rbv("DataSource")]
    signed_data: A[SignalRW[bool], PvSuffix.rbv("SignedData")]

    # Acquisition Status
    armed: A[SignalR[bool], PvSuffix("Armed")]
    bit_depth_image: A[SignalR[int], PvSuffix("BitDepthImage_RBV")]
    count_cutoff: A[SignalR[float], PvSuffix("CountCutoff_RBV")]

    # Stream Interface
    stream_enable: A[SignalRW[bool], PvSuffix.rbv("StreamEnable")]
    stream_state: A[SignalR[str], PvSuffix("StreamState_RBV")]
    stream_decompress: A[SignalRW[bool], PvSuffix.rbv("StreamDecompress")]
    stream_hdr_detail: A[SignalRW[EigerStreamHdrDetail], PvSuffix.rbv("StreamHdrDetail")]
    stream_hdr_appendix: A[SignalRW[str], PvSuffix("StreamHdrAppendix")]
    stream_img_appendix: A[SignalRW[str], PvSuffix("StreamImgAppendix")]
    stream_dropped: A[SignalR[int], PvSuffix("StreamDropped_RBV")]

    # Monitor Interface
    monitor_enable: A[SignalRW[bool], PvSuffix.rbv("MonitorEnable")]
    monitor_state: A[SignalR[str], PvSuffix("MonitorState_RBV")]
    monitor_timeout: A[SignalRW[float], PvSuffix.rbv("MonitorTimeout")]

    # Acquisition Metadata
    beam_x: A[SignalRW[float], PvSuffix.rbv("BeamX")]
    beam_y: A[SignalRW[float], PvSuffix.rbv("BeamY")]
    det_dist: A[SignalRW[float], PvSuffix.rbv("DetDist")]
    wavelength: A[SignalRW[float], PvSuffix.rbv("Wavelength")]

    # Detector Metadata
    chi_start: A[SignalRW[float], PvSuffix.rbv("ChiStart")]
    chi_incr: A[SignalRW[float], PvSuffix.rbv("ChiIncr")]
    kappa_start: A[SignalRW[float], PvSuffix.rbv("KappaStart")]
    kappa_incr: A[SignalRW[float], PvSuffix.rbv("KappaIncr")]
    omega_start: A[SignalRW[float], PvSuffix.rbv("OmegaStart")]
    omega_incr: A[SignalRW[float], PvSuffix.rbv("OmegaIncr")]
    phi_start: A[SignalRW[float], PvSuffix.rbv("PhiStart")]
    phi_incr: A[SignalRW[float], PvSuffix.rbv("PhiIncr")]
    two_theta_start: A[SignalRW[float], PvSuffix.rbv("TwoThetaStart")]
    two_theta_incr: A[SignalRW[float], PvSuffix.rbv("TwoThetaIncr")]

    # Minimum change allowed
    wavelength_eps: A[SignalRW[float], PvSuffix.rbv("WavelengthEps")]
    energy_eps: A[SignalRW[float], PvSuffix.rbv("EnergyEps")]

    # FileWriter Interface
    fw_enable: A[SignalRW[bool], PvSuffix.rbv("FWEnable")]
    fw_state: A[SignalR[str], PvSuffix("FWState_RBV")]
    fw_compression: A[SignalRW[bool], PvSuffix.rbv("FWCompression")]
    fw_name_pattern: A[SignalRW[str], PvSuffix.rbv("FWNamePattern")]
    sequence_id: A[SignalR[int], PvSuffix("SequenceId")]
    save_files: A[SignalRW[bool], PvSuffix.rbv("SaveFiles")]
    file_owner: A[SignalRW[str], PvSuffix.rbv("FileOwner")]
    file_owner_grp: A[SignalRW[str], PvSuffix.rbv("FileOwnerGrp")]
    file_perms: A[SignalRW[float], PvSuffix.rbv("FilePerms")]
    fw_free: A[SignalR[float], PvSuffix("FWFree_RBV")]
    fw_auto_remove: A[SignalRW[bool], PvSuffix.rbv("FWAutoRemove")]
    fw_nimgs_per_file: A[SignalRW[int], PvSuffix.rbv("FWNImagesPerFile")]

    def __init__(
        self,
        prefix: str = "",
        with_pvi: bool = False,
        name: str = "",
        connector: DeviceConnector | None = None,
    ) -> None:
        super().__init__(prefix, with_pvi, name, connector)
        self.image_data_type = derived_signal_r(
            _image_data_type, bit_depth=self.bit_depth_image, signed=self.signed_data
        )
        self.image_color_mode, _ = soft_signal_r_and_setter(ADBaseColorMode, ADBaseColorMode.MONO)


class Eiger1DriverIO(EigerDriverIO):
    """Eiger1 driver (``eiger1.template``)."""

    link0: A[SignalR[bool], PvSuffix("Link0_RBV")]
    link1: A[SignalR[bool], PvSuffix("Link1_RBV")]
    link2: A[SignalR[bool], PvSuffix("Link2_RBV")]
    link3: A[SignalR[bool], PvSuffix("Link3_RBV")]
    dcu_buffer_free: A[SignalR[float], PvSuffix("DCUBufferFree_RBV")]
    fw_clear: A[SignalRW[float], PvSuffix("FWClear")]


class Eiger2DriverIO(EigerDriverIO):
    """Eiger2 driver (``eiger2.template``)."""

    # Detector Status
    hv_reset_time: A[SignalRW[float], PvSuffix.rbv("HVResetTime")]
    hv_reset: A[SignalRW[bool], PvSuffix("HVReset")]
    hv_state: A[SignalR[str], PvSuffix("HVState_RBV")]

    # Acquisition Setup
    threshold1_enable: A[SignalRW[bool], PvSuffix.rbv("Threshold1Enable")]
    threshold2_energy: A[SignalRW[float], PvSuffix.rbv("Threshold2Energy")]
    threshold2_enable: A[SignalRW[bool], PvSuffix.rbv("Threshold2Enable")]
    threshold_diff_enable: A[SignalRW[bool], PvSuffix.rbv("ThresholdDiffEnable")]
    counting_mode: A[SignalRW[EigerCountingMode], PvSuffix.rbv("CountingMode")]

    # Trigger Setup
    ext_gate_mode: A[SignalRW[EigerExtGateMode], PvSuffix.rbv("ExtGateMode")]
    trigger_start_delay: A[SignalRW[float], PvSuffix.rbv("TriggerStartDelay")]

    # Stream Interface
    stream_version: A[SignalRW[EigerStreamVersion], PvSuffix.rbv("StreamVersion")]
    stream_as_ts_source: A[SignalRW[bool], PvSuffix.rbv("StreamAsTSSource")]

    # FileWriter Interface
    fw_hdf5_format: A[SignalRW[EigerHDF5Format], PvSuffix.rbv("FWHDF5Format")]


@dataclass
class EigerTriggerLogic(DetectorTriggerLogic):
    """Trigger logic for ADEiger.

    ``NumImages``, ``NumTriggers`` and ``ManualTrigger`` are latched by the driver at arm, so
    every ``prepare_*`` first ends any armed series; the next ``start_acquiring`` arms a new one
    with the new values. ADEiger does not implement ``ImageMode``, so it is never set.

    Parameters
    ----------
    driver : EigerDriverIO
        The Eiger driver.
    """

    driver: EigerDriverIO

    def config_sigs(self) -> set[SignalR]:
        return {self.driver.dead_time}

    def get_deadtime(self, config_values: SignalDict) -> float:
        # DeadTime_RBV is the DCU's detector_readout_time in seconds, refreshed by the IOC
        # after every AcquireTime / ThresholdEnergy change.
        return config_values[self.driver.dead_time]

    async def _disarm(self) -> None:
        if await self.driver.armed.get_value():
            await stop_busy_record(self.driver.acquire)
            await wait_for_value(self.driver.armed, False, timeout=DEFAULT_TIMEOUT)

    async def _set_exposure(self, livetime: float, deadtime: float) -> None:
        if livetime == 0:
            return
        if deadtime == 0:
            deadtime = await self.driver.dead_time.get_value()
        # The DCU rejects frame_time < count_time + readout, so count_time goes first.
        await self.driver.acquire_time.set(livetime)
        await self.driver.acquire_period.set(livetime + deadtime)

    async def prepare_internal(self, num: int, livetime: float, deadtime: float) -> None:
        # num is the number of frames each software trigger produces
        await self._disarm()
        await self.driver.trigger_mode.set(EigerTriggerMode.INTERNAL_SERIES)
        await asyncio.gather(
            self.driver.manual_trigger.set(True),
            self.driver.num_triggers.set(_MAX_NUM_TRIGGERS),
            self.driver.num_images.set(num or _MAX_NUM_IMAGES),
        )
        await self._set_exposure(livetime, deadtime)

    async def prepare_edge(self, num: int, livetime: float) -> None:
        # One edge -> one internally timed image
        await self._disarm()
        await self.driver.trigger_mode.set(EigerTriggerMode.EXTERNAL_SERIES)
        await asyncio.gather(
            self.driver.manual_trigger.set(False),
            self.driver.num_images.set(1),
            self.driver.num_triggers.set(num or _MAX_NUM_IMAGES),
        )
        await self._set_exposure(livetime, 0.0)

    async def prepare_level(self, num: int) -> None:
        # Gate width is the exposure
        await self._disarm()
        await self.driver.trigger_mode.set(EigerTriggerMode.EXTERNAL_ENABLE)
        await asyncio.gather(
            self.driver.manual_trigger.set(False),
            self.driver.num_images.set(1),
            self.driver.num_triggers.set(num or _MAX_NUM_IMAGES),
        )

    async def default_trigger_info(self):
        return await trigger_info_from_num_images(self.driver)


class EigerAcquireLogic(DetectorAcquireLogic):
    """Arm once per staged scan, software-trigger each internally triggered point.

    Parameters
    ----------
    driver : EigerDriverIO
        The Eiger driver.
    on_armed : callable, optional
        Called with ``(SequenceId, ArrayCounter)`` right after every arm, so the data logic
        can attribute subsequent frames to the new file series.
    """

    def __init__(
        self,
        driver: EigerDriverIO,
        on_armed: Callable[[int, int], None] | None = None,
    ) -> None:
        self.driver = driver
        self._on_armed = on_armed
        self.acquire_status: AsyncStatus | None = None

    async def start_acquiring(self) -> None:
        if not await self.driver.armed.get_value():
            # A previous series may still be processing files: Acquire=1 is ignored while
            # DetectorState is Acquire, so wait for the busy record to clear first.
            await wait_for_value(self.driver.acquire, False, timeout=DEFAULT_TIMEOUT)
            self.acquire_status = await set_and_wait_for_other_value(
                set_signal=self.driver.acquire,
                set_value=True,
                match_signal=self.driver.armed,
                match_value=True,
                wait_for_set_completion=False,
                timeout=DEFAULT_TIMEOUT,
            )
            if self._on_armed is not None:
                # The driver posts SequenceId and Armed in the same callback batch, so the
                # SequenceId monitor may lag the Armed one; read both uncached.
                sequence_id, first_collection = await asyncio.gather(
                    self.driver.sequence_id.get_value(cached=False),
                    self.driver.array_counter.get_value(cached=False),
                )
                self._on_armed(sequence_id, first_collection)
        if await self.driver.manual_trigger.get_value():
            # Software trigger into the armed series
            await self.driver.trigger_.set(1.0)

    async def wait_for_idle(self) -> None:
        # Internally triggered series stay armed between points; frame arrival is already
        # awaited by StandardDetector. Externally triggered series end on their own after
        # NumTriggers pulses, so wait for the driver to finish processing files.
        if not await self.driver.manual_trigger.get_value():
            if self.acquire_status:
                await self.acquire_status
            await wait_for_good_state(
                self.driver.detector_state,
                {ADState.IDLE, ADState.ABORTED},
                timeout=DEFAULT_TIMEOUT,
            )

    async def ensure_stopped(self) -> None:
        # Acquire=0 aborts and disarms the series
        await stop_busy_record(self.driver.acquire)
        await wait_for_value(self.driver.armed, False, timeout=DEFAULT_TIMEOUT)
        await self.driver.manual_trigger.set(False)


class EigerFileWriterDataProvider(StreamableDataProvider):
    """Stream documents for the Eiger FileWriter data files of one scan.

    A scan produces ``{filename}_{SequenceId}_data_{k:06d}.h5`` files: a new ``k`` every
    ``images_per_file`` frames and a new ``SequenceId`` whenever a series is (re)armed. One
    ``stream_resource`` is emitted per data file with resource-local ``stream_datum`` indices;
    bluesky assigns the event ``seq_nums``. Series boundaries are reported through
    `record_series`; file boundaries are arithmetic.

    Parameters
    ----------
    directory_uri : str
        URI of the directory holding the data files, with trailing separator.
    filename : str
        Filename stem; ``FWNamePattern`` is ``f"{filename}_$id"``.
    images_per_file : int
        ``FWNImagesPerFile`` readback.
    resource : StreamResourceInfo
        Description of the ``/entry/data/data`` dataset.
    collections_written_signal : SignalR[int]
        ``ArrayCounter``; frames counted by the IOC.
    """

    def __init__(
        self,
        directory_uri: str,
        filename: str,
        images_per_file: int,
        resource: StreamResourceInfo,
        collections_written_signal: SignalR[int],
    ) -> None:
        self.collections_written_signal = collections_written_signal
        self._directory_uri = directory_uri
        self._filename = filename
        self._images_per_file = images_per_file
        self._resource = resource
        self._composer = ComposeStreamResource()
        # Arms not yet reached by the emitted documents: (SequenceId, first ArrayCounter)
        self._series: deque[tuple[int, int]] = deque()
        self._current: tuple[int, int] | None = None
        self._file_index = 0
        self._bundle: ComposeStreamResourceBundle | None = None
        # Event index at which the current bundle's file started
        self._bundle_start = 0
        # Events emitted so far
        self._last_emitted = 0

    def record_series(self, sequence_id: int, first_collection: int) -> None:
        """Register a newly armed series.

        Parameters
        ----------
        sequence_id : int
            ``SequenceId`` assigned by the driver at arm.
        first_collection : int
            ``ArrayCounter`` value at arm; frames from here on belong to this series.
        """
        self._series.append((sequence_id, first_collection))

    async def make_datakeys(self, collections_per_event: int) -> dict[str, DataKey]:
        resource = self._resource
        return {
            resource.data_key: DataKey(
                source=resource.source,
                shape=[collections_per_event, *resource.shape],
                dtype="array",
                dtype_numpy=resource.dtype_numpy,
                external="STREAM:",
            )
        }

    async def make_stream_docs(
        self, collections_written: int, collections_per_event: int
    ) -> AsyncIterator[StreamAsset]:
        cpe = collections_per_event
        ipf = self._images_per_file
        if ipf % cpe:
            raise ValueError(f"FWNImagesPerFile={ipf} is not a multiple of collections_per_event={cpe}")
        target = collections_written // cpe
        while self._last_emitted < target:
            if self._series and self._series[0][1] // cpe <= self._last_emitted:
                # A new series starts here
                self._current = self._series.popleft()
                self._bundle = None
            if self._current is None:
                raise RuntimeError("Eiger frames counted before any series was armed")
            seq, first = self._current
            # 1-based, matches the DCU's image_nr_start=1
            file_index = (self._last_emitted * cpe - first) // ipf + 1
            if self._bundle is None or file_index != self._file_index:
                self._file_index = file_index
                self._bundle = self._composer(
                    mimetype="application/x-hdf5",
                    uri=f"{self._directory_uri}{self._filename}_{seq}_data_{file_index:06d}.h5",
                    data_key=self._resource.data_key,
                    parameters={"chunk_shape": self._resource.chunk_shape, **self._resource.parameters},
                    uid=None,
                    validate=True,
                )
                self._bundle_start = self._last_emitted
                yield "stream_resource", self._bundle.stream_resource_doc
            # Emit up to the end of this data file, or the start of the next series
            stop = min(target, (first + file_index * ipf) // cpe)
            if self._series:
                stop = min(stop, self._series[0][1] // cpe)
            yield (
                "stream_datum",
                self._bundle.compose_stream_datum(
                    {"start": self._last_emitted - self._bundle_start, "stop": stop - self._bundle_start}
                ),
            )
            self._last_emitted = stop


@dataclass
class EigerFileWriterDataLogic(DetectorDataLogic):
    """Data logic for the Eiger FileWriter files downloaded by the IOC.

    Parameters
    ----------
    driver : EigerDriverIO
        The Eiger driver.
    path_provider : PathProvider
        Provides the directory and filename for the DCU data files.
    data_source : EigerDataSource, default EigerDataSource.FILE_WRITER
        Where the IOC counts frames from. ``FILE_WRITER`` sizes each data file to one
        software trigger so every point publishes as soon as its file is parsed; ``STREAM``
        counts frames live from the ZMQ stream and keeps the IOC's file size.
    datakey_suffix : str, default ""
        Suffix appended to the detector name to form the data key.
    """

    driver: EigerDriverIO
    path_provider: PathProvider
    data_source: EigerDataSource = EigerDataSource.FILE_WRITER
    datakey_suffix: str = ""
    _provider: EigerFileWriterDataProvider | None = field(default=None, init=False, repr=False)

    def __post_init__(self) -> None:
        if self.data_source is EigerDataSource.NONE:
            raise ValueError(
                "data_source must be FILE_WRITER or STREAM: with DataSource=None the IOC produces no "
                "NDArrays and frames cannot be counted"
            )

    def record_series(self, sequence_id: int, first_collection: int) -> None:
        provider = error_if_none(self._provider, "prepare_unbounded must run before the Eiger is armed")
        provider.record_series(sequence_id, first_collection)

    def get_hinted_fields(self, datakey_name: str) -> Sequence[str]:
        return [datakey_name]

    async def prepare_unbounded(self, datakey_name: str) -> StreamableDataProvider:
        driver = self.driver
        path_info = self.path_provider(datakey_name)
        # Directory creation happens when FilePath is processed, so set the depth first
        await driver.create_directory.set(path_info.create_dir_depth)
        if isinstance(path_info.directory_path, PureWindowsPath):
            directory = f"{path_info.directory_path}\\"
        else:
            directory = f"{path_info.directory_path}/"
        coros = [
            driver.file_path.set(directory),
            driver.fw_name_pattern.set(f"{path_info.filename}_$id"),
            driver.fw_enable.set(True),
            driver.save_files.set(True),
            driver.data_source.set(self.data_source),
            # StandardDetector counts from the raw ArrayCounter and the driver never resets it
            driver.array_counter.set(0),
        ]
        if self.data_source is EigerDataSource.STREAM:
            coros.append(driver.stream_enable.set(True))
        if isinstance(driver, Eiger2DriverIO):
            # Only the Legacy layout has a 3-D /entry/data/data
            coros.append(driver.fw_hdf5_format.set(EigerHDF5Format.LEGACY))
        await asyncio.gather(*coros)
        if not await driver.file_path_exists.get_value():
            raise FileNotFoundError(f"Path {directory} doesn't exist or not writable!")
        # Trigger-logic prepare has already run, so these are the values latched at arm
        num_images, num_triggers, manual, configured = await asyncio.gather(
            driver.num_images.get_value(),
            driver.num_triggers.get_value(),
            driver.manual_trigger.get_value(),
            driver.fw_nimgs_per_file.get_value(),
        )
        # Frames produced per software trigger (manual) or per series (external): both are a
        # whole number of events, which a data file boundary must never split.
        frames_per_unit = num_images if manual else num_images * num_triggers
        if self.data_source is EigerDataSource.FILE_WRITER:
            # Frames are only counted when a data file is parsed, so a file must close at
            # every point
            requested = frames_per_unit
        else:
            # Frames are counted live; keep the IOC's size but align it to whole units
            requested = max(frames_per_unit, configured - configured % frames_per_unit)
        await driver.fw_nimgs_per_file.set(requested)
        images_per_file = await driver.fw_nimgs_per_file.get_value(cached=False)
        if manual and images_per_file < requested:
            # A manual series never ends on its own, so a clamped file size would leave the
            # last partial file of every trigger unparsed
            raise ValueError(
                f"{requested} images per trigger exceeds the detector limit of {images_per_file} images per file"
            )
        bit_depth, ny, nx = await asyncio.gather(
            driver.bit_depth_image.get_value(),
            driver.array_size_y.get_value(),
            driver.array_size_x.get_value(),
        )
        # DCU files are always unsigned; SignedData only affects the IOC's NDArrays
        resource = StreamResourceInfo(
            data_key=datakey_name,
            shape=(ny, nx),
            chunk_shape=(1, ny, nx),
            dtype_numpy=np.dtype(f"uint{bit_depth}").str,
            parameters={"dataset": "/entry/data/data"},
            source=driver.full_file_name.source,
        )
        self._provider = EigerFileWriterDataProvider(
            path_info.directory_uri,
            path_info.filename,
            images_per_file,
            resource,
            driver.array_counter,
        )
        return self._provider


def eiger_array_description(driver: EigerDriverIO) -> NDArrayDescription:
    """NDArray description for plugin writers fed by an Eiger driver.

    Parameters
    ----------
    driver : EigerDriverIO
        Driver whose ``image_data_type`` / ``image_color_mode`` describe the NDArrays.

    Returns
    -------
    NDArrayDescription
        Shape from ``ArraySizeY_RBV`` x ``ArraySizeX_RBV``, dtype from the Eiger bit depth.
    """
    return NDArrayDescription(
        shape_signals=[driver.array_size_y, driver.array_size_x],
        data_type_signal=driver.image_data_type,
        color_mode_signal=driver.image_color_mode,
    )


class EigerDetector(AreaDetector[EigerDriverIO]):
    """An ADEiger detector.

    One Eiger series is used per staged scan: the detector arms on the first ``trigger()`` /
    ``kickoff()`` (at ``prepare()`` for external triggers), internally triggered points are
    ``Trigger`` PV software triggers into that series, and ``unstage()`` ends it.

    Parameters
    ----------
    prefix : str
        EPICS PV prefix for the detector.
    *writer_factories : ADWriterFactory
        Factories for areaDetector file writer plugins and their data logics. Their
        ``datakey_suffix`` must differ from ``""`` when ``path_provider`` is given.
    path_provider : PathProvider, optional
        Enables the Eiger FileWriter data logic, writing DCU data files to the provided
        directory.
    data_source : EigerDataSource, default EigerDataSource.FILE_WRITER
        How the IOC counts frames for the FileWriter data logic.
    driver_cls : type[EigerDriverIO], default Eiger2DriverIO
        `Eiger2DriverIO` or `Eiger1DriverIO`.
    driver_suffix : str, default "cam1:"
        PV suffix for the driver.
    plugins : Mapping[str, NDPluginBaseIO], optional
        Additional areaDetector plugins to include.
    config_sigs : Sequence[SignalR], optional
        Additional signals to include in configuration.
    name : str, optional
        Name for the detector device.
    """

    def __init__(
        self,
        prefix: str,
        *writer_factories: ADWriterFactory,
        path_provider: PathProvider | None = None,
        data_source: EigerDataSource = EigerDataSource.FILE_WRITER,
        driver_cls: type[EigerDriverIO] = Eiger2DriverIO,
        driver_suffix: str = "cam1:",
        plugins: Mapping[str, NDPluginBaseIO] | None = None,
        config_sigs: Sequence[SignalR] = (),
        name: str = "",
    ) -> None:
        driver = driver_cls(prefix + driver_suffix)
        data_logic = (
            EigerFileWriterDataLogic(driver, path_provider, data_source=data_source) if path_provider else None
        )
        # The default description would read the disabled DataType_RBV
        factories = tuple(
            replace(f, array_description=eiger_array_description) if f.array_description is None else f
            for f in writer_factories
        )
        if data_logic and any(f.datakey_suffix == data_logic.datakey_suffix for f in factories):
            # describe() merges datakeys by dict update, so a collision silently drops a stream
            raise ValueError(
                "writer_factories must use a distinct datakey_suffix when the Eiger FileWriter data logic is enabled"
            )
        super().__init__(
            driver,
            prefix,
            *factories,
            acquire_logic=EigerAcquireLogic(driver, on_armed=data_logic.record_series if data_logic else None),
            trigger_logic=EigerTriggerLogic(driver),
            plugins=plugins,
            config_sigs=config_sigs,
            name=name,
        )
        if data_logic:
            self.add_detector_logics(data_logic)
