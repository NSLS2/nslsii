"""ophyd-async support for the areaDetector ADEiger IOC (Dectris EIGER and EIGER2).

See https://areadetector.github.io/areaDetector/ADEiger/eiger.html. The detector is armed once
per scan; internally triggered points are software triggers into that series. Data is stored
either by the IOC's HDF5 plugin fed from the DCU stream (recommended) or as the DCU FileWriter
files saved by the IOC; see `EigerDetector`.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Callable, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import PureWindowsPath
from typing import Annotated as A
from typing import Literal, TypeVar

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
    NDFileIO,
    NDPluginBaseIO,
    trigger_info_from_num_images,
)
from ophyd_async.epics.core import PvSuffix, stop_busy_record, wait_for_good_state

# NumImages for unbounded requests; the driver clamps to the DCU limit and the readback
# reflects the clamped value.
_MAX_NUM_IMAGES = 999_999
# NumTriggers for internally triggered series: the number of software triggers a series accepts.
# Deliberately modest: with the FileWriter source the driver allocates one file slot per trigger
# at arm, and in both sources it computes NumImages * NumTriggers as a 32-bit int. A scan with
# more points simply exhausts the series, and the next point arms a new one.
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
    """Records common to Eiger1 and Eiger2 (``eigerBase.template``)."""

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
        # DataType_RBV and ColorMode_RBV are disabled in the template; derive them instead so
        # plugin writers describe the NDArrays correctly.
        self.data_type = derived_signal_r(
            _image_data_type, bit_depth=self.bit_depth_image, signed=self.signed_data
        )
        self.color_mode, _ = soft_signal_r_and_setter(ADBaseColorMode, ADBaseColorMode.MONO)


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


class Pilatus4DriverIO(Eiger2DriverIO):
    """Pilatus4 driver (``pilatus4.template``): Eiger2 plus two more thresholds."""

    threshold3_enable: A[SignalRW[bool], PvSuffix.rbv("Threshold3Enable")]
    threshold3_energy: A[SignalRW[float], PvSuffix.rbv("Threshold3Energy")]
    threshold4_enable: A[SignalRW[bool], PvSuffix.rbv("Threshold4Enable")]
    threshold4_energy: A[SignalRW[float], PvSuffix.rbv("Threshold4Energy")]


@dataclass
class EigerTriggerLogic(DetectorTriggerLogic):
    """Trigger logic for ADEiger.

    Every ``prepare_*`` ends any armed series (its parameters are latched at arm) and selects
    where the IOC gets frames from:

    - ``STREAM``: the DCU's ZMQ stream, with the FileWriter disabled. Frames are dropped if the
      IOC falls behind, which surfaces as a timeout.
    - ``FILE_WRITER``: DCU data files, one per trigger. Lossless.

    Parameters
    ----------
    driver : EigerDriverIO
        The Eiger driver.
    source : EigerDataSource, default EigerDataSource.STREAM
        ``STREAM`` when a plugin stores the data, ``FILE_WRITER`` with
        `EigerFileWriterDataLogic`.
    """

    driver: EigerDriverIO
    source: Literal[EigerDataSource.STREAM, EigerDataSource.FILE_WRITER] = EigerDataSource.STREAM

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

    async def _prepare_series(self, trigger_mode: EigerTriggerMode, manual: bool, num: int) -> None:
        # frames is what one software trigger (manual) or the whole series (external) produces,
        # and hence the size of one DCU data file
        frames = num or _MAX_NUM_IMAGES
        await self._disarm()
        await self.driver.trigger_mode.set(trigger_mode)
        coros = [
            self.driver.manual_trigger.set(manual),
            self.driver.num_images.set(frames if manual else 1),
            self.driver.num_triggers.set(_MAX_NUM_TRIGGERS if manual else frames),
            self.driver.data_source.set(self.source),
        ]
        if self.source is EigerDataSource.STREAM:
            coros += [
                self.driver.stream_enable.set(True),
                # The driver only downloads (and removes) DCU files when it is the FileWriter
                # source or saving them; a still-enabled FileWriter would fill the DCU disk.
                self.driver.fw_enable.set(False),
            ]
        else:
            coros += [
                self.driver.fw_enable.set(True),
                self.driver.save_files.set(True),
                # One DCU data file per trigger, so every trigger is counted as soon as its
                # file is downloaded
                # WARN: The file may not be flushed to disk yet.
                # See https://github.com/areaDetector/ADEiger/issues/112 for more.
                self.driver.fw_nimgs_per_file.set(frames),
            ]
        await asyncio.gather(*coros)
        if self.source is EigerDataSource.FILE_WRITER:
            images_per_file = await self.driver.fw_nimgs_per_file.get_value(cached=False)
            if images_per_file < frames:
                # The DCU clamps silently, and the data logic relies on one file per trigger
                raise ValueError(f"{frames} images per data file exceeds the detector limit of {images_per_file}")

    async def prepare_internal(self, num: int, livetime: float, deadtime: float) -> None:
        await self._prepare_series(EigerTriggerMode.INTERNAL_SERIES, manual=True, num=num)
        await self._set_exposure(livetime, deadtime)

    async def prepare_edge(self, num: int, livetime: float) -> None:
        # One edge -> one internally timed image
        await self._prepare_series(EigerTriggerMode.EXTERNAL_SERIES, manual=False, num=num)
        await self._set_exposure(livetime, 0.0)

    async def prepare_level(self, num: int) -> None:
        # Gate width is the exposure
        await self._prepare_series(EigerTriggerMode.EXTERNAL_ENABLE, manual=False, num=num)

    async def default_trigger_info(self):
        return await trigger_info_from_num_images(self.driver)


class EigerAcquireLogic(DetectorAcquireLogic):
    """Arm once per scan; each internally triggered point is a ``Trigger`` PV software trigger.

    Parameters
    ----------
    driver : EigerDriverIO
        The Eiger driver.
    on_file_start : callable, optional
        Called with ``(SequenceId, ArrayCounter)`` when the DCU FileWriter starts a data file:
        at every software trigger, or at arm for an externally triggered series.
    """

    def __init__(
        self,
        driver: EigerDriverIO,
        on_file_start: Callable[[int, int], None] | None = None,
    ) -> None:
        self.driver = driver
        self._on_file_start = on_file_start
        self.acquire_status: AsyncStatus | None = None

    async def start_acquiring(self) -> None:
        manual = await self.driver.manual_trigger.get_value()
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
            if not manual:
                # An externally triggered series is one data file
                await self._file_started()
        if manual:
            # Each software trigger fills one data file
            await self._file_started()
            await self.driver.trigger_.set(1.0)

    async def _file_started(self) -> None:
        if self._on_file_start is not None:
            # The driver posts SequenceId and Armed in the same callback batch, so the
            # SequenceId monitor may lag the Armed one; read both uncached.
            sequence_id, first_frame = await asyncio.gather(
                self.driver.sequence_id.get_value(cached=False),
                self.driver.array_counter.get_value(cached=False),
            )
            self._on_file_start(sequence_id, first_frame)

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


@dataclass
class _DataFile:
    sequence_id: int
    index: int
    first_frame: int
    bundle: ComposeStreamResourceBundle | None = None


class EigerFileWriterDataProvider(StreamableDataProvider):
    """Stream documents for the Eiger FileWriter data files of one scan.

    Every data file holds exactly one trigger's frames and gets one ``stream_resource``.
    `record_file` reports each file as the DCU starts it; files are named
    ``{filename}_{SequenceId}_data_{k:06d}.h5`` with ``k`` counting files within a series.

    Parameters
    ----------
    directory_uri : str
        URI of the directory holding the data files, with trailing separator.
    filename : str
        Filename stem given to ``FWNamePattern`` as ``{filename}_$id``.
    resource : StreamResourceInfo
        Description of the ``/entry/data/data`` dataset.
    collections_written_signal : SignalR[int]
        ``ArrayCounter``.
    """

    def __init__(
        self,
        directory_uri: str,
        filename: str,
        resource: StreamResourceInfo,
        collections_written_signal: SignalR[int],
    ) -> None:
        self.collections_written_signal = collections_written_signal
        self._directory_uri = directory_uri
        self._filename = filename
        self._resource = resource
        self._composer = ComposeStreamResource()
        self._files: list[_DataFile] = []
        # Position in _files of the file being emitted, and frames emitted so far
        self._current = 0
        self._emitted = 0

    def record_file(self, sequence_id: int, first_frame: int) -> None:
        """Register the data file the DCU is starting.

        Parameters
        ----------
        sequence_id : int
            ``SequenceId`` of the armed series.
        first_frame : int
            ``ArrayCounter`` now; frames from here on land in this file.
        """
        last = self._files[-1] if self._files else None
        index = last.index + 1 if last and last.sequence_id == sequence_id else 1
        self._files.append(_DataFile(sequence_id, index, first_frame))

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
        while self._emitted < collections_written:
            next_file = self._files[self._current + 1] if self._current + 1 < len(self._files) else None
            if next_file is not None and next_file.first_frame <= self._emitted:
                self._current += 1
                continue
            file = self._files[self._current]
            stop = collections_written if next_file is None else min(collections_written, next_file.first_frame)
            if file.bundle is None:
                file.bundle = self._composer(
                    mimetype="application/x-hdf5",
                    uri=f"{self._directory_uri}{self._filename}_{file.sequence_id}_data_{file.index:06d}.h5",
                    data_key=self._resource.data_key,
                    parameters={"chunk_shape": self._resource.chunk_shape, **self._resource.parameters},
                    uid=None,
                    validate=True,
                )
                yield "stream_resource", file.bundle.stream_resource_doc
            yield (
                "stream_datum",
                file.bundle.compose_stream_datum(
                    {
                        "start": (self._emitted - file.first_frame) // collections_per_event,
                        "stop": (stop - file.first_frame) // collections_per_event,
                    }
                ),
            )
            self._emitted = stop


@dataclass
class EigerFileWriterDataLogic(DetectorDataLogic):
    """Data logic referencing the DCU FileWriter files saved by the IOC.

    One data file and one ``stream_resource`` per trigger.

    Limitation: ADEiger has no PV that reports when a downloaded file has been written to disk,
    so documents are emitted when the file has been downloaded and decoded, while the IOC may
    still be writing it. In practice the write finishes first, but it is not guaranteed.

    Parameters
    ----------
    driver : EigerDriverIO
        The Eiger driver.
    path_provider : PathProvider
        Directory and filename for the data files.
    datakey_suffix : str, default ""
        Suffix appended to the detector name to form the data key.
    """

    driver: EigerDriverIO
    path_provider: PathProvider
    datakey_suffix: str = ""
    _provider: EigerFileWriterDataProvider | None = field(default=None, init=False, repr=False)

    def record_file(self, sequence_id: int, first_frame: int) -> None:
        provider = error_if_none(self._provider, "prepare_unbounded must run before the Eiger is armed")
        provider.record_file(sequence_id, first_frame)

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
            # StandardDetector counts from the raw ArrayCounter and the driver never resets it
            driver.array_counter.set(0),
        ]
        if isinstance(driver, Eiger2DriverIO):
            # Only the Legacy layout has a 3-D /entry/data/data
            coros.append(driver.fw_hdf5_format.set(EigerHDF5Format.LEGACY))
        await asyncio.gather(*coros)
        if not await driver.file_path_exists.get_value():
            raise FileNotFoundError(f"Path {directory} doesn't exist or not writable!")
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
            resource,
            driver.array_counter,
        )
        return self._provider


EigerDriverIOT = TypeVar("EigerDriverIOT", bound=EigerDriverIO)


class EigerDetector(AreaDetector[EigerDriverIOT]):
    """An ADEiger detector.

    The detector is armed once per scan and stays armed between points; ``unstage()`` ends the
    series. Data can be stored two ways:

    - ``ADWriterFactory.hdf(path_provider)`` (recommended): the IOC's HDF5 plugin writes one
      file per scan from the DCU stream. The Dectris master file is not produced.
    - ``fw_path_provider=``: the IOC saves the DCU FileWriter files, one per point, including
      the master file. Lossless, but see `EigerFileWriterDataLogic` for its limitation.

    Parameters
    ----------
    prefix : str
        EPICS PV prefix for the detector.
    *writer_factories : ADWriterFactory
        areaDetector file writer plugins. Use a non-empty ``datakey_suffix`` when combined with
        ``fw_path_provider``.
    driver_cls : type[EigerDriverIO]
        `Eiger1DriverIO`, `Eiger2DriverIO` or `Pilatus4DriverIO`; ``driver`` is typed
        accordingly.
    fw_path_provider : PathProvider, optional
        Save the DCU FileWriter files to this location.
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
        driver_cls: type[EigerDriverIOT],
        fw_path_provider: PathProvider | None = None,
        driver_suffix: str = "cam1:",
        plugins: Mapping[str, NDPluginBaseIO] | None = None,
        config_sigs: Sequence[SignalR] = (),
        name: str = "",
    ) -> None:
        driver = driver_cls(prefix + driver_suffix)
        data_logic = EigerFileWriterDataLogic(driver, fw_path_provider) if fw_path_provider else None
        if data_logic and any(f.datakey_suffix == data_logic.datakey_suffix for f in writer_factories):
            # describe() merges datakeys by dict update, so a collision silently drops a stream
            raise ValueError(
                "writer_factories must use a distinct datakey_suffix when the Eiger FileWriter data logic is enabled"
            )
        super().__init__(
            driver,
            prefix,
            *writer_factories,
            acquire_logic=EigerAcquireLogic(driver, on_file_start=data_logic.record_file if data_logic else None),
            trigger_logic=EigerTriggerLogic(
                driver, source=EigerDataSource.FILE_WRITER if data_logic else EigerDataSource.STREAM
            ),
            plugins=plugins,
            config_sigs=config_sigs,
            name=name,
        )
        if data_logic:
            self.add_detector_logics(data_logic)
