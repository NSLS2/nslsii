import asyncio
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Annotated as A

import numpy as np
from ophyd_async.core import (
    Array1D,
    Device,
    DetectorTriggerLogic,
    DeviceVector,
    SignalDict,
    SignalR,
    SignalRW,
    StrictEnum,
)
from ophyd_async.epics.adcore import (
    ADAcquireLogic,
    ADBaseIO,
    ADWriterFactory,
    AreaDetector,
    NDPluginBaseIO,
    NDROIStatIO,
    trigger_info_from_num_images,
)
from ophyd_async.epics.core import EpicsDevice, PvSuffix, epics_signal_r


class Xspress3TriggerMode(StrictEnum):
    """Trigger modes of the Xspress3 driver (``det1:TriggerMode``)."""

    SOFTWARE = "Software"
    INTERNAL = "Internal"
    IDC = "IDC"
    TTL_VETO_ONLY = "TTL Veto Only"
    TTL_BOTH = "TTL Both"
    LVDS_VETO_ONLY = "LVDS Veto Only"
    LVDS_BOTH = "LVDS Both"
    SOFTWARE_INTERNAL = "Software + Internal"
    TTL_INTERNAL = "TTL + Internal"


class Xspress3DriverIO(ADBaseIO):
    """Driver for Quantum Detectors Xspress3 readout electronics.

    This mirrors the interface provided by xspress3App/Db/xspress3.template.
    Note that the template disables ImageMode, AcquirePeriod, DataType and
    ColorMode: the number of frames is set with NumImages only, and the data
    type follows the deadtime correction setting (UInt32 off, Float64 on).
    """

    trigger_mode: A[SignalRW[Xspress3TriggerMode], PvSuffix.rbv("TriggerMode")]
    erase: A[SignalRW[bool], PvSuffix("ERASE")]
    # No readback record for EraseOnStart
    erase_on_start: A[SignalRW[bool], PvSuffix("EraseOnStart")]
    deadtime_correction: A[SignalRW[bool], PvSuffix.rbv("CTRL_DTC")]

    # Trailing underscore as Device.connect() already exists
    connect_: A[SignalRW[bool], PvSuffix("CONNECT")]
    disconnect: A[SignalRW[bool], PvSuffix("DISCONNECT")]
    connected: A[SignalR[bool], PvSuffix("CONNECTED")]

    num_channels: A[SignalR[int], PvSuffix("NUM_CHANNELS_RBV")]
    max_num_channels: A[SignalR[int], PvSuffix("MAX_NUM_CHANNELS_RBV")]
    num_cards: A[SignalR[int], PvSuffix("NUM_CARDS_RBV")]
    max_frames: A[SignalR[int], PvSuffix("MAX_FRAMES_RBV")]
    frame_count: A[SignalR[int], PvSuffix("FRAME_COUNT_RBV")]
    max_spectra: A[SignalR[int], PvSuffix("MAX_SPECTRA_RBV")]

    config_path: A[SignalRW[str], PvSuffix.rbv("CONFIG_PATH")]


class Xspress3SCAIO(EpicsDevice):
    """One SCA (scaler) attribute of a channel.

    This mirrors the interface provided by ADCore/db/NDAttributeN.template,
    loaded for each scaler by iocBoot/common/DefineSCAROI.cmd.
    """

    value: A[SignalR[float], PvSuffix("Value_RBV")]
    value_sum: A[SignalR[float], PvSuffix("ValueSum_RBV")]


class Xspress3ChannelIO(Device):
    """Per-channel spectra, ROIs and scalers of an Xspress3.

    The PVs come from the plugins set up by iocBoot/common/DefineSCAROI.cmd for
    each channel ``n``: ``MCAn:`` / ``MCASUMn:`` std arrays, the ``MCAnROI:``
    ROI stats plugin and the ``CnSCA:`` attribute plugin.

    :param prefix: EPICS PV prefix of the whole detector, e.g. ``XSP3_4Chan:``
    :param channel: Channel number, starting from 1
    :param num_rois: Number of ROIs to connect to, the IOC defines up to 48
    """

    def __init__(self, prefix: str, channel: int, num_rois: int = 8, name: str = ""):
        # Spectrum of the latest frame, and summed over the acquisition by PROC1
        self.spectrum = epics_signal_r(
            Array1D[np.float64], f"{prefix}MCA{channel}:ArrayData"
        )
        self.spectrum_sum = epics_signal_r(
            Array1D[np.float64], f"{prefix}MCASUM{channel}:ArrayData"
        )
        self.rois = NDROIStatIO(f"{prefix}MCA{channel}ROI:", num_channels=num_rois)

        # Scaler layout from XSP3_SCALER_* in xspress3Support/xspress3.h, plus
        # the extra deadtime attributes the driver adds after them
        sca = f"{prefix}C{channel}SCA:"
        self.time = Xspress3SCAIO(f"{sca}0:")
        self.reset_ticks = Xspress3SCAIO(f"{sca}1:")
        self.reset_count = Xspress3SCAIO(f"{sca}2:")
        self.all_event = Xspress3SCAIO(f"{sca}3:")
        self.all_good = Xspress3SCAIO(f"{sca}4:")
        self.in_window_0 = Xspress3SCAIO(f"{sca}5:")
        self.in_window_1 = Xspress3SCAIO(f"{sca}6:")
        self.pileup = Xspress3SCAIO(f"{sca}7:")
        self.event_width = Xspress3SCAIO(f"{sca}8:")
        self.dead_time_factor = Xspress3SCAIO(f"{sca}9:")
        self.dead_time_percent = Xspress3SCAIO(f"{sca}10:")
        super().__init__(name=name)


#: The driver sets up the internal time frame generator with
#: XSP3_ITFG_GAP_MODE_1US, i.e. a 1 microsecond gap between frames
XSPRESS3_MIN_DEADTIME = 1e-6


@dataclass
class Xspress3TriggerLogic(DetectorTriggerLogic):
    """Trigger logic for the Xspress3.

    - internal: frames of ``livetime`` timed by the Xspress3's own frame
      generator ("Internal")
    - external edge: each TTL rising edge starts a frame of ``livetime``
      ("TTL + Internal")
    - external level: frames are the high periods of the TTL gate
      ("TTL Veto Only")

    The Xspress3 has no continuous image mode, so an unbounded acquisition
    (``num=0``) asks for as many frames as the IOC was configured for.

    :param driver: The Xspress3 driver
    :param deadtime: Minimum gap between externally triggered frames
    """

    driver: Xspress3DriverIO
    deadtime: float = XSPRESS3_MIN_DEADTIME

    def get_deadtime(self, config_values: SignalDict) -> float:
        return self.deadtime

    async def _prepare(
        self, trigger_mode: Xspress3TriggerMode, num: int, livetime: float = 0.0
    ):
        if num == 0:
            num = await self.driver.max_frames.get_value()
        coros = [
            self.driver.trigger_mode.set(trigger_mode),
            self.driver.num_images.set(num),
            # Otherwise spectra accumulate on top of the previous acquisition
            self.driver.erase_on_start.set(True),
        ]
        if livetime:
            coros.append(self.driver.acquire_time.set(livetime))
        await asyncio.gather(*coros)

    async def prepare_internal(self, num: int, livetime: float, deadtime: float):
        # The gap between internal frames is fixed by the driver, so deadtime
        # can't be set
        await self._prepare(Xspress3TriggerMode.INTERNAL, num, livetime)

    async def prepare_edge(self, num: int, livetime: float):
        await self._prepare(Xspress3TriggerMode.TTL_INTERNAL, num, livetime)

    async def prepare_level(self, num: int):
        await self._prepare(Xspress3TriggerMode.TTL_VETO_ONLY, num)

    async def default_trigger_info(self):
        return await trigger_info_from_num_images(self.driver)


class Xspress3Detector(AreaDetector[Xspress3DriverIO]):
    """Create an Xspress3 AreaDetector instance.

    The PV layout matches the example IOCs in the xspress3 module
    (iocs/xspress3IOC/iocBoot), i.e. the driver at ``{prefix}det1:`` and the
    HDF5 writer at ``{prefix}HDF1:``, so ``ADWriterFactory.hdf(path_provider)``
    works with its defaults.

    :param prefix: EPICS PV prefix for the detector, e.g. ``XSP3_4Chan:``
    :param writer_factories: Factories for file writer plugins and their data logics
    :param num_channels: Number of detector elements the IOC was started with
    :param num_rois: Number of ROIs per channel to connect to (up to 48)
    :param deadtime: Minimum gap between externally triggered frames
    :param driver_suffix: Suffix for the driver PV, defaults to "det1:"
    :param plugins: Additional areaDetector plugins to include
    :param config_sigs: Additional signals to include in configuration
    :param name: Name for the detector device
    """

    def __init__(
        self,
        prefix: str,
        *writer_factories: ADWriterFactory,
        num_channels: int,
        num_rois: int = 8,
        deadtime: float = XSPRESS3_MIN_DEADTIME,
        driver_suffix: str = "det1:",
        plugins: dict[str, NDPluginBaseIO] | None = None,
        config_sigs: Sequence[SignalR] = (),
        name: str = "",
    ) -> None:
        driver = Xspress3DriverIO(prefix + driver_suffix)
        self.channels = DeviceVector(
            {
                i: Xspress3ChannelIO(prefix, i, num_rois=num_rois)
                for i in range(1, num_channels + 1)
            }
        )
        super().__init__(
            driver,
            prefix,
            *writer_factories,
            acquire_logic=ADAcquireLogic(driver),
            trigger_logic=Xspress3TriggerLogic(driver, deadtime),
            plugins=plugins,
            # Deadtime correction changes the data type of the frames
            config_sigs=(driver.deadtime_correction, *config_sigs),
            name=name,
        )
