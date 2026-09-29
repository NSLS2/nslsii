from typing import Annotated as A

import numpy as np
from ophyd_async.core import Array1D, Device, SignalR, SignalRW, StrictEnum
from ophyd_async.epics.adcore import ADBaseIO, NDROIStatIO
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
