"""ADCore IO extensions not yet supplied by ophyd-async."""

from collections.abc import Mapping
from typing import Annotated as A

import numpy as np

from ophyd_async.core import DeviceMap, SignalR, SignalRW, non_zero
from ophyd_async.epics.adcore import NDPluginBaseIO
from ophyd_async.epics.core import EpicsDevice, EpicsOptions, PvSuffix

__all__ = ["NDTimeSeriesIO", "NDTimeSeriesNIO"]


class NDTimeSeriesNIO(EpicsDevice):
    """Records for one signal address in an ``NDTimeSeries`` plugin.

    Parameters
    ----------
    prefix : str
        PV prefix of this channel record set, such as ``"PREFIX:TS:Current1:"``.
    name : str, optional
        Ophyd device name.
    """

    name_: A[SignalRW[str], PvSuffix("Name")]
    time_series: A[SignalR[np.ndarray], PvSuffix("TimeSeries")]


class NDTimeSeriesIO(NDPluginBaseIO):
    """Generic ADCore ``NDTimeSeries`` plugin IO.

    Parameters
    ----------
    prefix : str
        PV prefix of the ``NDTimeSeries`` plugin, such as ``"PREFIX:TS:"``.
    channels : Mapping[str, str], optional
        Mapping from a user-facing channel key to its suffix below ``prefix``.
        For example, ``{"current_1": "Current1:"}`` creates
        ``channels["current_1"]`` at ``"PREFIX:TS:Current1:"``.
    name : str, optional
        Ophyd device name.

    Notes
    -----
    ``NDTimeSeriesIO`` owns shared plugin configuration and acquisition state.
    Each ``NDTimeSeriesNIO`` child exposes the waveform for one plugin address.
    """
    ts_acquire: A[SignalRW[bool], PvSuffix("TSAcquire"), EpicsOptions(wait=non_zero)]
    ts_acquiring: A[SignalR[bool], PvSuffix("TSAcquiring")]
    ts_read: A[SignalRW[bool], PvSuffix("TSRead")]
    ts_num_points: A[SignalRW[int], PvSuffix("TSNumPoints")]
    ts_current_point: A[SignalR[int], PvSuffix("TSCurrentPoint")]
    ts_time_per_point_link: A[SignalRW[float], PvSuffix("TSTimePerPointLink")]
    ts_time_per_point: A[SignalRW[float], PvSuffix.rbv("TSTimePerPoint")]
    ts_averaging_time: A[SignalRW[float], PvSuffix.rbv("TSAveragingTime")]
    ts_num_average: A[SignalR[int], PvSuffix("TSNumAverage")]
    ts_elapsed_time: A[SignalR[float], PvSuffix("TSElapsedTime")]
    ts_acquire_mode: A[SignalRW[str], PvSuffix.rbv("TSAcquireMode")]
    ts_timestamp: A[SignalR[np.ndarray], PvSuffix("TSTimestamp")]
    ts_time_axis: A[SignalR[np.ndarray], PvSuffix("TSTimeAxis")]

    def __init__(
        self,
        prefix: str,
        *,
        channels: Mapping[str, str] | None = None,
        name: str = "",
    ) -> None:
        self.channels = DeviceMap(
            {
                channel_name: NDTimeSeriesNIO(f"{prefix}{channel_suffix}")
                for channel_name, channel_suffix in (channels or {}).items()
            }
        )
        super().__init__(prefix=prefix, name=name)
