"""ophyd-async support for the SynApps scaler record (scalerRecord) and the
SIS calc-record extension.

https://github.com/epics-modules/scaler
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Annotated as A

from bluesky.protocols import Triggerable

from ophyd_async.core import (
    DEFAULT_TIMEOUT,
    AsyncStatus,
    DeviceVector,
    SignalR,
    SignalRW,
    StrictEnum,
    set_and_wait_for_value,
)
from ophyd_async.core import StandardReadableFormat as Format
from ophyd_async.core import StandardReadable
from ophyd_async.epics.core import (
    EpicsDevice,
    PvSuffix,
    epics_signal_r,
    epics_signal_rw,
)

__all__ = [
    "ScalerCountMode",
    "ScalerGate",
    "ScalerChannel",
    "ScalerCalculation",
    "Scaler",
]


class ScalerCountMode(StrictEnum):
    """Scaler record `.CONT` field: one-shot counting vs. free-running."""

    ONE_SHOT = "OneShot"
    AUTO_COUNT = "AutoCount"


class ScalerGate(StrictEnum):
    """Scaler channel `.Gn` gate-enable field."""

    NO = "N"
    YES = "Y"


class ScalerChannel(StandardReadable):
    """One numbered channel (`.Sn`/`.NMn`/`.PRn`/`.Gn`) of a scaler record.

    Parameters
    ----------
    prefix : str
        PV prefix of the parent scaler record.
    ch_num : int
        1-indexed scaler channel number.
    hinted : bool, default False
        If True, flag `value` as hinted.
    name : str, default ""
        Name of the device.
    """

    def __init__(
        self, prefix: str, ch_num: int, hinted: bool = False, name: str = ""
    ) -> None:
        self.ch_num = ch_num
        value_format = (
            Format.HINTED_UNCACHED_SIGNAL if hinted else Format.UNCACHED_SIGNAL
        )
        with self.add_children_as_readables(value_format):
            self.value = epics_signal_r(float, f"{prefix}.S{ch_num}")
        with self.add_children_as_readables(Format.CONFIG_SIGNAL):
            self.channel_name = epics_signal_rw(str, f"{prefix}.NM{ch_num}")
            self.preset = epics_signal_rw(float, f"{prefix}.PR{ch_num}")
            self.gate = epics_signal_rw(ScalerGate, f"{prefix}.G{ch_num}")
        super().__init__(name=name)


class ScalerCalculation(StandardReadable):
    """One `{prefix}_calcN` SIS scaler calc-record (the "SIS extension").

    Parameters
    ----------
    prefix : str
        PV prefix of the parent scaler record.
    calc_num : int
        1-indexed SIS calc record number (1-8).
    hinted : bool, default False
        If True, flag `value` as hinted.
    name : str, default ""
        Name of the device.
    """

    def __init__(
        self, prefix: str, calc_num: int, hinted: bool = False, name: str = ""
    ) -> None:
        self.calc_num = calc_num
        value_format = (
            Format.HINTED_UNCACHED_SIGNAL if hinted else Format.UNCACHED_SIGNAL
        )
        with self.add_children_as_readables(value_format):
            self.value = epics_signal_r(float, f"{prefix}_calc{calc_num}.VAL")
        with self.add_children_as_readables(Format.CONFIG_SIGNAL):
            self.equation = epics_signal_rw(str, f"{prefix}_calc{calc_num}.CALC$")
        super().__init__(name=name)


class Scaler(StandardReadable, EpicsDevice, Triggerable):
    """SynApps scaler record (`scalerRecord`), e.g. a Struck SIS3820 VME scaler.

    Covers plain step-scan counting (`trigger()`) and the optional SIS
    calc-record extension (`num_calculations`).

    Parameters
    ----------
    prefix : str
        PV prefix of the scaler record, for example ``"XF:99ID-ES{Sclr:1}"``.
    num_channels : int, default 32
        How many of the 32 hardware channels to connect.
    hinted_channels : sequence of int, default ()
        1-indexed channel numbers to flag hinted.
    num_calculations : int, default 0
        How many of the 8 SIS calc records to connect (0 disables it).
    hinted_calculations : sequence of int, default ()
        1-indexed calc numbers to flag hinted.
    name : str, default ""
        Name of the device.
    """

    count: A[SignalRW[int], PvSuffix(".CNT")]
    count_mode: A[SignalRW[ScalerCountMode], PvSuffix(".CONT"), Format.CONFIG_SIGNAL]
    delay: A[SignalRW[float], PvSuffix(".DLY"), Format.CONFIG_SIGNAL]
    auto_count_delay: A[SignalRW[float], PvSuffix(".DLY1"), Format.CONFIG_SIGNAL]
    elapsed_time: A[SignalR[float], PvSuffix(".T"), Format.UNCACHED_SIGNAL]
    frequency: A[SignalR[float], PvSuffix(".FREQ"), Format.CONFIG_SIGNAL]
    preset_time: A[SignalRW[float], PvSuffix(".TP"), Format.CONFIG_SIGNAL]
    auto_count_time: A[SignalRW[float], PvSuffix(".TP1"), Format.CONFIG_SIGNAL]
    update_rate: A[SignalRW[float], PvSuffix(".RATE")]
    auto_count_update_rate: A[SignalRW[float], PvSuffix(".RAT1")]
    egu: A[SignalRW[str], PvSuffix(".EGU"), Format.CONFIG_SIGNAL]

    def __init__(
        self,
        prefix: str,
        num_channels: int = 32,
        hinted_channels: Sequence[int] = (),
        num_calculations: int = 0,
        hinted_calculations: Sequence[int] = (),
        name: str = "",
    ) -> None:
        with self.add_children_as_readables():
            self.channels = DeviceVector(
                {
                    i: ScalerChannel(prefix, i, hinted=i in hinted_channels)
                    for i in range(1, num_channels + 1)
                }
            )
            if num_calculations:
                self.calculations = DeviceVector(
                    {
                        i: ScalerCalculation(
                            prefix, i, hinted=i in hinted_calculations
                        )
                        for i in range(1, num_calculations + 1)
                    }
                )
        if num_calculations:
            with self.add_children_as_readables(Format.CONFIG_SIGNAL):
                self.enable_calculations = epics_signal_rw(
                    bool, f"{prefix}_calcEnable"
                )
        super().__init__(prefix=prefix, name=name)

    @AsyncStatus.wrap
    async def stage(self) -> None:
        await self.count_mode.set(ScalerCountMode.ONE_SHOT)
        await super().stage()

    @AsyncStatus.wrap
    async def trigger(self) -> None:
        preset_time = await self.preset_time.get_value()
        timeout = preset_time + DEFAULT_TIMEOUT if preset_time > 0 else DEFAULT_TIMEOUT
        await set_and_wait_for_value(self.count, 1, match_value=0, timeout=timeout)
