"""ophyd-async support for the SynApps scaler record (scalerRecord) and the
Struck SIS3820 multi-channel-scaler (MCS) and SIS calc-record extensions used
to build it into a fly-scan buffer.

Consolidates the following nearly-identical beamline implementations into one
configurable device: CSX/TES/XFM `Scaler`+`FixedScalerCH`+`ScalerMCA`, SRX
`SRXScaler`, FXI `FXIScaler`, IOS `DodgyEpicsScaler`, and HXN
`StruckScaler`/`HxnTriggeringScaler` (github.com/NSLS-II-HXN/hxntools).

https://github.com/epics-modules/scaler
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from functools import cached_property
from typing import Annotated as A

import numpy as np
from pydantic import Field, NonNegativeInt

from ophyd_async.core import (
    DEFAULT_TIMEOUT,
    Array1D,
    AsyncStatus,
    ConfinedModel,
    DeviceVector,
    FlyableLogic,
    SignalR,
    SignalRW,
    StandardFlyable,
    StrictEnum,
    TriggerableCommand,
    set_and_wait_for_value,
    wait_for_value,
)
from ophyd_async.core import StandardReadableFormat as Format
from ophyd_async.core import StandardReadable
from ophyd_async.epics.core import (
    EpicsDevice,
    PvSuffix,
    epics_signal_r,
    epics_signal_rw,
    epics_triggerable_command,
)

from bluesky.protocols import Triggerable

__all__ = [
    "ScalerCountMode",
    "ScalerGate",
    "ScalerChannelAdvance",
    "ScalerChannel",
    "ScalerCalculation",
    "ScalerMCS",
    "ScalerMCSFlyInfo",
    "ScalerMCSFlyableLogic",
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


class ScalerChannelAdvance(StrictEnum):
    """MCS `ChannelAdvance` field: what paces the multi-channel-scaler buffer."""

    INTERNAL = "Internal"
    EXTERNAL = "External"


class ScalerChannel(StandardReadable):
    """One numbered channel (`.Sn`/`.NMn`/`.PRn`/`.Gn`) of a scaler record.

    `value` is read with `Format.UNCACHED_SIGNAL` (or `HINTED_UNCACHED_SIGNAL`
    if `hinted=True`): every `read()` does an explicit, uncached get rather
    than relying on a cached monitor value. Combined with `Scaler.trigger()`
    only completing once counting has actually finished (see below), this
    removes the race that motivated the classic `EpicsSignalROLazyier`/
    `DodgyEpicsSignal` retry-on-`None` subclasses: there is no longer a window
    where a stale/absent value can be read back.

    Parameters
    ----------
    prefix : str
        PV prefix of the parent scaler record, for example
        ``"XF:99ID-ES{Sclr:1}"``.
    ch_num : int
        1-indexed scaler channel number, used to build the ``.Sn``/``.NMn``/
        ``.PRn``/``.Gn`` suffixes.
    hinted : bool, default False
        If True, flag `value` as hinted (shown in the default LiveTable/LivePlot).
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

    Matches HXN's `hxntools.struck_scaler.EpicsScalerWithCalc`/
    `MinimalCalcRecord`: 8 calc records (`_calc1`..`_calc8`), each with a
    `.VAL` readback and a `.CALC$` equation string.

    Parameters
    ----------
    prefix : str
        PV prefix of the parent scaler record.
    calc_num : int
        1-indexed SIS calc record number (1-8).
    hinted : bool, default False
        If True, flag `value` as hinted (shown in the default LiveTable/LivePlot).
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


class ScalerMCSFlyInfo(ConfinedModel):
    """Info for a `ScalerMCS` fly scan, passed to `bps.prepare(scaler.mcs, ...)`."""

    number_of_points: NonNegativeInt = Field(
        description="number of external channel-advance pulses to buffer"
    )
    dwell_time: float = Field(
        default=0.0,
        ge=0,
        description=(
            "fixed dwell time per point; 0.0 means paced entirely by the "
            "external advance pulses rather than an internal timer"
        ),
    )


@dataclass
class ScalerMCSFlyableLogic(FlyableLogic[ScalerMCSFlyInfo, None]):
    """Fly-control logic for a `ScalerMCS`, backing its `flyable_logic`.

    Mirrors the PandA `StaticSeqTableFlyableLogic`/`StaticPcompFlyableLogic`
    (`ophyd_async.fastcs.panda._fly_logic`): a plain `@dataclass` holding the
    signals it drives, carrying no state between `on_prepare`/`on_kickoff`/
    `on_complete` (hence `FlyableLogic[ScalerMCSFlyInfo, None]`) since
    `StandardFlyable` threads the prepare -> kickoff -> complete context and
    enforces call ordering for us.
    """

    channel_advance: SignalRW[ScalerChannelAdvance]
    nuse_all: SignalRW[int]
    preset_real: SignalRW[float]
    dwell: SignalRW[float]
    erase_start: TriggerableCommand
    stop_all: TriggerableCommand
    acquiring: SignalR[bool]

    async def on_prepare(self, value: ScalerMCSFlyInfo) -> None:
        await self.stop_all.trigger()
        await self.channel_advance.set(ScalerChannelAdvance.EXTERNAL)
        await self.nuse_all.set(value.number_of_points)
        await self.preset_real.set(0.0)
        await self.dwell.set(value.dwell_time)

    async def on_kickoff(self, ctx: None) -> None:
        await self.erase_start.trigger()
        await wait_for_value(self.acquiring, True, timeout=1)

    async def on_complete(self, ctx: None) -> None:
        await wait_for_value(self.acquiring, False, timeout=None)

    async def stop(self) -> None:
        await self.stop_all.trigger()
        await wait_for_value(self.acquiring, False, timeout=1)


class ScalerMCS(StandardFlyable[ScalerMCSFlyInfo, None], StandardReadable):
    """Struck SIS3820 multi-channel-scaler (MCS) buffer/fly-mode extension.

    Instead of one scalar per `trigger()`, the MCS accumulates one reading per
    hardware channel-advance pulse into a per-channel waveform ("mcaN"),
    paced externally (e.g. by a zebra or encoder) rather than by `.CNT`. This
    is the "MCA fly buffer" mode used by SRX/FXI/HXN fly scans and replaces
    HXN's `HxnTriggeringScaler`/`HxnModalBase` internal-vs-external mode
    switch.

    `ScalerMCS` is always a flyer: it mixes in `StandardFlyable` directly
    (the same pattern `ophyd_async.epics.motor.Motor` uses), so a plan drives
    it with the ordinary `Preparable`/`Flyable` plan stubs --
    `bps.prepare(scaler.mcs, ScalerMCSFlyInfo(...))`, `bps.kickoff(scaler.mcs)`,
    `bps.complete(scaler.mcs)` -- with no separate flyer object to construct.
    The actual arm/kickoff/complete sequence lives in `ScalerMCSFlyableLogic`,
    a plain signal-holding logic object (not the device itself), matching how
    `Motor`/`MotorFlyableMovableLogic` keep device and logic separate.

    `buffers` are read directly (`await scaler.mcs.buffers[n].get_value()`)
    after `bps.complete(scaler.mcs)` returns, not through `read()`/
    `describe()`: they hold one fly-scan's worth of samples, not a
    single-point reading, matching how every fetched beamline implementation
    already consumes them (a bulk `.get()` after the external scan finishes,
    not a per-event callback). There is no `StandardDetector`/`DetectorDataLogic`
    backing these buffers: no fetched beamline implementation streams them
    through a bluesky-managed persistent writer (SRX exports them via a
    bespoke `ZebraSaver` caproto IOC entirely outside of ophyd), and
    `StandardDetector.kickoff()` hard-requires at least one streamable
    `DetectorDataLogic` provider (`ophyd_async.core._detector.StandardDetector.kickoff`,
    raises `ValueError` otherwise) -- every first-party streamable
    `DetectorDataLogic` in ophyd-async 0.21.3 is HDF/TIFF/ODIN file-writer
    backed, so forcing this extension through `StandardDetector` would mean
    inventing a fake streaming data logic with no real consumer, which is
    more complexity than the flyer approach, not less.

    Parameters
    ----------
    prefix : str
        PV prefix of the parent scaler record.
    num_channels : int, default 32
        How many of the 32 hardware MCS buffer channels to connect.
    mca_suffix : callable(int) -> str, default ``lambda i: f"mca{i}"``
        Per-channel PV suffix template. Sites differ: CSX/TES use zero-padded
        ``mca{01..20}``, SRX/FXI use unpadded ``mca{1..32}``, HXN uses
        ``Mca:{1..32}`` (colon). Default matches SRX/FXI.
    name : str, default ""
        Name of the device.

    Notes
    -----
    Several MCS fields are typed `str` rather than a `StrictEnum` because
    their exact mbbo/mbbi state tables could not be confirmed from any
    fetched beamline source (only `ChannelAdvance`'s "Internal"/"External"
    and `CountOnStart`'s "Yes" are confirmed literal values actually put to
    the PV in the fetched sources) -- unverified, confirm against the SIS3820
    MCS IOC `.dbd`/template before narrowing these to a `StrictEnum`.

    The following fields declared on `StruckScaler`/`SRXScaler`/`FXIScaler`
    are deliberately omitted: `asyn`, `client_wait`, `enable_client_wait`,
    `set_client_wait`, `snl_connected`, `user_led`, `wfrm`, `mux_output`,
    `do_read_all`, `read_all`, `read_all_once`, `set_acquiring`. None of these
    are referenced anywhere outside their bare declaration in any of the 6
    fetched beamline implementations (SNL-sequencer/vendor-diagnostic
    plumbing); add one if a site needs it.
    """

    def __init__(
        self,
        prefix: str,
        num_channels: int = 32,
        mca_suffix: Callable[[int], str] = lambda i: f"mca{i}",
        name: str = "",
    ) -> None:
        self.buffers = DeviceVector(
            {
                i: epics_signal_r(Array1D[np.float64], f"{prefix}{mca_suffix(i)}")
                for i in range(1, num_channels + 1)
            }
        )
        with self.add_children_as_readables(Format.CONFIG_SIGNAL):
            self.acquire_mode = epics_signal_rw(str, f"{prefix}AcquireMode")
            self.input_mode = epics_signal_rw(str, f"{prefix}InputMode")
            self.output_mode = epics_signal_rw(str, f"{prefix}OutputMode")
            self.output_polarity = epics_signal_rw(str, f"{prefix}OutputPolarity")
            self.channel_advance = epics_signal_rw(
                ScalerChannelAdvance, f"{prefix}ChannelAdvance"
            )
            self.nuse_all = epics_signal_rw(int, f"{prefix}NuseAll")
            self.prescale = epics_signal_rw(float, f"{prefix}Prescale")
            self.preset_real = epics_signal_rw(float, f"{prefix}PresetReal")
            self.dwell = epics_signal_rw(float, f"{prefix}Dwell")
            self.channel1_source = epics_signal_rw(str, f"{prefix}Channel1Source")
            self.count_on_start = epics_signal_rw(str, f"{prefix}CountOnStart")
            self.disable_auto_count = epics_signal_rw(
                str, f"{prefix}DisableAutoCount"
            )
        self.acquiring = epics_signal_r(bool, f"{prefix}Acquiring")
        self.hardware_acquiring = epics_signal_r(bool, f"{prefix}HardwareAcquiring")
        self.current_channel = epics_signal_r(int, f"{prefix}CurrentChannel")
        self.elapsed_real = epics_signal_r(float, f"{prefix}ElapsedReal")
        self.max_channels = epics_signal_r(int, f"{prefix}MaxChannels")
        self.model = epics_signal_r(str, f"{prefix}Model")
        self.firmware = epics_signal_r(str, f"{prefix}Firmware")
        self.software_channel_advance = epics_triggerable_command(
            f"{prefix}SoftwareChannelAdvance"
        )
        self.start_all = epics_triggerable_command(f"{prefix}StartAll")
        self.stop_all = epics_triggerable_command(f"{prefix}StopAll")
        self.erase_all = epics_triggerable_command(f"{prefix}EraseAll")
        self.erase_start = epics_triggerable_command(f"{prefix}EraseStart")
        super().__init__(name=name)

    @cached_property
    def flyable_logic(self) -> ScalerMCSFlyableLogic:
        return ScalerMCSFlyableLogic(
            channel_advance=self.channel_advance,
            nuse_all=self.nuse_all,
            preset_real=self.preset_real,
            dwell=self.dwell,
            erase_start=self.erase_start,
            stop_all=self.stop_all,
            acquiring=self.acquiring,
        )


class Scaler(StandardReadable, EpicsDevice, Triggerable):
    """SynApps scaler record (`scalerRecord`), e.g. a Struck SIS3820 VME scaler.

    Covers plain step-scan counting (`trigger()`), the optional SIS
    calc-record extension (`num_calculations`), and the optional SIS3820 MCS
    fly-buffer extension (`with_mcs`, exposed as `self.mcs`; `self.mcs` is
    itself `Preparable`/`Flyable`, see `ScalerMCS`).

    Parameters
    ----------
    prefix : str
        PV prefix of the scaler record, for example ``"XF:99ID-ES{Sclr:1}"``.
    num_channels : int, default 32
        How many of the 32 hardware channels to connect.
    hinted_channels : sequence of int, default ()
        1-indexed channel numbers to flag hinted (shown in the default
        LiveTable/LivePlot); all `num_channels` channels are always connected
        and included in `read()`/`describe()` regardless.
    num_calculations : int, default 0
        How many of the 8 SIS calc records to connect (0 disables the
        extension entirely; HXN uses 8).
    hinted_calculations : sequence of int, default ()
        1-indexed calc numbers to flag hinted.
    with_mcs : bool, default False
        Attach the SIS3820 MCS fly-buffer extension as `self.mcs`
        (`ScalerMCS`).
    mca_suffix : callable(int) -> str, default ``lambda i: f"mca{i}"``
        Per-channel MCS waveform PV suffix template, see `ScalerMCS`.
    name : str, default ""
        Name of the device.

    Notes
    -----
    Per-scan dynamic channel-count changes (SRX's classic `set_num_channels`)
    are not reproduced: `num_channels`/`hinted_channels` are fixed at
    construction, matching how every other ophyd-async device (e.g.
    `ophyd_async.epics.motor.Motor`) exposes a static, connect-time-defined
    `read()`/`describe()` schema. A site that needs a different active
    channel count for different experiments should construct a `Scaler` with
    the channel set it needs.

    `self.mcs` is a plain attribute, not added via
    `add_children_as_readables()`: it is deliberately excluded from
    `Scaler`'s own `read()`/`read_configuration()` and from the `stage()`/
    `unstage()` cascade (`Scaler.stage()` never arms/disarms the MCS). A plan
    that wants to fly stages `scaler.mcs` itself, the same way it would stage
    any other `Flyable` participant in the scan.
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
        with_mcs: bool = False,
        mca_suffix: Callable[[int], str] = lambda i: f"mca{i}",
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
        if with_mcs:
            self.mcs = ScalerMCS(prefix, num_channels=num_channels, mca_suffix=mca_suffix)
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
