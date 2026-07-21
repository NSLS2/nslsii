import time
from typing import Annotated as A

from ophyd_async.core import (
    AsyncStatus,
    SignalR,
    SignalRW,
    StandardReadable,
    StrictEnum,
    observe_value,
)
from ophyd_async.core import StandardReadableFormat as Format
from ophyd_async.epics.core import EpicsDevice, PvSuffix


class Eurotherm3kControlMode(StrictEnum):
    AUTOMATIC = "Automatic"
    MANUAL = "Manual"


class Eurotherm3kOnOff(StrictEnum):
    OFF = "Off"
    ON = "On"


class Eurotherm3kRampRateUnit(StrictEnum):
    PER_SECOND = "C/s"
    PER_MINUTE = "C/min"


class Eurotherm3kLoop(StandardReadable, EpicsDevice):
    """One PID control loop of a Eurotherm 3000-series controller.

    Behaves as a settable temperature axis: ``set(T)`` writes the setpoint and
    completes once the readback has settled within ``tolerance`` of ``T``.
    """

    # Settle behaviour (override per instance, e.g. ``dev.loop1.tolerance = 0.5``)
    tolerance = 1.0  # deg: |readback - setpoint| band counted as "there"
    settle_time = 0.0  # s: must stay in band this long (0.0 = first touch)
    move_timeout = 3600.0  # s: overall timeout for a move

    setpoint: A[SignalRW[float], PvSuffix.rbv("SP", ":RBV")]
    readback: A[SignalR[float], PvSuffix("PV:RBV"), Format.HINTED_SIGNAL]
    working_setpoint: A[SignalR[float], PvSuffix("WSP:RBV")]
    ramp_rate: A[SignalRW[float], PvSuffix.rbv("RR", ":RBV"), Format.CONFIG_SIGNAL]
    ramp_rate_unit: A[
        SignalR[Eurotherm3kRampRateUnit], PvSuffix("RR:UNIT"), Format.CONFIG_SIGNAL
    ]
    p: A[SignalRW[float], PvSuffix.rbv("P", ":RBV"), Format.CONFIG_SIGNAL]
    i: A[SignalRW[float], PvSuffix.rbv("I", ":RBV"), Format.CONFIG_SIGNAL]
    d: A[SignalRW[float], PvSuffix.rbv("D", ":RBV"), Format.CONFIG_SIGNAL]
    control_mode: A[
        SignalRW[Eurotherm3kControlMode],
        PvSuffix.rbv("MAN", ":RBV"),
        Format.CONFIG_SIGNAL,
    ]
    autotune: A[SignalRW[Eurotherm3kOnOff], PvSuffix.rbv("AUTOTUNE", ":RBV")]
    output: A[SignalRW[float], PvSuffix.rbv("O", ":RBV")]
    output_high: A[
        SignalRW[float], PvSuffix.rbv("OUTPHI", ":RBV"), Format.CONFIG_SIGNAL
    ]
    output_low: A[SignalRW[float], PvSuffix.rbv("OUTPLO", ":RBV"), Format.CONFIG_SIGNAL]
    loop_break_time: A[
        SignalRW[float], PvSuffix.rbv("LBT", ":RBV"), Format.CONFIG_SIGNAL
    ]

    @AsyncStatus.wrap
    async def set(self, value: float) -> None:
        # 1. command the setpoint (waits for the write to be accepted)
        await self.setpoint.set(value)
        # 2. watch the readback until it settles, or time out overall
        in_band_since: float | None = None
        async for current in observe_value(
            self.readback, done_timeout=self.move_timeout
        ):
            if abs(current - value) <= self.tolerance:
                if self.settle_time <= 0:
                    return  # "first touch" mode: done
                now = time.monotonic()
                if in_band_since is None:
                    in_band_since = now  # entered the band, start the hold timer
                elif now - in_band_since >= self.settle_time:
                    return  # held in band long enough: done
            else:
                in_band_since = None  # left the band, reset the hold timer


class Eurotherm3k(StandardReadable, EpicsDevice):
    """Eurotherm 3000-series controller (nsls2.ioc_deploy eurotherm3k role)."""

    loop1: A[Eurotherm3kLoop, PvSuffix("LOOP1:"), Format.CHILD]
    loop2: A[Eurotherm3kLoop, PvSuffix("LOOP2:"), Format.CHILD]

    # --- Global (controller-wide) ---
    disable: A[SignalRW[str], PvSuffix("DISABLE"), Format.CONFIG_SIGNAL]
    programmer_number: A[
        SignalRW[int], PvSuffix.rbv("PROGNUM", ":RBV"), Format.CONFIG_SIGNAL
    ]
    programmer_run: A[SignalRW[bool], PvSuffix.rbv("PROGRUN", ":RBV")]
    programmer_reset: A[SignalRW[bool], PvSuffix("PROGRESET")]
    programmer_status: A[SignalR[str], PvSuffix("PROGSTAT:RBV")]
    recipe_select: A[
        SignalRW[str], PvSuffix.rbv("RECSEL", ":RBV"), Format.CONFIG_SIGNAL
    ]
    recipe_status: A[SignalR[str], PvSuffix("RECSTAT:RBV")]
