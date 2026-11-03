from dataclasses import dataclass
from functools import cached_property
from typing import Annotated as A

from ophyd_async.core import (
    MovableLogic,
    SignalR,
    SignalRW,
    StandardMovable,
    StandardReadable,
    StrictEnum,
    TimeoutCalculator,
    set_and_wait_for_other_value,
    wait_for_value,
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


@dataclass
class _Eurotherm3kLoopLogic(MovableLogic[float]):
    loop: "Eurotherm3kLoop"

    async def calculate_timeout(self, old_position: float, new_position: float) -> float:
        return self.loop.move_timeout

    async def move(self, new_position: float, timeout: TimeoutCalculator) -> None:
        def in_band(value: float) -> bool:
            return abs(value - new_position) <= self.loop.tolerance

        def out_of_band(value: float) -> bool:
            return not in_band(value)

        await set_and_wait_for_other_value(
            self.setpoint,
            new_position,
            self.readback,
            in_band,
            timeout=timeout(),
        )
        if self.loop.settle_time <= 0:
            return

        while True:
            settle_time = self.loop.settle_time
            if settle_time <= 0:
                return
            remaining_timeout = timeout()
            hold_timeout = (
                settle_time if remaining_timeout is None or remaining_timeout >= settle_time else remaining_timeout
            )
            try:
                await wait_for_value(self.readback, out_of_band, timeout=hold_timeout)
            except TimeoutError:
                if remaining_timeout is None or remaining_timeout >= settle_time:
                    return
                raise
            await wait_for_value(self.readback, in_band, timeout=timeout())


class Eurotherm3kLoop(EpicsDevice, StandardReadable, StandardMovable[float]):
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
    ramp_rate_unit: A[SignalR[Eurotherm3kRampRateUnit], PvSuffix("RR:UNIT"), Format.CONFIG_SIGNAL]
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
    output_high: A[SignalRW[float], PvSuffix.rbv("OUTPHI", ":RBV"), Format.CONFIG_SIGNAL]
    output_low: A[SignalRW[float], PvSuffix.rbv("OUTPLO", ":RBV"), Format.CONFIG_SIGNAL]
    loop_break_time: A[SignalRW[float], PvSuffix.rbv("LBT", ":RBV"), Format.CONFIG_SIGNAL]

    @cached_property
    def movable_logic(self) -> MovableLogic[float]:
        return _Eurotherm3kLoopLogic(setpoint=self.setpoint, readback=self.readback, loop=self)


class Eurotherm3k(StandardReadable, EpicsDevice):
    """Eurotherm 3000-series controller (nsls2.ioc_deploy eurotherm3k role)."""

    loop1: A[Eurotherm3kLoop, PvSuffix("LOOP1:"), Format.CHILD]
    loop2: A[Eurotherm3kLoop, PvSuffix("LOOP2:"), Format.CHILD]

    # --- Global (controller-wide) ---
    disable: A[SignalRW[str], PvSuffix("DISABLE"), Format.CONFIG_SIGNAL]
    programmer_number: A[SignalRW[int], PvSuffix.rbv("PROGNUM", ":RBV"), Format.CONFIG_SIGNAL]
    programmer_run: A[SignalRW[bool], PvSuffix.rbv("PROGRUN", ":RBV")]
    programmer_reset: A[SignalRW[bool], PvSuffix("PROGRESET")]
    programmer_status: A[SignalR[str], PvSuffix("PROGSTAT:RBV")]
    recipe_select: A[SignalRW[str], PvSuffix.rbv("RECSEL", ":RBV"), Format.CONFIG_SIGNAL]
    recipe_status: A[SignalR[str], PvSuffix("RECSTAT:RBV")]
