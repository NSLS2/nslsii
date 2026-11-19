"""ophyd-async slits with gap and center control of each pair of blades."""

import asyncio
import logging
from dataclasses import dataclass

import bluesky.plan_stubs as bps
from bluesky.protocols import Callback, Locatable, Location, Subscribable
from ophyd_async.core import (
    AsyncMovable,
    AsyncStatus,
    StandardReadable,
    WatchableAsyncStatus,
    WatcherUpdate,
    derived_signal_r,
    forward_watcher_updates,
    observe_value,
)
from ophyd_async.core import StandardReadableFormat as Format
from ophyd_async.epics.core import EpicsDevice
from ophyd_async.epics.motor import Motor as AsyncEpicsMotor

logger = logging.getLogger(__name__)


async def _tolerance(blade: AsyncEpicsMotor) -> float:
    """A blade's positioning tolerance: its retry deadband (``.RDBD``), within which the
    motor record stops correcting, or its resolution if larger."""
    deadband, resolution = await asyncio.gather(
        blade.deadband.get_value(), blade.motor_resolution.get_value()
    )
    return max(deadband, abs(resolution))


async def _blade_position(blade: AsyncEpicsMotor) -> tuple[float, float]:
    """Return a blade's commanded position and its readback.

    The commanded position is the setpoint, unless the blade is at rest further from it
    than its positioning tolerance, e.g. after an IOC reboot restored a stale setpoint,
    or a position redefinition. Then the setpoint doesn't say where the blade is, so the
    readback is used instead. The tolerance is given by `_tolerance`.
    """
    setpoint, readback, done, tolerance = await asyncio.gather(
        blade.user_setpoint.get_value(),
        blade.user_readback.get_value(),
        blade.motor_done_move.get_value(),
        _tolerance(blade),
    )
    # While moving, the setpoint is where the blade is going, so build on that
    if not done:
        return setpoint, readback
    if abs(setpoint - readback) <= tolerance:
        return setpoint, readback
    logger.warning(
        f"{blade.name} is at rest at {readback}, {abs(setpoint - readback)} from its "
        f"setpoint {setpoint} (tolerance {tolerance}); using its readback"
    )
    return readback, readback


@dataclass(frozen=True)
class _BladePairMove:
    """A gap or center move of a pair of blades stopped partway.

    Holds its absolute blade targets, and the blades' readbacks where they stopped, to
    tell whether they have been moved since.
    """

    axis: "_BladePairAxis"
    value: float
    low_target: float
    high_target: float
    low_readback: float
    high_readback: float


@dataclass
class _BladePairState:
    """Move state shared by the gap and the center of one pair of blades.

    A move stopped partway, e.g. by a pause, is remembered with its absolute blade
    targets, so a replay of it after resuming goes to the same targets. Recomputing them
    would keep the center (or gap) where the blades stopped, not the one commanded.
    """

    #: Whether a gap or center move of the pair is in progress
    moving: bool = False
    #: Whether the move in progress was stopped
    stopped: bool = False
    #: The last move stopped partway, until the pair is next moved
    interrupted_move: _BladePairMove | None = None


class _BladePairAxis(
    StandardReadable, AsyncMovable[float], Locatable[float], Subscribable[float]
):
    """A gap or center of a pair of slit blades, moved by moving both blades.

    Each blade moves with its own motor's timeout, and progress is reported to
    watchers as the gap or center. Subscribing follows the readback, as a motor's
    subscription follows its user readback.

    Parameters
    ----------
    low : AsyncEpicsMotor
        The low blade (inboard or bottom).
    high : AsyncEpicsMotor
        The high blade (outboard or top).
    name : str
        Device name.
    units : str
        Units of the gap or center, as reported to watchers.
    precision : int
        Decimal places of the gap or center, as reported to watchers.
    pair_state : _BladePairState, optional
        State shared with the other axis of the same pair of blades, so a move of one
        forgets an interrupted move of the other. `Slits` shares one per pair.
    """

    def __init__(
        self,
        low: AsyncEpicsMotor,
        high: AsyncEpicsMotor,
        name: str = "",
        units: str = "mm",
        precision: int = 3,
        pair_state: _BladePairState | None = None,
    ):
        # A tuple, so the blades are not adopted as children of this device
        self._blades = (low, high)
        self._pair_state = pair_state if pair_state is not None else _BladePairState()
        self._units = units
        self._precision = precision
        with self.add_children_as_readables(Format.HINTED_SIGNAL):
            self.readback = derived_signal_r(
                self._forward,
                derived_units=units,
                derived_precision=precision,
                low=low,
                high=high,
            )
        super().__init__(name=name)

    def set_name(self, name: str, *, child_name_separator: str | None = None) -> None:
        """Name the readback after this device, as a motor's readback is."""
        super().set_name(name, child_name_separator=child_name_separator)
        self.readback.set_name(name)

    def subscribe_reading(self, function: Callback[float]) -> None:
        """Subscribe to the readback."""
        self.readback.subscribe_reading(function)

    def clear_sub(self, function: Callback[float]) -> None:
        """Remove a readback subscription."""
        self.readback.clear_sub(function)

    @staticmethod
    def _forward(low: float, high: float) -> float:
        """Forward kinematics: the gap or center from the blade positions."""
        raise NotImplementedError

    @staticmethod
    def _backward(value: float, low: float, high: float) -> tuple[float, float]:
        """Backward kinematics: blade targets that give ``value``."""
        raise NotImplementedError

    async def _positions(self) -> tuple[float, float, float, float]:
        """The blades' commanded positions and readbacks, as (low, high, low, high)."""
        (low_sp, low_rb), (high_sp, high_rb) = await asyncio.gather(
            *(_blade_position(blade) for blade in self._blades)
        )
        return low_sp, high_sp, low_rb, high_rb

    async def locate(self) -> Location[float]:
        """Return the setpoint and readback computed from both blades.

        The setpoint is computed from each blade's commanded position, as `set` uses.
        """
        low_sp, high_sp, low_rb, high_rb = await self._positions()
        return Location(
            setpoint=self._forward(low_sp, high_sp),
            readback=self._forward(low_rb, high_rb),
        )

    @WatchableAsyncStatus.wrap
    async def set(self, value: float):
        """Move both blades so the gap or center reaches ``value``.

        The blade targets are computed from the blades' setpoints rather than their
        readbacks, so the value that is kept (the center for a gap move, the gap for a
        center move) is the one last commanded. Using readbacks would reset it to where
        the blades happened to land each time, letting it drift by up to a deadband per
        move, e.g. over a gap scan.

        A blade at rest further from its setpoint than its deadband, e.g. after an IOC
        reboot left a stale setpoint, uses its readback instead, so the kept value is the
        real one. A warning is logged when that happens. Only the first move after such a
        mismatch uses the readback: that move writes a fresh setpoint for the blade, so
        later moves use setpoints again.

        If the previous move of this pair of blades was this axis to the same ``value``
        and it was stopped partway, e.g. by a pause, its absolute blade targets are
        reused. A RunEngine replaying the move after resuming then finishes the move
        originally commanded, rather than one recomputed around where the blades
        stopped. Any other move of the pair through its gap or center forgets it, as does
        either blade having moved further than its deadband from where it stopped, e.g. by
        moving its motor directly while paused, or the run ending without resuming (an
        abort, halt or stop after the pause); the move is then computed afresh.
        """
        low, high = self._blades
        state = self._pair_state
        low_sp, high_sp, low_rb, high_rb = await self._positions()
        interrupted = state.interrupted_move
        if (
            interrupted is not None
            and interrupted.axis is self
            and interrupted.value == value
            and await self._still_where_stopped(interrupted, low_rb, high_rb)
        ):
            low_target, high_target = interrupted.low_target, interrupted.high_target
        else:
            low_target, high_target = self._backward(value, low_sp, high_sp)
        initial = self._forward(low_rb, high_rb)

        async def move_blades():
            await asyncio.gather(low.set(low_target), high.set(high_target))

        state.interrupted_move = None
        state.moving = True
        state.stopped = False
        try:
            move_status = AsyncStatus(move_blades())
            async for current in observe_value(self.readback, done_status=move_status):
                yield WatcherUpdate(
                    current=current,
                    initial=initial,
                    target=value,
                    name=self.name,
                    unit=self._units,
                    precision=self._precision,
                )
            # Re-raise a blade's failure; observe_value only stops when the move ends
            await move_status
        finally:
            state.moving = False
            if state.stopped:
                low_stopped, high_stopped = await asyncio.gather(
                    low.user_readback.get_value(), high.user_readback.get_value()
                )
                state.interrupted_move = _BladePairMove(
                    self, value, low_target, high_target, low_stopped, high_stopped
                )

    async def _still_where_stopped(
        self, move: _BladePairMove, low_readback: float, high_readback: float
    ) -> bool:
        """Whether both blades are within tolerance of where ``move`` stopped them."""
        low_tolerance, high_tolerance = await asyncio.gather(
            *(_tolerance(blade) for blade in self._blades)
        )
        if (
            abs(low_readback - move.low_readback) <= low_tolerance
            and abs(high_readback - move.high_readback) <= high_tolerance
        ):
            return True
        logger.info(
            f"{self.name}: blades moved since its move to {move.value} was stopped; "
            "computing the move afresh instead of resuming it"
        )
        return False

    async def stop(self, success: bool = False):
        """Stop both blades, so pausing or aborting the RunEngine stops a move.

        A move in progress is remembered as stopped partway, so replaying it reuses its
        blade targets; see `set`. Called with no move in progress, it forgets any such
        move instead: the RunEngine stops every device a plan moved when the run ends, so
        after a pause that is then aborted, halted or stopped, rather than resumed, the
        next move is computed afresh.
        """
        if self._pair_state.moving:
            self._pair_state.stopped = True
        else:
            self._pair_state.interrupted_move = None
        await asyncio.gather(*(blade.stop(success=success) for blade in self._blades))


class SlitGap(_BladePairAxis):
    """Separation between a pair of slit blades; setting it keeps the commanded center."""

    @staticmethod
    def _forward(low: float, high: float) -> float:
        return high - low

    @staticmethod
    def _backward(value: float, low: float, high: float) -> tuple[float, float]:
        center = (low + high) / 2
        return center - value / 2, center + value / 2


class SlitCenter(_BladePairAxis):
    """Midpoint of a pair of slit blades; setting it keeps the commanded gap."""

    @staticmethod
    def _forward(low: float, high: float) -> float:
        return (low + high) / 2

    @staticmethod
    def _backward(value: float, low: float, high: float) -> tuple[float, float]:
        shift = value - (low + high) / 2
        return low + shift, high + shift


class Slits(
    StandardReadable,
    EpicsDevice,
    AsyncMovable[
        tuple[tuple[float, float], tuple[float, float]]
        | tuple[float, float, float, float]
    ],
):
    """Generic slits with inboard, outboard, bottom, and top motors.

    Exposes gap and center as movable :class:`SlitGap` and :class:`SlitCenter`
    devices for both the horizontal (inboard/outboard) and vertical (bottom/top)
    axes. Setting a gap keeps the commanded center fixed; setting a center keeps the
    commanded gap fixed. Both are computed from the blade setpoints, so they don't drift
    with the blades' positioning error over repeated moves.

    Parameters
    ----------
    prefix : str, optional
        PV prefix shared by all four blades, e.g. ``"XF:31ID1-OP{Slt:1-Ax:"``; the
        blade motors are ``<prefix>I}Mtr``, ``O}Mtr``, ``B}Mtr`` and ``T}Mtr``.
    name : str
        Device name.
    horizontal_prefix : str, optional
        PV prefix of the inboard/outboard pair, if it differs from ``prefix``.
    vertical_prefix : str, optional
        PV prefix of the bottom/top pair, if it differs from ``prefix``.

    Raises
    ------
    ValueError
        If a pair has no prefix, i.e. ``prefix`` is omitted without giving that
        pair's own prefix.
    """

    def __init__(
        self,
        prefix: str | None = None,
        name: str = "",
        horizontal_prefix: str | None = None,
        vertical_prefix: str | None = None,
    ):
        horizontal_prefix = horizontal_prefix if horizontal_prefix is not None else prefix
        vertical_prefix = vertical_prefix if vertical_prefix is not None else prefix
        if horizontal_prefix is None or vertical_prefix is None:
            raise ValueError(
                "Give a prefix shared by all blades, or both horizontal_prefix and vertical_prefix."
            )
        with self.add_children_as_readables(Format.CHILD):
            self.inboard = AsyncEpicsMotor(horizontal_prefix + "I}Mtr")
            self.outboard = AsyncEpicsMotor(horizontal_prefix + "O}Mtr")
            self.bottom = AsyncEpicsMotor(vertical_prefix + "B}Mtr")
            self.top = AsyncEpicsMotor(vertical_prefix + "T}Mtr")

        horizontal, vertical = _BladePairState(), _BladePairState()
        with self.add_children_as_readables(Format.CHILD):
            self.horizontal_gap = SlitGap(self.inboard, self.outboard, pair_state=horizontal)
            self.horizontal_center = SlitCenter(
                self.inboard, self.outboard, pair_state=horizontal
            )
            self.vertical_gap = SlitGap(self.bottom, self.top, pair_state=vertical)
            self.vertical_center = SlitCenter(self.bottom, self.top, pair_state=vertical)

        self._stopped = False
        super().__init__(name=name)

    @WatchableAsyncStatus.wrap
    async def set(
        self,
        value: tuple[tuple[float, float], tuple[float, float]]
        | tuple[float, float, float, float],
    ):
        """Set the horizontal and vertical gaps and centers.

        Parameters
        ----------
        value : tuple[tuple[float, float], tuple[float, float]]
            ((horizontal_gap, horizontal_center), (vertical_gap, vertical_center))
        """
        try:
            (h_gap, h_center), (v_gap, v_center) = value
        except (TypeError, ValueError):
            try:
                h_gap, h_center, v_gap, v_center = value
            except (TypeError, ValueError):
                raise ValueError(
                    "Slit set value must be ((h_gap, h_center), (v_gap, v_center)) "
                    "or (h_gap, h_center, v_gap, v_center)"
                ) from None

        # Set gaps first to preserve centers, then set centers to preserve gaps.
        self._stopped = False
        gaps = [self.horizontal_gap.set(h_gap), self.vertical_gap.set(v_gap)]
        async for update in forward_watcher_updates(gaps, self.name, combine=False):
            yield update
        if self._stopped:
            # Stopped during the gaps, e.g. by a pause; don't go on to the centers.
            return

        centers = [
            self.horizontal_center.set(h_center),
            self.vertical_center.set(v_center),
        ]
        async for update in forward_watcher_updates(centers, self.name, combine=False):
            yield update

    async def stop(self, success: bool = False):
        """Stop all four blades, so pausing or aborting the RunEngine stops a move.

        Stopping through the gaps marks any gap or center move in progress as stopped
        partway, so replaying this set after a pause reuses its blade targets.
        """
        self._stopped = True
        await asyncio.gather(
            self.horizontal_gap.stop(success=success),
            self.vertical_gap.stop(success=success),
        )


def move_slits(
    slits: Slits,
    horizontal_center: float | None = None,
    vertical_center: float | None = None,
    horizontal_gap: float | None = None,
    vertical_gap: float | None = None,
    relative: bool = False,
):
    """Move pair(s) of slit blades by their center line and gap.

    Only the values given move; the others stay where they are. A center move
    shifts both blades of the pair together, keeping the gap, and a gap move
    opens or closes both blades about the center line.

    Parameters
    ----------
    slits : Slits
        The slits to move.
    horizontal_center : float, optional
        Center of the inboard/outboard pair, in mm.
    vertical_center : float, optional
        Center of the bottom/top pair, in mm.
    horizontal_gap : float, optional
        Gap between the inboard and outboard blades, in mm.
    vertical_gap : float, optional
        Gap between the bottom and top blades, in mm.
    relative : bool, default False
        Treat the given values as shifts from the current positions.

    Raises
    ------
    ValueError
        If no center or gap is given.
    """
    # A gap and a center on one axis drive the same two blades, so gaps go first
    stages = [
        [(slits.horizontal_gap, horizontal_gap), (slits.vertical_gap, vertical_gap)],
        [
            (slits.horizontal_center, horizontal_center),
            (slits.vertical_center, vertical_center),
        ],
    ]
    if all(value is None for stage in stages for _, value in stage):
        raise ValueError("Give at least one center or gap to move the slits.")
    move = bps.mvr if relative else bps.mv
    for stage in stages:
        args = [
            x for signal, value in stage if value is not None for x in (signal, value)
        ]
        if args:
            # Resuming after a pause rewinds here, so this stage is moved again.
            yield from bps.checkpoint()
            yield from move(*args)
