"""ophyd-async slits with gap and center control of each pair of blades."""

import asyncio

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
    """

    def __init__(
        self,
        low: AsyncEpicsMotor,
        high: AsyncEpicsMotor,
        name: str = "",
        units: str = "mm",
        precision: int = 3,
    ):
        # A tuple, so the blades are not adopted as children of this device
        self._blades = (low, high)
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

    async def locate(self) -> Location[float]:
        """Return the setpoint and readback computed from both blades."""
        low, high = self._blades
        low_sp, high_sp, low_rb, high_rb = await asyncio.gather(
            low.user_setpoint.get_value(),
            high.user_setpoint.get_value(),
            low.user_readback.get_value(),
            high.user_readback.get_value(),
        )
        return Location(
            setpoint=self._forward(low_sp, high_sp),
            readback=self._forward(low_rb, high_rb),
        )

    @WatchableAsyncStatus.wrap
    async def set(self, value: float):
        """Move both blades so the gap or center reaches ``value``."""
        low, high = self._blades
        low_rb, high_rb = await asyncio.gather(
            low.user_readback.get_value(), high.user_readback.get_value()
        )
        low_target, high_target = self._backward(value, low_rb, high_rb)
        initial = self._forward(low_rb, high_rb)

        async def move_blades():
            await asyncio.gather(low.set(low_target), high.set(high_target))

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

    async def stop(self, success: bool = False):
        """Stop both blades, so pausing or aborting the RunEngine stops a move."""
        await asyncio.gather(*(blade.stop(success=success) for blade in self._blades))


class SlitGap(_BladePairAxis):
    """Separation between a pair of slit blades; setting it keeps the center."""

    @staticmethod
    def _forward(low: float, high: float) -> float:
        return high - low

    @staticmethod
    def _backward(value: float, low: float, high: float) -> tuple[float, float]:
        center = (low + high) / 2
        return center - value / 2, center + value / 2


class SlitCenter(_BladePairAxis):
    """Midpoint of a pair of slit blades; setting it keeps the gap."""

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
    axes. Setting a gap keeps the center fixed; setting a center keeps the gap fixed.

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

        with self.add_children_as_readables(Format.CHILD):
            self.horizontal_gap = SlitGap(self.inboard, self.outboard)
            self.horizontal_center = SlitCenter(self.inboard, self.outboard)
            self.vertical_gap = SlitGap(self.bottom, self.top)
            self.vertical_center = SlitCenter(self.bottom, self.top)

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
        if len(value) == 2 and all(len(v) == 2 for v in value):
            (h_gap, h_center), (v_gap, v_center) = value
        elif len(value) == 4:
            h_gap, h_center, v_gap, v_center = value
        else:
            raise ValueError(
                "Slit set value must be ((h_gap, h_center), (v_gap, v_center)) "
                "or (h_gap, h_center, v_gap, v_center)"
            )

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
        """Stop all four blades, so pausing or aborting the RunEngine stops a move."""
        self._stopped = True
        await asyncio.gather(
            self.inboard.stop(success=success),
            self.outboard.stop(success=success),
            self.bottom.stop(success=success),
            self.top.stop(success=success),
        )


def move_slits(
    slits: Slits,
    horizontal_center: float | None = None,
    vertical_center: float | None = None,
    horizontal_gap: float | None = None,
    vertical_gap: float | None = None,
    relative: bool = False,
):
    """Move a pair of slit blades by their center line and gap.

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
