"""ophyd-async devices for common multi-axis motor stages.

PV names follow the NSLS-II convention ``{prefix}-Ax:{axis}}}Mtr``, e.g. with
``prefix="XF:31ID1-ES{Stg:1"`` (or ``"XF:31ID1-ES{Stg:1-Ax:"``) the X axis motor is
``XF:31ID1-ES{Stg:1-Ax:X}Mtr``.
"""

import asyncio
from collections.abc import Mapping, Sequence
from typing import ClassVar, get_type_hints

from bluesky.protocols import Movable, Stoppable
from ophyd_async.core import StandardReadable, WatchableAsyncStatus, forward_watcher_updates
from ophyd_async.core import StandardReadableFormat as Format
from ophyd_async.epics.motor import Motor

StagePosition = Sequence[float | None] | Mapping[str, float]


class MotorizedStage(StandardReadable, Movable[StagePosition], Stoppable):
    """Base class for a stage made up of several EPICS motor record axes.

    Axes are inferred from the subclass's ``Motor`` annotations, in definition order;
    each annotated attribute becomes a ``Motor`` child. For example, annotating
    ``x: Motor`` and ``y: Motor`` in a subclass will create ``x`` and ``y`` motor
    children, by default with PV names following the convention ``{prefix}{axis.upper()}}}Mtr``.

    Parameters
    ----------
    prefix : str
        PV prefix shared by all axes, e.g. ``"XF:31ID1-ES{Stg:1-Ax:"``. The trailing
        ``-Ax:`` is optional and added if missing, so ``"XF:31ID1-ES{Stg:1"`` is equivalent.
    name : str
        Name of the device.
    axis_suffixes : Mapping[str, str], optional
        Per-axis override of the PV axis component, e.g. ``{"x": "X1"}``. Defaults to
        the upper-cased attribute name.
    motor_suffix : str
        Suffix appended after the axis component, ``"}Mtr"`` by default.
    append_ax : bool
        Append ``-Ax:`` to ``prefix`` when it is missing, ``True`` by default. Set to
        ``False`` for stages whose PVs don't follow the NSLS-II naming convention, in
        which case ``prefix`` is used exactly as given.
    """

    _axes: ClassVar[tuple[str, ...]] = ()

    def __init_subclass__(cls, **kwargs):
        """Automatically infer list of axes from the subclass's Motor annotations."""

        super().__init_subclass__(**kwargs)
        cls._axes = tuple(
            attr
            for attr, hint in get_type_hints(cls).items()
            if isinstance(hint, type) and issubclass(hint, Motor)
        )

    def __init__(
        self,
        prefix: str,
        name: str = "",
        axis_suffixes: Mapping[str, str] | None = None,
        motor_suffix: str = "}Mtr",
        append_ax: bool = True,
    ):
        axis_suffixes = dict(axis_suffixes or {})
        if unknown := set(axis_suffixes) - set(self._axes):
            raise ValueError(
                f"Unknown axes {sorted(unknown)} for {type(self).__name__}, "
                f"expected a subset of {list(self._axes)}"
            )

        if append_ax and not prefix.endswith("-Ax:"):
            prefix = f"{prefix}-Ax:"
        with self.add_children_as_readables():
            for axis in self._axes:
                pv_axis = axis_suffixes.get(axis, axis.upper())
                setattr(self, axis, Motor(f"{prefix}{pv_axis}{motor_suffix}"))
        super().__init__(name=name)

    @property
    def axes(self) -> dict[str, Motor]:
        """The stage's motors, keyed by axis name, in axis order."""
        return {axis: getattr(self, axis) for axis in self._axes}

    def _targets(self, value: StagePosition) -> dict[str, float]:
        if isinstance(value, Mapping):
            if unknown := set(value) - set(self._axes):
                raise ValueError(f"Unknown axes {sorted(unknown)}, expected {list(self._axes)}")
            targets = dict(value)
        else:
            if len(value) != len(self._axes):
                raise ValueError(
                    f"{type(self).__name__} expects {len(self._axes)} positions "
                    f"({', '.join(self._axes)}), got {len(value)}"
                )
            targets = dict(zip(self._axes, value, strict=True))
        return {axis: pos for axis, pos in targets.items() if pos is not None}


    @WatchableAsyncStatus.wrap
    async def set(self, value: StagePosition):
        """Move one or more axes together.

        Parameters
        ----------
        value : Sequence[float | None] | Mapping[str, float]
            Either one position per axis in axis order (``None`` leaves that axis where
            it is), or a mapping of axis name to position for just the axes to move.
        """

        statuses = [self.axes[axis].set(pos) for axis, pos in self._targets(value).items()]
        if statuses:
            async for update in forward_watcher_updates(statuses, self.name, combine=False):
                yield update

    async def stop(self, success: bool = False):
        """Stop all axes, so pausing or aborting the RunEngine stops a move.
        
        Parameters
        ----------
        success : bool, default False
            Whether or not to mark the move as successful. Default false.
        """

        await asyncio.gather(*(motor.stop(success=success) for motor in self.axes.values()))


class XYStage(MotorizedStage):
    """Stage with X and Y axes."""

    x: Motor
    y: Motor


class XYZStage(XYStage):
    """Stage with X, Y and Z axes."""

    z: Motor


class XZStage(MotorizedStage):
    """Stage with X and Z axes."""

    x: Motor
    z: Motor


class YZStage(MotorizedStage):
    """Stage with Y and Z axes."""

    y: Motor
    z: Motor
