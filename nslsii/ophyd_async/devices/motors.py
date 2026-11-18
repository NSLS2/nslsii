"""ophyd-async devices for common multi-axis motor stages.

PV names follow the NSLS-II convention ``{prefix}-Ax:{axis}}}Mtr``, e.g. with
``prefix="XF:31ID1-ES{Stg:1"`` (or ``"XF:31ID1-ES{Stg:1-Ax:"``) the X axis motor is
``XF:31ID1-ES{Stg:1-Ax:X}Mtr``.
"""

import asyncio
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Annotated, Any, ClassVar, get_origin, get_type_hints

from bluesky.protocols import Locatable, Location, Stoppable
from ophyd_async.core import StandardReadable, WatchableAsyncStatus, forward_watcher_updates
from ophyd_async.core import StandardReadableFormat as Format
from ophyd_async.epics.motor import Motor

StagePosition = Sequence[float | None] | Mapping[str, float]


@dataclass(frozen=True)
class AxisSuffix:
    """``Annotated`` metadata giving an axis's default PV axis component.

    For example ``theta: Annotated[Motor, AxisSuffix("Th")]`` uses ``...-Ax:Th}Mtr``
    rather than ``...-Ax:THETA}Mtr``.
    """

    suffix: str


class MotorizedStage(StandardReadable, Locatable[StagePosition], Stoppable):
    """Base class for a stage made up of several EPICS motor record axes.

    Axes are inferred from the subclass's ``Motor`` annotations, in definition order;
    each annotated attribute becomes a ``Motor`` child. For example, annotating
    ``x: Motor`` and ``y: Motor`` in a subclass will create ``x`` and ``y`` motor
    children, by default with PV names following the convention ``{prefix}{axis.upper()}}}Mtr``.

    Axes may also be annotated with a subclass of ``Motor``, which is then used to create
    that axis. Such subclasses must take the PV prefix as their only positional argument;
    any other constructor arguments must be keyword arguments, passed via ``axis_kwargs``
    or as a mapping in ``typing.Annotated`` metadata, e.g.
    ``z: Annotated[CustomMotor, {"some_option": 1.0}]``. Kwargs given via ``axis_kwargs``
    take precedence over those given via ``Annotated``.

    An axis's default PV axis component can be set with ``AxisSuffix`` in its
    ``Annotated`` metadata, e.g. ``theta: Annotated[Motor, AxisSuffix("Th")]``. Values
    passed via ``axis_suffixes`` take precedence over it.

    Parameters
    ----------
    prefix : str
        PV prefix shared by all axes, e.g. ``"XF:31ID1-ES{Stg:1-Ax:"``. The trailing
        ``-Ax:`` is optional and added if missing, so ``"XF:31ID1-ES{Stg:1"`` is equivalent.
    name : str
        Name of the device.
    axis_suffixes : Mapping[str, str], optional
        Per-axis override of the PV axis component, e.g. ``{"x": "X1"}``. Defaults to
        the axis's ``AxisSuffix``, or the upper-cased attribute name if it has none.
    axis_kwargs : Mapping[str, Mapping[str, Any]], optional
        Per-axis keyword arguments passed to that axis's motor constructor, e.g.
        ``{"z": {"some_option": 1.0}}`` for an axis annotated with a custom ``Motor``
        subclass.
    motor_suffix : str
        Suffix appended after the axis component, ``"}Mtr"`` by default.
    append_ax : bool
        Append ``-Ax:`` to ``prefix`` when it is missing, ``True`` by default. Set to
        ``False`` for stages whose PVs don't follow the NSLS-II naming convention, in
        which case ``prefix`` is used exactly as given.
    """

    _axes: ClassVar[dict[str, type[Motor]]] = {}
    _axis_default_kwargs: ClassVar[dict[str, dict[str, Any]]] = {}
    _axis_default_suffixes: ClassVar[dict[str, str]] = {}

    def __init_subclass__(cls, **kwargs):
        """Automatically infer the axes, their motor types, and any default motor kwargs
        and PV axis components given via ``Annotated`` from the subclass's Motor annotations."""

        super().__init_subclass__(**kwargs)
        cls._axes = {}
        cls._axis_default_kwargs = {}
        cls._axis_default_suffixes = {}
        for attr, hint in get_type_hints(cls, include_extras=True).items():
            metadata: tuple[Any, ...] = ()
            if get_origin(hint) is Annotated:
                hint, metadata = hint.__origin__, hint.__metadata__
            if isinstance(hint, type) and issubclass(hint, Motor):
                cls._axes[attr] = hint
                cls._axis_default_kwargs[attr] = {
                    key: value
                    for item in metadata
                    if isinstance(item, Mapping)
                    for key, value in item.items()
                }
                for item in metadata:
                    if isinstance(item, AxisSuffix):
                        cls._axis_default_suffixes[attr] = item.suffix

    def __init__(
        self,
        prefix: str,
        name: str = "",
        axis_suffixes: Mapping[str, str] | None = None,
        axis_kwargs: Mapping[str, Mapping[str, Any]] | None = None,
        motor_suffix: str = "}Mtr",
        append_ax: bool = True,
    ):
        axis_suffixes = dict(axis_suffixes or {})
        axis_kwargs = dict(axis_kwargs or {})
        if unknown := (set(axis_suffixes) | set(axis_kwargs)) - set(self._axes):
            raise ValueError(
                f"Unknown axes {sorted(unknown)} for {type(self).__name__}, "
                f"expected a subset of {list(self._axes)}"
            )

        if append_ax and not prefix.endswith("-Ax:"):
            prefix = f"{prefix}-Ax:"
        with self.add_children_as_readables():
            for axis, motor_cls in self._axes.items():
                pv_axis = axis_suffixes.get(
                    axis, self._axis_default_suffixes.get(axis, axis.upper())
                )
                kwargs = {**self._axis_default_kwargs[axis], **axis_kwargs.get(axis, {})}
                motor = motor_cls(f"{prefix}{pv_axis}{motor_suffix}", **kwargs)
                setattr(self, axis, motor)
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

    async def locate(self) -> Location[StagePosition]:
        """Return the setpoint and readback of every axis, each as a tuple in axis order."""

        locations = await asyncio.gather(*(motor.locate() for motor in self.axes.values()))
        return Location(
            setpoint=tuple(loc["setpoint"] for loc in locations),
            readback=tuple(loc["readback"] for loc in locations),
        )

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


class XYThetaStage(XYStage):
    """Stage with X and Y axes and a theta rotation (PV axis ``Th``)."""

    theta: Annotated[Motor, AxisSuffix("Th")]


class XYZThetaStage(XYZStage):
    """Stage with X, Y and Z axes and a theta rotation (PV axis ``Th``)."""

    theta: Annotated[Motor, AxisSuffix("Th")]


class XYPitchStage(XYStage):
    """Stage with X and Y axes and pitch (PV axis ``P``)."""

    pitch: Annotated[Motor, AxisSuffix("P")]


class XYPitchRollYawStage(XYPitchStage):
    """Stage with X and Y axes and pitch, roll and yaw rotations (PV axes ``P``, ``R``, ``Yaw``)."""

    roll: Annotated[Motor, AxisSuffix("R")]
    yaw: Annotated[Motor, AxisSuffix("Yaw")]


class XYZPitchRollYawStage(MotorizedStage):
    """Six-axis stage, e.g. a hexapod, with X, Y and Z axes and pitch, roll and yaw
    rotations (PV axes ``P``, ``R``, ``Yaw``)."""

    x: Motor
    y: Motor
    z: Motor
    pitch: Annotated[Motor, AxisSuffix("P")]
    roll: Annotated[Motor, AxisSuffix("R")]
    yaw: Annotated[Motor, AxisSuffix("Yaw")]


class KBMirror(StandardReadable, Stoppable):
    """Kirkpatrick-Baez mirror pair, made up of a horizontally and a vertically focusing mirror.

    Each mirror is a ``MotorizedStage``, by default an ``XYPitchStage``, with PV prefix
    ``{prefix}{Mir:KBH-Ax:`` for the horizontal mirror and ``{prefix}{Mir:KBV-Ax:`` for
    the vertical one, e.g. with ``prefix="XF:17IDB-OP:AMX"`` the horizontal mirror's pitch
    motor is ``XF:17IDB-OP:AMX{Mir:KBH-Ax:P}Mtr``.

    Parameters
    ----------
    prefix : str
        PV prefix up to the device component, e.g. ``"XF:17IDB-OP:AMX"``. A trailing
        ``{Mir`` or ``{Mir:`` is also accepted.
    name : str
        Name of the device.
    horizontal_cls, vertical_cls : type[MotorizedStage]
        Stage classes used for each mirror, ``XYPitchStage`` by default. Use a custom
        ``MotorizedStage`` subclass for mirrors with other axes, e.g. upstream and
        downstream legs.
    horizontal_id, vertical_id : str
        Device ids of each mirror, ``"KBH"`` and ``"KBV"`` by default.
    horizontal_kwargs, vertical_kwargs : Mapping[str, Any], optional
        Keyword arguments passed to each mirror's stage constructor, e.g.
        ``{"axis_suffixes": {"x": "XU"}}``.
    """

    horizontal: MotorizedStage
    vertical: MotorizedStage

    def __init__(
        self,
        prefix: str,
        name: str = "",
        horizontal_cls: type[MotorizedStage] = XYPitchStage,
        vertical_cls: type[MotorizedStage] = XYPitchStage,
        horizontal_id: str = "KBH",
        vertical_id: str = "KBV",
        horizontal_kwargs: Mapping[str, Any] | None = None,
        vertical_kwargs: Mapping[str, Any] | None = None,
    ):
        prefix = prefix.removesuffix(":").removesuffix("{Mir")
        with self.add_children_as_readables():
            self.horizontal = horizontal_cls(
                f"{prefix}{{Mir:{horizontal_id}", **(horizontal_kwargs or {})
            )
            self.vertical = vertical_cls(f"{prefix}{{Mir:{vertical_id}", **(vertical_kwargs or {}))
        super().__init__(name=name)

    async def stop(self, success: bool = False):
        """Stop both mirrors."""

        await asyncio.gather(
            self.horizontal.stop(success=success), self.vertical.stop(success=success)
        )
