"""ophyd-async two-button shutters and gate valves."""

import asyncio
import logging
import random
from collections.abc import Callable
from typing import Generic, Literal, TypeVar

from bluesky.protocols import Reading, Subscribable
from ophyd_async.core import (
    DEFAULT_TIMEOUT,
    AsyncMovable,
    AsyncStatus,
    Device,
    DeviceMock,
    StandardReadable,
    StrictEnum,
    TriggerableCommand,
    callback_on_mock_execute,
    callback_on_mock_put,
    derived_signal_r,
    default_mock_class,
    set_mock_value,
    soft_signal_rw,
    wait_for_value,
)
from ophyd_async.core import StandardReadableFormat as Format
from ophyd_async.epics.core import (
    epics_signal_r,
    epics_signal_rw,
    epics_triggerable_command,
)

logger = logging.getLogger(__name__)


class ShutterStatus(StrictEnum):
    """Shutter position status reported by EPICS."""

    OPEN = "Open"
    CLOSED = "Not Open"


ShutterStatusT = TypeVar("ShutterStatusT", bound=StrictEnum)


class MockShutter(DeviceMock["Shutter"]):
    """Mock shutter that flips status after a random 0-3 s actuation delay."""

    async def connect(self, device: "Shutter") -> None:
        set_mock_value(device.status, device._status_enum.CLOSED)

        async def _actuate(target: StrictEnum) -> None:
            await asyncio.sleep(random.uniform(0.0, 3.0))
            set_mock_value(device.status, target)

        async def _open() -> None:
            await _actuate(device._status_enum.OPEN)

        async def _close() -> None:
            await _actuate(device._status_enum.CLOSED)

        callback_on_mock_execute(device.open_cmd, _open)
        callback_on_mock_execute(device.close_cmd, _close)


@default_mock_class(MockShutter)
class Shutter(
    StandardReadable,
    Subscribable[bool],
    AsyncMovable[bool | Literal[0, 1] | Literal["Open", "Close", "Closed"] | ShutterStatusT],
    Generic[ShutterStatusT]
):
    """Two-button photon shutter or gate valve, opened and closed by separate command PVs.

    Parameters
    ----------
    prefix : str
        PV prefix, e.g. ``"XF:27IDA-PPS{PSh}"``.
    name : str
        Device name.
    cmd_value : Literal[0, 1], default 1
        Value written to the open or close command PV to actuate it, 1 by default.
        Some valves are actuated by writing 0.
    retries : int
        How many times to re-send the command if the status hasn't reached the target
        within ``retry_period``, for shutters whose controller sometimes drops a
        command. 0 by default, i.e. the command is sent once.
    retry_period : float
        Seconds to wait for the status after each send before retrying, 1 by default.
        After the last send, the move waits up to ``DEFAULT_TIMEOUT`` instead.
    status_enum : type[StrictEnum]
        Enum of the ``Pos-Sts`` states, ``ShutterStatus`` (``"Open"``/``"Not Open"``) by
        default. Devices whose status PV reports other strings pass their own enum, which
        must have ``OPEN`` and ``CLOSED`` members, e.g.::

            class ValveStatus(StrictEnum):
                OPEN = "Not Closed"
                CLOSED = "Closed"

            valve = Shutter("XF:23ID2-VA{Mir:3-GV:1}", status_enum=ValveStatus)

        Being a ``StrictEnum``, it must match the PV's states exactly, so a mismatch
        fails when the device connects.
    settle_time : float
        Initial value of the ``settle_time`` signal, 0 by default.

    Attributes
    ----------
    is_open : SignalR[bool]
        Whether the status reports the shutter open. This is the shutter's reading,
        named after the shutter, and what `subscribe_reading` follows, so reads,
        monitors and subscriptions all report the same bool.
    status : SignalR[ShutterStatusT]
        Readback of the shutter position, as the status enum. Not read itself.
    settle_time : SignalRW[float]
        Seconds to wait after the status confirms a move before the move completes,
        for shutters whose status changes before they have fully opened or closed.
    enabled : SignalR[bool]
        Whether the shutter is enabled, i.e. can be actuated.
    failed_to_open : SignalR[bool]
        Set by the controller when an open command did not open the shutter.
    failed_to_close : SignalR[bool]
        Set by the controller when a close command did not close the shutter.
    open_cmd : TriggerableCommand
        Command to open the shutter.
    close_cmd : TriggerableCommand
        Command to close the shutter.
    """

    def __init__(
        self,
        prefix: str,
        name: str = "",
        *,
        cmd_value: Literal[0, 1] = 1,
        retries: int = 0,
        retry_period: float = 1.0,
        status_enum: type[ShutterStatusT] = ShutterStatus,
        settle_time: float = 0.0,
    ):
        if missing := {"OPEN", "CLOSED"} - set(status_enum.__members__):
            raise TypeError(
                f"status_enum {status_enum.__name__} has no {', '.join(sorted(missing))} "
                "member; it needs both OPEN and CLOSED"
            )
        if cmd_value not in (0, 1):
            raise ValueError(f"cmd_value must be 0 or 1, got {cmd_value!r}")
        if retries < 0:
            raise ValueError(f"retries must be 0 or more, got {retries}")
        if retry_period <= 0:
            raise ValueError(f"retry_period must be positive, got {retry_period}")
        if settle_time < 0:
            raise ValueError(f"settle_time must be 0 or more, got {settle_time}")
        self._retries = retries
        self._retry_period = retry_period
        self._status_enum = status_enum
        # Set while a move, including its retries and settling, is in progress;
        # _idle is set whenever it isn't, so a close can wait for the move to finish
        self._moving = False
        self._idle = asyncio.Event()
        self._idle.set()

        self.status = epics_signal_r(status_enum, f"{prefix}Pos-Sts")
        # The shutter's one reading, so reads, monitors and subscriptions all agree
        with self.add_children_as_readables(Format.HINTED_SIGNAL):
            self.is_open = derived_signal_r(self._is_open, status=self.status)
        with self.add_children_as_readables(Format.CONFIG_SIGNAL):
            self.enabled = epics_signal_r(bool, f"{prefix}Enbl-Sts")
            self.settle_time = soft_signal_rw(float, initial_value=settle_time, units="s")
        self.failed_to_open = epics_signal_r(bool, f"{prefix}Sts:FailOpn-Sts")
        self.failed_to_close = epics_signal_r(bool, f"{prefix}Sts:FailCls-Sts")
        self.open_cmd = epics_triggerable_command(f"{prefix}Cmd:Opn-Cmd", cmd_value)
        self.close_cmd = epics_triggerable_command(f"{prefix}Cmd:Cls-Cmd", cmd_value)
        super().__init__(name=name)

    @AsyncStatus.wrap
    async def set(self, value: bool | Literal[0, 1] | Literal["Open", "Close", "Closed"] | ShutterStatusT):
        """Open (True) or close (False) the shutter, and wait until the status confirms it.

        Nothing is sent if the shutter is already in the requested state. Otherwise,
        once the status confirms the move, waits ``settle_time`` before completing.

        Only one move runs at a time. A request to close made while another move is in
        progress (including its retries and settling) waits for that move to finish and
        then closes, so closing, e.g. in a plan's cleanup after an error, can't be
        refused and a pending open can't leave the shutter open afterwards.

        Parameters
        ----------
        value : bool | Literal[0, 1] | Literal["Open", "Close", "Closed"] | ShutterStatusT
            The desired state: ``True``, ``1``, ``"Open"`` or the status enum's ``OPEN``
            member to open; ``False``, ``0``, ``"Close"``, ``"Closed"`` or ``CLOSED`` to close.

        Raises
        ------
        ValueError
            If ``value`` is none of these, e.g. a member of a different status enum.
        RuntimeError
            If asked to open while another move of this shutter is still in progress.
        """
        should_open = self._should_open(value)
        if should_open and self._moving:
            raise RuntimeError(f"Can't open {self.name} while a previous set is in progress")
        while self._moving:
            await self._idle.wait()
        # No await between the check above and taking the guard here
        self._moving = True
        self._idle.clear()
        try:
            cmd = self.open_cmd if should_open else self.close_cmd
            target = getattr(self._status_enum, "OPEN" if should_open else "CLOSED")
            if await self.status.get_value() == target:
                return
            await self._actuate(cmd, target)
            await asyncio.sleep(await self.settle_time.get_value())
        finally:
            self._moving = False
            self._idle.set()

    def _should_open(
        self, value: bool | Literal[0, 1] | Literal["Open", "Close", "Closed"] | ShutterStatusT
    ) -> bool:
        """Whether ``value`` asks for the shutter to be open."""
        # Status enum members are also strs, so match them before the strings
        if isinstance(value, self._status_enum):
            return value == getattr(self._status_enum, "OPEN")
        elif isinstance(value, int) and value in (0, 1):
            return bool(value)
        elif value in ("Open", "Close", "Closed"):
            return value == "Open"
        raise ValueError(
            f"Can't set {self.name} to {value!r}; expected True/False, 0/1, 'Open'/'Close'/'Closed', "
            f"or {self._status_enum.__name__}.OPEN/CLOSED"
        )

    async def _actuate(self, cmd: TriggerableCommand, target: StrictEnum) -> None:
        """Send ``cmd`` until the status reports ``target``, retrying as configured."""
        for attempt in range(self._retries + 1):
            await cmd.execute()
            last = attempt == self._retries
            try:
                await wait_for_value(
                    self.status,
                    target,
                    timeout=DEFAULT_TIMEOUT if last else self._retry_period,
                )
                return
            except TimeoutError:
                if last:
                    raise
                logger.warning(
                    "%s did not report %s within %s s; re-sending command (retry %d of %d)",
                    self.name,
                    getattr(target, "value"),
                    self._retry_period,
                    attempt + 1,
                    self._retries,
                )

    def _is_open(self, status: StrictEnum) -> bool:
        return status == getattr(self._status_enum, "OPEN")

    def set_name(self, name: str, *, child_name_separator: str | None = None) -> None:
        """Name the ``is_open`` reading after the shutter, as a motor's readback is."""
        super().set_name(name, child_name_separator=child_name_separator)
        self.is_open.set_name(self.name)

    def subscribe_reading(
        self, function: Callable[[dict[str, Reading[bool]]], None]
    ) -> None:
        """Subscribe to whether the shutter is open, as read under the shutter's name.

        Parameters
        ----------
        function : Callable[[dict[str, Reading[bool]]], None]
            A function to call with the new readings. Subscribing the same function
            again doesn't add a second subscription, so one `clear_sub` removes it.
        """
        self.is_open.subscribe_reading(function)

    def clear_sub(self, function: Callable[[dict[str, Reading[bool]]], None]) -> None:
        """Remove a subscription previously passed to `subscribe_reading`.

        Parameters
        ----------
        function : Callable[[dict[str, Reading[bool]]], None]
            The callback to remove.
        """
        self.is_open.clear_sub(function)


class MockInterlockAcknowledge(DeviceMock["InterlockAcknowledge"]):
    """Mock interlock whose status follows the acknowledge request at once."""

    async def connect(self, device: "InterlockAcknowledge") -> None:
        acknowledged = device.acknowledged
        set_mock_value(acknowledged, True)
        callback_on_mock_put(
            device.request, lambda value, **_: set_mock_value(acknowledged, value)
        )


@default_mock_class(MockInterlockAcknowledge)
class InterlockAcknowledge(Device, AsyncMovable[bool]):
    """A PPS shutter's interlock acknowledge.

    Setting True acknowledges the interlock and waits until the PPS reports it
    acknowledged; nothing is written if it already is.

    Attributes
    ----------
    request : EpicsSignalRW[bool]
        The acknowledge request (``Intlk:Ack-Sel``).
    acknowledged : EpicsSignalR[bool]
        Whether the interlock is acknowledged (``Intlk:Ack-Sts``).
    """

    def __init__(self, prefix: str, name: str = ""):
        self.request = epics_signal_rw(bool, f"{prefix}Intlk:Ack-Sel")
        self.acknowledged = epics_signal_r(bool, f"{prefix}Intlk:Ack-Sts")
        super().__init__(name=name)

    @AsyncStatus.wrap
    async def set(self, value: bool):
        if await self.acknowledged.get_value() == value:
            return
        await self.request.set(value)
        await wait_for_value(self.acknowledged, value, timeout=DEFAULT_TIMEOUT)


class PPSShutter(Shutter):
    """PPS shutter whose interlock must be acknowledged after it trips.

    Takes the same arguments as `Shutter`.

    Attributes
    ----------
    interlock_ack : InterlockAcknowledge
        Move it to True to acknowledge the interlock.
    """

    def __init__(self, prefix: str, name: str = "", **kwargs):
        self.interlock_ack = InterlockAcknowledge(prefix)
        super().__init__(prefix, name=name, **kwargs)
