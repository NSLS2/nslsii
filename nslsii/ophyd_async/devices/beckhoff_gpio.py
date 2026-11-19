"""ophyd-async devices for Beckhoff EK9000 bus couplers with EL terminals (GPIO).

PV names follow the NSLS-II standard used by the ``elio`` role in
``nsls2.ioc_deploy`` together with https://github.com/NSLS2/elio-ioc:

* Bus coupler status: ``{SYS}{{{DEV}}}:<Name>``, e.g. ``XF:31ID1-ES{GPIO:1}:EbusStatus``
* Terminal channels: ``{SYS}{{{DEV}_{POS}}}<Kind>:Ch<N>-<Suffix>``, e.g.
  ``XF:31ID1-ES{GPIO:1_3}DI:Ch1-Sts``
"""

from collections.abc import Iterable, Mapping
from enum import StrEnum
from typing import Any

from bluesky.protocols import Movable
from ophyd_async.core import (
    AsyncStatus,
    DeviceVector,
    SignalR,
    SignalRW,
    StandardReadable,
)
from ophyd_async.core import StandardReadableFormat as Format
from ophyd_async.epics.core import epics_signal_r, epics_signal_rw


class BeckhoffChannelKind(StrEnum):
    """Channel type component of elio PV names, e.g. ``DI`` in ``DI:Ch1-Sts``."""

    DIGITAL_INPUT = "DI"
    DIGITAL_OUTPUT = "DO"
    RELAY_OUTPUT = "Rly"
    ANALOG_INPUT = "AI"
    ANALOG_OUTPUT = "AO"
    TEMPERATURE = "T"  # Either thermocouple or RTD


class BeckhoffChannel(StandardReadable):
    """Base class for a single channel of a Beckhoff EL terminal."""

    _kind: BeckhoffChannelKind
    _suffix: str
    _datatype: type
    _writable: bool = False

    def __init__(self, terminal_prefix: str, channel: int, name: str = ""):
        pv = f"{terminal_prefix}{self._kind}:Ch{channel}-{self._suffix}"
        fmt = Format.UNCACHED_SIGNAL if self._writable else Format.HINTED_SIGNAL
        with self.add_children_as_readables(fmt):
            if self._writable:
                self.value = epics_signal_rw(self._datatype, pv)
            else:
                self.value = epics_signal_r(self._datatype, pv)
        with self.add_children_as_readables(Format.CONFIG_SIGNAL):
            self.description = epics_signal_rw(str, f"{pv}.DESC")
        super().__init__(name=name)

    def set_name(self, name: str, *, child_name_separator: str | None = None) -> None:
        super().set_name(name, child_name_separator=child_name_separator)
        # Use the channel's own name as the data key for its value
        self.value.set_name(self.name)


class BeckhoffDigitalInput(BeckhoffChannel):
    _kind = BeckhoffChannelKind.DIGITAL_INPUT
    _suffix = "Sts"
    _datatype = bool
    value: SignalR[bool]


class BeckhoffDigitalOutput(BeckhoffChannel, Movable[bool]):
    _kind = BeckhoffChannelKind.DIGITAL_OUTPUT
    _suffix = "Cmd"
    _datatype = bool
    _writable = True
    value: SignalRW[bool]

    @AsyncStatus.wrap
    async def set(self, value: bool):
        await self.value.set(value)


class BeckhoffRelayOutput(BeckhoffDigitalOutput):
    _kind = BeckhoffChannelKind.RELAY_OUTPUT


class BeckhoffAnalogInput(BeckhoffChannel):
    _kind = BeckhoffChannelKind.ANALOG_INPUT
    _suffix = "I"
    _datatype = float
    value: SignalR[float]


class BeckhoffTempInput(BeckhoffAnalogInput):
    _kind = BeckhoffChannelKind.TEMPERATURE


class BeckhoffAnalogOutput(BeckhoffChannel, Movable[float]):
    _kind = BeckhoffChannelKind.ANALOG_OUTPUT
    _suffix = "SP"
    _datatype = float
    _writable = True
    value: SignalRW[float]

    @AsyncStatus.wrap
    async def set(self, value: float):
        await self.value.set(value)


# Supported terminal models -> (channel class, number of channels), as provided by elio-ioc
BECKHOFF_TERMINAL_MODELS: dict[str, tuple[type[BeckhoffChannel], int]] = {
    "EL1034": (BeckhoffDigitalInput, 4),
    "EL1124": (BeckhoffDigitalInput, 4),
    "EL1808": (BeckhoffDigitalInput, 8),
    "EL2124": (BeckhoffDigitalOutput, 4),
    "EL2798": (BeckhoffRelayOutput, 8),
    "EL2808": (BeckhoffDigitalOutput, 8),
    "EL3008": (BeckhoffAnalogInput, 8),
    "EL3068": (BeckhoffAnalogInput, 8),
    "EL3154": (BeckhoffAnalogInput, 4),
    "EL3164": (BeckhoffAnalogInput, 4),
    "EL3314": (BeckhoffTempInput, 4),
    "EL3318": (BeckhoffTempInput, 8),
    "EL4008": (BeckhoffAnalogOutput, 8),
    "EL4038": (BeckhoffAnalogOutput, 8),
    "EL4104": (BeckhoffAnalogOutput, 4),
    "EL4134": (BeckhoffAnalogOutput, 4),
    "EL9505": (BeckhoffDigitalInput, 2),
}


class BeckhoffTerminal(StandardReadable):
    """A single Beckhoff EL terminal (slice) at a given position on the EK9000 bus."""

    def __init__(self, prefix: str, model: str, name: str = ""):
        if model not in BECKHOFF_TERMINAL_MODELS:
            raise ValueError(
                f"Unsupported Beckhoff terminal model {model!r}. "
                f"Supported models: {', '.join(BECKHOFF_TERMINAL_MODELS)}"
            )
        self.model = model
        channel_cls, num_channels = BECKHOFF_TERMINAL_MODELS[model]
        with self.add_children_as_readables():
            self.channels = DeviceVector(
                {chan: channel_cls(prefix, chan) for chan in range(1, num_channels + 1)}
            )
        super().__init__(name=name)


class BeckhoffEK9000Status(StandardReadable):
    """Status and configuration PVs of the EK9000 bus coupler (ek9000_status.db)."""

    def __init__(self, prefix: str, name: str = ""):
        with self.add_children_as_readables(Format.CONFIG_SIGNAL):
            self.analog_output_size = epics_signal_r(int, f"{prefix}AnalogOutputSize")
            self.analog_input_size = epics_signal_r(int, f"{prefix}AnalogInputSize")
            self.digital_input_size = epics_signal_r(int, f"{prefix}DigitalInputSize")
            self.digital_output_size = epics_signal_r(int, f"{prefix}DigitalOutputSize")
            self.hardware_version = epics_signal_r(int, f"{prefix}HardwareVersion")
            self.software_version_main = epics_signal_r(int, f"{prefix}SoftwareVersionMain")
            self.software_version_sub = epics_signal_r(int, f"{prefix}SoftwareVersionSub")
            self.software_version_beta = epics_signal_r(int, f"{prefix}SoftwareVersionBeta")
            self.serial_number = epics_signal_r(int, f"{prefix}SerialNumber")
            self.day_of_mfg = epics_signal_r(int, f"{prefix}DayOfMfg")
            self.month_of_mfg = epics_signal_r(int, f"{prefix}MonthOfMfg")
            self.year_of_mfg = epics_signal_r(int, f"{prefix}YearOfMfg")
            # WatchdogExpTime reads back the same coupler register that WatchdogTime writes
            self.watchdog_time = epics_signal_rw(
                int, read_pv=f"{prefix}WatchdogExpTime", write_pv=f"{prefix}WatchdogTime"
            )
            self.watchdog_type = epics_signal_rw(int, f"{prefix}WatchdogType")
            self.fallback_mode = epics_signal_rw(int, f"{prefix}FallbackMode")
            self.write_lock = epics_signal_rw(int, f"{prefix}WriteLock")
        self.fallbacks_triggered = epics_signal_r(int, f"{prefix}FallbacksTriggered")
        self.tcp_connections = epics_signal_r(int, f"{prefix}TCPConnections")
        self.ebus_status = epics_signal_r(int, f"{prefix}EbusStatus")
        self.ebus_control = epics_signal_rw(int, f"{prefix}EBusControl")
        super().__init__(name=name)


def _normalize_terminals(
    terminals: Mapping[int, str] | Iterable[Mapping[str, Any]],
) -> dict[int, str]:
    if isinstance(terminals, Mapping):
        return {int(pos): str(model) for pos, model in terminals.items()}
    normalized: dict[int, str] = {}
    for terminal in terminals:
        pos = int(terminal["POS"])
        if pos in normalized:
            raise ValueError(f"Duplicate terminal position {pos}")
        normalized[pos] = str(terminal["MODEL"])
    return normalized


class BeckhoffGPIO(StandardReadable):
    """Beckhoff EK9000 bus coupler with a set of EL terminals, deployed with the NSLS-II elio IOC.

    Parameters
    ----------
    sys : str
        The ``SYS`` portion of the PV prefix, e.g. ``"XF:31ID1-ES"``.
    dev : str
        The ``DEV`` portion of the PV prefix, e.g. ``"GPIO:1"``.
    terminals : Mapping[int, str] | Iterable[Mapping[str, Any]]
        Either a mapping of terminal position to model, e.g. ``{1: "EL3068", 2: "EL4008"}``,
        or the ``terminals`` list from the ``nsls2.ioc_deploy`` elio IOC config,
        e.g. ``[{"POS": 1, "MODEL": "EL3068"}, {"POS": 2, "MODEL": "EL4008"}]``.
    name : str
        Name of the device.

    Channels are accessed as ``gpio.terminals[POS].channels[CHAN]``.
    """

    def __init__(
        self,
        sys: str,
        dev: str,
        terminals: Mapping[int, str] | Iterable[Mapping[str, Any]],
        name: str = "",
    ):
        with self.add_children_as_readables():
            self.coupler = BeckhoffEK9000Status(f"{sys}{{{dev}}}:")
            self.terminals = DeviceVector(
                {
                    pos: BeckhoffTerminal(f"{sys}{{{dev}_{pos}}}", model)
                    for pos, model in sorted(_normalize_terminals(terminals).items())
                }
            )
        super().__init__(name=name)

    @classmethod
    def from_ioc_config(cls, ioc_config: Mapping[str, Any], name: str = "") -> "BeckhoffGPIO":
        """Create a device from an ``nsls2.ioc_deploy`` elio IOC configuration dictionary."""
        env = ioc_config["environment"]
        return cls(env["SYS"], env["DEV"], ioc_config["terminals"], name=name)
