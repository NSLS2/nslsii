"""An ophyd-async device for the PandABox.

The device in this module extends the base HDFPanda device to include
block configurations standard in the NSLS-II configuration.
"""

from collections.abc import Mapping, Sequence

from ophyd_async.core import (
    Device,
    DeviceVector,
    PathProvider,
    SignalR,
    SignalRW,
    StrictEnum,
    SubsetEnum,
    soft_signal_rw,
)
from ophyd_async.fastcs.panda import (
    HDFPanda,
    InencBlock,
    PandaBitMux,
    PandaPosMux,
    PandaTimeUnits,
    PcompBlock,
    PulseBlock,
)


class PandaDivFirstPulse(StrictEnum):
    OUTN = "OutN"
    OUTD = "OutD"


class PandaPosOutCapture(SubsetEnum):
    NO = "No"
    VALUE = "Value"
    DIFF = "Diff"
    SUM = "Sum"
    MEAN = "Mean"
    MIN = "Min"
    MAX = "Max"


class PandaPulseBlock(PulseBlock):
    trig: SignalRW[PandaBitMux]
    delay_units: SignalRW[PandaTimeUnits]
    width_units: SignalRW[PandaTimeUnits]
    step_units: SignalRW[PandaTimeUnits]
    out: SignalR[bool]


class PandaPcompBlock(PcompBlock):
    inp: SignalRW[PandaPosMux]
    pre_start: SignalRW[int]
    out: SignalR[bool]


class PandaInencBlock(InencBlock):
    val: SignalR[int]
    val_units: SignalRW[str]
    val_capture: SignalRW[PandaPosOutCapture]
    val_dataset: SignalRW[str]


class PandaDivBlock(Device):
    enable: SignalRW[PandaBitMux]
    inp: SignalRW[PandaBitMux]
    divisor: SignalRW[int]
    first_pulse: SignalRW[PandaDivFirstPulse]
    outd: SignalR[bool]
    outn: SignalR[bool]


class PandaTtlOutBlock(Device):
    val: SignalRW[PandaBitMux]


class PandaBitsBlock(Device):
    a: SignalRW[bool]
    b: SignalRW[bool]
    c: SignalRW[bool]
    d: SignalRW[bool]
    outa: SignalR[bool]
    outb: SignalR[bool]
    outc: SignalR[bool]
    outd: SignalR[bool]


class NSLS2Panda(HDFPanda):
    """HDF-writing PandA with typed blocks used by NSLS-II flyscan logic.

    Parameters
    ----------
    prefix : str
        The PV prefix of the PandABlocks IOC.
    path_provider : PathProvider
        Provides the path for the PandA HDF5 files.
    name : str, optional
        The device name.
    ttl_outputs : Mapping[int, str] | None, optional
        Name of the device wired to each TTL output, e.g.
        ``{1: "det_a", 3: "det_b"}``. Stored in the `ttl_output_devices` signal,
        so it can be changed at runtime, including from a plan.
    """

    pulse: DeviceVector[PandaPulseBlock]  # type: ignore[assignment]
    pcomp: DeviceVector[PandaPcompBlock]  # type: ignore[assignment]
    inenc: DeviceVector[PandaInencBlock]
    div: DeviceVector[PandaDivBlock]
    ttlout: DeviceVector[PandaTtlOutBlock]
    bits: PandaBitsBlock

    def __init__(
        self,
        prefix: str,
        path_provider: PathProvider,
        name: str = "",
        ttl_outputs: Mapping[int, str] | None = None,
    ):
        super().__init__(prefix, path_provider, name=name)
        # Element i is the name of the device wired to TTL output i + 1 ("" if none).
        self.ttl_output_devices = soft_signal_rw(
            Sequence[str],
            initial_value=ttl_outputs_to_names(ttl_outputs or {}),
            setter=self.check_ttl_output_devices,
        )
        self.add_config_signals(self.ttl_output_devices)
        self.set_name(self.name)

    def check_ttl_output_devices(self, names: Sequence[str] | None) -> None:
        """Raise if any wired TTL output is beyond the PandA's TTLOUT blocks.

        The number of blocks is only known once connected, so this is a no-op before.
        """
        num_ttl_blocks = len(self.ttlout)
        wired = ttl_outputs_from_names(names or [])
        if num_ttl_blocks and wired and max(wired) > num_ttl_blocks:
            raise ValueError(
                f"{self.name} has {num_ttl_blocks} TTL outputs, but devices are "
                f"wired to {sorted(n for n in wired if n > num_ttl_blocks)}."
            )


def ttl_outputs_to_names(ttl_outputs: Mapping[int, str]) -> list[str]:
    """Convert ``{ttl_number: device_name}`` to the `ttl_output_devices` format."""
    if any(ttl_number < 1 for ttl_number in ttl_outputs):
        raise ValueError(f"TTL output numbers start at 1, got {sorted(ttl_outputs)}.")
    size = max(ttl_outputs, default=0)
    return [ttl_outputs.get(n, "") for n in range(1, size + 1)]


def ttl_outputs_from_names(names: Sequence[str]) -> dict[int, str]:
    """Convert a `ttl_output_devices` value to ``{ttl_number: device_name}``."""
    return {n: name for n, name in enumerate(names, start=1) if name}


