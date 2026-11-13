"""Single axis PandA-triggered flyscans for NSLS-II."""

import asyncio
import math
from collections import defaultdict
from dataclasses import dataclass, replace
from typing import Any, Literal, Mapping, Sequence

from ophyd_async.core import (
    ConfinedModel,
    DetectorTrigger,
    Device,
    FlyableLogic,
    FlyMotorInfo,
    StandardDetector,
    TriggerInfo,
    wait_for_value,
)

from ophyd_async.fastcs.panda import PandaPcompDirection
from ophyd_async.core import SignalR, SignalRW

from ..ophyd_async.devices.motors import PandAConnectedMotor
from ..ophyd_async.devices.panda import (
    NSLS2Panda,
    PandaPosOutCapture,
    ttl_outputs_from_names,
    ttl_outputs_to_names,
)
from bluesky import plan_patterns, plan_stubs as bps, preprocessors as bpp
from bluesky.protocols import Movable
from pydantic import ConfigDict


@dataclass(frozen=True)
class PandADatasetInfo:

    name: str
    units: str
    scale: float = 1.0
    offset: float = 0.0
    capture: PandaPosOutCapture = PandaPosOutCapture.VALUE


@dataclass(frozen=True, kw_only=True)
class PandADataset(PandADatasetInfo):

    signal: SignalR


def _pos_out_attributes(signal: SignalR) -> dict[str, SignalRW]:
    """Find the dataset, units, scale, offset and capture signals of a PandA pos_out."""
    block = signal.parent
    field = None
    if block is not None:
        field = next((name for name, child in block.children() if child is signal), None)
    if block is None or field is None:
        raise ValueError(f"Signal {signal.name} is not attached to a PandA block.")

    attributes = {}
    for attr in ("dataset", "units", "scale", "offset", "capture"):
        attr_signal = getattr(block, f"{field}_{attr}", None)
        if not isinstance(attr_signal, SignalRW):
            raise ValueError(
                f"Signal {signal.name} is not a PandA position output, "
                f"{block.name} has no '{field}_{attr}' signal."
            )
        attributes[attr] = attr_signal
    return attributes


def _invert_pcomp_direction(direction: PandaPcompDirection) -> PandaPcompDirection:
    match direction:
        case PandaPcompDirection.POSITIVE:
            return PandaPcompDirection.NEGATIVE
        case PandaPcompDirection.NEGATIVE:
            return PandaPcompDirection.POSITIVE
        case _:
            return PandaPcompDirection.EITHER


class SingleAxisFlyscanInfo(ConfinedModel):
    """Information for a single axis flyscan.

    Attributes
    ----------
    start : int
        The start position for the flyscan, in encoder counts
    input_encoder_number : Literal[1, 2, 3, 4]
        The input encoder number to use for the flyscan
    invert_encoder : bool
        Whether to invert the encoder signal
    num_pulses : int
        The number of pulses to send during the flyscan
    direction : PandaPcompDirection
        The direction of the flyscan, either positive or negative
    pulse_width : float | int
        The width of each pulse, in counts for position based scans, s for time based
    pulse_step : float | int
        The step between pulses, in counts for position based scans, s for time based
    pulse_divisors : Mapping[int, int] | None
        Divisor of the PULSE1 train for each TTL output to drive; others are untouched
    min_acquire_period : float
        The shortest livetime + deadtime of any detector
    livetime : float
        Width of each PULSE1 pulse in position based scans, s
    time_based : bool
        If true, equally spaced in time triggers. Otherwise, equally spaced in position
    position_dataset : PandADatasetInfo
        How to record the input encoder position; the signal is deduced from
        input_encoder_number
    addtl_datasets : Sequence[PandADataset]
        Additional PandA position outputs to record
    """

    # PandADataset holds a SignalR, which pydantic cannot validate.
    model_config = ConfigDict(arbitrary_types_allowed=True)

    start: int
    direction: PandaPcompDirection
    num_pulses: int
    pulse_width: float | int
    pulse_step: float | int
    pulse_divisors: Mapping[int, int] | None = None
    min_acquire_period: float
    livetime: float
    time_based: bool
    input_encoder_number: Literal[1, 2, 3, 4] = 1
    invert_encoder: bool = True
    position_dataset: PandADatasetInfo
    addtl_datasets: Sequence[PandADataset] = ()


class SingleAxisFlyableLogic(FlyableLogic[SingleAxisFlyscanInfo, None]):
    """Logic class for setting up a PandABox for a single axis flyscan."""

    def __init__(self, panda: NSLS2Panda) -> None:
        self.panda = panda

    async def on_prepare(self, value: SingleAxisFlyscanInfo) -> None:
        pcomp = self.panda.pcomp[1]
        pulse = self.panda.pulse[1]
        pulse_dividers = self.panda.div
        ttl_outputs = self.panda.ttlout
        bita = self.panda.bits.a

        pulse_divisors = value.pulse_divisors or {}
        if any(divisor < 1 for divisor in pulse_divisors.values()):
            raise ValueError(f"Pulse divisors must be >= 1, got {dict(pulse_divisors)}.")
        unknown_ttls = set(pulse_divisors) - set(ttl_outputs.keys())
        if unknown_ttls:
            raise ValueError(
                f"TTL outputs {sorted(unknown_ttls)} do not exist on {self.panda.name}."
            )

        # PULSE1.OUT is the undivided train, so only divisors > 1 need a DIV block.
        divided = sorted({d for d in pulse_divisors.values() if d > 1})
        if len(divided) > len(pulse_dividers):
            raise ValueError(
                f"Requested {len(divided) + 1} distinct pulse trains, but only "
                f"{len(pulse_dividers) + 1} are supported "
                f"(PULSE1 plus {len(pulse_dividers)} dividers)."
            )
        div_numbers = dict(zip(divided, sorted(pulse_dividers.keys())))

        # Resolve before creating any coroutines so a bad dataset fails cleanly.
        position_signal = self.panda.inenc[value.input_encoder_number].val
        position_dataset = value.position_dataset
        if value.invert_encoder:
            position_dataset = replace(position_dataset, scale=-position_dataset.scale)
        dataset_attributes = [
            (position_dataset, _pos_out_attributes(position_signal)),
            *(
                (dataset, _pos_out_attributes(dataset.signal))
                for dataset in value.addtl_datasets
                if dataset.signal is not position_signal
            ),
        ]

        # Setup the pcomp block to watch the requested encoder input, crossing start in the direction specified.
        coros = [
            pcomp.dir.set(value.direction if not value.invert_encoder else _invert_pcomp_direction(value.direction)),
            pcomp.start.set(value.start),
            pcomp.inp.set(f"INENC{value.input_encoder_number}.VAL"),
            bita.set(False),
        ]

        # DIV inp, enable and first_pulse are assumed to be set in the PandA layout.
        coros.extend(
            [
                pulse_dividers[div_number].divisor.set(divisor)
                for divisor, div_number in div_numbers.items()
            ]
        )

        # Only drive TTL outputs wired to this scan's detectors; others may serve other hardware.
        for ttl_number, divisor in pulse_divisors.items():
            source = "PULSE1.OUT" if divisor == 1 else f"DIV{div_numbers[divisor]}.OUTD"
            coros.append(ttl_outputs[ttl_number].val.set(source))

        if not value.time_based:
            # In position based mode
            coros.extend(
                [
                    pcomp.pulses.set(value.num_pulses),
                    pcomp.width.set(int(value.pulse_width)),
                    pcomp.step.set(int(value.pulse_step)),
                    pulse.pulses.set(1),
                    pulse.width.set(value.livetime),
                    pulse.step.set(value.min_acquire_period),
                ]
            )
        else:
            coros.extend(
                [
                    pcomp.pulses.set(1),
                    pcomp.width.set(1),
                    pcomp.step.set(2),
                    pulse.pulses.set(value.num_pulses),
                    pulse.width.set(value.pulse_width),
                    pulse.step.set(value.pulse_step),
                ]
            )

        for dataset, attributes in dataset_attributes:
            coros.extend(
                [
                    attributes["dataset"].set(dataset.name),
                    attributes["units"].set(dataset.units),
                    attributes["scale"].set(dataset.scale),
                    attributes["offset"].set(dataset.offset),
                    attributes["capture"].set(dataset.capture),
                ]
            )
        await asyncio.gather(*coros)

        # Only enable the pcomp block after all the configuration
        # has been set.
        await bita.set(True)

    async def on_kickoff(self, ctx: None) -> None:
        await wait_for_value(self.panda.pcomp[1].active, True, timeout=10)

    async def on_complete(self, ctx: None) -> None:
        await wait_for_value(self.panda.pcomp[1].active, False, timeout=None)

    async def stop(self):
        await self.panda.bits.a.set(False)
        await wait_for_value(self.panda.pcomp[1].active, False, timeout=1)


def calculate_move_time_for_flyscan(
    trigger_infos: Sequence[TriggerInfo],
    travel_distance: float,
    max_motor_velocity: float,
) -> float:
    """Calculate the time for a motor move during a flyscan.

    The motor travel and acquisition happen concurrently. The total time is
    whichever takes longer: the motor travel time or the total acquisition time.

    Parameters
    ----------
    trigger_infos : Sequence[TriggerInfo]
        The trigger information for each acquisition channel.
    travel_distance : float
        The distance the motor will travel during the flyscan.
    max_motor_velocity : float
        The maximum velocity of the motor.

    Returns
    -------
    float
        The time for the motor move during the flyscan.
    """
    if max_motor_velocity <= 0:
        raise ValueError(
            f"Motor max velocity must be positive to plan a flyscan, got {max_motor_velocity}."
        )
    fastest_possible_move_time = travel_distance / max_motor_velocity
    longest_total_acq_time = max(
        (ti.livetime + ti.deadtime) * ti.number_of_exposures for ti in trigger_infos
    )

    return max(fastest_possible_move_time, longest_total_acq_time)


@dataclass(frozen=True)
class TriggeredDetector:
    detector: StandardDetector
    trigger_info: TriggerInfo


def set_ttl_output(panda: NSLS2Panda, ttl_number: int, device: Device | None = None):
    """Plan stub recording which device is wired to a PandA TTL output.

    Passing no ``device`` marks the output as unwired.
    """
    names = yield from bps.rd(panda.ttl_output_devices)
    ttl_outputs = ttl_outputs_from_names(names)
    ttl_outputs[ttl_number] = device.name if device is not None else ""
    yield from bps.mv(panda.ttl_output_devices, ttl_outputs_to_names(ttl_outputs))


def construct_fly_info_models(
    detectors: Sequence[TriggeredDetector],
    start_position: float,
    stop_position: float,
    encoder_resolution: float,
    max_motor_velocity: float,
    current_position: float,
    current_enc_position: int,
    position_dataset_name: str,
    position_dataset_units: str,
    ttl_outputs: Mapping[int, str] | None = None,
    input_encoder_number: Literal[1, 2, 3, 4] = 1,
    invert_encoder: bool = True,
    time_based: bool = False,
    datasets: Sequence[PandADataset] = ()
) -> tuple[SingleAxisFlyscanInfo, FlyMotorInfo]:
    """Construct the fly info models for a single axis flyscan.

    Returns
    -------
    tuple[SingleAxisFlyscanInfo, FlyMotorInfo]
        The fly info models for a single axis flyscan.
    """

    trigger_infos = [config.trigger_info for config in detectors]

    # The detector needing the most pulses runs off the undivided PULSE1 train.
    fastest_trigger_info = max(trigger_infos, key=lambda ti: ti.number_of_exposures)
    num_pulses = fastest_trigger_info.number_of_exposures
    min_acquire_period = fastest_trigger_info.livetime + fastest_trigger_info.deadtime

    # Detectors wired to the PandA get a pulse train divided down to their exposures.
    wired_names = set((ttl_outputs or {}).values())
    for config in detectors:
        num_exposures = config.trigger_info.number_of_exposures
        if num_exposures < num_pulses and config.detector.name not in wired_names:
            raise ValueError(
                f"Detector {config.detector.name} needs {num_exposures} of the "
                f"{num_pulses} pulses but is not wired to a PandA TTL output, so it "
                "cannot be given a divided pulse train."
            )

    pulse_divisors = {}
    for ttl_number, device_name in (ttl_outputs or {}).items():
        config = next(
            (c for c in detectors if c.detector.name == device_name), None
        )
        if config is None:
            continue
        num_exposures = config.trigger_info.number_of_exposures
        if num_pulses % num_exposures != 0:
            raise ValueError(
                f"Detector {config.detector.name} needs {num_exposures} exposures, "
                f"which does not evenly divide the {num_pulses} total pulses."
            )
        pulse_divisors[ttl_number] = num_pulses // num_exposures

    enc_sign = -1 if invert_encoder else 1
    dist_to_start = (start_position - current_position) / encoder_resolution
    start_in_counts = round(current_enc_position + enc_sign * dist_to_start)
    signed_travel_counts = round((stop_position - start_position) / encoder_resolution)

    travel_counts = abs(signed_travel_counts)
    move_time = calculate_move_time_for_flyscan(
        trigger_infos,
        abs(stop_position - start_position),
        max_motor_velocity,
    )

    if not time_based:
        if num_pulses < 2:
            raise ValueError("Position based flyscans need at least 2 pulses.")
        if travel_counts % (num_pulses - 1) != 0:
            # Subtract one from pulses because the num of steps is one less than
            # the number of pulses
            raise ValueError(
                f"Travel distance in counts ({travel_counts}) is not evenly divisible "
                f"by the number of pulses ({num_pulses - 1})."
            )
        elif travel_counts < (num_pulses - 1) * 2:
            raise ValueError(
                f"Travel distance in counts ({travel_counts}) is less than the minimum"
                f" required for the number of pulses ({num_pulses}). At least two "
                f"counts are required between each pulse, one for livetime, one "
                f"for deadtime({2 * (num_pulses - 1)})."
            )
        pulse_width = 1
        pulse_step = travel_counts // (num_pulses - 1)
    else:
        pulse_width = fastest_trigger_info.livetime
        # The first pulse fires at the start, so num_pulses - 1 steps reach the stop.
        pulse_step = move_time / (num_pulses - 1) if num_pulses > 1 else move_time

    flyer_info = SingleAxisFlyscanInfo(
        start=start_in_counts,
        num_pulses=num_pulses,
        direction=PandaPcompDirection.POSITIVE
        if signed_travel_counts > 0
        else PandaPcompDirection.NEGATIVE,
        pulse_width=pulse_width,
        pulse_step=pulse_step,
        pulse_divisors=pulse_divisors,
        min_acquire_period=min_acquire_period,
        livetime=fastest_trigger_info.livetime,
        time_based=time_based,
        input_encoder_number=input_encoder_number,
        invert_encoder=invert_encoder,
        # Scale/offset map encoder counts back to absolute motor position.
        # The flyer negates the scale for inverted encoders, so offset uses enc_sign.
        position_dataset=PandADatasetInfo(
            name=position_dataset_name,
            units=position_dataset_units,
            scale=encoder_resolution,
            offset=start_position - enc_sign * start_in_counts * encoder_resolution,
        ),
        addtl_datasets=datasets,
    )

    motor_info = FlyMotorInfo(
        start_position=start_position,
        end_position=stop_position,
        time_for_move=move_time,
    )
    return flyer_info, motor_info


def single_axis_flyscan(
    detectors: Sequence[TriggeredDetector],
    panda: NSLS2Panda,
    motor: PandAConnectedMotor,
    start_position: float,
    stop_position: float,
    reset_position: bool = True,
    time_based: bool = True,
    stream_name: str = "primary",
    min_flush_period: float = 1,
    datasets: Sequence[PandADataset] = (),
    md: dict[str, Any] | None = None,
):
    """Fly ``motor`` once, triggering detectors from the PandA.

    Parameters
    ----------
    detectors : Sequence[TriggeredDetector]
        The detectors and their trigger info. The detector needing the most
        exposures gets every PULSE1 pulse; the rest get divided trains through the
        TTL outputs recorded in ``panda.ttl_output_devices``.
    panda : NSLS2Panda
        The PandA generating triggers and recording positions.
    motor : PandAConnectedMotor
        The motor to fly, read back through a PandA encoder input.
    start_position : float
        Where triggering starts, in motor units.
    stop_position : float
        Where triggering stops, in motor units.
    reset_position : bool, optional
        Whether to move ``motor`` back to where it started afterwards.
    time_based : bool, optional
        Whether triggers are equally spaced in time rather than position.
    stream_name : str, optional
        The event stream name.
    min_flush_period : float, optional
        The minimum period between collects, in s.
    datasets : Sequence[PandADataset], optional
        Additional PandA position outputs to record.
    md : dict[str, Any] | None, optional
        Extra metadata for the run's start document.
    """
    if not detectors:
        raise ValueError("At least one detector is required.")

    all_detectors = [*(config.detector for config in detectors), panda]

    # The detector needing the most pulses runs off the undivided PULSE1 train.
    fastest_trigger_info = max(
        (config.trigger_info for config in detectors),
        key=lambda ti: ti.number_of_exposures,
    )

    # Level-triggered exposures follow the PULSE1 width, which comes from the fastest detector.
    mismatched = [
        config.detector.name
        for config in detectors
        if config.trigger_info.trigger == DetectorTrigger.EXTERNAL_LEVEL
        and not (
            math.isclose(config.trigger_info.livetime, fastest_trigger_info.livetime)
            and math.isclose(config.trigger_info.deadtime, fastest_trigger_info.deadtime)
        )
    ]
    if mismatched:
        raise ValueError(
            f"Level-triggered detectors {mismatched} must use the same livetime "
            f"({fastest_trigger_info.livetime}) and deadtime "
            f"({fastest_trigger_info.deadtime}) as the fastest detector, which "
            "sets the PandA pulse width."
        )
    
    # Construct ephemeral flyer for the single axis flyscan
    single_axis_panda_flyer = SingleAxisFlyableLogic(panda).with_device()
    all_devices = [*all_detectors, single_axis_panda_flyer, motor]

    # Read once so rewiring mid-plan can't change this scan.
    ttl_output_names = yield from bps.rd(panda.ttl_output_devices)
    # Catches the constructor's wiring, which is set before the TTLOUT blocks are known.
    panda.check_ttl_output_devices(ttl_output_names)
    ttl_outputs = ttl_outputs_from_names(ttl_output_names)

    _md = {
        "detectors": {
            det_config.detector.name: det_config.trigger_info.model_dump(mode="json")
            for det_config in detectors
        },
        "panda": panda.name,
        "ttl_outputs": {str(n): name for n, name in ttl_outputs.items()},
        "motor": motor.name,
        "time_based": time_based,
        "start_position": start_position,
        "stop_position": stop_position,
        "plan_name": "single_axis_flyscan",
        # TODO: Create a single axis flyscan spec
        "tiled_specs": [
            
        ],
    }
    _md.update(md or {})

    # Motor.prepare changes position and velocity without 'set' Msgs, so stash them here.
    initial_motor_state: dict[str, float] = {}

    @bpp.stage_decorator(all_devices)
    @bpp.run_decorator(md=_md)
    def _body():

        # Get various initial motor parameters
        encoder_res = yield from bps.rd(motor.encoder_resolution)
        max_velocity = yield from bps.rd(motor.max_velocity)
        current_position = yield from bps.rd(motor.user_readback)
        initial_motor_state["position"] = current_position
        initial_motor_state["velocity"] = yield from bps.rd(motor.velocity)
        motor_units = yield from bps.rd(motor.motor_egu)
        input_encoder_num = yield from bps.rd(motor.input_encoder_num)
        invert_encoder = yield from bps.rd(motor.inverse_encoder)
        current_enc = yield from bps.rd(panda.inenc[input_encoder_num].val)

        # PandA captures once per pulse, grouped into events like the fastest detector.
        panda_trigger_info = TriggerInfo(
            number_of_events=fastest_trigger_info.number_of_events,
            livetime=fastest_trigger_info.livetime,
            deadtime=fastest_trigger_info.deadtime,
            trigger=DetectorTrigger.EXTERNAL_LEVEL,
            collections_per_event=(
                fastest_trigger_info.collections_per_event
                * fastest_trigger_info.exposures_per_collection
            ),
        )

        flyer_info, motor_info = construct_fly_info_models(
            detectors=detectors,
            start_position=start_position,
            stop_position=stop_position,
            encoder_resolution=encoder_res,
            max_motor_velocity=max_velocity,
            current_position=current_position,
            current_enc_position=current_enc,
            position_dataset_name=motor.name,
            position_dataset_units=motor_units,
            ttl_outputs=ttl_outputs,
            input_encoder_number=input_encoder_num,
            invert_encoder=invert_encoder,
            time_based=time_based,
            datasets=datasets,
        )

        yield from bps.prepare(motor, motor_info, group="prepare")
        yield from bps.wait(group="prepare")

        # Arm the PandA and detectors only once the motor is at its run-up position,
        # so no frames are captured before kickoff.
        yield from bps.prepare(single_axis_panda_flyer, flyer_info, group="arm")
        yield from bps.prepare(panda, panda_trigger_info, group="arm")
        for det_config in detectors:
            yield from bps.prepare(det_config.detector, det_config.trigger_info, group="arm")

        yield from bps.wait(group="arm")

        slowest_trigger_info = max(
            (config.trigger_info for config in detectors),
            key=lambda ti: ti.livetime + ti.deadtime,
        )
        yield from bps.fly_stub(
            all_devices,
            collect_flush_period=max(
                min_flush_period, slowest_trigger_info.livetime + slowest_trigger_info.deadtime
            ),
            stream_name=stream_name,
        )

    def _cleanup():
        yield from bps.abs_set(motor.motor_stop, 1)
        # Disarm position compare so a later move can't fire triggers.
        yield from bps.mv(panda.bits.a, False)
        if "velocity" in initial_motor_state:
            yield from bps.mv(motor.velocity, initial_motor_state["velocity"])
        if "position" in initial_motor_state and reset_position:
            yield from bps.mv(motor, initial_motor_state["position"])

    return (yield from bpp.finalize_wrapper(_body(), _cleanup()))


@dataclass(frozen=True)
class StepAxis:
    """A stepped axis of an N-D grid, as in :func:`bluesky.plans.grid_scan`."""

    motor: Movable
    start: float
    stop: float
    num: int
    # Reverse direction on alternate passes; not allowed on the outermost axis.
    snake: bool = False


def single_axis_flyscan_nd(
    detectors: Sequence[TriggeredDetector],
    panda: NSLS2Panda,
    motor: PandAConnectedMotor,
    step_axes: Sequence[StepAxis] = (),
    *,
    start_position: float,
    stop_position: float,
    reset_positions: bool = True,
    num_repeats: int = 1,
    delay_between_points: float = 0.0,
    alternate_flyscan_dir: bool = False,
    time_based: bool = True,
    stream_name: str = "primary",
    min_flush_period: float = 1,
    datasets: Sequence[PandADataset] = (),
    md: dict[str, Any] | None = None,
):
    """N-D step scan: a single axis flyscan at each point of an N-D grid.

    With ``reset_positions``, the step motors and flown motor are returned to their
    starting positions when the scan ends.

    Parameters
    ----------
    detectors : Sequence[TriggeredDetector]
        The detectors and their trigger info for each flyscan.
    panda : NSLS2Panda
        The PandA generating triggers and recording positions.
    motor : PandAConnectedMotor
        The motor flown during each flyscan.
    step_axes : Sequence[StepAxis], optional
        The stepped axes, outermost (slowest) first. With no axes, the flyscan
        is repeated in place.
    start_position : float
        The flyscan start position of ``motor``.
    stop_position : float
        The flyscan stop position of ``motor``.
    reset_positions : bool, optional
        Whether to return all motors to where they started when the scan ends.
    num_repeats : int, optional
        The number of times to run the whole trajectory.
    delay_between_points : float, optional
        Seconds to wait between consecutive flyscans.
    alternate_flyscan_dir : bool, optional
        Whether to reverse the flyscan direction at every other grid point.
    time_based : bool, optional
        Whether triggers are equally spaced in time rather than position.
    stream_name : str, optional
        The event stream name for each flyscan.
    min_flush_period : float, optional
        The minimum period between collects during each flyscan, in s.
    datasets : Sequence[PandADataset], optional
        Additional PandA position outputs to record in each flyscan.
    md : dict[str, Any] | None, optional
        Extra metadata added to every flyscan's start document.
    """
    if step_axes and step_axes[0].snake:
        raise ValueError("The outermost axis cannot be snaked.")
    motors = [axis.motor for axis in step_axes]

    # Construct the grid of positions for the scan.
    if step_axes:
        outer = step_axes[0]
        pattern_args: list[Any] = [outer.motor, outer.start, outer.stop, outer.num]
        for axis in step_axes[1:]:
            pattern_args += [axis.motor, axis.start, axis.stop, axis.num, axis.snake]
        grid = plan_patterns.outer_product(pattern_args)
    else:
        grid = [{}]  # When no axes are specified, create a single-point grid.

    # Index of each point along every axis, in the same (snaked) order as the grid.
    coordinates: list[tuple[int, ...]] = [()]
    for axis in step_axes:
        coordinates = [
            (*prefix, k)
            for j, prefix in enumerate(coordinates)
            for k in (
                reversed(range(axis.num)) if axis.snake and j % 2 else range(axis.num)
            )
        ]

    # Repeat the trajectory the specified number of times.
    trajectory = grid * num_repeats
    coordinates = coordinates * num_repeats

    # The flown motor only gets reset once at the end, not after every flyscan.
    initial_motor_position: dict[str, float] = {}

    @bpp.reset_positions_decorator(motors if reset_positions else [])
    def _body():
        initial_motor_position["position"] = yield from bps.rd(motor.user_readback)
        num_points = len(trajectory)
        pos_cache: defaultdict[Any, Any] = defaultdict(lambda: None)
        previous_uid: str | None = None
        for i, (point, coordinate) in enumerate(zip(trajectory, coordinates, strict=True)):
            # A deferred pause stops here; resuming starts this point afresh.
            yield from bps.checkpoint()
            if i > 0 and delay_between_points > 0:
                yield from bps.sleep(delay_between_points)

            # Only moves the axes whose position changed since the last point.
            yield from bps.move_per_step(point, pos_cache)

            reverse = alternate_flyscan_dir and i % 2 == 1
            point_md: dict[str, Any] = {
                "plan_name": "single_axis_flyscan_nd",
                **(md or {}),
                "grid_index": i,
                "grid_coordinate": coordinate,
                "num_grid_points": num_points,
                "grid_positions": {m.name: pos for m, pos in point.items()},
            }
            if previous_uid is not None:
                point_md["previous_grid_point_uid"] = previous_uid

            previous_uid = yield from single_axis_flyscan(
                detectors,
                panda,
                motor,
                start_position=stop_position if reverse else start_position,
                stop_position=start_position if reverse else stop_position,
                reset_position=False,
                time_based=time_based,
                stream_name=stream_name,
                min_flush_period=min_flush_period,
                datasets=datasets,
                md=point_md,
            )

    def _cleanup():
        if reset_positions and "position" in initial_motor_position:
            yield from bps.mv(motor, initial_motor_position["position"])

    return (yield from bpp.finalize_wrapper(_body(), _cleanup()))
