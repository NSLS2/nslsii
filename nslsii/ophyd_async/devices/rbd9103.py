from ophyd_async.core import (
    StandardReadable,
    SignalR,
    SignalRW,
    StrictEnum,
    AsyncStatus,
)
from ophyd_async.core import StandardReadableFormat as Format
from ophyd_async.epics.core import PvSuffix, EpicsDevice
from typing import Annotated as A


class RBD9103Range(StrictEnum):
    RNG_AUTO = "Auto"
    RNG_2_NA = "2nA"
    RNG_20_NA = "20nA"
    RNG_200_NA = "200nA"
    RNG_2_UA = "2uA"
    RNG_20_UA = "20uA"
    RNG_200_UA = "200uA"
    RNG_2_MA = "2mA"


class RBD9103Input(StrictEnum):
    NORMAL = "Normal"
    GROUNDED = "Grounded"


class RBD9103SamplingMode(StrictEnum):
    SINGLE = "Single"
    MULTIPLE = "Multiple"
    CONTINUOUS = "Continuous"


class RBD9103InRangeState(StrictEnum):
    OK = "OK"
    UNDER = "Under"
    OVER = "Over"


class RBD9103Filter(StrictEnum):
    FLTR_00 = "0"
    FLTR_02 = "2"
    FLTR_04 = "4"
    FLTR_08 = "8"
    FLTR_16 = "16"
    FLTR_32 = "32"
    FLTR_64 = "64"


class RBD9103(StandardReadable, EpicsDevice):
    # Define all the signals for the RBD9103 device
    range: A[SignalRW[RBD9103Range], PvSuffix.rbv("Range"), Format.CONFIG_SIGNAL]
    range_actual: A[SignalR[RBD9103Range], PvSuffix("RangeActual_RBV")]
    sampling_rate: A[
        SignalRW[float], PvSuffix.rbv("SamplingRate"), Format.CONFIG_SIGNAL
    ]
    sampling_rate_actual: A[SignalR[float], PvSuffix("SamplingRateActual_RBV")]
    offset_null: A[SignalRW[bool], PvSuffix.rbv("OffsetNull"), Format.CONFIG_SIGNAL]
    input: A[SignalRW[RBD9103Input], PvSuffix.rbv("InputGnd"), Format.CONFIG_SIGNAL]
    bias: A[SignalRW[bool], PvSuffix.rbv("Bias"), Format.CONFIG_SIGNAL]
    filter: A[SignalRW[RBD9103Filter], PvSuffix.rbv("Filter"), Format.CONFIG_SIGNAL]
    sample: A[SignalRW[bool], PvSuffix.rbv("Sample")]
    sampling_mode: A[
        SignalRW[RBD9103SamplingMode],
        PvSuffix.rbv("SamplingMode"),
        Format.HINTED_SIGNAL,
    ]
    num_samples: A[SignalRW[int], PvSuffix.rbv("NumSamples")]
    sample_counter: A[SignalR[int], PvSuffix("SampleCounter_RBV")]
    avg_only_stable: A[
        SignalRW[bool], PvSuffix.rbv("AvgOnlyStable"), Format.CONFIG_SIGNAL
    ]
    current: A[SignalR[float], PvSuffix("Current_RBV"), Format.HINTED_SIGNAL]
    avg_current: A[SignalR[float], PvSuffix("AvgCurrent_RBV"), Format.HINTED_SIGNAL]
    current_units: A[SignalR[str], PvSuffix("CurrentUnits_RBV"), Format.CONFIG_SIGNAL]
    stable: A[SignalR[bool], PvSuffix("Stable_RBV"), Format.HINTED_SIGNAL]
    avg_current_units: A[
        SignalR[str], PvSuffix("AvgCurrentUnits_RBV"), Format.CONFIG_SIGNAL
    ]
    in_range: A[
        SignalR[RBD9103InRangeState], PvSuffix("InRange_RBV"), Format.HINTED_SIGNAL
    ]

    def __init__(self, prefix: str, timeout=5.0, name: str = ""):
        super().__init__(prefix=prefix, name=name)
        self.base_timeout = timeout
        self._min_sampling_rate = 20
        self._max_sampling_rate = 9999

    @AsyncStatus.wrap
    async def stage(self):
        set_sampling_rate = await self.sampling_rate.get_value()
        if (
            set_sampling_rate < self._min_sampling_rate
            or set_sampling_rate > self._max_sampling_rate
        ):
            raise ValueError(
                f"Sampling rate must be between {self._min_sampling_rate} and {self._max_sampling_rate} ms"
            )

        sampling = await self.sample.get_value()
        if sampling:
            await self.sample.set(False)

        await self.sampling_mode.set(RBD9103SamplingMode.CONTINUOUS)
        await self.sample.set(True)

        return super().stage()
