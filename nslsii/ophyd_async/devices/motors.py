
from ophyd_async.core import soft_signal_r_and_setter
from ophyd_async.epics.motor import Motor as AsyncEpicsMotor
from ophyd_async.core import StandardReadableFormat as Format

class PandAConnectedMotor(AsyncEpicsMotor):
    """EPICS motor connected to a PandABox encoder input."""

    def __init__(self, prefix: str, input_encoder_num: int = 1, inverse_encoder: bool = True, name: str = ""):
        with self.add_children_as_readables(Format.CONFIG_SIGNAL):
            self.input_encoder_num, _ = soft_signal_r_and_setter(int, input_encoder_num)
            self.inverse_encoder, _ = soft_signal_r_and_setter(bool, inverse_encoder)
        super().__init__(prefix, name=name)
