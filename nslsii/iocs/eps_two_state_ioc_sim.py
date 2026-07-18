#!/usr/bin/env python3
from caproto.server import pvproperty, PVGroup
from caproto.server import template_arg_parser, run
from caproto import ChannelType
import contextvars
import functools

internal_process = contextvars.ContextVar("internal_process", default=False)


def no_reentry(func):
    @functools.wraps(func)
    async def inner(*args, **kwargs):
        if internal_process.get():
            return
        try:
            internal_process.set(True)
            return await func(*args, **kwargs)
        finally:
            internal_process.set(False)

    return inner


class EPSTwoStateIOC(PVGroup):
    """
    Simulates an EPS Two State device including multiple-attempt issue
    for two-state device.

    This IOC is used to simulate an EPS Two State Device, for testing
    or development  purposes. Known EPS two state devices include Photon
    shutters, gate valves and Pneumatic actuators at NSLS-II. It simulates
    known issues with some of these including: A hardware error (when some
    attempts at sending the command fail and we need to 'kick' the device
    a few times to get it to actuate), A position status error (it sometimes
    does not reach the final state but remains 'between states') and
    an enable state change error (there is an 'enable' PV controlled by
    the control room that determines if the device can be operated or not).
    A parameter (described below) allows each of these error paths to be
    tested against.

    Parameters
    ----------
    retries : int, optional
        Number of attempts required for changing state,
        default is 2
    enbl_sts_val: str, optional
        Enumerated string that enables the state change,
        default is True
    hw_error_val: str, optional
        Enumerated string that activates the hardware error,
        default is False
    sts_error_val: str, optional
        Enumerated string that activates the Pos-Sts error,
        default is False
    """

    def __init__(self, retries=2, enbl_sts_val="True", hw_error_val="False", sts_error_val="False", **kwargs):

        super().__init__(**kwargs)

        self._max_retries = retries
        self._num_retries = 0

        self._enbl_sts_val = enbl_sts_val

        self._hw_error_val = hw_error_val
        self._sts_error_val = sts_error_val

    # Pos-Sts two-state PV

    _pos_states = ["Open", "Not Open"]  # two position states

    pos_sts = pvproperty(
        value="Open", enum_strings=_pos_states, dtype=ChannelType.ENUM, read_only=True, name="Pos-Sts"
    )

    # Opn-Cmd and Cls-Cmd PVs used by client for changing state

    _cmd_states = ["None", "Done"]  # two command states

    state1_cmd = pvproperty(
        value=_cmd_states[0], enum_strings=["None", "Open"], dtype=ChannelType.ENUM, name="Cmd:Opn-Cmd"
    )
    state2_cmd = pvproperty(
        value=_cmd_states[0], enum_strings=["None", "Close"], dtype=ChannelType.ENUM, name="Cmd:Cls-Cmd"
    )

    _fail_states = ["False", "True"]

    fail_to_state1 = pvproperty(
        value=_fail_states[0],
        enum_strings=_fail_states,
        dtype=ChannelType.ENUM,
        read_only=True,
        name="Sts:FailOpn-Sts",
    )
    fail_to_state2 = pvproperty(
        value=_fail_states[0],
        enum_strings=_fail_states,
        dtype=ChannelType.ENUM,
        read_only=True,
        name="Sts:FailCls-Sts",
    )

    # Enbl-Sts PV that enables/disables the state change

    _enbl_states = ["False", "True"]

    enbl_sts = pvproperty(value="", enum_strings=_enbl_states, dtype=ChannelType.ENUM, name="Enbl-Sts")

    # Hardware error status

    _hw_error_states = ["False", "True"]

    hw_error_sts = pvproperty(value="", enum_strings=_hw_error_states, dtype=ChannelType.ENUM, name="HwError-Sts")

    # Pos-Sts error status

    _sts_error_states = ["False", "True"]

    sts_error_sts = pvproperty(
        value="", enum_strings=_sts_error_states, dtype=ChannelType.ENUM, name="StsError-Sts"
    )

    # PV Startup/Putter Methods

    @enbl_sts.startup
    async def enbl_sts(self, instance, async_lib):
        await instance.write(value=self._enbl_sts_val)

    @hw_error_sts.startup
    async def hw_error_sts(self, instance, async_lib):
        await instance.write(value=self._hw_error_val)

    @sts_error_sts.startup
    async def sts_error_sts(self, instance, async_lib):
        await instance.write(value=self._sts_error_val)

    @state1_cmd.startup
    async def state1_cmd(self, instance, async_lib):
        instance.async_lib = async_lib

    @state1_cmd.putter
    @no_reentry
    async def state1_cmd(self, instance, value):
        if value == "Open":
            await self.state1_cmd.write(value)
            await instance.async_lib.library.sleep(1)
            await self.pos_sts.write("Open")
        return "None"

    @state2_cmd.startup
    async def state2_cmd(self, instance, async_lib):
        instance.async_lib = async_lib

    @state2_cmd.putter
    @no_reentry
    async def state2_cmd(self, instance, value):
        if value == "Close":
            await self.state2_cmd.write(value)
            await instance.async_lib.library.sleep(1)
            await self.pos_sts.write("Not Open")
        return "None"

    @enbl_sts.putter
    async def enbl_sts(self, instance, value):
        self._enbl_sts_val = value
        return value

    @hw_error_sts.putter
    async def hw_error_sts(self, instance, value):
        self._hw_error_val = value
        return value

    @sts_error_sts.putter
    async def sts_error_sts(self, instance, value):
        self._sts_error_val = value
        return value

    # Internal Methods

    async def _state_cmd_put(self, instance, value, state_val, fail_to_state):
        if value == self._cmd_states[0]:  # if None -> do nothing
            return self._cmd_states[0]
        if self._pos_sts_val == state_val:  # if in state -> do nothing
            return self._cmd_states[0]
        if self._enbl_sts_val == "False":  # if changes not enabled -> fail
            await fail_to_state.write(value="True")
            return self._cmd_states[0]
        self._num_retries += 1
        if self._num_retries < self._max_retries:
            return self._cmd_states[0]
        else:
            self._num_retries = 0
        if self._hw_error_val == "True":  # if hw error -> fail
            await fail_to_state.write(value="True")
            return self._cmd_states[1]
        else:
            await fail_to_state.write(value="False")
        if self._sts_error_val == "True":  # if sts error -> don't change sts
            return self._cmd_states[1]
        await self.pos_sts.write(value=state_val)
        self._pos_sts_val = state_val
        return self._cmd_states[0]


if __name__ == "__main__":
    parser, split_args = template_arg_parser(default_prefix="eps2state:", desc="EPS Two State IOC.")

    retries_help = "Number of attempts required for changing state"
    enable_help = "State change is enabled"
    hwerror_help = "HW error is activated"
    stserror_help = "Pos-Sts error is activated"

    parser.add_argument("--retries", help=retries_help, required=False, default=2, type=int)
    parser.add_argument("--enable", help=enable_help, required=False, default="True", type=str)
    parser.add_argument("--hwerror", help=hwerror_help, required=False, default="False", type=str)
    parser.add_argument("--stserror", help=stserror_help, required=False, default="False", type=str)

    args = parser.parse_args()
    ioc_options, run_options = split_args(args)

    ioc = EPSTwoStateIOC(
        retries=args.retries,
        enbl_sts_val=args.enable,
        hw_error_val=args.hwerror,
        sts_error_val=args.stserror,
        **ioc_options,
    )

    run(ioc.pvdb, **run_options)
