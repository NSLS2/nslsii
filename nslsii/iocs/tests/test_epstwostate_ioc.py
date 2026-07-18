import os
import subprocess
import sys
import time

from ophyd import Device, EpicsSignal, EpicsSignalRO
from ophyd.device import Component as Cpt, FormattedComponent as FmtCpt


class EPSTwoStateDevice(Device):
    def __init__(self, *, prefix, name, **kwargs):
        super().__init__(prefix=prefix, name=name, **kwargs)

    status = Cpt(EpicsSignalRO, "Pos-Sts", string=True)

    opn_cmd = FmtCpt(EpicsSignal, "{self.prefix}Cmd:Opn-Cmd", string=True, kind="omitted")
    cls_cmd = FmtCpt(EpicsSignal, "{self.prefix}Cmd:Cls-Cmd", string=True, kind="omitted")

    fail_to_opn = Cpt(EpicsSignalRO, "Sts:FailOpn-Sts", string=True)

    fail_to_cls = Cpt(EpicsSignalRO, "Sts:FailCls-Sts", string=True)

    enbl_sts = Cpt(EpicsSignal, "Enbl-Sts", string=True, kind="omitted")

    hw_error_sts = Cpt(EpicsSignal, "HwError-Sts", string=True, kind="omitted")

    sts_error_sts = Cpt(EpicsSignal, "StsError-Sts", string=True, kind="omitted")


def test_epstwostate_ioc():

    stdout = subprocess.PIPE
    stdin = None

    ioc_process = subprocess.Popen(
        [sys.executable, "-m", "caproto.tests.example_runner", "nslsii.iocs.eps_two_state_ioc_sim"],
        stdout=stdout,
        stdin=stdin,
        env=os.environ,
    )

    print("nslsii.iocs.epc_two_state_ioc_sim is now running")

    time.sleep(5)

    # Wrap the rest in a try-except to ensure the ioc is killed before exiting
    try:
        eps = EPSTwoStateDevice(prefix="eps2state:", name="eps")

        # 1. check the ioc-device connection and initial values
        sts_val = eps.status.get()
        assert sts_val == "Open"

        opn_cmd_val = eps.opn_cmd.get()
        assert opn_cmd_val == "None"

        cls_cmd_val = eps.cls_cmd.get()
        assert cls_cmd_val == "None"

        fail_to_opn_val = eps.fail_to_opn.get()
        assert fail_to_opn_val == "False"

        fail_to_cls_val = eps.fail_to_cls.get()
        assert fail_to_cls_val == "False"

        enbl_sts_val = eps.enbl_sts.get()
        assert enbl_sts_val == "True"

        hw_error_val = eps.hw_error_sts.get()
        assert hw_error_val == "False"

        sts_error_val = eps.sts_error_sts.get()
        assert sts_error_val == "False"

        # 2. try to close with one attempt

        eps.cls_cmd.put(1)

        cls_cmd_val = eps.cls_cmd.get()
        assert cls_cmd_val == "None"

        fail_to_cls_val = eps.fail_to_cls.get()
        assert fail_to_cls_val == "False"

        sts_val = eps.status.get()
        assert sts_val == "Open"

        # 3. try to close with second attempt

        eps.cls_cmd.put(1)

        cls_cmd_val = eps.cls_cmd.get()
        assert cls_cmd_val == "None"

        fail_to_cls_val = eps.fail_to_cls.get()
        assert fail_to_cls_val == "False"

        sts_val = eps.status.get()
        assert sts_val == "Closed"

        # 4. try to open with one attempt

        eps.opn_cmd.put(1)

        opn_cmd_val = eps.opn_cmd.get()
        assert opn_cmd_val == "None"

        fail_to_opn_val = eps.fail_to_opn.get()
        assert fail_to_opn_val == "False"

        sts_val = eps.status.get()
        assert sts_val == "Closed"

        # 5. try to open with second attempt

        eps.opn_cmd.put(1)

        opn_cmd_val = eps.opn_cmd.get()
        assert opn_cmd_val == "None"

        fail_to_opn_val = eps.fail_to_opn.get()
        assert fail_to_opn_val == "False"

        sts_val = eps.status.get()
        assert sts_val == "Open"

        # 6. disable changes and try to close

        eps.enbl_sts.put(0)

        eps.cls_cmd.put(1)
        eps.cls_cmd.put(1)

        cls_cmd_val = eps.cls_cmd.get()
        assert cls_cmd_val == "None"

        fail_to_cls_val = eps.fail_to_cls.get()
        assert fail_to_cls_val == "True"

        sts_val = eps.status.get()
        assert sts_val == "Open"

        # 7. enable changes, enable the hw error, and try to close

        eps.enbl_sts.put(1)
        eps.hw_error_sts.put(1)

        eps.cls_cmd.put(1)
        eps.cls_cmd.put(1)

        cls_cmd_val = eps.cls_cmd.get()
        assert cls_cmd_val == "Done"

        fail_to_cls_val = eps.fail_to_cls.get()
        assert fail_to_cls_val == "True"

        sts_val = eps.status.get()
        assert sts_val == "Open"

        # 8. disable the hw error, enable the sts error, and try to close

        eps.hw_error_sts.put(0)
        eps.sts_error_sts.put(1)

        eps.cls_cmd.put(1)
        eps.cls_cmd.put(1)

        cls_cmd_val = eps.cls_cmd.get()
        assert cls_cmd_val == "Done"

        fail_to_cls_val = eps.fail_to_cls.get()
        assert fail_to_cls_val == "False"

        sts_val = eps.status.get()
        assert sts_val == "Open"

        # 9. disable the sts error and try to close

        eps.sts_error_sts.put(0)

        eps.cls_cmd.put(1)
        eps.cls_cmd.put(1)

        cls_cmd_val = eps.cls_cmd.get()
        assert cls_cmd_val == "None"

        fail_to_cls_val = eps.fail_to_cls.get()
        assert fail_to_cls_val == "False"

        sts_val = eps.status.get()
        assert sts_val == "Closed"

    finally:
        # Ensure that for any exception the ioc sub-process is terminated
        # before raising.
        ioc_process.terminate()
