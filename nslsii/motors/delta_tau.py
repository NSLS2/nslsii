"""
Ophyd devices and signals for Delta Tau devices, such as Programmable Multi-Axis Controller (PMAC-PC)
"""

import logging

from ophyd import Component as Cpt
from ophyd import Device, EpicsSignal, EpicsSignalRO, Kind


class PMACStatus(EpicsSignalRO):
    """
    A signal to read the status of the PMAC-PC controller by checking the bit 11 of the status register.
    """

    def get(self):
        return int(super().get() >> 11 & 1 == 0)


class PMACKillSwitch(Device):
    """
    A device to kill the PMAC-PC controller.
    In the named version of the EPICS record (e.g., XF:23ID1-ES{Dif-Ax:Del}), the status is expected at Sts:4-Sts.
    Whereas in the axis numbered version (e.g., XF:23ID1-CT{MC:12-Ax:1}), the status is expected at Sts:1-Sts.
    """

    kill = Cpt(EpicsSignal, "Cmd:Kill-Cmd.PROC", kind=Kind.omitted)
    status = Cpt(PMACStatus, "Sts:4-Sts", kind=Kind.normal)

    def set(self, value, *args, **kwargs):
        """
        Set the kill switch to the given value
        """
        if value != 1:
            logging.getLogger(__name__).warning(
                "The value of the PMACKiller should only ever be set to 1. Changing the setpoint to 1 now."
            )
            value = 1
        self.kill.set(value, *args, **kwargs)

    def get(self, *args, **kwargs):
        """
        Get the value of the kill switch
        """
        return self.status.get(*args, **kwargs)
