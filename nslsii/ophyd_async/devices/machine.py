"""NSLS-II accelerator and storage ring devices."""

from ophyd_async.core import (
    DeviceMock,
    StandardReadable,
    StrictEnum,
    default_mock_class,
    set_mock_value,
)
from ophyd_async.core import StandardReadableFormat as Format
from ophyd_async.epics.core import EpicsDevice, epics_signal_r


class NSLS2OpsMode(StrictEnum):
    """Operating modes for the NSLS-II storage ring."""

    OPERATIONS = "Operations"
    SETUP = "Setup"
    ACCEL_STUDIES = "Accel Studies"
    BEAMLINE_STUDIES = "Beamline Studies"
    FAILURE = "Failure"
    MAINTENANCE = "Maintenance"
    SHUTDOWN = "Shutdown"
    UNSCHEDULED_OPS = "Unscheduled Ops"
    DECAY_MODE_OPS = "Decay Mode Ops"


class NSLS2MasterShutterPermit(StrictEnum):
    """States of the storage ring master shutter permit."""

    DISABLE = "DISABLE"
    ENABLE = "ENABLE"


class MockStorageRing(DeviceMock["NSLS2StorageRing"]):
    """Mock storage ring device.

    Sets the beam current to 450 mA, to not trip a suspender by default in mock mode.
    """

    async def connect(self, device: "NSLS2StorageRing"):
        set_mock_value(device.beam_current, 450)
        set_mock_value(device.beam_current_c03, 450)
        set_mock_value(device.operating_mode, NSLS2OpsMode.OPERATIONS)
        set_mock_value(device.master_shutter_permit, NSLS2MasterShutterPermit.ENABLE)


@default_mock_class(MockStorageRing)
class NSLS2StorageRing(StandardReadable, EpicsDevice):
    """NSLS-II storage ring device.

    Parameters
    ----------
    name : str, optional
        The name of the device.
    read_c03_beam_current : bool, optional
        Whether to read the beam current from the DCCT in cell 3.
        Defaults to False.

    Attributes
    ----------
    operating_mode : SignalR[NSLS2OpsMode]
        The current operating mode of the storage ring.
    beam_current : SignalR[float]
        The beam current of the storage ring, in mA.
    beam_current_c03 : SignalR[float]
        The beam current from the DCCT in cell 3, in mA, which some beamlines use instead
        of ``beam_current``. Only included in read/describe if read_c03_beam_current is True.
    lifetime : SignalR[float]
        The beam lifetime, in hours.
    energy : SignalR[float]
        The ring energy, from the bending magnet setting.
    fill_target : SignalR[float]
        The desired beam current of the fill pattern, in mA.
    master_shutter_permit : SignalR[NSLS2MasterShutterPermit]
        Status of the storage ring master shutter permit,
        ``NSLS2MasterShutterPermit.ENABLE`` when beamline shutters may be opened.
    """

    def __init__(self, name: str = "", read_c03_beam_current: bool = False):
        self.master_shutter_permit = epics_signal_r(
            NSLS2MasterShutterPermit, "SR-EPS{PLC:1}Sts:MstrSh-Sts"
        )

        with self.add_children_as_readables(Format.CONFIG_SIGNAL):
            self.operating_mode = epics_signal_r(NSLS2OpsMode, "SR-OPS{}Mode-Sts")

        with self.add_children_as_readables():
            self.beam_current = epics_signal_r(float, "SR:OPS-BI{DCCT:1}I:Real-I")
            self.lifetime = epics_signal_r(float, "SR:OPS-BI{DCCT:1}Lifetime-I")
            self.energy = epics_signal_r(float, "SR{}Energy_SRBend")
            self.fill_target = epics_signal_r(float, "SR-HLA{}FillPattern:DesireImA")
        
        self.beam_current_c03 = epics_signal_r(float, "SR:C03-BI{DCCT:1}I:Real-I")
        if read_c03_beam_current:
            self.add_readables([self.beam_current_c03])

        super().__init__(name=name)


__all__ = [
    "NSLS2MasterShutterPermit",
    "NSLS2OpsMode",
    "MockStorageRing",
    "NSLS2StorageRing",
]
