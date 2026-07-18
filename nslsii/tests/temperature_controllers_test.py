from nslsii.temperature_controllers import Eurotherm, SetInProgress
from bluesky.plan_stubs import mv
from bluesky import RunEngine
from bluesky.utils import FailedStatus
import subprocess
import os
import sys
import pytest


@pytest.fixture
def RE():
    return RunEngine()


@pytest.mark.xfail()
def test_Eurotherm(RE):
    """Tests the Eurotherm ophyd device.

    Parameters
    ----------
    RE : object
        Bluesky RunEngine for use in testing.
    """

    stdout = subprocess.PIPE
    stdin = None

    # Start up an IOC based on the thermo_sim device in caproto.ioc_examples
    ioc_process = subprocess.Popen(
        [sys.executable, "-m", "caproto.tests.example_runner", "nslsii.iocs.thermo_sim"],
        stdout=stdout,
        stdin=stdin,
        env=os.environ,
    )

    print("caproto.ioc_examples.thermo_sim is now running")

    # Wrap the rest in a try-except to ensure the ioc is killed before exiting
    try:
        euro = Eurotherm("thermo:", name="euro")
        print("euro object is defined")

        # move the Eurotherm.
        RE(mv(euro, 100))

        # check that the readback value is within euro.tolerance of 100
        assert abs(euro.readback.get() - 100) <= euro.tolerance.get()
        assert len(euro.readback._callbacks["value"]) == 0  # ensure cb is gone

        # test that the set will fail after 'timeout'
        euro.timeout.set(1)
        with pytest.raises(FailedStatus):
            RE(mv(euro, 100))
        # ensure callback is removed
        assert len(euro.readback._callbacks["value"]) == 0
        euro.timeout.set(500)  # reset to default for the following tests.

        # test that the lock prevents setting while set in progress
        with pytest.raises(SetInProgress):
            for i in range(2):  # The previous set may or may not be complete
                euro.set(100)

    finally:
        # Ensure that for any exception the ioc sub-process is terminated
        # before raising.
        ioc_process.terminate()
