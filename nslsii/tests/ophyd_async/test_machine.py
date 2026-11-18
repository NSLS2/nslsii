import pytest
from ophyd_async.core import init_devices

from nslsii.ophyd_async.devices import NSLS2MasterShutterPermit, NSLS2OpsMode, NSLS2StorageRing


@pytest.mark.asyncio
async def test_storage_ring_pvs_and_mock_defaults():
    async with init_devices(mock=True):
        ring = NSLS2StorageRing()

    sources = {
        ring.operating_mode: "SR-OPS{}Mode-Sts",
        ring.master_shutter_permit: "SR-EPS{PLC:1}Sts:MstrSh-Sts",
        ring.beam_current: "SR:OPS-BI{DCCT:1}I:Real-I",
        ring.beam_current_c03: "SR:C03-BI{DCCT:1}I:Real-I",
        ring.lifetime: "SR:OPS-BI{DCCT:1}Lifetime-I",
        ring.energy: "SR{}Energy_SRBend",
        ring.fill_target: "SR-HLA{}FillPattern:DesireImA",
    }
    for signal, pv in sources.items():
        assert signal.source == f"mock+ca://{pv}"

    assert await ring.beam_current.get_value() == 450
    assert await ring.operating_mode.get_value() == NSLS2OpsMode.OPERATIONS
    assert await ring.master_shutter_permit.get_value() == NSLS2MasterShutterPermit.ENABLE

    config = await ring.read_configuration()
    assert set(config) == {
        "ring-beam_current",
        "ring-lifetime",
        "ring-energy",
        "ring-fill_target",
    }
