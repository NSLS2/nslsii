from pathlib import Path
from unittest.mock import call

import pytest
import pytest_asyncio
from ophyd_async.core import (
    DetectorTrigger,
    StaticFilenameProvider,
    StaticPathProvider,
    TriggerInfo,
    callback_on_mock_put,
    get_mock_put,
    init_devices,
    set_mock_value,
)
from ophyd_async.epics import adcore
from ophyd_async.testing import assert_has_calls

from nslsii.ophyd_async.devices import (
    XSPRESS3_MIN_DEADTIME,
    Xspress3Detector,
    Xspress3TriggerMode,
    xspress3_hdf_writer,
)


@pytest.fixture
def static_path_provider(tmp_path: Path) -> StaticPathProvider:
    return StaticPathProvider(StaticFilenameProvider("xsp3"), tmp_path)


@pytest_asyncio.fixture
async def xspress3(static_path_provider: StaticPathProvider) -> Xspress3Detector:
    async with init_devices(mock=True):
        xspress3 = Xspress3Detector(
            "XSP3:",
            adcore.ADWriterFactory.hdf(static_path_provider),
            num_channels=4,
        )
    writer = xspress3.get_plugin("hdf", adcore.NDFileHDF5IO)
    set_mock_value(writer.file_path_exists, True)
    set_mock_value(xspress3.driver.max_frames, 16384)
    return xspress3


def test_pvs_correct(xspress3: Xspress3Detector):
    assert xspress3.driver.acquire.source == "mock+ca://XSP3:det1:Acquire_RBV"
    assert xspress3.driver.trigger_mode.source == "mock+ca://XSP3:det1:TriggerMode_RBV"
    assert xspress3.hdf.file_path.source == "mock+ca://XSP3:HDF1:FilePath_RBV"
    assert list(xspress3.channels) == [1, 2, 3, 4]
    channel = xspress3.channels[3]
    assert channel.spectrum.source == "mock+ca://XSP3:MCA3:ArrayData"
    assert channel.spectrum_sum.source == "mock+ca://XSP3:MCASUM3:ArrayData"
    assert channel.rois.channels[2].total.source == (
        "mock+ca://XSP3:MCA3ROI:2:Total_RBV"
    )
    assert channel.dead_time_factor.value.source == ("mock+ca://XSP3:C3SCA:9:Value_RBV")
    assert channel.dead_time_percent.value.source == (
        "mock+ca://XSP3:C3SCA:10:Value_RBV"
    )


def test_num_rois():
    xspress3 = Xspress3Detector("XSP3:", num_channels=1, num_rois=48)
    assert len(xspress3.channels[1].rois.channels) == 48


@pytest.mark.asyncio
async def test_supported_triggers_and_deadtime(xspress3: Xspress3Detector):
    triggers, deadtime = await xspress3.get_trigger_deadtime()
    assert triggers == {
        DetectorTrigger.INTERNAL,
        DetectorTrigger.EXTERNAL_EDGE,
        DetectorTrigger.EXTERNAL_LEVEL,
    }
    assert deadtime == XSPRESS3_MIN_DEADTIME


@pytest.mark.asyncio
async def test_deadtime_override():
    xspress3 = Xspress3Detector("XSP3:", num_channels=1, deadtime=0.001)
    _, deadtime = await xspress3.get_trigger_deadtime()
    assert deadtime == 0.001


@pytest.mark.asyncio
async def test_prepare_internal(xspress3: Xspress3Detector):
    await xspress3.prepare(TriggerInfo(number_of_events=3, livetime=0.5))
    assert_has_calls(
        xspress3.driver,
        [
            call.trigger_mode.put(Xspress3TriggerMode.INTERNAL),
            call.num_images.put(3),
            call.erase_on_start.put(True),
            call.acquire_time.put(0.5),
        ],
    )


@pytest.mark.asyncio
async def test_prepare_never_touches_disabled_records(xspress3: Xspress3Detector):
    # ImageMode and AcquirePeriod are disabled in xspress3.template
    await xspress3.prepare(TriggerInfo(number_of_events=3, livetime=0.5, deadtime=1))
    get_mock_put(xspress3.driver.image_mode).assert_not_called()
    get_mock_put(xspress3.driver.acquire_period).assert_not_called()


@pytest.mark.asyncio
async def test_prepare_external_edge(xspress3: Xspress3Detector):
    await xspress3.prepare(
        TriggerInfo(
            trigger=DetectorTrigger.EXTERNAL_EDGE, number_of_events=5, livetime=0.1
        )
    )
    assert_has_calls(
        xspress3.driver,
        [
            call.trigger_mode.put(Xspress3TriggerMode.TTL_INTERNAL),
            call.num_images.put(5),
            call.erase_on_start.put(True),
            call.acquire_time.put(0.1),
            call.acquire.put(True),
        ],
    )


@pytest.mark.asyncio
async def test_prepare_external_level(xspress3: Xspress3Detector):
    await xspress3.prepare(
        TriggerInfo(trigger=DetectorTrigger.EXTERNAL_LEVEL, number_of_events=2)
    )
    assert_has_calls(
        xspress3.driver,
        [
            call.trigger_mode.put(Xspress3TriggerMode.TTL_VETO_ONLY),
            call.num_images.put(2),
            call.erase_on_start.put(True),
            call.acquire.put(True),
        ],
    )


@pytest.mark.asyncio
async def test_prepare_forever_uses_max_frames(xspress3: Xspress3Detector):
    await xspress3.prepare(TriggerInfo(number_of_events=0))
    assert_has_calls(
        xspress3.driver,
        [
            call.trigger_mode.put(Xspress3TriggerMode.INTERNAL),
            call.num_images.put(16384),
            call.erase_on_start.put(True),
        ],
    )


@pytest.mark.asyncio
async def test_config_includes_deadtime_correction(xspress3: Xspress3Detector):
    config = await xspress3.read_configuration()
    assert set(config) == {
        "xspress3-driver-acquire_period",
        "xspress3-driver-acquire_time",
        "xspress3-driver-deadtime_correction",
    }


@pytest.mark.asyncio
async def test_describe_after_prepare(xspress3: Xspress3Detector):
    set_mock_value(xspress3.driver.array_size_x, 4096)
    set_mock_value(xspress3.driver.array_size_y, 4)
    set_mock_value(xspress3.driver.data_type, adcore.ADBaseDataType.UINT32)
    await xspress3.prepare(TriggerInfo(number_of_events=1))
    description = await xspress3.describe()
    assert description["xspress3"]["shape"] == [1, 4, 4096]
    assert description["xspress3"]["dtype_numpy"] == "<u4"


@pytest.mark.asyncio
async def test_trigger(xspress3: Xspress3Detector):
    set_mock_value(xspress3.driver.array_size_x, 4096)
    set_mock_value(xspress3.driver.array_size_y, 4)
    set_mock_value(xspress3.driver.data_type, adcore.ADBaseDataType.UINT32)
    await xspress3.stage()
    callback_on_mock_put(
        xspress3.driver.acquire,
        lambda v: set_mock_value(xspress3.hdf.num_captured, 1),
    )
    await xspress3.trigger()
    assert await xspress3.hdf.num_captured.get_value() == 1


@pytest.mark.asyncio
async def test_sliced_writer_describes_one_datakey_per_channel(
    static_path_provider: StaticPathProvider,
):
    async with init_devices(mock=True):
        xspress3 = Xspress3Detector(
            "XSP3:",
            xspress3_hdf_writer(static_path_provider, num_channels=4),
            num_channels=4,
        )
    writer = xspress3.get_plugin("hdf", adcore.NDFileHDF5IO)
    set_mock_value(writer.file_path_exists, True)
    set_mock_value(xspress3.driver.max_frames, 16384)
    set_mock_value(xspress3.driver.array_size_x, 4096)
    set_mock_value(xspress3.driver.array_size_y, 4)
    set_mock_value(xspress3.driver.data_type, adcore.ADBaseDataType.UINT32)

    await xspress3.prepare(TriggerInfo(number_of_events=1))
    description = await xspress3.describe()

    assert set(description) >= {
        "xspress3",
        "xspress3-channel1",
        "xspress3-channel2",
        "xspress3-channel3",
        "xspress3-channel4",
    }
    assert description["xspress3"]["shape"] == [1, 4, 4096]
    assert description["xspress3-channel2"]["shape"] == [1, 4096]
    assert description["xspress3-channel2"]["dtype_numpy"] == "<u4"
