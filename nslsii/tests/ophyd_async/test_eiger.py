import h5py
import pytest
from ophyd_async.core import (
    DetectorTrigger,
    StaticPathProvider,
    TriggerInfo,
    callback_on_mock_put,
    get_mock_put,
    init_devices,
    set_mock_value,
)
from ophyd_async.epics.adcore import ADWriterFactory

from nslsii.ophyd_async.devices import (
    EigerDataSource,
    EigerDetector,
    EigerHDF5Format,
    EigerTriggerLogic,
    EigerTriggerMode,
)

pytestmark = pytest.mark.asyncio


def puts(signal) -> list:
    return [call.args[0] for call in get_mock_put(signal).call_args_list]


async def make_detector(path_provider: StaticPathProvider, **kwargs) -> EigerDetector:
    async with init_devices(mock=True):
        det = EigerDetector("X:", path_provider=path_provider, name="det", **kwargs)
    return det


async def collect(det: EigerDetector) -> list[tuple[str, dict]]:
    return [doc async for doc in det.collect_asset_docs()]


def assert_docs(
    docs: list[tuple[str, dict]], path_provider: StaticPathProvider, filename: str, indices: dict[str, int]
) -> None:
    assert [name for name, _ in docs] == ["stream_resource", "stream_datum"]
    resource, datum = docs[0][1], docs[1][1]
    assert resource["uri"] == f"{path_provider('det').directory_uri}{filename}"
    assert resource["mimetype"] == "application/x-hdf5"
    assert resource["data_key"] == "det"
    assert resource["parameters"]["dataset"] == "/entry/data/data"
    assert resource["parameters"]["chunk_shape"] == (1, 514, 1030)
    assert datum["stream_resource"] == resource["uid"]
    assert datum["indices"] == indices


async def test_prepare_internal_defers_arming(path_provider, fake_ioc):
    det = await make_detector(path_provider)
    fake_ioc(det.driver)
    await det.stage()
    await det.prepare(TriggerInfo(number_of_events=3, livetime=0.1))
    driver = det.driver
    assert await driver.trigger_mode.get_value() == EigerTriggerMode.INTERNAL_SERIES
    assert await driver.manual_trigger.get_value() is True
    assert await driver.num_triggers.get_value() == 10_000
    assert await driver.num_images.get_value() == 3
    assert await driver.acquire_time.get_value() == 0.1
    assert await driver.acquire_period.get_value() == pytest.approx(0.1 + 3e-6)
    assert True not in puts(driver.acquire)
    assert await driver.armed.get_value() is False


@pytest.mark.parametrize(
    "trigger, trigger_mode",
    [
        (DetectorTrigger.EXTERNAL_EDGE, EigerTriggerMode.EXTERNAL_SERIES),
        (DetectorTrigger.EXTERNAL_LEVEL, EigerTriggerMode.EXTERNAL_ENABLE),
    ],
)
async def test_prepare_external_arms(path_provider, fake_ioc, trigger, trigger_mode):
    det = await make_detector(path_provider)
    fake_ioc(det.driver)
    await det.stage()
    await det.prepare(TriggerInfo(trigger=trigger, number_of_events=3))
    driver = det.driver
    assert await driver.trigger_mode.get_value() == trigger_mode
    assert await driver.manual_trigger.get_value() is False
    assert await driver.num_images.get_value() == 1
    assert await driver.num_triggers.get_value() == 3
    assert puts(driver.acquire).count(True) == 1
    assert await driver.armed.get_value() is True


async def test_deadtime_from_driver(path_provider, fake_ioc):
    det = await make_detector(path_provider)
    fake_ioc(det.driver)
    triggers, deadtime = await det.get_trigger_deadtime()
    assert triggers == {DetectorTrigger.INTERNAL, DetectorTrigger.EXTERNAL_EDGE, DetectorTrigger.EXTERNAL_LEVEL}
    assert deadtime == 3e-6
    assert "det-driver-dead_time" in await det.read_configuration()


async def test_filewriter_step_scan_single_series(path_provider, fake_ioc):
    det = await make_detector(path_provider)
    fake_ioc(det.driver)
    driver = det.driver
    await det.stage()
    await det.prepare(TriggerInfo(collections_per_event=2))
    assert await driver.file_path.get_value() == f"{path_provider('det').directory_path}/"
    assert await driver.fw_name_pattern.get_value() == "abc_$id"
    assert await driver.data_source.get_value() == EigerDataSource.FILE_WRITER
    assert await driver.fw_enable.get_value() is True
    assert await driver.save_files.get_value() is True
    assert puts(driver.fw_auto_remove) == []
    assert await driver.fw_hdf5_format.get_value() == EigerHDF5Format.LEGACY
    assert await driver.array_counter.get_value() == 0
    assert await driver.fw_nimgs_per_file.get_value() == 2
    datakey = (await det.describe())["det"]
    assert datakey["dtype_numpy"] == "<u2"
    assert datakey["shape"] == [2, 514, 1030]
    assert datakey["external"] == "STREAM:"

    for point in (1, 2, 3):
        await det.trigger()
        assert_docs(await collect(det), path_provider, f"abc_42_data_{point:06d}.h5", {"start": 0, "stop": 1})
        assert await driver.sequence_id.get_value() == 42
    assert puts(driver.acquire).count(True) == 1
    assert len(puts(driver.trigger_)) == 3

    await det.unstage()
    assert puts(driver.acquire)[-1] is False
    assert await driver.armed.get_value() is False
    assert await driver.manual_trigger.get_value() is False


async def test_reprepare_while_armed_starts_new_series(path_provider, fake_ioc):
    det = await make_detector(path_provider)
    fake_ioc(det.driver)
    driver = det.driver
    await det.stage()
    await det.prepare(TriggerInfo(collections_per_event=2))
    await det.trigger()
    assert_docs(await collect(det), path_provider, "abc_42_data_000001.h5", {"start": 0, "stop": 1})

    await det.prepare(TriggerInfo(collections_per_event=2, livetime=0.2))
    assert puts(driver.acquire) == [False, True, False]
    assert await driver.armed.get_value() is False

    await det.trigger()
    assert await driver.sequence_id.get_value() == 43
    assert_docs(await collect(det), path_provider, "abc_43_data_000001.h5", {"start": 0, "stop": 1})


async def test_clamped_num_triggers_exhausts_and_rearms(path_provider, fake_ioc):
    det = await make_detector(path_provider)
    fake_ioc(det.driver)
    driver = det.driver
    # The DCU clamps NumTriggers to its maximum; the readback shows the clamped value
    callback_on_mock_put(driver.num_triggers, lambda v: min(v, 2))
    await det.stage()
    await det.prepare(TriggerInfo(collections_per_event=2))
    assert await driver.num_triggers.get_value() == 2

    await det.trigger()
    assert_docs(await collect(det), path_provider, "abc_42_data_000001.h5", {"start": 0, "stop": 1})
    await det.trigger()
    assert_docs(await collect(det), path_provider, "abc_42_data_000002.h5", {"start": 0, "stop": 1})
    assert await driver.armed.get_value() is False

    await det.trigger()
    assert await driver.sequence_id.get_value() == 43
    assert_docs(await collect(det), path_provider, "abc_43_data_000001.h5", {"start": 0, "stop": 1})
    assert puts(driver.acquire).count(True) == 2


async def test_external_fly_scan(path_provider, fake_ioc):
    det = await make_detector(path_provider)
    ioc = fake_ioc(det.driver)
    driver = det.driver
    await det.stage()
    await det.prepare(TriggerInfo(trigger=DetectorTrigger.EXTERNAL_EDGE, number_of_events=4))
    assert await driver.fw_nimgs_per_file.get_value() == 4

    await det.kickoff()
    await ioc.external_pulses(4)
    await det.complete()
    assert_docs(await collect(det), path_provider, "abc_42_data_000001.h5", {"start": 0, "stop": 4})


async def test_missing_directory(path_provider, fake_ioc):
    det = await make_detector(path_provider)
    fake_ioc(det.driver)
    set_mock_value(det.driver.file_path_exists, False)
    with pytest.raises(FileNotFoundError):
        await det.prepare(TriggerInfo())


@pytest.mark.parametrize(
    "trigger_info",
    [
        TriggerInfo(collections_per_event=2),
        TriggerInfo(trigger=DetectorTrigger.EXTERNAL_EDGE, number_of_events=2),
    ],
)
async def test_images_per_file_clamped_by_detector(path_provider, fake_ioc, trigger_info):
    det = await make_detector(path_provider)
    fake_ioc(det.driver)
    # Every trigger's frames (or the whole external series) must fit in one DCU data file
    callback_on_mock_put(det.driver.fw_nimgs_per_file, lambda v: min(v, 1))
    with pytest.raises(ValueError, match="exceeds the detector limit"):
        await det.prepare(trigger_info)


async def test_trigger_logic_rejects_no_source(path_provider, fake_ioc):
    det = await make_detector(path_provider)
    with pytest.raises(ValueError, match="STREAM or FILE_WRITER"):
        EigerTriggerLogic(det.driver, source=EigerDataSource.NONE)


async def test_plugin_writer_step_scan(path_provider, fake_ioc, tmp_path):
    async with init_devices(mock=True):
        det = EigerDetector("X:", ADWriterFactory.hdf(path_provider), name="det")
    fake_ioc(det.driver, hdf=det.hdf, frame_shape=(4, 6))
    driver = det.driver
    await det.stage()
    await det.prepare(TriggerInfo(collections_per_event=2))
    # Frames come from the ZMQ stream; the FileWriter is off so the DCU disk does not fill
    assert await driver.data_source.get_value() == EigerDataSource.STREAM
    assert await driver.stream_enable.get_value() is True
    assert await driver.fw_enable.get_value() is False
    assert puts(driver.save_files) == []
    assert puts(driver.fw_nimgs_per_file) == []
    assert await det.hdf.capture.get_value() is True
    datakey = (await det.describe())["det"]
    assert datakey["dtype_numpy"] == "<u2"
    assert datakey["shape"] == [2, 4, 6]

    await det.trigger()
    docs = await collect(det)
    assert [name for name, _ in docs] == ["stream_resource", "stream_datum"]
    assert docs[0][1]["uri"] == f"{path_provider('det').directory_uri}abc.h5"
    assert docs[0][1]["parameters"]["dataset"] == "/entry/data/data"
    assert docs[1][1]["indices"] == {"start": 0, "stop": 1}
    await det.trigger()
    docs = await collect(det)
    assert [name for name, _ in docs] == ["stream_datum"]
    assert docs[0][1]["indices"] == {"start": 1, "stop": 2}
    assert puts(driver.acquire).count(True) == 1
    assert puts(det.hdf.flush_now) == [True, True]
    with h5py.File(tmp_path / "abc.h5") as f:
        assert f["/entry/data/data"].shape == (4, 4, 6)


async def test_plugin_writer_uses_eiger_dtype(path_provider, fake_ioc):
    async with init_devices(mock=True):
        det = EigerDetector("X:", ADWriterFactory.hdf(path_provider), name="det")
    fake_ioc(det.driver, hdf=det.hdf)
    set_mock_value(det.driver.bit_depth_image, 32)
    set_mock_value(det.driver.signed_data, True)
    await det.stage()
    await det.prepare(TriggerInfo())
    assert (await det.describe())["det"]["dtype_numpy"] == "<i4"


async def test_plugin_writer_datakey_collision(path_provider, fake_ioc):
    with pytest.raises(ValueError, match="distinct datakey_suffix"):
        EigerDetector("X:", ADWriterFactory.hdf(path_provider), path_provider=path_provider)
