import asyncio

import pytest
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
from ophyd_async.epics.adcore import ADState, ADWriterFactory

from nslsii.ophyd_async.devices import (
    EigerDataSource,
    EigerDetector,
    EigerDriverIO,
    EigerFileWriterDataLogic,
    EigerHDF5Format,
    EigerTriggerMode,
)

pytestmark = pytest.mark.asyncio

DIRECTORY_URI = "file://localhost/data/"


@pytest.fixture
def path_provider() -> StaticPathProvider:
    return StaticPathProvider(StaticFilenameProvider("abc"), "/data")


def puts(signal) -> list:
    return [call.args[0] for call in get_mock_put(signal).call_args_list]


class FakeIOC:
    """Emulates the ADEiger driver on a mock EigerDriverIO.

    Parameters
    ----------
    driver : EigerDriverIO
        Mock-mode driver to seed and attach put callbacks to.
    triggers_per_series : int, optional
        If given, the series disarms itself once this many software triggers have been
        counted (``NumTriggers`` exhausted).
    """

    def __init__(self, driver: EigerDriverIO, triggers_per_series: int | None = None) -> None:
        self.driver = driver
        self.triggers_per_series = triggers_per_series
        self.triggers = 0
        self._tasks: list[asyncio.Task] = []
        set_mock_value(driver.bit_depth_image, 16)
        set_mock_value(driver.signed_data, False)
        set_mock_value(driver.array_size_x, 1030)
        set_mock_value(driver.array_size_y, 514)
        set_mock_value(driver.dead_time, 3e-6)
        set_mock_value(driver.detector_state, ADState.IDLE)
        set_mock_value(driver.sequence_id, 41)
        set_mock_value(driver.array_counter, 7)
        set_mock_value(driver.file_path_exists, True)
        set_mock_value(driver.fw_nimgs_per_file, 1000)
        callback_on_mock_put(driver.acquire, self._on_acquire)
        callback_on_mock_put(driver.trigger_, self._on_trigger)

    async def _on_acquire(self, value: bool) -> None:
        if value:
            self.triggers = 0
            set_mock_value(self.driver.sequence_id, await self.driver.sequence_id.get_value() + 1)
            set_mock_value(self.driver.armed, True)
        else:
            set_mock_value(self.driver.armed, False)

    async def _on_trigger(self, _: float) -> None:
        self.triggers += 1
        last = self.triggers_per_series is not None and self.triggers >= self.triggers_per_series
        self._tasks.append(asyncio.create_task(self._count_frames(1, end_series=last)))

    async def _count_frames(self, num_triggers: int, end_series: bool) -> None:
        num_images, counter = await asyncio.gather(
            self.driver.num_images.get_value(), self.driver.array_counter.get_value()
        )
        set_mock_value(self.driver.array_counter, counter + num_triggers * num_images)
        if end_series:
            set_mock_value(self.driver.armed, False)
            set_mock_value(self.driver.acquire, False)

    async def external_pulses(self, num_triggers: int) -> None:
        await self._count_frames(num_triggers, end_series=True)


async def make_detector(path_provider: StaticPathProvider, **kwargs) -> EigerDetector:
    async with init_devices(mock=True):
        det = EigerDetector("X:", path_provider=path_provider, name="det", **kwargs)
    return det


async def collect(det: EigerDetector) -> list[tuple[str, dict]]:
    return [doc async for doc in det.collect_asset_docs()]


def assert_docs(docs: list[tuple[str, dict]], uri: str, indices: dict[str, int]) -> None:
    assert [name for name, _ in docs] == ["stream_resource", "stream_datum"]
    resource, datum = docs[0][1], docs[1][1]
    assert resource["uri"] == uri
    assert resource["mimetype"] == "application/x-hdf5"
    assert resource["data_key"] == "det"
    assert resource["parameters"]["dataset"] == "/entry/data/data"
    assert resource["parameters"]["chunk_shape"] == (1, 514, 1030)
    assert datum["stream_resource"] == resource["uid"]
    assert datum["indices"] == indices


async def test_prepare_internal_defers_arming(path_provider):
    det = await make_detector(path_provider)
    FakeIOC(det.driver)
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
async def test_prepare_external_arms(path_provider, trigger, trigger_mode):
    det = await make_detector(path_provider)
    FakeIOC(det.driver)
    await det.stage()
    await det.prepare(TriggerInfo(trigger=trigger, number_of_events=3))
    driver = det.driver
    assert await driver.trigger_mode.get_value() == trigger_mode
    assert await driver.manual_trigger.get_value() is False
    assert await driver.num_images.get_value() == 1
    assert await driver.num_triggers.get_value() == 3
    assert puts(driver.acquire).count(True) == 1
    assert await driver.armed.get_value() is True


async def test_deadtime_from_driver(path_provider):
    det = await make_detector(path_provider)
    FakeIOC(det.driver)
    triggers, deadtime = await det.get_trigger_deadtime()
    assert triggers == {DetectorTrigger.INTERNAL, DetectorTrigger.EXTERNAL_EDGE, DetectorTrigger.EXTERNAL_LEVEL}
    assert deadtime == 3e-6
    assert "det-driver-dead_time" in await det.read_configuration()


async def test_step_scan_single_series(path_provider):
    det = await make_detector(path_provider)
    FakeIOC(det.driver)
    driver = det.driver
    await det.stage()
    await det.prepare(TriggerInfo(collections_per_event=2))
    assert await driver.file_path.get_value() == "/data/"
    assert await driver.fw_name_pattern.get_value() == "abc_$id"
    assert await driver.data_source.get_value() == EigerDataSource.FILE_WRITER
    assert await driver.fw_enable.get_value() is True
    assert await driver.save_files.get_value() is True
    assert await driver.fw_hdf5_format.get_value() == EigerHDF5Format.LEGACY
    assert await driver.array_counter.get_value() == 0
    assert await driver.fw_nimgs_per_file.get_value() == 2
    datakey = (await det.describe())["det"]
    assert datakey["dtype_numpy"] == "<u2"
    assert datakey["shape"] == [2, 514, 1030]
    assert datakey["external"] == "STREAM:"

    for point in (1, 2, 3):
        await det.trigger()
        assert_docs(await collect(det), f"{DIRECTORY_URI}abc_42_data_{point:06d}.h5", {"start": 0, "stop": 1})
        assert await driver.sequence_id.get_value() == 42
    assert puts(driver.acquire).count(True) == 1
    assert len(puts(driver.trigger_)) == 3

    await det.unstage()
    assert puts(driver.acquire)[-1] is False
    assert await driver.armed.get_value() is False
    assert await driver.manual_trigger.get_value() is False


async def test_reprepare_while_armed_starts_new_series(path_provider):
    det = await make_detector(path_provider)
    FakeIOC(det.driver)
    driver = det.driver
    await det.stage()
    await det.prepare(TriggerInfo(collections_per_event=2))
    await det.trigger()
    assert_docs(await collect(det), f"{DIRECTORY_URI}abc_42_data_000001.h5", {"start": 0, "stop": 1})

    await det.prepare(TriggerInfo(collections_per_event=2, livetime=0.2))
    assert puts(driver.acquire) == [False, True, False]
    assert await driver.armed.get_value() is False

    await det.trigger()
    assert await driver.sequence_id.get_value() == 43
    assert_docs(await collect(det), f"{DIRECTORY_URI}abc_43_data_000001.h5", {"start": 0, "stop": 1})


async def test_exhausted_series_rearms(path_provider):
    det = await make_detector(path_provider)
    FakeIOC(det.driver, triggers_per_series=2)
    driver = det.driver
    await det.stage()
    await det.prepare(TriggerInfo(collections_per_event=2))

    await det.trigger()
    assert_docs(await collect(det), f"{DIRECTORY_URI}abc_42_data_000001.h5", {"start": 0, "stop": 1})
    await det.trigger()
    assert_docs(await collect(det), f"{DIRECTORY_URI}abc_42_data_000002.h5", {"start": 0, "stop": 1})
    assert await driver.armed.get_value() is False

    await det.trigger()
    assert await driver.sequence_id.get_value() == 43
    assert_docs(await collect(det), f"{DIRECTORY_URI}abc_43_data_000001.h5", {"start": 0, "stop": 1})
    assert puts(driver.acquire).count(True) == 2


async def test_external_fly_scan(path_provider):
    det = await make_detector(path_provider)
    ioc = FakeIOC(det.driver)
    driver = det.driver
    await det.stage()
    await det.prepare(TriggerInfo(trigger=DetectorTrigger.EXTERNAL_EDGE, number_of_events=4))
    assert await driver.fw_nimgs_per_file.get_value() == 4

    await det.kickoff()
    await ioc.external_pulses(4)
    await det.complete()
    assert_docs(await collect(det), f"{DIRECTORY_URI}abc_42_data_000001.h5", {"start": 0, "stop": 4})


async def test_stream_mode_aligns_file_size(path_provider):
    det = await make_detector(path_provider, data_source=EigerDataSource.STREAM)
    FakeIOC(det.driver)
    driver = det.driver
    await det.stage()
    await det.prepare(TriggerInfo(collections_per_event=3))
    assert await driver.stream_enable.get_value() is True
    assert await driver.data_source.get_value() == EigerDataSource.STREAM
    assert await driver.fw_nimgs_per_file.get_value() == 999


async def test_missing_directory(path_provider):
    det = await make_detector(path_provider)
    FakeIOC(det.driver)
    set_mock_value(det.driver.file_path_exists, False)
    with pytest.raises(FileNotFoundError):
        await det.prepare(TriggerInfo())


async def test_images_per_file_clamped_by_detector(path_provider):
    det = await make_detector(path_provider)
    FakeIOC(det.driver)
    callback_on_mock_put(det.driver.fw_nimgs_per_file, lambda v: min(v, 1))
    with pytest.raises(ValueError, match="exceeds the detector limit"):
        await det.prepare(TriggerInfo(collections_per_event=2))


async def test_data_source_none_rejected(path_provider):
    det = await make_detector(path_provider)
    with pytest.raises(ValueError, match="FILE_WRITER or STREAM"):
        EigerFileWriterDataLogic(det.driver, path_provider, data_source=EigerDataSource.NONE)


async def test_plugin_writer_uses_eiger_dtype(path_provider):
    async with init_devices(mock=True):
        det = EigerDetector("X:", ADWriterFactory.hdf(path_provider), name="det")
    FakeIOC(det.driver)
    set_mock_value(det.driver.bit_depth_image, 32)
    set_mock_value(det.driver.signed_data, True)
    set_mock_value(det.hdf.file_path_exists, True)
    await det.stage()
    await det.prepare(TriggerInfo())
    assert (await det.describe())["det"]["dtype_numpy"] == "<i4"


async def test_plugin_writer_datakey_collision(path_provider):
    with pytest.raises(ValueError, match="distinct datakey_suffix"):
        EigerDetector("X:", ADWriterFactory.hdf(path_provider), path_provider=path_provider)
