import asyncio
from pathlib import PurePath
from unittest.mock import call

import bluesky.plans as bp
import h5py
import numpy as np
import pytest
from bluesky import RunEngine
from bluesky.run_engine import call_in_bluesky_event_loop
from bluesky_tiled_plugins import TiledWriter
from ophyd_async.core import (
    DetectorTrigger,
    StaticFilenameProvider,
    StaticPathProvider,
    TriggerInfo,
    callback_on_mock_execute,
    callback_on_mock_put,
    get_mock,
    get_mock_put,
    set_mock_put_proceeds,
    set_mock_value,
    soft_signal_rw,
    wait_for_value,
)
from ophyd_async.epics.adcore import ADBaseColorMode, ADBaseDataType, ADState
from tiled.client import from_uri
from tiled.server import SimpleTiledServer

from nslsii.ophyd_async.devices import (
    Xspress3Detector,
    Xspress3DriverIO,
    Xspress3HDFWriterFactory,
    Xspress3LevelTriggerMode,
    Xspress3TriggerLogic,
    Xspress3TriggerMode,
)


def make_detector(
    *,
    channel_numbers=(1,),
    mca_roi_numbers=(1, 2, 3, 4),
    writer_suffix="HDF1:",
    include_roi_streams=False,
    include_sca_streams=False,
    hinted_streams=None,
    **kwargs,
) -> Xspress3Detector:
    path_provider = StaticPathProvider(StaticFilenameProvider("xspress3"), PurePath("/tmp"))
    writer = Xspress3HDFWriterFactory(
        path_provider,
        writer_suffix=writer_suffix,
        include_roi_streams=include_roi_streams,
        include_sca_streams=include_sca_streams,
        hinted_streams=hinted_streams,
    )
    return Xspress3Detector(
        "XF:TEST{Xsp:1}:",
        writer,
        channel_numbers=channel_numbers,
        mca_roi_numbers=mca_roi_numbers,
        name="xs",
        **kwargs,
    )


async def connect_and_seed(
    detector: Xspress3Detector,
    *,
    channels: int | None = None,
    bins: int = 4096,
) -> None:
    await detector.connect(mock=True)
    if channels is None:
        channels = max(detector.channels, default=1)
    for signal, value in (
        (detector.driver.array_size_x, bins),
        (detector.driver.array_size_y, channels),
        (detector.driver.array_size_z, 0),
        (detector.driver.data_type, ADBaseDataType.UINT32),
        (detector.driver.color_mode, ADBaseColorMode.MONO),
        (detector.driver.detector_state, ADState.IDLE),
        (detector.hdf.file_path_exists, True),
        (detector.hdf.num_frames_chunks, 1),
        (detector.hdf.num_captured, 0),
        (detector.hdf.num_capture_calc_disable, 0),
    ):
        set_mock_value(signal, value)


@pytest.mark.asyncio
async def test_canonical_sources_nonconsecutive_vectors_and_suffix_overrides():
    detector = make_detector(
        channel_numbers=(4, 1),
        mca_roi_numbers=(48, 2),
        driver_suffix="driver:",
        writer_suffix="writer:",
        include_roi_reset=True,
    )
    await detector.connect(mock=True)

    assert tuple(detector.channels) == (1, 4)
    assert tuple(detector.channels[1].rois) == (2, 48)
    assert detector.driver.trigger_mode.source == ("mock+ca://XF:TEST{Xsp:1}:driver:TriggerMode_RBV")
    assert detector.driver.array_callbacks.source == ("mock+ca://XF:TEST{Xsp:1}:driver:ArrayCallbacks_RBV")
    assert detector.driver.erase.source == "mock+ca://XF:TEST{Xsp:1}:driver:ERASE"
    assert detector.driver.reset.source == "mock+ca://XF:TEST{Xsp:1}:driver:RESET"
    assert detector.driver.erase_on_start.source == ("mock+ca://XF:TEST{Xsp:1}:driver:EraseOnStart")
    assert detector.driver.soft_trigger.source == ("mock+ca://XF:TEST{Xsp:1}:driver:SoftTrigger_RBV")
    assert detector.hdf.num_capture_calc_disable.source == ("mock+ca://XF:TEST{Xsp:1}:writer:NumCapture_CALC.DISA")
    assert detector.channels[4].spectrum.source == ("mock+ca://XF:TEST{Xsp:1}:MCA4:ArrayData")
    assert detector.channels[4].spectrum_sum.source == ("mock+ca://XF:TEST{Xsp:1}:MCASUM4:ArrayData")
    assert detector.channels[4].rois[48].time_series_total.source == (
        "mock+ca://XF:TEST{Xsp:1}:MCA4ROI:48:TSTotal"
    )
    assert detector.channels[4].scalers.dt_percent.source == ("mock+ca://XF:TEST{Xsp:1}:C4SCA:10:Value_RBV")
    assert detector.channels[1].rois[2].reset.source == ("mock+ca://XF:TEST{Xsp:1}:C1_ROI2:Reset")
    assert detector.channels[1].rois[48].reset.source == ("mock+ca://XF:TEST{Xsp:1}:C1_ROI48:Reset")
    configuration = await detector.describe_configuration()
    expected_configuration = {
        detector.driver.trigger_mode.name,
        detector.driver.num_images.name,
        detector.driver.num_channels.name,
        detector.level_trigger_mode.name,
        detector.driver.ctrl_dtc.name,
        detector.driver.erase_on_start.name,
        detector.driver.acquire_time.name,
        detector.driver.acquire_period.name,
    }
    assert expected_configuration <= configuration.keys()


@pytest.mark.parametrize(
    ("kwargs", "match"),
    [
        ({"channel_numbers": (0,)}, "channel"),
        ({"channel_numbers": (25,)}, "channel"),
        ({"channel_numbers": (1.0,)}, "integer"),
        ({"channel_numbers": (1, 1)}, "unique"),
        ({"mca_roi_numbers": (0,)}, "ROI"),
        ({"mca_roi_numbers": (49,)}, "ROI"),
        ({"mca_roi_numbers": (1, 1)}, "unique"),
    ],
)
def test_channel_roi_validation(kwargs, match):
    with pytest.raises(ValueError, match=match):
        make_detector(**kwargs)


@pytest.mark.parametrize(
    ("profile", "channel_numbers", "roi_numbers"),
    [
        ("BMM", range(1, 5), range(1, 21)),
        ("CHX", (1,), range(1, 17)),
        ("CMS-OPLS", (1,), (1, 2, 3, 4)),
        ("HXN", range(1, 5), (1, 2, 3, 4)),
        ("IOS", (1,), (1, 2, 3, 4)),
        ("ISS", range(1, 5), (1, 2, 3, 4)),
        ("QAS", range(1, 7), (1, 2, 3, 4)),
        ("QAS-Xspress3X-SRX", range(1, 9), ()),
        ("LiX-TES", range(1, 5), ()),
        ("XFM", range(1, 5), (1, 2, 3, 4)),
    ],
)
def test_profile_parameterization(profile, channel_numbers, roi_numbers):
    detector = make_detector(
        channel_numbers=channel_numbers,
        mca_roi_numbers=roi_numbers,
    )

    assert detector.name == "xs", profile
    assert tuple(detector.channels) == tuple(channel_numbers)
    assert all(tuple(channel.rois) == tuple(roi_numbers) for channel in detector.channels.values())


def test_detector_is_topology_source_for_writer():
    path_provider = StaticPathProvider(StaticFilenameProvider("xspress3"), PurePath("/tmp"))
    writer = Xspress3HDFWriterFactory(
        path_provider,
        writer_suffix="writer:",
        include_roi_streams=True,
        include_sca_streams=False,
        hinted_streams=("channel1-roi2",),
    )
    detector = Xspress3Detector(
        "XF:TEST{Xsp:1}:",
        writer,
        channel_numbers=(4, 1),
        mca_roi_numbers=(48, 2),
        name="xs",
    )

    assert writer.path_provider is path_provider
    assert writer.writer_suffix == "writer:"
    assert writer.include_roi_streams is True
    assert writer.include_sca_streams is False
    assert writer.hinted_streams == ("channel1-roi2",)
    assert tuple(detector.channels) == (1, 4)
    assert all(tuple(channel.rois) == (2, 48) for channel in detector.channels.values())


@pytest.mark.asyncio
async def test_detector_without_writer_exposes_default_topology_without_streaming():
    detector = Xspress3Detector("XF:TEST{Xsp:1}:", name="xs")

    assert tuple(detector.channels) == (1,)
    assert tuple(detector.channels[1].rois) == (1, 2, 3, 4)
    assert detector.hints == {"fields": []}

    await detector.connect(mock=True)
    set_mock_value(detector.driver.detector_state, ADState.IDLE)
    await detector.stage()
    await detector.prepare(TriggerInfo())

    assert await detector.describe() == {}
    assert [document async for document in detector.collect_asset_docs()] == []

    await detector.unstage()
    await detector.warmup()


def test_default_hints_bulk_spectrum():
    detector = make_detector()

    assert detector.hints == {"fields": ["xs"]}


def test_configured_roi_hints_are_detector_prefixed():
    detector = make_detector(
        channel_numbers=(1, 2),
        mca_roi_numbers=(1, 2),
        include_roi_streams=True,
        hinted_streams=("channel1-roi1", "channel2-roi2"),
    )

    assert detector.hints == {"fields": ["xs-channel1-roi1", "xs-channel2-roi2"]}


@pytest.mark.parametrize(
    ("kwargs", "match"),
    [
        ({"hinted_streams": ("channel2",)}, "not emitted"),
        ({"hinted_streams": ("channel1-roi1",)}, "not emitted"),
        ({"hinted_streams": ("channel1-all_good",)}, "not emitted"),
        ({"hinted_streams": ("unknown",)}, "not emitted"),
        (
            {
                "mca_roi_numbers": (1,),
                "include_roi_streams": True,
                "hinted_streams": ("channel1-roi2",),
            },
            "not emitted",
        ),
        ({"hinted_streams": ("channel1", "channel1")}, "unique"),
    ],
)
def test_invalid_hinted_streams_fail_during_detector_construction(kwargs, match):
    with pytest.raises(ValueError, match=match):
        make_detector(**kwargs)


@pytest.mark.asyncio
async def test_level_trigger_mode_has_safe_default():
    detector = make_detector()

    assert await detector.level_trigger_mode.get_value() is Xspress3LevelTriggerMode.TTL_VETO_ONLY


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "level_trigger_mode",
    list(Xspress3LevelTriggerMode),
)
async def test_supported_level_trigger_modes_are_mutable(level_trigger_mode):
    detector = make_detector()

    await detector.level_trigger_mode.set(level_trigger_mode)

    assert await detector.level_trigger_mode.get_value() is level_trigger_mode


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "level_trigger_mode",
    [
        Xspress3TriggerMode.SOFTWARE,
        Xspress3TriggerMode.INTERNAL,
        Xspress3TriggerMode.IDC,
        Xspress3TriggerMode.SOFTWARE_INTERNAL,
        Xspress3TriggerMode.TTL_INTERNAL,
    ],
)
async def test_rejects_non_level_trigger_modes(level_trigger_mode):
    detector = make_detector()

    with pytest.raises(ValueError):
        await detector.level_trigger_mode.set(level_trigger_mode)


@pytest.mark.asyncio
@pytest.mark.parametrize(("num_images", "collections_per_event"), [(4, 4), (0, 1)])
async def test_default_trigger_info_uses_internal_num_images(num_images, collections_per_event):
    driver = Xspress3DriverIO("XF:TEST{Xsp:1}:det1:")
    logic = Xspress3TriggerLogic(
        driver,
        soft_signal_rw(
            Xspress3LevelTriggerMode,
            Xspress3LevelTriggerMode.TTL_VETO_ONLY,
        ),
    )
    await driver.connect(mock=True)
    set_mock_value(driver.num_images, num_images)

    trigger_info = await logic.default_trigger_info()

    assert trigger_info.trigger is DetectorTrigger.INTERNAL
    assert trigger_info.number_of_events == 1
    assert trigger_info.collections_per_event == collections_per_event
    assert trigger_info.livetime == 0
    assert trigger_info.deadtime == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("trigger", "expected_mode"),
    [
        (DetectorTrigger.EXTERNAL_EDGE, Xspress3TriggerMode.TTL_INTERNAL),
        (DetectorTrigger.EXTERNAL_LEVEL, Xspress3TriggerMode.TTL_BOTH),
    ],
)
async def test_external_prepare_arms_before_kickoff_and_complete_waits(trigger, expected_mode):
    detector = make_detector(channel_numbers=(1, 2))
    await connect_and_seed(detector, channels=2)
    if trigger is DetectorTrigger.EXTERNAL_LEVEL:
        await detector.level_trigger_mode.set(Xspress3LevelTriggerMode.TTL_BOTH)
    await detector.stage()

    trigger_info = TriggerInfo(
        trigger=trigger,
        livetime=0.01,
        number_of_events=2,
    )
    await detector.prepare(trigger_info)
    assert await detector.driver.trigger_mode.get_value() is expected_mode
    assert await detector.driver.acquire.get_value() is True
    acquire_put_count = get_mock_put(detector.driver.acquire).await_count

    await detector.kickoff()
    assert get_mock_put(detector.driver.acquire).await_count == acquire_put_count
    complete = detector.complete()
    await asyncio.sleep(0)
    assert not complete.done
    set_mock_value(detector.hdf.num_captured, 2)
    set_mock_value(detector.driver.acquire, False)
    await complete
    await detector.unstage()


@pytest.mark.asyncio
async def test_internal_trigger_waits_for_busy_completion_and_frame_count():
    detector = make_detector(channel_numbers=(1, 2))
    await connect_and_seed(detector, channels=2)
    await detector.stage()
    await detector.prepare(TriggerInfo(trigger=DetectorTrigger.INTERNAL, number_of_events=1))

    set_mock_put_proceeds(detector.driver.acquire, False)
    status = detector.trigger()
    await wait_for_value(detector.driver.acquire, True, timeout=1)
    assert not status.done

    set_mock_value(detector.hdf.num_captured, 1)
    set_mock_value(detector.driver.acquire, False)
    await asyncio.sleep(0)
    assert not status.done

    set_mock_put_proceeds(detector.driver.acquire, True)
    await status
    await detector.unstage()


@pytest.mark.asyncio
async def test_trigger_info_rejects_unsupported_acquisitions():
    detector = make_detector()
    await connect_and_seed(detector)

    with pytest.raises(ValueError, match="deadtime"):
        await detector.prepare(TriggerInfo(deadtime=0.1))
    with pytest.raises(ValueError, match="unbounded"):
        await detector.prepare(TriggerInfo(number_of_events=0))
    with pytest.raises(ValueError, match="Multiple exposures"):
        await detector.prepare(TriggerInfo(exposures_per_collection=2))


@pytest.mark.asyncio
async def test_unstage_stops_acquisition_and_capture_after_cancellation():
    detector = make_detector()
    await connect_and_seed(detector)
    await detector.stage()
    await detector.prepare(TriggerInfo())

    set_mock_put_proceeds(detector.driver.acquire, False)
    trigger_status = detector.trigger()
    await wait_for_value(detector.driver.acquire, True, timeout=1)
    trigger_status.task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await trigger_status
    set_mock_put_proceeds(detector.driver.acquire, True)

    assert await detector.hdf.capture.get_value() is True
    await detector.unstage()

    assert await detector.driver.acquire.get_value() is False
    assert await detector.hdf.capture.get_value() is False
    assert await detector.hdf.num_capture_calc_disable.get_value() == 1


@pytest.mark.asyncio
async def test_all_data_views_describe_and_emit_advancing_stream_documents():
    detector = make_detector(
        channel_numbers=(1, 2),
        mca_roi_numbers=(1,),
        include_roi_streams=True,
        include_sca_streams=True,
    )
    await connect_and_seed(detector, channels=2)
    await detector.stage()
    await detector.prepare(TriggerInfo(number_of_events=2, collections_per_event=2))

    description = await detector.describe()
    assert description["xs"]["shape"] == [2, 2, 4096]
    assert description["xs-channel1"]["shape"] == [2, 4096]
    assert description["xs-channel1-roi1"]["shape"] == [2]
    assert description["xs-channel1-all_good"]["shape"] == [2]
    assert all(datakey["dtype"] == "array" for datakey in description.values())
    assert detector.hints == {"fields": ["xs"]}

    set_mock_value(detector.hdf.num_captured, 2)
    first_docs = [document async for document in detector.collect_asset_docs()]
    resources = {doc[1]["data_key"]: doc[1] for doc in first_docs if doc[0] == "stream_resource"}
    datums = [doc[1] for doc in first_docs if doc[0] == "stream_datum"]
    assert resources.keys() == description.keys()
    assert len(datums) == len(description)
    assert all(datum["indices"] == {"start": 0, "stop": 1} for datum in datums)
    assert resources["xs"]["parameters"] == {
        "chunk_shape": (1, 2, 4096),
        "dataset": "/entry/data/data",
    }
    assert resources["xs-channel1"]["parameters"] == {
        "chunk_shape": (1, 4096),
        "dataset": "/entry/data/data",
        "slice": ":,0,:",
    }
    assert resources["xs-channel1-roi1"]["parameters"]["dataset"] == ("/entry/instrument/NDAttributes/CHAN1ROI1")
    assert resources["xs-channel1-all_good"]["parameters"]["dataset"] == (
        "/entry/instrument/NDAttributes/CHAN1SCA4"
    )
    assert resources["xs-channel1-event_width"]["parameters"]["dataset"] == (
        "/entry/instrument/NDAttributes/CHAN1EventWidth"
    )

    set_mock_value(detector.hdf.num_captured, 4)
    second_docs = [document async for document in detector.collect_asset_docs()]
    assert {name for name, _ in second_docs} == {"stream_datum"}
    assert all(document["indices"] == {"start": 1, "stop": 2} for _, document in second_docs)
    await detector.unstage()


@pytest.mark.asyncio
@pytest.mark.xfail(
    reason="Requires the upstream one-dimensional stream dtype fix",
    strict=True,
)
async def test_one_dimensional_and_batched_scalar_datakey_types():
    detector = make_detector(
        mca_roi_numbers=(1,),
        include_roi_streams=True,
    )
    await connect_and_seed(detector)
    await detector.prepare(TriggerInfo())
    single = await detector.describe()
    assert single["xs-channel1"]["dtype"] == "array"
    assert single["xs-channel1"]["shape"] == [1, 4096]
    assert single["xs-channel1-roi1"]["dtype"] == "number"
    assert single["xs-channel1-roi1"]["shape"] == [1]

    await detector.prepare(TriggerInfo(collections_per_event=3))
    batched = await detector.describe()
    assert batched["xs-channel1-roi1"]["dtype"] == "array"
    assert batched["xs-channel1-roi1"]["shape"] == [3]
    await detector.unstage()


@pytest.mark.asyncio
async def test_repeated_prepare_forces_unbounded_num_capture():
    detector = make_detector()
    await connect_and_seed(detector)
    await detector.prepare(TriggerInfo())
    set_mock_value(detector.hdf.num_capture, 23)

    await detector.prepare(TriggerInfo(livetime=0.02))

    assert await detector.hdf.num_capture.get_value() == 0
    assert await detector.hdf.num_capture_calc_disable.get_value() == 1
    await detector.unstage()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("include_roi_streams", "include_sca_streams", "expected_count"),
    [
        (False, False, 2),
        (True, False, 3),
        (False, True, 13),
    ],
)
async def test_roi_and_sca_stream_flags_are_independent(
    include_roi_streams,
    include_sca_streams,
    expected_count,
):
    detector = make_detector(
        mca_roi_numbers=(1,),
        include_roi_streams=include_roi_streams,
        include_sca_streams=include_sca_streams,
    )
    await connect_and_seed(detector)
    set_mock_value(detector.driver.nd_attributes_file, "<invalid XML is ignored>")
    await detector.prepare(TriggerInfo())

    description = await detector.describe()
    assert len(description) == expected_count
    assert {"xs", "xs-channel1"} <= description.keys()
    assert ("xs-channel1-roi1" in description) is include_roi_streams
    assert ("xs-channel1-all_good" in description) is include_sca_streams
    for key in description.keys() - {"xs", "xs-channel1"}:
        assert description[key]["dtype_numpy"] == np.dtype(np.float64).str
    await detector.unstage()


@pytest.mark.asyncio
async def test_roi_programming_is_ordered_and_validated():
    detector = make_detector(mca_roi_numbers=(1,))
    await detector.connect(mock=True)
    roi = detector.channels[1].rois[1]

    await roi.set_roi_bins(10, 21, enabled=True)

    assert get_mock(roi).mock_calls == [
        call.enabled.put(False),
        call.min_x.put(10),
        call.size_x.put(11),
        call.enabled.put(True),
    ]
    assert await roi.min_x.get_value() == 10
    assert await roi.size_x.get_value() == 11
    assert await roi.enabled.get_value() is True

    for bounds, error in (
        ((1.0, 2), TypeError),
        ((-1, 2), ValueError),
        ((2, 2), ValueError),
        ((3, 2), ValueError),
    ):
        with pytest.raises(error):
            await roi.set_roi_bins(*bounds)


@pytest.mark.asyncio
async def test_warmup_restores_state_after_success_and_failure():
    detector = make_detector()
    await connect_and_seed(detector)
    original = (
        False,
        Xspress3TriggerMode.TTL_BOTH,
        7,
        0.75,
        False,
    )
    for signal, value in zip(
        (
            detector.driver.array_callbacks,
            detector.driver.trigger_mode,
            detector.driver.num_images,
            detector.driver.acquire_time,
            detector.driver.erase_on_start,
        ),
        original,
        strict=True,
    ):
        set_mock_value(signal, value)

    await detector.warmup(0.1)
    restored = await asyncio.gather(
        detector.driver.array_callbacks.get_value(),
        detector.driver.trigger_mode.get_value(),
        detector.driver.num_images.get_value(),
        detector.driver.acquire_time.get_value(),
        detector.driver.erase_on_start.get_value(),
    )
    assert tuple(restored) == original
    get_mock_put(detector.hdf.capture).assert_not_awaited()
    assert await detector.hdf.num_capture_calc_disable.get_value() == 1

    def fail_when_enabling(value):
        if value:
            raise RuntimeError("injected warmup failure")

    with callback_on_mock_put(detector.driver.array_callbacks, fail_when_enabling):
        with pytest.raises(RuntimeError, match="injected warmup failure"):
            await detector.warmup(0.2)

    restored = await asyncio.gather(
        detector.driver.array_callbacks.get_value(),
        detector.driver.trigger_mode.get_value(),
        detector.driver.num_images.get_value(),
        detector.driver.acquire_time.get_value(),
        detector.driver.erase_on_start.get_value(),
    )
    assert tuple(restored) == original
    assert await detector.hdf.num_capture_calc_disable.get_value() == 1


@pytest.mark.asyncio
async def test_zero_array_shape_fails_before_capture_and_directs_warmup():
    detector = make_detector()
    await connect_and_seed(detector, bins=0)

    with pytest.raises(ValueError, match=r"call warmup\(\) first"):
        await detector.prepare(TriggerInfo())
    assert await detector.hdf.capture.get_value() is False


@pytest.mark.filterwarnings("ignore:The ``noload`` loader strategy is deprecated")
def test_count_writes_xspress_streams_to_tiled(tmp_path):
    data_directory = tmp_path / "external"
    data_directory.mkdir()
    frame = np.arange(4096, dtype=np.uint32).reshape(1, 1, 4096)
    with h5py.File(data_directory / "xspress3.h5", "w") as file:
        file.create_dataset("/entry/data/data", data=frame, chunks=frame.shape)

    writer_factory = Xspress3HDFWriterFactory(
        StaticPathProvider(StaticFilenameProvider("xspress3"), data_directory)
    )
    detector = Xspress3Detector(
        "XF:TEST{Xsp:1}:",
        writer_factory,
        name="xs",
    )
    event_loop = asyncio.new_event_loop()
    run_engine = RunEngine({}, loop=event_loop)
    try:
        call_in_bluesky_event_loop(connect_and_seed(detector))
        set_mock_value(detector.driver.array_callbacks, True)

        async def forward_erase_frame():
            if await detector.driver.array_callbacks.get_value() and await detector.hdf.capture.get_value():
                captured = await detector.hdf.num_captured.get_value()
                set_mock_value(detector.hdf.num_captured, captured + 1)

        async def finish_acquisition(value):
            if value:
                callbacks_enabled = await detector.driver.array_callbacks.get_value()
                capture_enabled = await detector.hdf.capture.get_value()
                captured = await detector.hdf.num_captured.get_value()
                if callbacks_enabled and capture_enabled:
                    if await detector.driver.erase_on_start.get_value():
                        captured += 1
                    captured += 1
                loop = asyncio.get_running_loop()

                def finish():
                    set_mock_value(detector.hdf.num_captured, captured)
                    set_mock_value(detector.driver.detector_state, ADState.IDLE)
                    set_mock_value(detector.driver.acquire, False)

                loop.call_soon(finish)

        callback_on_mock_execute(detector.driver.erase, forward_erase_frame)
        callback_on_mock_put(detector.driver.acquire, finish_acquisition)

        with SimpleTiledServer(
            directory=tmp_path / "tiled",
            readable_storage=[data_directory],
        ) as server:
            client = from_uri(server.uri, trust_env=False)
            writer = TiledWriter(client, batch_size=1)

            (uid,) = run_engine(bp.count([detector]), writer)

            run = client[uid]
            bulk = np.asarray(run["primary/xs"].read())
            channel = np.asarray(run["primary/xs-channel1"].read())
            assert bulk.shape == (1, 1, 1, 4096)
            assert channel.shape == (1, 1, 4096)
            np.testing.assert_array_equal(bulk, frame[np.newaxis, ...])
            np.testing.assert_array_equal(channel, frame[np.newaxis, :, 0, :])
    finally:
        event_loop.call_soon_threadsafe(event_loop.stop)
        run_engine._th.join()
        event_loop.close()
