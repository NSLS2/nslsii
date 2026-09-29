import asyncio

import h5py
import numpy as np
import pytest
from ophyd_async.core import StaticFilenameProvider, StaticPathProvider, callback_on_mock_put, set_mock_value
from ophyd_async.epics.adcore import ADState, NDFileHDF5IO

from nslsii.ophyd_async.devices import EigerDriverIO


class FakeEigerIOC:
    """Emulates the ADEiger driver (and optionally an NDFileHDF5 plugin) on mock devices.

    Parameters
    ----------
    driver : EigerDriverIO
        Mock-mode driver to seed and attach put callbacks to.
    hdf : NDFileHDF5IO, optional
        Mock-mode HDF5 plugin. Its ``NumCaptured_RBV`` follows the driver's ``ArrayCounter`` and
        each frame is appended to ``{FilePath}{FileName}.h5:/entry/data/data`` with the frame's
        index as its pixel value.
    frame_shape : tuple[int, int], default (514, 1030)
        ``(ArraySizeY, ArraySizeX)``.
    """

    def __init__(
        self,
        driver: EigerDriverIO,
        hdf: NDFileHDF5IO | None = None,
        frame_shape: tuple[int, int] = (514, 1030),
    ) -> None:
        self.driver = driver
        self.hdf = hdf
        self.frame_shape = frame_shape
        self.triggers = 0
        self._tasks: list[asyncio.Task] = []
        set_mock_value(driver.bit_depth_image, 16)
        set_mock_value(driver.signed_data, False)
        set_mock_value(driver.array_size_y, frame_shape[0])
        set_mock_value(driver.array_size_x, frame_shape[1])
        set_mock_value(driver.dead_time, 3e-6)
        set_mock_value(driver.detector_state, ADState.IDLE)
        set_mock_value(driver.sequence_id, 41)
        set_mock_value(driver.array_counter, 7)
        set_mock_value(driver.file_path_exists, True)
        set_mock_value(driver.fw_nimgs_per_file, 1000)
        callback_on_mock_put(driver.acquire, self._on_acquire)
        callback_on_mock_put(driver.trigger_, self._on_trigger)
        if hdf is not None:
            set_mock_value(hdf.file_path_exists, True)

    async def _on_acquire(self, value: bool) -> None:
        if value:
            self.triggers = 0
            self.data_files = 0
            set_mock_value(self.driver.sequence_id, await self.driver.sequence_id.get_value() + 1)
            set_mock_value(self.driver.armed, True)
        else:
            set_mock_value(self.driver.armed, False)

    async def _on_trigger(self, _: float) -> None:
        # Like controlTask: the series accepts NumTriggers software triggers, then disarms
        self.triggers += 1
        last = self.triggers >= await self.driver.num_triggers.get_value()
        self._tasks.append(asyncio.create_task(self._count_frames(1, end_series=last)))

    async def _count_frames(self, num_triggers: int, end_series: bool) -> None:
        num_images, counter = await asyncio.gather(
            self.driver.num_images.get_value(), self.driver.array_counter.get_value()
        )
        num_frames = num_triggers * num_images
        # Files land before the IOC counts their frames
        if self.hdf is not None:
            await self._write_plugin_frames(num_frames)
        elif await self.driver.save_files.get_value():
            await self._write_dcu_file(counter, num_frames)
        set_mock_value(self.driver.array_counter, counter + num_frames)
        if end_series:
            set_mock_value(self.driver.armed, False)
            set_mock_value(self.driver.acquire, False)

    async def _write_plugin_frames(self, num_frames: int) -> None:
        """Append to ``{FilePath}{FileName}.h5`` as NDFileHDF5 would."""
        assert self.hdf is not None
        file_path, file_name, captured = await asyncio.gather(
            self.hdf.file_path.get_value(),
            self.hdf.file_name.get_value(),
            self.hdf.num_captured.get_value(),
        )
        with h5py.File(f"{file_path}{file_name}.h5", "a") as f:
            if "/entry/data/data" not in f:
                f.create_dataset(
                    "/entry/data/data",
                    shape=(0, *self.frame_shape),
                    maxshape=(None, *self.frame_shape),
                    chunks=(1, *self.frame_shape),
                    dtype=np.uint16,
                )
            self._fill(f["/entry/data/data"], captured, num_frames)
        set_mock_value(self.hdf.num_captured, captured + num_frames)

    async def _write_dcu_file(self, first_frame: int, num_frames: int) -> None:
        """Save one DCU data file, ``{FilePath}{FWNamePattern with $id}_data_{k:06d}.h5``."""
        file_path, pattern, sequence_id = await asyncio.gather(
            self.driver.file_path.get_value(),
            self.driver.fw_name_pattern.get_value(),
            self.driver.sequence_id.get_value(),
        )
        self.data_files += 1
        name = pattern.replace("$id", str(sequence_id))
        with h5py.File(f"{file_path}{name}_data_{self.data_files:06d}.h5", "w") as f:
            dataset = f.create_dataset(
                "/entry/data/data",
                shape=(0, *self.frame_shape),
                maxshape=(None, *self.frame_shape),
                chunks=(1, *self.frame_shape),
                dtype=np.uint16,
            )
            self._fill(dataset, first_frame, num_frames)

    @staticmethod
    def _fill(dataset: h5py.Dataset, first_frame: int, num_frames: int) -> None:
        # Frame k of the scan has pixel value k
        start = dataset.shape[0]
        dataset.resize(start + num_frames, axis=0)
        for i in range(num_frames):
            dataset[start + i] = first_frame + i

    async def external_pulses(self, num_triggers: int) -> None:
        await self._count_frames(num_triggers, end_series=True)


@pytest.fixture
def fake_ioc() -> type[FakeEigerIOC]:
    return FakeEigerIOC


@pytest.fixture
def path_provider(tmp_path) -> StaticPathProvider:
    return StaticPathProvider(StaticFilenameProvider("abc"), tmp_path)
