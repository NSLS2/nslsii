import datetime
import itertools
import logging
import time as ttime
import warnings
from collections import deque
from pathlib import Path

import cv2
import h5py
import numpy as np
from event_model import compose_resource
from ophyd import Component as Cpt
from ophyd import Device, Signal
from ophyd.sim import NullStatus, new_uid

from nslsii.iocs.caproto_saver import ExternalFileReference

logger = logging.getLogger("vstream")


class VideoStreamDet(Device):
    image = Cpt(ExternalFileReference, kind="normal")
    mean = Cpt(Signal, value=0.0, kind="hinted")
    exposure_time = Cpt(Signal, value=1.0, kind="config")

    def __init__(
        self,
        *args,
        root_dir="/tmp/video-stream-data",
        assets_dir=None,
        video_stream_url=None,
        frame_shape=(1080, 1920),
        **kwargs,
    ):
        warnings.warn(f"This class {self.__class__.__name__} will be removed in the future.")

        super().__init__(*args, **kwargs)

        self._root_dir = root_dir
        self._assets_dir = assets_dir
        self._video_stream_url = video_stream_url

        self._asset_docs_cache = deque()
        self._resource_document = None
        self._datum_factory = None

        self._frame_shape = frame_shape

    def stage(self):
        super().stage()
        date = datetime.datetime.now()
        self._assets_dir = date.strftime("%Y/%m/%d")
        data_file = f"{new_uid()}.h5"

        self._resource_document, self._datum_factory, _ = compose_resource(
            start={"uid": "needed for compose_resource() but will be discarded"},
            spec="VIDEO_STREAM_HDF5",
            root=self._root_dir,
            resource_path=str(Path(self._assets_dir) / Path(data_file)),
            resource_kwargs={},
        )

        self._data_file = str(
            Path(self._resource_document["root"]) / Path(self._resource_document["resource_path"])
        )

        # now discard the start uid, a real one will be added later
        self._resource_document.pop("run_start")
        self._asset_docs_cache.append(("resource", self._resource_document))

        logger.debug(f"{self._data_file = }")

        self._h5file_desc = h5py.File(self._data_file, "x")
        group = self._h5file_desc.create_group("/entry")
        self._dataset = group.create_dataset(
            "averaged",
            data=np.full(fill_value=np.nan, shape=(1, *self._frame_shape)),
            maxshape=(None, *self._frame_shape),
            chunks=(1, *self._frame_shape),
            dtype="float64",
            compression="lzf",
        )
        self._counter = itertools.count()

    def trigger(self, *args, **kwargs):
        super().trigger(*args, **kwargs)

        frames = []
        times = []

        start = ttime.monotonic()
        i = 0
        cap = cv2.VideoCapture(self._video_stream_url)
        while True:
            logger.debug(f"Iteration: {i}")
            i += 1
            ret, frame = cap.read()
            frames.append(frame)
            times.append(ttime.time())

            # cv2.imshow('Video', frame)
            logger.debug(f"shape: {frame.shape}")

            if ttime.monotonic() - start >= self.exposure_time.get():
                break

            if cv2.waitKey(1) == 27:
                exit(0)

        frames = np.array(frames)
        logger.debug(f"original shape: {frames.shape}")
        # Averaging over all frames and summing 3 RGB channels
        averaged = frames.mean(axis=0).sum(axis=-1)

        current_frame = next(self._counter)
        self._dataset.resize((current_frame + 1, *self._frame_shape))
        logger.debug(f"{self._dataset = }\n{self._dataset.shape = }")
        self._dataset[current_frame, :, :] = averaged

        datum_document = self._datum_factory(datum_kwargs={"frame": current_frame})
        self._asset_docs_cache.append(("datum", datum_document))

        self.image.put(datum_document["datum_id"])
        self.mean.put(averaged.mean())

        return NullStatus()

    def describe(self):
        res = super().describe()
        res[self.image.name].update(dict(shape=self._frame_shape))
        return res

    def unstage(self):
        super().unstage()
        del self._dataset
        self._h5file_desc.close()
        self._resource_document = None
        self._datum_factory = None

    def collect_asset_docs(self):
        items = list(self._asset_docs_cache)
        self._asset_docs_cache.clear()
        for item in items:
            yield item
