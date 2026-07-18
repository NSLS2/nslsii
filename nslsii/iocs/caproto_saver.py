from __future__ import annotations

from collections import deque
from io import BytesIO
import os
import re
import textwrap
import threading
import time as ttime
import uuid
from enum import Enum
from pathlib import Path

import numpy as np
import requests
from caproto import ChannelType
from caproto.ioc_examples.mini_beamline import no_reentry
from caproto.server import PVGroup, pvproperty, run, template_arg_parser
from ophyd import Component as Cpt
from ophyd import Device, EpicsSignal, EpicsSignalRO, Kind, Signal
from ophyd.status import SubscriptionStatus
from event_model import compose_resource

from .utils import now, save_hdf5_nd

from PIL import Image


class AcqStatuses(Enum):
    """Enum class for acquisition statuses."""

    IDLE = "idle"
    ACQUIRING = "acquiring"


class DirExistsStatuses(Enum):
    DOES_NOT_EXIST = "does not exist"
    PERMISSION_ERROR = "insufficient permissions"
    EXISTS = "exists"


class UIDOptions(Enum):
    NONE = "none"
    SHORT = "short"
    FULL = "full"


class OnOffStates(Enum):
    DISABLE = "disable"
    ENABLE = "enable"


class CaprotoSaveIOC(PVGroup):
    """Generic Caproto Save IOC"""

    write_dir = pvproperty(
        value="/tmp",
        doc="The directory to write data to. It support datetime formatting, e.g. '/tmp/det/%Y/%m/%d/'",
        string_encoding="utf-8",
        dtype=ChannelType.CHAR,
        max_length=255,
    )
    file_name = pvproperty(
        value="test.h5",
        doc="The file name of the file to write to. It support <str>.format() based formatting, e.g. 'scan_{num:06d}.h5'",
        string_encoding="utf-8",
        dtype=ChannelType.CHAR,
        max_length=255,
    )
    full_file_path = pvproperty(
        value="",
        doc="Full path to the data file",
        dtype=str,
        read_only=True,
        max_length=255,
    )

    directory_exists = pvproperty(
        value=DirExistsStatuses.DOES_NOT_EXIST.value,
        doc="Record specifying whether or not the target directory exists or not",
        dtype=ChannelType.ENUM,
        read_only=True,
        enum_strings=[x.value for x in DirExistsStatuses],
    )

    uid_type = pvproperty(
        value=UIDOptions.NONE.value,
        doc="UUID to include automatically in file name",
        dtype=ChannelType.ENUM,
        enum_strings=[x.value for x in UIDOptions],
    )

    use_frame_num = pvproperty(
        value=OnOffStates.DISABLE.value,
        doc="Enable auto-incrementing frame counter suffix for filenames",
        dtype=ChannelType.ENUM,
        enum_strings=[x.value for x in OnOffStates],
    )

    # TODO: check non-negative value in @frame_num.putter.
    frame_num = pvproperty(value=0, doc="Frame counter", dtype=int)

    acquire = pvproperty(
        value=AcqStatuses.IDLE.value,
        enum_strings=[x.value for x in AcqStatuses],
        dtype=ChannelType.ENUM,
        doc="Acquire signal to save a dataset.",
    )

    def __init__(self, *args, update_rate=10.0, **kwargs):
        super().__init__(*args, **kwargs)

        self._update_rate = update_rate
        self._update_period = 1.0 / update_rate

        self._request_queue = None
        self._response_queue = None

        self._sanitizer = re.compile(pattern=r"[\":<>|\*\?\s]")

    queue = pvproperty(value=0, doc="A PV to facilitate threading-based queue")

    @queue.startup
    async def queue(self, instance, async_lib):
        """The startup behavior of the count property to set up threading queues."""
        # pylint: disable=unused-argument
        self._request_queue = async_lib.ThreadsafeQueue(maxsize=1)
        self._response_queue = async_lib.ThreadsafeQueue(maxsize=1)

        # Start a separate thread that consumes requests and sends responses.
        thread = threading.Thread(
            target=self.saver,
            daemon=True,
            kwargs={
                "request_queue": self._request_queue,
                "response_queue": self._response_queue,
            },
        )
        thread.start()

    async def _update_full_file_path(self, write_dir=None, file_name=None, use_frame_num=None, uid_type=None):

        if use_frame_num is None:
            use_num = self.use_frame_num.value
        else:
            use_num = use_frame_num

        frame_num_str = ""
        if use_num == OnOffStates.ENABLE.value:
            frame_num = self.frame_num.value
            frame_num_str = f"_{frame_num:06}"

        if uid_type is None:
            uid_to_use = self.uid_type.value
        else:
            uid_to_use = uid_type
        uid_str = ""
        if uid_to_use == UIDOptions.SHORT.value:
            uid_str = f"_{str(uuid.uuid4())[:8]}"
        elif uid_to_use == UIDOptions.FULL.value:
            uid_str = f"_{str(uuid.uuid4())}"

        if write_dir is None:
            local_write_dir = Path(self.write_dir.value)
        else:
            local_write_dir = Path(write_dir)

        if file_name is None:
            full_file_name = self.file_name.value
        else:
            full_file_name = file_name

        try:
            filename_and_ext = os.path.splitext(full_file_name)
            base_filename = filename_and_ext[0]
            extension = filename_and_ext[1]
        except IndexError:
            # Case that we didn't get an extension
            base_filename = full_file_name
            extension = ""

        full_file_path = local_write_dir / f"{base_filename}{frame_num_str}{uid_str}{extension}"

        full_file_path = self._sanitizer.sub("_", str(full_file_path))

        print(f"{now()}: {full_file_path = }")

        await self.full_file_path.write(full_file_path)

    async def _use_frame_num_callback(self, instance, value):
        await self._update_full_file_path(use_frame_num=value)
        return value

    async def _uid_type_callback(self, instance, value):
        await self._update_full_file_path(uid_type=value)
        return value

    async def _file_name_callback(self, instance, value):
        await self._update_full_file_path(file_name=value)
        return value

    async def _write_dir_callback(self, instance, value):
        """The stage method to perform preparation of a dataset to save the data."""

        local_write_dir = Path(value)

        if os.path.exists(local_write_dir):
            if os.access(local_write_dir, os.W_OK):
                await self.directory_exists.write(DirExistsStatuses.EXISTS.value)
                await self._update_full_file_path(write_dir=value)
            else:
                await self.directory_exists.write(DirExistsStatuses.PERMISSION_ERROR.value)
        else:
            await self.directory_exists.write(DirExistsStatuses.DOES_NOT_EXIST.value)

        if self.directory_exists.value == DirExistsStatuses.EXISTS.value:
            return value
        else:
            print(f"Directory access error for directory {value}! - {self.directory_exists.value}")
            return ""

    @write_dir.putter
    async def write_dir(self, *args, **kwargs):
        """The write_dir callback method."""
        return await self._write_dir_callback(*args, **kwargs)

    @file_name.putter
    async def file_name(self, *args, **kwargs):
        """The file name callback method."""
        return await self._file_name_callback(*args, **kwargs)

    @uid_type.putter
    async def uid_type(self, *args, **kwargs):
        """The file name callback method."""
        return await self._uid_type_callback(*args, **kwargs)

    @use_frame_num.putter
    async def use_frame_num(self, *args, **kwargs):
        """The file name callback method."""
        return await self._use_frame_num_callback(*args, **kwargs)

    async def _get_current_dataset(self, frame):
        """The method to return a desired dataset.

        See https://scikit-image.org/docs/stable/auto_examples/data/plot_3d.html
        for details about the dataset returned by the base class' method.
        """
        return np.random.random((480, 640))

    @acquire.putter
    @no_reentry
    async def acquire(self, instance, value):
        """The acquire method to perform an individual acquisition of a data point."""
        if (
            value != AcqStatuses.ACQUIRING.value
            # or self.stage.value not in [True, StageStates.STAGED.value]
        ):
            return False

        if instance.value in [True, AcqStatuses.ACQUIRING.value] and value == AcqStatuses.ACQUIRING.value:
            print(f"The device is already acquiring. Please wait until the '{AcqStatuses.IDLE.value}' status.")
            return True

        if self.directory_exists.value != DirExistsStatuses.EXISTS.value:
            print("Target write directory does not exist or cannot be written to!")
            return False

        await self.acquire.write(AcqStatuses.ACQUIRING.value)

        # Delegate saving the resulting data to a blocking callback in a thread.
        payload = {
            "filename": self.full_file_path.value,
            "data": await self._get_current_dataset(frame=self.frame_num.value),
            "uid": str(uuid.uuid4()),
            "timestamp": ttime.time(),
            "frame_number": self.frame_num.value,
        }

        await self._request_queue.async_put(payload)
        response = await self._response_queue.async_get()

        if response["success"]:
            # Increment the counter only on a successful saving of the file.
            await self.frame_num.write(self.frame_num.value + 1)

        # await self.acquire.write(AcqStatuses.IDLE.value)

        return False

    async def on_startup(self, async_lib):
        for key in self.pvdb:
            print(key)

        await self._write_dir_callback(None, "/tmp")

    @staticmethod
    def saver(request_queue, response_queue):
        """The saver callback for threading-based queueing."""
        while True:
            received = request_queue.get()
            filename = received["filename"]
            data = received["data"]
            frame_number = received["frame_number"]
            try:
                save_hdf5_nd(fname=filename, data=data, mode="x", group_path="enc1")
                print(f"{now()}: saved {frame_number=} {data.shape} data into:\n  {filename}")

                success = True
                error_message = ""
            except Exception as exc:  # pylint: disable=broad-exception-caught
                success = False
                error_message = exc
                print(f"Cannot save file {filename!r} due to the following exception:\n{exc}")

            response = {"success": success, "error_message": error_message}
            response_queue.put(response)

    @staticmethod
    def check_args(parser, split_args):
        """Helper function to process caproto CLI args."""
        parsed_args = parser.parse_args()
        prefix = parsed_args.prefix
        if not prefix:
            parser.error("The 'prefix' argument must be specified.")

        ioc_opts, run_opts = split_args(parsed_args)
        return ioc_opts, run_opts


class AxisWebcamCaprotoSaver(CaprotoSaveIOC):
    """"""

    def __init__(self, *args, camera_host=None, **kwargs):
        self._camera_host = camera_host
        print(f"{camera_host = }")
        super().__init__(*args, **kwargs)

    @staticmethod
    def check_args(parser, split_args):
        """Helper function to process caproto CLI args."""
        parsed_args = parser.parse_args()
        prefix = parsed_args.prefix
        camera_host = parsed_args.camera_host
        if not prefix:
            parser.error("The 'prefix' argument must be specified.")
        if not camera_host:
            parser.error("The 'camera_host' argument must be specified.")

        ioc_opts, run_opts = split_args(parsed_args)

        ioc_opts["camera_host"] = parsed_args.camera_host
        return ioc_opts, run_opts

    async def _get_current_dataset(self, *args, **kwargs):  # pylint: disable=unused-argument
        url = f"http://{self._camera_host}/axis-cgi/jpg/image.cgi"
        resp = requests.get(url, timeout=10)
        img = Image.open(BytesIO(resp.content))

        dataset = np.asarray(img).sum(axis=-1)
        print(f"{now()}: {dataset.shape}")

        return dataset

    @staticmethod
    def saver(request_queue, response_queue):
        """The saver callback for threading-based queueing."""
        while True:
            received = request_queue.get()
            filename = received["filename"]
            data = received["data"]
            # 'frame_number' is not used for this exporter.
            try:
                save_hdf5_nd(fname=filename, data=data, dtype="|u1", mode="a")
                # TODO: Change all of these prints to use the caproto logger instead
                print(f"{now()}: saved data into: {filename}")

                success = True
                error_message = ""
            except Exception as exc:  # pylint: disable=broad-exception-caught
                success = False
                error_message = exc
                print(f"Cannot save file {filename!r} due to the following exception:\n{exc}")

            response = {"success": success, "error_message": error_message}
            response_queue.put(response)


class ExternalFileReference(Signal):
    """
    A pure software Signal that describe()s an image in an external file.
    """

    def describe(self):
        resource_document_data = super().describe()
        resource_document_data[self.name].update(
            dict(
                external="FILESTORE:",
                dtype="array",
            )
        )
        return resource_document_data


class CaprotoSaverDevice(Device):
    """An ophyd Device which works with the base caproto extension IOC."""

    write_dir = Cpt(EpicsSignal, "write_dir", string=True)
    file_name = Cpt(EpicsSignal, "file_name", string=True)
    full_file_path = Cpt(EpicsSignalRO, "full_file_path", string=True)
    frame_num = Cpt(EpicsSignal, "frame_num")
    acquire = Cpt(EpicsSignal, "acquire", string=True)
    directory_exists = Cpt(EpicsSignalRO, "directory_exists", string=True)
    use_frame_num = Cpt(EpicsSignal, "use_frame_num", string=True)
    uid_type = Cpt(EpicsSignal, "uid_type", string=True)

    data = Cpt(ExternalFileReference, kind=Kind.normal)

    def __init__(self, *args, md=None, extension="h5", handler_spec="AD_HDF5", root_dir=None, **kwargs):
        super().__init__(*args, **kwargs)
        if root_dir is None:
            msg = "The 'root_dir' kwarg cannot be None"
            raise RuntimeError(msg)
        self._root_dir = root_dir
        self._resource_document, self._datum_factory = None, None
        self._asset_docs_cache = deque()
        self._md = md or {}
        self._handler_spec = handler_spec

        self.stage_sigs["uid_type"] = "full"
        self.stage_sigs["file_name"] = f"{self.name}.{extension}"

    def _update_paths(self):
        self._root_dir = self.root_path_str

    @property
    def root_path_str(self):
        beamline = os.getenv("ENDSTATION_ACRONYM", os.getenv("BEAMLINE_ACRONYM", "TST")).lower()
        # These three beamlines have a -new suffix in their
        if beamline in ["xpd", "fxi", "qas"]:
            beamline = f"{beamline}-new"
        root_path = f"/nsls2/data/{beamline}/proposals/{self._md.get('cycle', '')}/{self._md.get('data_session', '')}/assets/{self.name}"
        return root_path

    @property
    def shape(self):
        """Property that contains the shape of the data"""

        return (1080, 1920)

    @property
    def dtype_numpy(self):
        """dtype_str for use in the descriptor"""
        return "<f4"

    def collect_asset_docs(self):
        """The method to collect resource/datum documents."""
        items = list(self._asset_docs_cache)
        self._asset_docs_cache.clear()
        yield from items

    def stage(self):
        self._update_paths()
        self.write_dir.put(self._root_dir)

        assert self.directory_exists.get() == "exists"

        super().stage()

        # Clear asset docs cache which may have some documents from the previous failed run.
        self._asset_docs_cache.clear()

        self._resource_document, self._datum_factory, _ = compose_resource(
            start={"uid": "needed for compose_resource() but will be discarded"},
            spec=self._handler_spec,
            root="/",
            resource_path=self.full_file_path.get(),
            resource_kwargs={},
        )

        # now discard the start uid, a real one will be added later
        self._resource_document.pop("run_start")
        self._asset_docs_cache.append(("resource", self._resource_document))

        # Update caproto IOC parameters:

    def describe(self):
        res = super().describe()
        res[self.data.name].update({"shape": self.shape, "dtype_str": self.dtype_numpy})
        return res

    def trigger(self):

        def done_callback(value, old_value, **kwargs):
            """The callback function used by ophyd's SubscriptionStatus."""
            # print(f"{old_value = } -> {value = }")
            if old_value == "acquiring" and value == "idle":
                return True
            return False

        status = SubscriptionStatus(self.acquire, run=False, callback=done_callback)

        # Reuse the counter from the caproto IOC
        self.acquire.put(1)

        datum_document = self._datum_factory(datum_kwargs={})
        self._asset_docs_cache.append(("datum", datum_document))

        self.data.put(datum_document["datum_id"])

        return status

    def unstage(self):
        self._resource_document = None
        self._datum_factory = None
        super().unstage()


class TwoDimCaprotoCam(CaprotoSaverDevice):
    def __init__(self, *args, shape=(1080, 1920), dtype_numpy="|u1", **kwargs):
        super().__init__(*args, **kwargs)
        self._shape = shape
        self._dtype_numpy = dtype_numpy

    @property
    def shape(self):
        return self._shape

    @property
    def dtype_numpy(self):
        return self._dtype_numpy


def start_caproto_ioc(cls, parser, split_args):

    ioc_options, run_options = cls.check_args(parser, split_args)
    ioc = cls(**ioc_options)
    run(ioc.pvdb, startup_hook=ioc.on_startup, **run_options)


def start_axis_ioc():
    parser, split_args = template_arg_parser(
        default_prefix="", desc=textwrap.dedent(AxisWebcamCaprotoSaver.__doc__)
    )
    parser.add_argument("-c", "--camera-host", help="URL of the axis camera stream", required=True, type=str)
    start_caproto_ioc(AxisWebcamCaprotoSaver, parser, split_args)


if __name__ == "__main__":
    start_axis_ioc()
