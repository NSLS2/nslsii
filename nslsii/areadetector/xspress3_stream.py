import datetime
import logging
from collections import deque
from pathlib import Path
from uuid import uuid4

from databroker.assets.handlers import Xspress3HDF5Handler
from event_model import compose_stream_resource
from ophyd import Component as Cpt, Kind, Signal
from ophyd.areadetector.plugins import HDF5Plugin_V34 as HDF5Plugin

from .xspress3 import Xspress3ExternalFileReference


logger = logging.getLogger(__name__)


class Xspress3StreamExternalFileReference(Xspress3ExternalFileReference):
    """Stream-asset counterpart to Xspress3ExternalFileReference."""

    def read(self):
        return {}

    def describe(self):
        res = super().describe()
        res[self.name]["external"] = "STREAM:"
        # Tiled stream nodes add a leading sequence dimension to the per-row shape.
        res[self.name]["dims"] = ("time", *self.dims)
        return res


class Xspress3HDF5StreamPlugin(HDF5Plugin):
    """Xspress3 HDF5 plugin that emits stream asset documents."""

    root_path = Cpt(Signal, kind=Kind.config)
    path_template = Cpt(Signal, kind=Kind.config)

    def __init__(
        self,
        *args,
        root_path,
        path_template,
        resource_kwargs=None,
        spec=Xspress3HDF5Handler.HANDLER_NAME,
        **kwargs,
    ):
        """

        Parameters
        ----------
        args:
            passed to the parent class
        root_path:
            the "non-semantic" part of the data path, for example /nsls2/data
        path_template:
            path to the data directory, which must include the root_path,
            and may include %Y, %m, %d and other strftime replacements,
            for example /nsls2/data/tst/xspress3/2020/01/01
        resource_kwargs:
            placed in stream resource parameters
        spec:
            data handler name recorded in stream resource parameters,
            Xspress3HDF5Handler.HANDLER_NAME by default
        kwargs:
            passed to the parent class
        """
        super().__init__(*args, **kwargs)
        self._stream_datum_composers = {}
        self._asset_docs_cache = None

        self.root_path.put(root_path)
        self.path_template.put(path_template)
        self.spec = spec
        if resource_kwargs is None:
            resource_kwargs = {}
        self.resource_kwargs = resource_kwargs

        self.stage_sigs[self.create_directory] = -3
        self.stage_sigs[self.auto_increment] = "Yes"
        self.stage_sigs[self.auto_save] = "Yes"
        self.stage_sigs[self.num_capture] = 0  # 0 means take as many as you want
        self.stage_sigs[self.enable] = 1
        self.stage_sigs[self.compression] = "zlib"

        # set hdf5 chunk size in a good way

        self.stage_sigs[self.file_template] = "%s%s_%6.6d.h5"
        self.stage_sigs[self.file_write_mode] = "Stream"

    @staticmethod
    def _build_data_dir_path(the_datetime, root_path, path_template):
        """
        Construct a data directory path from root_path and path_template.

        Parameters
        ----------
        the_datetime: datetime.datetime
            the date and time to use in formatting path_template
        root_path: str
            the "non-semantic" part of the data path, for example /nsls2/data/tst
        path_template: str
            path to the data directory, which must include the root_path,
            and may include %Y, %m, %d and other strftime replacements,
            for example /nsls2/data/tst/xspress3/%Y/%m/%d
        Return
        ------
          str
        """
        the_data_dir_path = the_datetime.strftime(path_template)
        the_full_data_dir_path = Path(root_path) / Path(the_data_dir_path)
        return str(the_full_data_dir_path)

    def _configure_references(self):
        parent_reference = self.parent.get_external_file_ref()
        channels = tuple(self.parent.iterate_channels())
        channel_references = tuple(channel.get_external_file_ref() for channel in channels)

        if parent_reference is not None and channel_references:
            channel_reference = channel_references[0]
            parent_reference.shape = (
                len(channel_references),
                *channel_reference.shape,
            )
            parent_reference.dims = ("channel", *channel_reference.dims)

        return parent_reference, channels

    def _compose_stream_resource(self, *, full_file_path, reference, channel_number=None):
        parameters = {
            **self.resource_kwargs,
            "dataset": "/entry/instrument/detector/data",
            "chunk_shape": (1, *reference.shape),
            "join_method": "stack",
            "spec": self.spec,
        }
        parameters.pop("slice", None)
        if channel_number is not None:
            parameters["slice"] = f":,{channel_number - 1},:"
        return compose_stream_resource(
            mimetype="application/x-hdf5",
            uri=full_file_path.absolute().as_uri(),
            data_key=reference.name,
            parameters=parameters,
        )

    def stage(self):
        parent_reference, channels = self._configure_references()
        logger.debug("staging '%s' of '%s'", self.name, self.parent.name)
        staged_devices = super().stage()

        self.array_counter.set(0).wait()

        the_full_data_dir_path = self._build_data_dir_path(
            the_datetime=datetime.datetime.now(),
            root_path=self.root_path.get(),
            path_template=self.path_template.get(),
        )
        self.file_path.set(the_full_data_dir_path).wait()
        self.file_name.set("-".join(str(uuid4()).split("-")[:-1])).wait()
        self.file_number.set(0).wait()

        file_path = self.file_path.get()
        file_name = self.file_name.get()
        file_number = self.file_number.get()
        full_file_path = Path(self.stage_sigs[self.file_template] % (file_path, file_name, file_number))

        self._asset_docs_cache = deque()
        self._stream_datum_composers = {}

        if parent_reference is not None and parent_reference.kind & Kind.normal:
            stream_resource, stream_datum_composer = self._compose_stream_resource(
                full_file_path=full_file_path,
                reference=parent_reference,
            )
            self._stream_datum_composers[parent_reference.name] = stream_datum_composer
            self._asset_docs_cache.append(("stream_resource", stream_resource))

        for channel in channels:
            channel_reference = channel.get_external_file_ref()
            if channel_reference is None or not channel_reference.kind & Kind.normal:
                continue
            stream_resource, stream_datum_composer = self._compose_stream_resource(
                full_file_path=full_file_path,
                reference=channel_reference,
                channel_number=channel.channel_number,
            )
            self._stream_datum_composers[channel_reference.name] = stream_datum_composer
            self._asset_docs_cache.append(("stream_resource", stream_resource))

        self.capture.set(1).wait()
        return staged_devices

    def unstage(self):
        self.capture.set(0).wait()
        return super().unstage()

    def generate_datum(self, key, timestamp, datum_kwargs):
        if key is not None:
            raise ValueError(f"'key' must be None but key='{key}'")

        try:
            frame = datum_kwargs["frame"]
        except KeyError as exc:
            raise ValueError("'frame' is required in datum_kwargs for Xspress3 stream data") from exc

        parent_reference = self.parent.get_external_file_ref()
        if parent_reference is not None and parent_reference.kind & Kind.normal:
            stream_datum = self._stream_datum_composers[parent_reference.name](
                indices={"start": frame, "stop": frame + 1}
            )
            self._asset_docs_cache.append(("stream_datum", stream_datum))

        for channel in self.parent.iterate_channels():
            channel_reference = channel.get_external_file_ref()
            if channel_reference is None or not channel_reference.kind & Kind.normal:
                continue
            stream_datum = self._stream_datum_composers[channel_reference.name](
                indices={"start": frame, "stop": frame + 1}
            )
            self._asset_docs_cache.append(("stream_datum", stream_datum))

    def collect_asset_docs(self):
        items = list(self._asset_docs_cache)
        self._asset_docs_cache.clear()
        for item in items:
            yield item
