import copy
from collections import Counter
from pathlib import Path
from unittest.mock import Mock
from urllib.parse import unquote, urlparse

import h5py
import numpy as np
import pytest
from bluesky import plans
from bluesky_tiled_plugins import TiledWriter
from event_model import DocumentNames
from ophyd import Component, Kind
from ophyd.areadetector import Xspress3Detector
from ophyd.sim import NullStatus, make_fake_device
from tiled.catalog import in_memory
from tiled.client import Context, from_context
from tiled.media_type_registration import default_serialization_registry
from tiled.server.app import build_app

from bluesky_tiled_plugins.exporters import json_seq_exporter
from bluesky_tiled_plugins.routers import validator

from nslsii.areadetector.xspress3 import Xspress3Trigger, build_xspress3_class
from nslsii.areadetector.xspress3_stream import (
    Xspress3HDF5StreamPlugin,
    Xspress3StreamExternalFileReference,
)


EXPECTED_DATA = np.arange(2 * 2 * 4096, dtype=np.uint32).reshape(2, 2, 4096)
DATASET_PATH = "/entry/instrument/detector/data"


@pytest.fixture
def tiled_client(tmp_path):
    asset_dir = tmp_path / "assets"
    asset_dir.mkdir()
    catalog = in_memory(
        specs=[{"name": "CatalogOfBlueskyRuns", "version": "3.0"}],
        writable_storage={
            "filesystem": str(asset_dir),
            "sql": f"duckdb:///{tmp_path}/tabular_data.db",
        },
        readable_storage=[str(asset_dir)],
    )
    serialization_registry = copy.copy(default_serialization_registry)
    serialization_registry.register("BlueskyRun", "application/json-seq", json_seq_exporter)
    app = build_app(
        catalog,
        serialization_registry=serialization_registry,
        include_routers=[validator.router],
    )
    context = Context.from_app(app)
    try:
        yield from_context(context), asset_dir
    finally:
        context.close()


class _SimulatedPluginMixin:
    def stage(self):
        self.stage_sigs[self.file_template] = "%s/%s_%6.6d.h5"
        staged_devices = super().stage()

        stream_resources = [
            document for name, document in self._asset_docs_cache if name == DocumentNames.stream_resource.value
        ]
        uri = stream_resources[0]["uri"]
        file_path = Path(unquote(urlparse(uri).path))

        file_path.parent.mkdir(parents=True, exist_ok=True)
        with h5py.File(file_path, "w") as file:
            file.create_dataset(DATASET_PATH, data=EXPECTED_DATA, chunks=(1, 2, 4096))

        return staged_devices


def _mock_signal_set(signal):
    def set_value(value, *args, **kwargs):
        signal.sim_put(value)
        return NullStatus()

    signal.set = Mock(side_effect=set_value)


def _mock_plugin_signals(plugin):
    signals = {getattr(plugin, signal) if isinstance(signal, str) else signal for signal in plugin.stage_sigs}
    signals.update(
        getattr(plugin, signal) for signal in ("array_counter", "file_path", "file_name", "file_number", "capture")
    )
    for signal in signals:
        if hasattr(signal, "sim_put"):
            _mock_signal_set(signal)


def _mock_acquisition_completion(detector):
    production_trigger = detector.trigger

    def trigger():
        status = production_trigger()
        detector.cam.acquire.put(0)
        return status

    detector.trigger = Mock(side_effect=trigger)


class SimulatedXspress3HDF5StreamPlugin(_SimulatedPluginMixin, Xspress3HDF5StreamPlugin):
    pass


def _build_fake_detector(asset_dir):
    detector_class = build_xspress3_class(
        channel_numbers=(1, 2),
        mcaroi_numbers=(),
        image_data_key="image",
        xspress3_parent_classes=(Xspress3Detector, Xspress3Trigger),
        external_file_reference_class=Xspress3StreamExternalFileReference,
        extra_class_members={
            "hdf5plugin": Component(
                SimulatedXspress3HDF5StreamPlugin,
                "HDF1:",
                name="h5p",
                root_path=str(asset_dir),
                path_template=str(asset_dir),
                resource_kwargs={},
            )
        },
    )
    fake_detector_class = make_fake_device(detector_class)
    detector = fake_detector_class(prefix="Xsp3:", name="det")

    _mock_plugin_signals(detector.hdf5plugin)
    _mock_acquisition_completion(detector)
    detector.hdf5plugin.array_size.depth.sim_put(1)
    detector.hdf5plugin.array_size.height.sim_put(1)
    detector.hdf5plugin.array_size.width.sim_put(1)
    detector.hdf5plugin.plugin_type.sim_put("NDFileHDF5")
    return detector


def test_xspress3_channel_stream_shapes(RE, tiled_client):
    client, asset_dir = tiled_client
    detector = _build_fake_detector(asset_dir)
    detector.read_attrs = ["image", "channel01.image", "channel02.image"]
    detector.image.kind = Kind.normal
    parent_key = detector.image.name
    channel_keys = [detector.channel01.image.name, detector.channel02.image.name]
    data_keys = [parent_key, *channel_keys]

    documents = []
    subscription = RE.subscribe(lambda name, document: documents.append((name, document)))
    try:
        writer = TiledWriter(client, validate=False)
        RE(plans.count([detector], 2), writer)
    finally:
        RE.unsubscribe(subscription)

    run = client.v3.values().last()
    stream = run["primary"]

    parent_node = stream[parent_key]
    assert parent_node.structure().shape == (2, 2, 4096)
    np.testing.assert_array_equal(parent_node.read(), EXPECTED_DATA)

    for channel_index, channel_key in enumerate(channel_keys):
        node = stream[channel_key]
        assert node.structure().shape == (2, 4096)
        actual = node.read()
        assert actual.shape == (2, 4096)
        np.testing.assert_array_equal(actual, EXPECTED_DATA[:, channel_index, :])

    stream_resources = {
        document["data_key"]: document
        for name, document in documents
        if name == DocumentNames.stream_resource.value
    }
    assert set(stream_resources) == set(data_keys)
    assert len(stream_resources) == 3
    parent_parameters = stream_resources[parent_key]["parameters"]
    assert "slice" not in parent_parameters
    assert tuple(parent_parameters["chunk_shape"]) == (1, 2, 4096)
    assert parent_parameters["dataset"] == DATASET_PATH
    assert parent_parameters["join_method"] == "stack"
    assert parent_parameters["spec"] == "XSP3"
    for channel_index, channel_key in enumerate(channel_keys):
        parameters = stream_resources[channel_key]["parameters"]
        assert stream_resources[channel_key]["mimetype"] == "application/x-hdf5"
        assert parameters["dataset"] == DATASET_PATH
        assert parameters["slice"] == f":,{channel_index},:"
        assert tuple(parameters["chunk_shape"]) == (1, 4096)
        assert parameters["join_method"] == "stack"
        assert parameters["spec"] == "XSP3"

    stream_datums = [document for name, document in documents if name == DocumentNames.stream_datum.value]
    assert len(stream_datums) == 6
    datum_counts = Counter(document["stream_resource"] for document in stream_datums)
    assert set(datum_counts) == {document["uid"] for document in stream_resources.values()}
    assert set(datum_counts.values()) == {2}

    assert (
        run.validate(
            fix_errors=False,
            try_reading=True,
            raise_on_error=True,
            write_notes=False,
        )
        is True
    )
