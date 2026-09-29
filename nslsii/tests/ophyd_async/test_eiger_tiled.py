"""End-to-end: RunEngine ``count`` -> TiledWriter -> SimpleTiledServer -> read back."""

from pathlib import Path

import bluesky.plans as bp
import numpy as np
import pytest
from bluesky_tiled_plugins import TiledWriter
from ophyd_async.core import init_devices
from ophyd_async.epics.adcore import ADWriterFactory
from tiled.client import from_uri
from tiled.server import SimpleTiledServer

from nslsii.ophyd_async.devices import Eiger2DriverIO, EigerDetector


@pytest.fixture
def tiled_client(tmp_path):
    with SimpleTiledServer(readable_storage=[tmp_path]) as server:
        yield from_uri(server.uri)


@pytest.mark.parametrize(
    "storage, files",
    [
        ("hdf_plugin", ["abc.h5"]),
        ("filewriter", ["abc_42_data_000001.h5", "abc_42_data_000002.h5", "abc_42_data_000003.h5"]),
    ],
)
def test_count_to_tiled(RE, path_provider, fake_ioc, tiled_client, storage, files):
    with init_devices(mock=True):
        if storage == "hdf_plugin":
            det = EigerDetector("X:", ADWriterFactory.hdf(path_provider), driver_cls=Eiger2DriverIO, name="det")
        else:
            det = EigerDetector("X:", driver_cls=Eiger2DriverIO, fw_path_provider=path_provider, name="det")
    fake_ioc(det.driver, hdf=det.hdf if storage == "hdf_plugin" else None, frame_shape=(4, 6))
    RE.subscribe(TiledWriter(tiled_client))
    uids: list[str] = []
    RE.subscribe(lambda _, doc: uids.append(doc["uid"]), "start")

    RE(bp.count([det], num=3))

    directory = Path(path_provider("det").directory_path)
    assert sorted(p.name for p in directory.glob("*.h5")) == files
    (uid,) = uids
    frames = np.asarray(tiled_client[uid]["primary"]["det"].read())
    # 3 events x 1 collection per event x frame; the emulator writes frame k with value k
    assert frames.shape == (3, 1, 4, 6)
    assert frames.dtype == np.uint16
    for k in range(3):
        assert (frames[k] == k).all()
