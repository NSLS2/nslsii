from __future__ import annotations

import datetime
from pathlib import Path

import h5py
import numpy as np


def now(as_object=False):
    """A helper function to return ISO 8601 formatted datetime string."""
    _now = datetime.datetime.now()
    if as_object:
        return _now
    return _now.isoformat()


def save_image(fname, data, file_format="jpeg", mode="x"):  # pylint: disable=unused-argument
    """The function to export the image data (e.g., to a JPEG file."""
    data.save(fname, file_format=file_format)


def save_hdf5_zebra(
    fname,
    data,
    dtype="float32",
    mode="x",
):
    """The function to export the 1-D data to an HDF5 file.

    Check https://docs.h5py.org/en/stable/high/file.html#opening-creating-files for modes:

        r           Readonly, file must exist (default)
        r+          Read/write, file must exist
        w           Create file, truncate if exists
        w- or x     Create file, fail if exists
        a           Read/write if exists, create otherwise
    """
    with h5py.File(fname, mode, libver="latest") as h5file_desc:
        for pvname, value in data.items():
            dataset = h5file_desc.create_dataset(
                pvname,
                data=value,
                dtype=dtype,
            )
            dataset.flush()


def save_hdf5_nd(
    fname,
    data,
    group_name="/entry",
    group_path="data/data",
    dtype="float32",
    mode="x",
):
    """The function to export the N-D data to an HDF5 file (N>1).

    Check https://docs.h5py.org/en/stable/high/file.html#opening-creating-files for modes:

        r           Readonly, file must exist (default)
        r+          Read/write, file must exist
        w           Create file, truncate if exists
        w- or x     Create file, fail if exists
        a           Read/write if exists, create otherwise
    """
    update_existing = Path(fname).is_file()
    with h5py.File(fname, mode, libver="latest") as h5file_desc:
        frame_shape = data.shape
        if not update_existing:
            group = h5file_desc.create_group(group_name)
            dataset = group.create_dataset(
                group_path,
                data=np.full(fill_value=np.nan, shape=(1, *frame_shape)),
                maxshape=(None, *frame_shape),
                chunks=(1, *frame_shape),
                dtype=dtype,
            )
            frame_num = 0
        else:
            dataset = h5file_desc[f"{group_name}/{group_path}"]
            frame_num = dataset.shape[0]

        # https://docs.h5py.org/en/stable/swmr.html
        h5file_desc.swmr_mode = True

        dataset.resize((frame_num + 1, *frame_shape))
        dataset[frame_num, ...] = data
        dataset.flush()
