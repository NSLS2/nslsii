import datetime

import pytest

from nslsii.areadetector.xspress3 import Xspress3HDF5Plugin


def test__build_data_dir_path():
    root_path = "/abc/def/ghi"
    path_template = "/abc/def/ghi/jkl/mno/%Y/%m/%d"

    the_full_data_dir_path = Xspress3HDF5Plugin._build_data_dir_path(
        the_datetime=datetime.datetime(year=2020, month=1, day=1), root_path=root_path, path_template=path_template
    )

    assert the_full_data_dir_path == "/abc/def/ghi/jkl/mno/2020/01/01"


def test__build_data_dir_path_relative_path_template():
    root_path = "/abc/def/ghi"
    path_template = "jkl/mno/%Y/%m/%d"

    the_full_data_dir_path = Xspress3HDF5Plugin._build_data_dir_path(
        the_datetime=datetime.datetime(year=2020, month=1, day=1), root_path=root_path, path_template=path_template
    )

    assert the_full_data_dir_path == "/abc/def/ghi/jkl/mno/2020/01/01"


@pytest.mark.skip("this test requires an IOC")
def test_default_spec():
    hdf5 = Xspress3HDF5Plugin(name="hdf5", root_path="", path_template="", resource_kwargs={})
    assert hdf5.spec == "XSP3"
