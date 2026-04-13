from contextlib import contextmanager  # noqa

import redis

import pytest

from bluesky.tests.conftest import RE  # noqa
from ophyd.tests.conftest import hw  # noqa

def pytest_addoption(parser):
    parser.addoption(
        "--xs3-root-path",
        action="store",
        default=None,
        help="path to bluesky 'root' directory where xspress3 writes data files"
    )

    parser.addoption(
        "--xs3-path-template",
        action="store",
        default=None,
        help="path to directory where xspress3 will write files"
    )

    parser.addoption(
        "--xs3-pv-prefix",
        action="store",
        default=None,
        help="PV prefix for xspress3, for example `XF:05IDD-ES{Xsp:1}:`"
    )

    parser.addoption(
        "--xs3-channel-numbers",
        action="store",
        default=None,
        help="comma-separated xspress3 channel numbers, for example `1,2,3`"
    )

    parser.addoption(
        "--xs3-mcaroi-numbers",
        action="store",
        default=None,
        help="comma-separated xspress3 mcaroi numbers, for example `1,2,3`"
    )


@pytest.fixture
def xs3_root_path(request):
    return request.config.getoption("--xs3-root-path")


@pytest.fixture
def xs3_path_template(request):
    return request.config.getoption("--xs3-path-template")


@pytest.fixture
def xs3_pv_prefix(request):
    return request.config.getoption("--xs3-pv-prefix")


@pytest.fixture
def xs3_channel_numbers(request):
    comma_separated_numbers = request.config.getoption("--xs3-channel-numbers")
    if comma_separated_numbers is None:
        return None
    else:
        number_list = [int(n) for n in comma_separated_numbers.split(",")]
        return number_list


@pytest.fixture
def xs3_mcaroi_numbers(request):
    comma_separated_numbers = request.config.getoption("--xs3-mcaroi-numbers")
    if comma_separated_numbers is None:
        return None
    else:
        number_list = [int(n) for n in comma_separated_numbers.split(",")]
        return number_list
