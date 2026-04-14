from datetime import datetime
from enum import Enum
import pytest
import os
from ophyd_async.core import StaticFilenameProvider

from nslsii.ophyd_async import (
    YMDGranularity,
    AcqModeFilenameProvider,
    NSLS2PathProvider,
)


class TomoFrameType(str, Enum):
    proj = "proj"
    flat = "flat"
    dark = "dark"


@pytest.fixture
def static_fp():
    return StaticFilenameProvider("test")


@pytest.fixture
def dummy_re_md_dict():
    md = {
        "data_session": "pass-000000",
        "cycle": "2024-3",
        "scan_id": 5,
    }
    return md


@pytest.mark.parametrize(
    (
        "ymd_granularity",
        "ymd_separator",
        "tla_override",
        "with_suffix",
        "include_scan_id_dir",
    ),
    [
        (YMDGranularity.none, "_", None, True, False),
        (YMDGranularity.year, os.path.sep, None, False, False),
        (YMDGranularity.month, "_", "not-tst", False, False),
        (YMDGranularity.day, os.path.sep, None, False, False),
        (YMDGranularity.day, "_", None, True, False),
        (YMDGranularity.day, os.path.sep, "not-tst", True, False),
    ],
)
def test_nsls2_path_provider(
    ymd_granularity,
    ymd_separator,
    tla_override,
    with_suffix,
    dummy_re_md_dict,
    static_fp,
    include_scan_id_dir,
):
    os.environ["BEAMLINE_ACRONYM"] = "tst"

    pp = NSLS2PathProvider(
        dummy_re_md_dict,
        filename_provider=static_fp,
        beamline_tla=tla_override,
        beamline_tla_suffix="-new" if with_suffix else None,
        granularity=ymd_granularity,
        separator=ymd_separator,
        include_scan_id_dir=include_scan_id_dir,
    )

    today = datetime.today()

    # Make sure we have to pass the datakey_name as an argument.
    with pytest.raises(
        TypeError, match="missing 1 required positional argument: 'datakey_name'"
    ):
        pp()

    info = pp("test")
    dirpath = str(info.directory_path)

    expected_startwith = f"/nsls2/data/{'tst' if not tla_override else tla_override}{'-new' if with_suffix else ''}/proposals/2024-3/pass-000000/assets/test"  # noqa: E501

    assert dirpath.startswith(expected_startwith)

    if ymd_granularity == YMDGranularity.none:
        assert info.create_dir_depth == 0
        assert dirpath.endswith("test")
    elif ymd_granularity == YMDGranularity.year:
        assert info.create_dir_depth == -1
        assert dirpath.endswith(str(today.year))
    elif ymd_granularity == YMDGranularity.month:
        assert info.create_dir_depth == -2
        assert dirpath.endswith(str(f"{today.year}{ymd_separator}{today.month:02}"))
    elif ymd_granularity == YMDGranularity.day and not include_scan_id_dir:
        assert info.create_dir_depth == -3
        assert dirpath.endswith(
            str(
                f"{today.year}{ymd_separator}{today.month:02}{ymd_separator}{today.day:02}"
            )
        )
    elif ymd_granularity == YMDGranularity.day and include_scan_id_dir:
        assert info.create_dir_depth == -4
        assert dirpath.endswith(
            str(
                f"{today.year}{ymd_separator}{today.month:02}{ymd_separator}{today.day:02}{os.path.sep}scan_000005"
            )
        )


@pytest.mark.parametrize(
    ("initial_mode", "include_datakey_name"),
    [
        (TomoFrameType.proj, True),
        (TomoFrameType.dark, False),
    ],
)
def test_acq_mode_filename_provider(initial_mode, include_datakey_name):
    am_fp = AcqModeFilenameProvider(
        TomoFrameType,
        initial_mode=initial_mode,
        include_datakey_name=include_datakey_name,
    )

    assert am_fp._mode_type == TomoFrameType
    assert am_fp._mode == initial_mode

    def _check_filename(expected_mode: TomoFrameType):
        filename = am_fp(datakey_name="test")
        if include_datakey_name:
            assert filename.startswith("test_" + expected_mode.value)
        else:
            assert filename.startswith(expected_mode.value)

    _check_filename(initial_mode)

    am_fp.switch_mode(TomoFrameType.dark)

    _check_filename(TomoFrameType.dark)

    with pytest.raises(
        ValueError, match="20 is not a valid option for <enum 'TomoFrameType'>!"
    ):
        am_fp.switch_mode(20)

    with pytest.raises(
        TypeError, match="Acquisition mode type must be a subclass of Enum!"
    ):
        am_fp = AcqModeFilenameProvider(0)

    with pytest.raises(
        ValueError,
        match="Initial acquisition mode 20 is not a valid option for <enum 'TomoFrameType'>!",
    ):
        am_fp = AcqModeFilenameProvider(TomoFrameType, initial_mode=20)
