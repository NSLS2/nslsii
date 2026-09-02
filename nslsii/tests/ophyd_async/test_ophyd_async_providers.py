from datetime import datetime
from pathlib import PurePosixPath, PureWindowsPath
from unittest.mock import patch
import pytest
import os
from ophyd_async.core import StaticFilenameProvider

from nslsii.ophyd_async import (
    YMDGranularity,
    TimestampFilenameProvider,
    REMetadataFilenameProvider,
    NSLS2PathProvider,
)
from nslsii.ophyd_async.providers import _drive_letter_to_path


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
    ("drive_letter", "expected"),
    [
        ("C", PureWindowsPath("C:\\")),
        ("c", PureWindowsPath("C:\\")),
        ("Z", PureWindowsPath("Z:\\")),
        ("D:", PureWindowsPath("D:\\")),
        ("d:", PureWindowsPath("D:\\")),
        ("E:\\", PureWindowsPath("E:\\")),
    ],
)
def test_drive_letter_to_path(drive_letter: str, expected: PureWindowsPath):
    assert _drive_letter_to_path(drive_letter) == expected


@pytest.mark.parametrize(
    "drive_letter",
    [
        "",
        "1",
        "CD",
        "12",
        ":",
        "C1",
    ],
)
def test_drive_letter_to_path_invalid(drive_letter: str):
    with pytest.raises(ValueError, match="single alphabetic character"):
        _drive_letter_to_path(drive_letter)


@pytest.mark.parametrize(
    (
        "ymd_granularity",
        "ymd_separator",
        "beamline_data_dir",
        "include_scan_id_dir",
        "windows_drive_letter",
    ),
    [
        (YMDGranularity.none, None, None, False, None),
        (YMDGranularity.year, None, None, False, None),
        (YMDGranularity.month, None, "not-tst", False, None),
        (YMDGranularity.day, None, None, False, None),
        (YMDGranularity.day, None, "not-tst", True, None),
        (YMDGranularity.none, None, None, False, "Z"),
        (YMDGranularity.day, None, None, False, "X:"),
        ("day", None, None, True, "y"),
        ("month", "_", "not-tst", False, "W:\\"),
    ],
)
def test_nsls2_path_provider(
    ymd_granularity: YMDGranularity | str,
    ymd_separator: str | None,
    beamline_data_dir: str | None,
    dummy_re_md_dict,
    static_fp: StaticFilenameProvider,
    include_scan_id_dir: bool,
    windows_drive_letter: str | None,
):
    os.environ["BEAMLINE_ACRONYM"] = "tst"

    pp = NSLS2PathProvider(
        dummy_re_md_dict,
        filename_provider=static_fp,
        beamline_data_dirname=beamline_data_dir,
        granularity=ymd_granularity,
        separator=ymd_separator,
        include_scan_id_dir=include_scan_id_dir,
        windows_drive_letter=windows_drive_letter,
    )

    today = datetime.today()

    # granularity may be passed as either the enum or its string name.
    granularity = (
        ymd_granularity if isinstance(ymd_granularity, YMDGranularity) else YMDGranularity[ymd_granularity]
    )
    beamline = beamline_data_dir or "tst"

    # Make sure we have to pass the datakey_name as an argument.
    with pytest.raises(TypeError, match="missing 1 required positional argument: 'datakey_name'"):
        pp()

    info = pp("test")
    dirpath = str(info.directory_path)

    if windows_drive_letter:
        # Windows drive letters are normalized to a single uppercase letter.
        effective_sep = ymd_separator or "\\"
        drive = windows_drive_letter[0].upper()
        assert isinstance(info.directory_path, PureWindowsPath)
        assert dirpath.startswith(f"{drive}:\\proposals\\2024-3\\pass-000000\\assets\\test")
    else:
        effective_sep = os.path.sep
        assert isinstance(info.directory_path, PurePosixPath)
        assert dirpath.startswith(f"/nsls2/data/{beamline}/proposals/2024-3/pass-000000/assets/test")

    # Read URI always uses POSIX paths
    assert info.directory_uri.startswith(
        f"file://localhost/nsls2/data/{beamline}/proposals/2024-3/pass-000000/assets/test"
    )

    if granularity == YMDGranularity.none:
        assert info.create_dir_depth == 0
        assert dirpath.endswith("test")
    elif granularity == YMDGranularity.year:
        assert info.create_dir_depth == -1
        assert dirpath.endswith(str(today.year))
    elif granularity == YMDGranularity.month:
        assert info.create_dir_depth == -2
        assert dirpath.endswith(f"{today.year}{effective_sep}{today.month:02}")
    elif granularity == YMDGranularity.day and not include_scan_id_dir:
        assert info.create_dir_depth == -3
        assert dirpath.endswith(f"{today.year}{effective_sep}{today.month:02}{effective_sep}{today.day:02}")
    elif granularity == YMDGranularity.day and include_scan_id_dir:
        assert info.create_dir_depth == -4
        assert dirpath.endswith(
            f"{today.year}{effective_sep}{today.month:02}{effective_sep}{today.day:02}{effective_sep}scan_000005"
        )


FIXED_NOW = datetime(2025, 3, 15, 10, 30, 45)


@pytest.mark.parametrize(
    ("timestamp_format", "datakey_name", "expected"),
    [
        ("%Y%m%d_%H%M%S", None, "20250315_103045"),
        ("%Y%m%d_%H%M%S", "det1", "det1_20250315_103045"),
        ("%Y-%m-%d", None, "2025-03-15"),
        ("%Y-%m-%d", "cam", "cam_2025-03-15"),
        ("%H%M%S", None, "103045"),
        ("%Y%m%d", "mydet", "mydet_20250315"),
    ],
)
def test_timestamp_filename_provider(timestamp_format, datakey_name, expected):
    fp = TimestampFilenameProvider(timestamp_format=timestamp_format)
    with patch("nslsii.ophyd_async.providers.datetime") as mock_dt:
        mock_dt.now.return_value = FIXED_NOW
        assert fp(datakey_name=datakey_name) == expected


@pytest.mark.parametrize(
    (
        "format_string",
        "timestamp_format",
        "metadata",
        "datakey_name",
        "separator",
        "include_timestamp",
        "prepend_datakey_name",
        "expected",
    ),
    [
        (
            "{scan_id:06}",
            "%Y%m%d_%H%M%S",
            {"scan_id": 5},
            None,
            "_",
            True,
            True,
            "000005_20250315_103045",
        ),
        (
            "{scan_id:06}",
            "%Y%m%d_%H%M%S",
            {"scan_id": 42},
            "det1",
            "_",
            True,
            True,
            "det1_000042_20250315_103045",
        ),
        (
            "{sample}",
            "%Y%m%d",
            {"sample": "nickel"},
            None,
            "_",
            True,
            True,
            "nickel_20250315",
        ),
        (
            "{sample}_{scan_id}",
            "%H%M%S",
            {"sample": "gold", "scan_id": 7},
            "cam",
            "_",
            True,
            True,
            "cam_gold_7_103045",
        ),
        # include_timestamp=False drops the timestamp portion.
        (
            "{scan_id:06}",
            "%Y%m%d_%H%M%S",
            {"scan_id": 5},
            "det1",
            "_",
            False,
            True,
            "det1_000005",
        ),
        # prepend_datakey_name=False drops the datakey name even when supplied.
        (
            "{scan_id:06}",
            "%Y%m%d_%H%M%S",
            {"scan_id": 5},
            "det1",
            "_",
            True,
            False,
            "000005_20250315_103045",
        ),
        # Custom separator is used between all parts.
        (
            "{scan_id:06}",
            "%Y%m%d_%H%M%S",
            {"scan_id": 42},
            "det1",
            "-",
            True,
            True,
            "det1-000042-20250315_103045",
        ),
    ],
)
def test_re_metadata_filename_provider(
    format_string,
    timestamp_format,
    metadata,
    datakey_name,
    separator,
    include_timestamp,
    prepend_datakey_name,
    expected,
):
    fp = REMetadataFilenameProvider(
        format_string=format_string,
        timestamp_format=timestamp_format,
        metadata_dict=metadata,
        separator=separator,
        include_timestamp=include_timestamp,
        prepend_datakey_name=prepend_datakey_name,
    )
    assert fp.format_string == format_string
    with patch("nslsii.ophyd_async.providers.datetime") as mock_dt:
        mock_dt.now.return_value = FIXED_NOW
        assert fp(datakey_name=datakey_name) == expected
