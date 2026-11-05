import sys
import types

import pytest

from nslsii.utils.profiles import (
    BLUE,
    BOLD,
    GREEN,
    ITALIC,
    RED,
    RESET,
    print_proposal_info,
    print_version_info,
)


@pytest.fixture
def fake_packages(monkeypatch: pytest.MonkeyPatch) -> None:
    versions = {
        "bluesky": "1.0.0",
        "ophyd_async": "2.0.0",
        "ophyd": "3.0.0",
        "tiled": "4.0.0",
        "databroker": "5.0.0",
        "fake_with_version": "6.0.0",
    }
    for name, version in versions.items():
        module = types.ModuleType(name)
        module.__version__ = version
        monkeypatch.setitem(sys.modules, name, module)
    monkeypatch.setitem(sys.modules, "fake_no_version", types.ModuleType("fake_no_version"))
    # A None entry in sys.modules makes import raise ImportError.
    monkeypatch.setitem(sys.modules, "fake_missing", None)


def test_print_proposal_info(capsys: pytest.CaptureFixture[str]) -> None:
    md = {"proposal": {"title": "My Title", "type": "General User", "pi_name": "Jane Doe"}}
    print_proposal_info(md)
    out = capsys.readouterr().out
    assert f"{BOLD}  - Title: {GREEN}{ITALIC}My Title{RESET}\n" in out
    assert f"{BOLD}  - Type:  {ITALIC}General User{RESET}\n" in out
    assert f"{BOLD}  - PI:    {ITALIC}Jane Doe{RESET}\n\n" in out
    assert "No active proposal" not in out


@pytest.mark.parametrize("md", [{}, {"proposal": {}}])
def test_print_proposal_info_no_proposal(capsys: pytest.CaptureFixture[str], md: dict) -> None:
    print_proposal_info(md)
    assert capsys.readouterr().out == f"{BOLD}{RED}No active proposal!{RESET}\n\n"


def test_print_proposal_info_missing_key_raises() -> None:
    with pytest.raises(KeyError):
        print_proposal_info({"proposal": {"title": "My Title"}})


@pytest.mark.usefixtures("fake_packages")
def test_print_version_info_defaults(capsys: pytest.CaptureFixture[str]) -> None:
    print_version_info()
    out = capsys.readouterr().out
    assert out.startswith(f"\n{BOLD}Version Information{RESET}\n")
    assert f"  {BOLD}bluesky{RESET}: {BLUE}1.0.0{RESET}\n" in out
    assert f"  {BOLD}ophyd_async{RESET}: {BLUE}2.0.0{RESET}\n" in out
    assert f"  {BOLD}tiled{RESET}: {BLUE}4.0.0{RESET}\n" in out
    assert f"{BOLD}ophyd{RESET}" not in out
    assert "databroker" not in out


@pytest.mark.usefixtures("fake_packages")
def test_print_version_info_optional_packages(capsys: pytest.CaptureFixture[str]) -> None:
    print_version_info(include_ophyd=True, include_databroker=True)
    out = capsys.readouterr().out
    assert f"  {BOLD}ophyd{RESET}: {BLUE}3.0.0{RESET}\n" in out
    assert f"  {BOLD}databroker{RESET}: {BLUE}5.0.0{RESET}\n" in out


@pytest.mark.usefixtures("fake_packages")
def test_print_version_info_additional_packages(capsys: pytest.CaptureFixture[str]) -> None:
    print_version_info(
        additional_packages=["fake_with_version", "fake_no_version", "fake_missing"]
    )
    out = capsys.readouterr().out
    assert f"  {BOLD}fake_with_version{RESET}: {BLUE}6.0.0{RESET}\n" in out
    assert f"  {BOLD}fake_no_version{RESET}: {BLUE}unknown{RESET}\n" in out
    assert f"  {BOLD}fake_missing{RESET}: {RED}not found{RESET}\n" in out
