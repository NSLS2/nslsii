import shutil
import subprocess
from pathlib import Path

import pytest
from pytest_mock import MockerFixture

from nslsii.utils import git_info

requires_git = pytest.mark.skipif(shutil.which("git") is None, reason="git not installed")


def _git(repo: Path, *args: str) -> None:
    subprocess.run(["git", *args], cwd=repo, check=True, capture_output=True)


@pytest.fixture
def git_repo(tmp_path: Path) -> Path:
    _git(tmp_path, "init", "-b", "main")
    _git(tmp_path, "config", "user.email", "test@example.com")
    _git(tmp_path, "config", "user.name", "Test")
    _git(tmp_path, "config", "commit.gpgsign", "false")
    (tmp_path / "file.txt").write_text("hello")
    _git(tmp_path, "add", "file.txt")
    _git(tmp_path, "commit", "-m", "initial")
    return tmp_path


def test_git_info_parses_subprocess_output(mocker: MockerFixture) -> None:
    check_output = mocker.patch(
        "nslsii.utils.version_info.subprocess.check_output",
        side_effect=["abc123\n", "main\n", " M file.txt\n"],
    )
    assert git_info(Path("/some/repo")) == ("abc123", "main", True)
    assert check_output.call_count == 3
    for call in check_output.call_args_list:
        assert call.kwargs["cwd"] == Path("/some/repo").resolve()


def test_git_info_clean_when_status_empty(mocker: MockerFixture) -> None:
    mocker.patch(
        "nslsii.utils.version_info.subprocess.check_output",
        side_effect=["abc123\n", "main\n", ""],
    )
    assert git_info(Path("/some/repo")) == ("abc123", "main", False)


@pytest.mark.parametrize(
    "exc",
    [subprocess.CalledProcessError(128, "git"), FileNotFoundError()],
)
def test_git_info_returns_nones_on_error(mocker: MockerFixture, exc: Exception) -> None:
    mocker.patch(
        "nslsii.utils.version_info.subprocess.check_output", side_effect=exc
    )
    assert git_info(Path("/some/repo")) == (None, None, None)


@requires_git
def test_git_info_clean_repo(git_repo: Path) -> None:
    expected_commit = subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=git_repo, text=True
    ).strip()
    assert git_info(git_repo) == (expected_commit, "main", False)


@requires_git
def test_git_info_dirty_repo(git_repo: Path) -> None:
    (git_repo / "file.txt").write_text("changed")
    _, _, dirty = git_info(git_repo)
    assert dirty is True


@requires_git
def test_git_info_untracked_file_is_dirty(git_repo: Path) -> None:
    (git_repo / "new.txt").write_text("new")
    _, _, dirty = git_info(git_repo)
    assert dirty is True


@requires_git
def test_git_info_reports_branch(git_repo: Path) -> None:
    _git(git_repo, "checkout", "-b", "feature")
    _, branch, _ = git_info(git_repo)
    assert branch == "feature"


@requires_git
def test_git_info_accepts_str_path(git_repo: Path) -> None:
    commit, _, _ = git_info(str(git_repo))
    assert commit is not None


@requires_git
def test_git_info_not_a_repo(tmp_path: Path) -> None:
    assert git_info(tmp_path) == (None, None, None)


def test_git_info_nonexistent_dir(tmp_path: Path) -> None:
    assert git_info(tmp_path / "does-not-exist") == (None, None, None)
