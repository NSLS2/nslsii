
from pathlib import Path
import subprocess

def git_info(git_dir: Path | str) -> tuple[str, str, bool] | tuple[None, None, None]:
    """Helper function to get metadata of local git repository"""
    cwd = Path(git_dir).resolve()

    try:
        commit = subprocess.check_output(
            ["git", "rev-parse", "HEAD"],
            cwd=cwd,
            text=True,
            stderr=subprocess.DEVNULL,
        ).strip()

        branch = subprocess.check_output(
            ["git", "rev-parse", "--abbrev-ref", "HEAD"],
            cwd=cwd,
            text=True,
            stderr=subprocess.DEVNULL,
        ).strip()

        dirty = bool(subprocess.check_output(
            ["git", "status", "--porcelain"],
            cwd=cwd,
            text=True,
            stderr=subprocess.DEVNULL,
        ).strip())

        return commit, branch, dirty

    except (subprocess.CalledProcessError, FileNotFoundError):
        return None, None, None
