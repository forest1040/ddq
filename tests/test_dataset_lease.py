from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

from svsim.dataset_lease import DatasetBusyError, DirectoryLease


pytestmark = pytest.mark.skipif(sys.platform == "win32", reason="directory flock is supported on Linux/macOS")


def _child_with_shared_lease(path: Path) -> subprocess.Popen[str]:
    code = (
        "import sys,time; from svsim.dataset_lease import DirectoryLease; "
        "lease=DirectoryLease(sys.argv[1]); print('ready', flush=True); time.sleep(60)"
    )
    process = subprocess.Popen(
        [sys.executable, "-c", code, str(path)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env={**os.environ, "PYTHONPATH": str(Path(__file__).parents[1] / "src")},
    )
    assert process.stdout is not None
    assert process.stdout.readline().strip() == "ready"
    return process


def test_shared_leases_coexist_and_exclusive_conflicts(tmp_path: Path) -> None:
    first = DirectoryLease(tmp_path)
    second = DirectoryLease(tmp_path)
    try:
        with pytest.raises(DatasetBusyError):
            DirectoryLease(tmp_path, exclusive=True)
    finally:
        first.close()
        second.close()


def test_exclusive_lease_blocks_shared_and_close_is_idempotent(tmp_path: Path) -> None:
    lease = DirectoryLease(tmp_path, exclusive=True)
    with pytest.raises(DatasetBusyError):
        DirectoryLease(tmp_path)
    lease.close()
    lease.close()
    with DirectoryLease(tmp_path):
        pass


def test_child_shared_lease_blocks_exclusive_until_normal_exit(tmp_path: Path) -> None:
    child = _child_with_shared_lease(tmp_path)
    try:
        with pytest.raises(DatasetBusyError):
            DirectoryLease(tmp_path, exclusive=True)
    finally:
        child.terminate()
        assert child.wait(timeout=5) != 0
    with DirectoryLease(tmp_path, exclusive=True):
        pass


def test_child_shared_lease_released_after_abnormal_exit(tmp_path: Path) -> None:
    child = _child_with_shared_lease(tmp_path)
    child.kill()
    child.wait(timeout=5)
    with DirectoryLease(tmp_path, exclusive=True):
        pass


def test_symlink_directory_is_rejected(tmp_path: Path) -> None:
    actual = tmp_path / "actual"
    actual.mkdir()
    link = tmp_path / "link"
    link.symlink_to(actual, target_is_directory=True)
    with pytest.raises(OSError):
        DirectoryLease(link)


def test_parent_symlink_and_dotdot_lock_the_filesystem_target(tmp_path: Path) -> None:
    outside = tmp_path / "elsewhere" / "dir"
    outside.mkdir(parents=True)
    actual = tmp_path / "elsewhere" / "dataset"
    actual.mkdir()
    lexical = tmp_path / "dataset"
    lexical.mkdir()
    alias = tmp_path / "link-to-outside"
    alias.symlink_to(outside, target_is_directory=True)
    requested = alias / ".." / "dataset"

    with DirectoryLease(requested):
        with pytest.raises(DatasetBusyError):
            DirectoryLease(actual, exclusive=True)
        with DirectoryLease(lexical, exclusive=True):
            pass


@pytest.mark.parametrize("exclusive", [1, 0, None, "false"])
def test_exclusive_requires_bool(tmp_path: Path, exclusive: object) -> None:
    with pytest.raises(TypeError, match="exclusive must be a bool"):
        DirectoryLease(tmp_path, exclusive=exclusive)  # type: ignore[arg-type]


def test_assert_current_detects_rename_and_symlink_replacement(tmp_path: Path) -> None:
    directory = tmp_path / "dataset"
    directory.mkdir()
    elsewhere = tmp_path / "renamed"
    lease = DirectoryLease(directory)
    try:
        directory.rename(elsewhere)
        with pytest.raises(FileNotFoundError):
            lease.assert_current()
        directory.symlink_to(elsewhere, target_is_directory=True)
        with pytest.raises(RuntimeError):
            lease.assert_current()
    finally:
        lease.close()


def test_open_failure_does_not_leave_fd_held(tmp_path: Path) -> None:
    target = tmp_path / "regular-file"
    target.write_text("x")
    with pytest.raises(OSError):
        DirectoryLease(target)
    with DirectoryLease(tmp_path, exclusive=True):
        pass


def test_context_manager_releases_on_exception(tmp_path: Path) -> None:
    with pytest.raises(RuntimeError, match="boom"):
        with DirectoryLease(tmp_path):
            raise RuntimeError("boom")
    with DirectoryLease(tmp_path, exclusive=True):
        pass
