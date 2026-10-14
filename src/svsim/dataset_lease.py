"""Advisory leases on dataset directories.

The lock is attached to the directory inode itself. This avoids creating a
lock file in a dataset and lets recovery distinguish a live runtime from an
abandoned one.
"""

from __future__ import annotations

import errno
import fcntl
import os
import stat
from pathlib import Path
from typing import TypeAlias


PathLike: TypeAlias = str | bytes | os.PathLike[str] | os.PathLike[bytes]


class DatasetBusyError(OSError):
    """The requested dataset lease conflicts with a lease already held."""


class DirectoryLease:
    """Hold a shared or exclusive flock on a directory inode."""

    def __init__(self, path: PathLike, exclusive: bool = False) -> None:
        if type(exclusive) is not bool:
            raise TypeError("exclusive must be a bool")
        requested = Path(os.fsdecode(os.fspath(path)))
        # Resolve parent components using filesystem semantics (including
        # symlinks and ``..``), while keeping the final component intact so
        # O_NOFOLLOW can reject a symlink at the directory being leased.
        parent = requested.parent.resolve(strict=True)
        self.path = str(parent / requested.name)
        self.exclusive = exclusive
        self._fd: int | None = None
        self._identity: tuple[int, int] | None = None

        flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
        fd = os.open(self.path, flags)
        try:
            info = os.fstat(fd)
            if not stat.S_ISDIR(info.st_mode):
                raise NotADirectoryError(errno.ENOTDIR, os.strerror(errno.ENOTDIR), self.path)
            try:
                operation = fcntl.LOCK_EX if self.exclusive else fcntl.LOCK_SH
                fcntl.flock(fd, operation | fcntl.LOCK_NB)
            except OSError as exc:
                if exc.errno in (errno.EACCES, errno.EAGAIN, errno.EWOULDBLOCK):
                    raise DatasetBusyError(exc.errno, exc.strerror, self.path) from exc
                raise
            self._identity = (info.st_dev, info.st_ino)
            self._fd = fd
        except BaseException:
            os.close(fd)
            raise

    @property
    def closed(self) -> bool:
        return self._fd is None

    def assert_current(self) -> None:
        """Raise if ``path`` no longer names the inode leased by this object."""
        if self._fd is None or self._identity is None:
            raise ValueError("lease is closed")
        current = os.stat(self.path, follow_symlinks=False)
        if not stat.S_ISDIR(current.st_mode) or (current.st_dev, current.st_ino) != self._identity:
            raise RuntimeError(f"dataset directory identity changed while leased: {self.path}")

    def close(self) -> None:
        fd, self._fd = self._fd, None
        self._identity = None
        if fd is not None:
            os.close(fd)

    def __enter__(self) -> DirectoryLease:
        if self._fd is None:
            raise ValueError("lease is closed")
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        self.close()
