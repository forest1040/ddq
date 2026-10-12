"""Synchronous file transport with explicit buffered or cache-avoiding I/O.

Linux ``O_DIRECT`` and macOS ``F_NOCACHE`` have different guarantees. This
module exposes those identities and never silently falls back to buffered I/O.
"""

from __future__ import annotations

import mmap
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Literal


BackendName = Literal["buffered", "linux-o-direct", "macos-f-nocache"]


@dataclass(frozen=True)
class StorageCapabilities:
    backend: BackendName
    direct_io: bool
    cache_avoidance: str
    alignment: int | None
    synchronous: bool = True
    durability_requires_flush: bool = True


class StorageFile:
    """A positional, synchronous file interface: read/write/flush/close."""

    def __init__(self, path: str | os.PathLike[str], mode: str = "r+b", *,
                 backend: str = "buffered", alignment: int = 4096) -> None:
        self.path = Path(path)
        self.mode = mode
        self._closed = False
        if alignment <= 0 or alignment & (alignment - 1):
            raise ValueError("alignment must be a positive power of two")
        if backend == "buffered":
            self.capabilities = StorageCapabilities("buffered", False, "none", None)
            flags = _mode_flags(mode)
            self.fd = os.open(self.path, flags, 0o666)
            self._direct = False
        elif backend == "direct":
            if sys.platform.startswith("linux"):
                self.capabilities = StorageCapabilities("linux-o-direct", True, "O_DIRECT", alignment)
                direct_flag = getattr(os, "O_DIRECT", None)
                if direct_flag is None:
                    raise NotImplementedError("O_DIRECT is unavailable in this Python build")
                self.fd = os.open(self.path, _mode_flags(mode) | direct_flag, 0o666)
                self._direct = True
            elif sys.platform == "darwin":
                self.capabilities = StorageCapabilities("macos-f-nocache", False, "F_NOCACHE", None)
                self.fd = os.open(self.path, _mode_flags(mode), 0o666)
                try:
                    import fcntl
                    f_nocache = getattr(fcntl, "F_NOCACHE", 48)  # Darwin <fcntl.h>
                    fcntl.fcntl(self.fd, f_nocache, 1)
                except BaseException:
                    os.close(self.fd)
                    self._closed = True
                    raise
                self._direct = False
            else:
                raise NotImplementedError(f"direct/no-cache I/O is unsupported on {sys.platform}")
        else:
            raise ValueError("backend must be 'buffered' or 'direct'")
        self.backend_identity = self.capabilities.backend
        self.alignment = alignment

    def read(self, offset: int, size: int) -> bytes:
        self._check_open()
        self._validate_range(offset, size)
        if size == 0:
            return b""
        if self.capabilities.backend == "linux-o-direct":
            self._validate_direct_range(offset, size)
            return self._direct_read(offset, size)
        return _pread_all(self.fd, offset, size)

    def write(self, offset: int, data: bytes | bytearray | memoryview) -> int:
        self._check_open()
        raw = memoryview(data).cast("B")
        self._validate_range(offset, len(raw))
        if len(raw) == 0:
            return 0
        if self.capabilities.backend == "linux-o-direct":
            self._validate_direct_range(offset, len(raw))
            return self._direct_write(offset, raw)
        return _pwrite_all(self.fd, offset, raw)

    def flush(self) -> None:
        self._check_open()
        os.fsync(self.fd)

    def read_into(self, offset: int, buffer) -> int:
        """Read into a caller-owned buffer; direct I/O requires aligned storage."""
        return self._transfer_buffer(offset, buffer, writing=False)

    def write_from(self, offset: int, buffer) -> int:
        """Write a caller-owned buffer without allocating a bounce buffer."""
        return self._transfer_buffer(offset, buffer, writing=True)

    def _transfer_buffer(self, offset, buffer, *, writing):
        self._check_open()
        view = memoryview(buffer).cast('B')
        try:
            self._validate_range(offset, len(view))
            if not writing and view.readonly:
                raise TypeError('Read destination must be writable')
            if self._direct:
                self._validate_direct_range(offset, len(view))
                if view.readonly or _buffer_address(view) % self.alignment:
                    raise ValueError('Direct I/O buffer address must be aligned and writable')
            done = 0
            while done < len(view):
                part = view[done:]
                try:
                    count = (os.pwritev if writing else os.preadv)(self.fd, [part], offset + done)
                finally:
                    part.release()
                if count == 0:
                    if writing: raise OSError('Zero-length write')
                    break
                done += count
                if self._direct and done < len(view) and done % self.alignment:
                    if writing: raise OSError('Partial direct write breaks alignment')
                    break
            return done
        finally:
            view.release()

    def close(self) -> None:
        if not self._closed:
            self._closed = True
            os.close(self.fd)

    def __enter__(self) -> StorageFile:
        self._check_open()
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        self.close()

    def _check_open(self) -> None:
        if self._closed:
            raise ValueError("I/O operation on closed storage file")

    @staticmethod
    def _validate_range(offset: int, size: int) -> None:
        if not isinstance(offset, int) or not isinstance(size, int):
            raise TypeError("offset and size must be integers")
        if offset < 0 or size < 0:
            raise ValueError("offset and size must be non-negative")

    def _validate_direct_range(self, offset: int, size: int) -> None:
        alignment = self.alignment
        if offset % alignment or size % alignment:
            raise ValueError(f"Linux O_DIRECT offset and length must be {alignment}-byte aligned")

    def _allocate_aligned(self, size: int) -> tuple[mmap.mmap, memoryview]:
        region = mmap.mmap(-1, size + self.alignment)
        whole = memoryview(region)
        address = _buffer_address(whole)
        shift = (-address) % self.alignment
        aligned = whole[shift:shift + size]
        if _buffer_address(aligned) % self.alignment:
            aligned.release()
            whole.release()
            region.close()
            raise RuntimeError("failed to allocate an aligned I/O buffer")
        whole.release()
        return region, aligned

    def _direct_read(self, offset: int, size: int) -> bytes:
        region, aligned = self._allocate_aligned(size)
        try:
            done = 0
            while done < size:
                count = os.preadv(self.fd, [aligned[done:]], offset + done)
                if count == 0:
                    break
                done += count
                if done < size and (done % self.alignment or (offset + done) % self.alignment):
                    break  # valid short read at EOF; another O_DIRECT call cannot be aligned
            return bytes(aligned[:done])
        finally:
            aligned.release()
            region.close()

    def _direct_write(self, offset: int, data: memoryview) -> int:
        region, aligned = self._allocate_aligned(len(data))
        try:
            aligned[:] = data
            done = 0
            while done < len(data):
                count = os.pwritev(self.fd, [aligned[done:]], offset + done)
                if count == 0:
                    raise OSError("zero-length write to O_DIRECT file")
                done += count
                if done < len(data) and (done % self.alignment or (offset + done) % self.alignment):
                    raise OSError("O_DIRECT returned a partial write that breaks alignment")
            return done
        finally:
            aligned.release()
            region.close()


def open_storage(path: str | os.PathLike[str], mode: str = "r+b", *,
                 backend: str = "buffered", alignment: int = 4096) -> StorageFile:
    """Open a storage file with the explicitly requested backend."""
    return StorageFile(path, mode, backend=backend, alignment=alignment)


def _mode_flags(mode: str) -> int:
    modes = {
        "rb": os.O_RDONLY,
        "r+b": os.O_RDWR,
        "wb": os.O_WRONLY | os.O_CREAT | os.O_TRUNC,
        "w+b": os.O_RDWR | os.O_CREAT | os.O_TRUNC,
    }
    try:
        return modes[mode] | getattr(os, "O_BINARY", 0)
    except KeyError as exc:
        raise ValueError(f"unsupported file mode: {mode}") from exc


def _pread_all(fd: int, offset: int, size: int) -> bytes:
    chunks: list[bytes] = []
    done = 0
    while done < size:
        chunk = os.pread(fd, size - done, offset + done)
        if not chunk:
            break
        chunks.append(chunk)
        done += len(chunk)
    return b"".join(chunks)


def _pwrite_all(fd: int, offset: int, data: memoryview) -> int:
    done = 0
    while done < len(data):
        count = os.pwrite(fd, data[done:], offset + done)
        if count == 0:
            raise OSError("zero-length write")
        done += count
    return done


def _buffer_address(view: memoryview) -> int:
    import ctypes
    return ctypes.addressof(ctypes.c_char.from_buffer(view))
