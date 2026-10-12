import os
import sys
import fcntl

import pytest

from svsim import transport
from svsim.transport import StorageFile, open_storage


def test_buffered_positional_round_trip_and_context_cleanup(tmp_path):
    path = tmp_path / "data.bin"
    with open_storage(path, "w+b") as storage:
        assert storage.capabilities.backend == "buffered"
        assert storage.write(4, b"payload") == 7
        storage.flush()
        assert storage.read(4, 7) == b"payload"
    with pytest.raises(ValueError, match="closed"):
        storage.read(0, 1)


def test_buffered_read_handles_eof_and_zero_length(tmp_path):
    path = tmp_path / "data.bin"
    path.write_bytes(b"abc")
    with StorageFile(path, "rb") as storage:
        assert storage.read(1, 8) == b"bc"
        assert storage.read(0, 0) == b""


def test_ranges_and_backend_are_explicit(tmp_path):
    path = tmp_path / "data.bin"
    path.write_bytes(b"x")
    with StorageFile(path, "r+b") as storage:
        with pytest.raises(ValueError):
            storage.read(-1, 1)
        with pytest.raises(ValueError):
            StorageFile(path, backend="mystery")
        with pytest.raises(ValueError, match="unsupported file mode"):
            StorageFile(path, mode="a+b")


def test_linux_branch_requests_odirect_and_does_not_fallback(monkeypatch, tmp_path):
    calls = []
    monkeypatch.setattr(transport.sys, "platform", "linux")
    monkeypatch.setattr(transport.os, "O_DIRECT", 0x4000, raising=False)
    monkeypatch.setattr(transport.os, "open", lambda path, flags, mode=0o666: calls.append((path, flags, mode)) or 41)
    monkeypatch.setattr(transport.os, "close", lambda fd: None)
    with StorageFile(tmp_path / "ignored", "r+b", backend="direct") as storage:
        assert storage.backend_identity == "linux-o-direct"
        assert storage.capabilities.alignment == 4096
    assert calls[0][1] & 0x4000


def test_mocked_linux_direct_io_uses_aligned_buffers(monkeypatch, tmp_path):
    monkeypatch.setattr(transport.sys, "platform", "linux")
    monkeypatch.setattr(transport.os, "O_DIRECT", 0x4000, raising=False)
    monkeypatch.setattr(transport.os, "open", lambda *args: 42)
    monkeypatch.setattr(transport.os, "close", lambda fd: None)
    def fake_pwritev(fd, buffers, offset):
        view = buffers[0]
        assert offset % 4096 == 0 and len(view) % 4096 == 0
        assert transport._buffer_address(view) % 4096 == 0
        return len(view)
    def fake_preadv(fd, buffers, offset):
        view = buffers[0]
        assert offset % 4096 == 0 and len(view) % 4096 == 0
        assert transport._buffer_address(view) % 4096 == 0
        view[:] = b"z" * len(view)
        return len(view)
    monkeypatch.setattr(transport.os, "pwritev", fake_pwritev)
    monkeypatch.setattr(transport.os, "preadv", fake_preadv)
    with StorageFile(tmp_path / "ignored", "r+b", backend="direct") as storage:
        assert storage.write(0, b"a" * 4096) == 4096
        assert storage.read(4096, 4096) == b"z" * 4096
        with pytest.raises(ValueError, match="aligned"):
            storage.read(1, 4096)


def test_mocked_linux_direct_io_loops_on_aligned_partial_transfers(monkeypatch, tmp_path):
    monkeypatch.setattr(transport.sys, "platform", "linux")
    monkeypatch.setattr(transport.os, "O_DIRECT", 0x4000, raising=False)
    monkeypatch.setattr(transport.os, "open", lambda *args: 42)
    monkeypatch.setattr(transport.os, "close", lambda fd: None)
    writes = []
    reads = []
    def fake_pwritev(fd, buffers, offset):
        view = buffers[0]
        assert transport._buffer_address(view) % 4096 == 0
        writes.append((offset, len(view)))
        return min(len(view), 4096)
    def fake_preadv(fd, buffers, offset):
        view = buffers[0]
        assert transport._buffer_address(view) % 4096 == 0
        reads.append((offset, len(view)))
        view[:4096] = b"r" * 4096
        return min(len(view), 4096)
    monkeypatch.setattr(transport.os, "pwritev", fake_pwritev)
    monkeypatch.setattr(transport.os, "preadv", fake_preadv)
    with StorageFile(tmp_path / "ignored", "r+b", backend="direct") as storage:
        assert storage.write(0, b"w" * 8192) == 8192
        assert storage.read(0, 8192) == b"r" * 8192
    assert writes == [(0, 8192), (4096, 4096)]
    assert reads == [(0, 8192), (4096, 4096)]


def test_macos_fcntl_failure_closes_descriptor(monkeypatch, tmp_path):
    closed = []
    monkeypatch.setattr(transport.sys, "platform", "darwin")
    monkeypatch.setattr(transport.os, "open", lambda *args: 91)
    monkeypatch.setattr(transport.os, "close", closed.append)
    def fail_fcntl(fd, command, value):
        assert (fd, command, value) == (91, getattr(fcntl, "F_NOCACHE", 48), 1)
        raise OSError("unsupported filesystem")
    monkeypatch.setattr(fcntl, "fcntl", fail_fcntl)
    with pytest.raises(OSError, match="unsupported filesystem"):
        StorageFile(tmp_path / "ignored", "r+b", backend="direct")
    assert closed == [91]


@pytest.mark.skipif(sys.platform != "darwin", reason="F_NOCACHE integration is macOS-only")
def test_macos_f_nocache_real_file_round_trip(tmp_path):
    path = tmp_path / "data.bin"
    with StorageFile(path, "w+b", backend="direct") as storage:
        assert storage.backend_identity == "macos-f-nocache"
        assert storage.write(3, b"non-aligned macOS payload") == len(b"non-aligned macOS payload")
        storage.flush()
        assert storage.read(3, len(b"non-aligned macOS payload")) == b"non-aligned macOS payload"


@pytest.mark.skipif(not sys.platform.startswith("linux"), reason="O_DIRECT integration is Linux-only")
def test_linux_o_direct_aligned_real_file_round_trip(tmp_path):
    path = tmp_path / "data.bin"
    data = b"q" * 4096
    try:
        with StorageFile(path, "w+b", backend="direct") as storage:
            assert storage.write(0, data) == len(data)
            storage.flush()
            assert storage.read(0, len(data)) == data
    except OSError as exc:
        pytest.skip(f"filesystem does not support O_DIRECT here: {exc}")


@pytest.mark.skipif(sys.platform != "darwin", reason="F_NOCACHE branch is macOS-only")
def test_macos_direct_does_not_require_linux_alignment(tmp_path):
    path = tmp_path / "data.bin"
    with StorageFile(path, "w+b", backend="direct") as storage:
        assert storage.write(1, b"abc") == 3
        assert storage.read(1, 3) == b"abc"
