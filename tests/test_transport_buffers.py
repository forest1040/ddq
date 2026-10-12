import os
import sys

import numpy as np
import pytest

from svsim.cache import BufferLease, MemoryBudget
from svsim.transport import open_storage


def test_buffered_read_into_write_from_use_caller_owned_buffers(tmp_path):
    path = tmp_path / 'buffered.bin'
    source = np.arange(16, dtype='<c16')
    with open_storage(path, 'w+b', backend='buffered', alignment=64) as f:
        assert f.write_from(0, memoryview(source)) == source.nbytes
        destination = np.empty_like(source)
        assert f.read_into(0, memoryview(destination)) == destination.nbytes
        np.testing.assert_array_equal(destination, source)
        assert f.capabilities.backend == 'buffered'
        destination[:] = 9
        assert f.write_from(0, destination) == destination.nbytes
        assert f.read(0, source.nbytes) == destination.tobytes()


def test_read_into_rejects_readonly_destination_and_returns_short_count(tmp_path):
    path = tmp_path / 'short.bin'
    path.write_bytes(b'abc')
    with open_storage(path, 'rb') as f:
        with pytest.raises(TypeError, match='writable'):
            f.read_into(0, memoryview(b'abc'))
        target = bytearray(8)
        assert f.read_into(0, target) == 3
        assert target[:3] == b'abc'


def test_direct_backend_uses_aligned_caller_buffers_when_supported(tmp_path):
    if sys.platform not in ('linux', 'darwin'):
        pytest.skip('direct/no-cache backend is unsupported on this platform')
    path = tmp_path / 'direct.bin'
    alignment = 4096
    size = alignment
    budget = MemoryBudget(2*BufferLease.allocation_cost(size,alignment))
    source = BufferLease(budget, size, alignment)
    target = BufferLease(budget, size, alignment)
    source.view[:] = bytes((i % 251 for i in range(size)))
    try:
        with open_storage(path, 'w+b', backend='direct', alignment=alignment) as f:
            assert f.capabilities.backend in {'linux-o-direct', 'macos-f-nocache'}
            assert f.write_from(0, source.view) == size
            assert f.read_into(0, target.view) == size
            assert target.view == source.view
    except (OSError, NotImplementedError) as exc:
        if sys.platform == 'linux':
            pytest.skip(f'O_DIRECT unavailable on this filesystem: {exc}')
        raise
    finally:
        target.close()
        source.close()


def test_linux_direct_rejects_misaligned_ranges_and_buffers(tmp_path):
    if not sys.platform.startswith('linux'):
        pytest.skip('Linux O_DIRECT contract')
    path = tmp_path / 'direct-alignment.bin'
    path.write_bytes(bytes(4096))
    try:
        f = open_storage(path, 'r+b', backend='direct', alignment=4096)
    except (OSError, NotImplementedError) as exc:
        pytest.skip(f'O_DIRECT unavailable: {exc}')
    try:
        with pytest.raises(ValueError, match='aligned'):
            f.read_into(1, bytearray(4096))
        with pytest.raises(ValueError, match='aligned'):
            f.read_into(0, bytearray(2048))
        with pytest.raises(ValueError, match='address must be aligned'):
            f.read_into(0, bytearray(4096))
    finally:
        f.close()
