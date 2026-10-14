"""Opt-in tests that exercise real Linux io_uring CQEs and file operations.

The deterministic fake-CQE protocol tests live in test_uring_backend.py. These
tests never run native initialization unless DDQ_NATIVE_URING=1 is explicit.
"""
from concurrent.futures import CancelledError
import errno
import os
import sys
import time

import pytest

from svsim.cache import BufferLease, MemoryBudget
from svsim.async_io import BackpressureError
from svsim.transport import open_storage
from svsim.uring_backend import NativeUringEngine


NATIVE_OPT_IN = os.environ.get('DDQ_NATIVE_URING') == '1'
pytestmark = pytest.mark.skipif(
    not NATIVE_OPT_IN,
    reason='native io_uring tests require explicit DDQ_NATIVE_URING=1',
)


def _engine(*, max_requests=4, budget=None):
    if not sys.platform.startswith('linux'):
        pytest.fail('DDQ_NATIVE_URING=1 requires Linux; native capability unavailable')
    try:
        return NativeUringEngine(max_requests=max_requests, budget=budget)
    except NotImplementedError as exc:
        pytest.fail(f'explicit native io_uring request but capability unavailable: {exc}')


def _wait(engine, request, timeout=5.0):
    deadline = time.monotonic() + timeout
    while not request.done and time.monotonic() < deadline:
        engine.poll()
        if not request.done:
            time.sleep(0.0001)
    assert request.done, 'native CQE did not arrive before bounded test deadline'
    if request.error is not None:
        raise request.error
    return request.result


def test_native_buffered_write_read_fsync_cqes_and_request_accounting(tmp_path):
    budget = MemoryBudget(2 * 1024 * 1024)
    engine = _engine(budget=budget)
    path = tmp_path / 'buffered.bin'
    size = 16 * 1024
    source = BufferLease(budget, size)
    target = BufferLease(budget, size)
    source.view[:] = bytes((index % 251 for index in range(size)))
    try:
        with open_storage(path, 'w+b', backend='buffered') as handle:
            request = engine.submit_write(handle, 0, source)
            assert source.pins == 1
            assert _wait(engine, request) == size
            assert request.mark_validated().validated
            assert source.pins == 0
            assert request.storage_metadata['resolved_backend'] == 'buffered'
            assert request.storage_metadata['transfer_bytes'] == size
            assert request.wall_seconds > 0
            assert request.queue_seconds is None or request.queue_seconds >= 0

            flush = engine.submit_flush(handle)
            assert _wait(engine, flush) is None
            assert flush.mark_validated().validated

            read = engine.submit_read(handle, 0, target)
            assert _wait(engine, read) == size
            assert read.mark_validated().validated
            assert target.view == source.view
            assert read.storage_metadata['offset'] == 0
    finally:
        source.close()
        target.close()
        engine.close()
    assert budget.used == 0


def test_native_o_direct_read_write_fsync_uses_aligned_bufferlease(tmp_path):
    alignment = 4096
    size = 2 * alignment
    budget = MemoryBudget(2 * 1024 * 1024)
    engine = _engine(budget=budget)
    path = tmp_path / 'direct.bin'
    path.write_bytes(bytes(size))
    source = BufferLease(budget, size, alignment)
    target = BufferLease(budget, size, alignment)
    source.view[:] = bytes((index % 239 for index in range(size)))
    try:
        try:
            handle = open_storage(path, 'r+b', backend='direct', alignment=alignment)
        except (OSError, NotImplementedError) as exc:
            pytest.fail(f'explicit native O_DIRECT test unavailable on this filesystem: {exc}')
        with handle:
            assert handle.capabilities.backend == 'linux-o-direct'
            write = engine.submit_write(handle, 0, source)
            assert _wait(engine, write) == size
            assert write.storage_metadata['resolved_backend'] == 'linux-o-direct'
            flush = engine.submit_flush(handle)
            assert _wait(engine, flush) is None
            read = engine.submit_read(handle, 0, target)
            assert _wait(engine, read) == size
            assert target.view == source.view
    finally:
        source.close()
        target.close()
        engine.close()
    assert budget.used == 0


def test_native_short_eof_is_terminal_error_and_retains_pin_through_callback(tmp_path):
    path = tmp_path / 'short-eof.bin'
    path.write_bytes(b'short read')
    budget = MemoryBudget(1024 * 1024)
    engine = _engine(budget=budget)
    target = BufferLease(budget, 4096)
    callback_pins = []
    try:
        with open_storage(path, 'rb', backend='buffered') as handle:
            request = engine.submit_read(handle, 0, target)
            request.add_done_callback(lambda completed: callback_pins.append(target.pins))
            with pytest.raises(OSError, match='Incomplete native read'):
                _wait(engine, request)
            assert request.result == len(b'short read')
            assert callback_pins == [1]
            assert target.pins == 0
            assert not engine.pending
    finally:
        target.close()
        engine.close()
    assert budget.used == 0


def test_native_closed_fd_produces_real_negative_cqe_and_releases_pin(tmp_path):
    path = tmp_path / 'closed-fd.bin'
    path.write_bytes(bytes(4096))
    budget = MemoryBudget(1024 * 1024)
    engine = _engine(budget=budget)
    lease = BufferLease(budget, 4096)
    try:
        handle = open_storage(path, 'r+b', backend='buffered')
        request = engine.submit_read(handle, 0, lease)
        handle.close()  # kick occurs in poll, so the kernel sees a closed fd
        with pytest.raises(OSError) as error:
            _wait(engine, request)
        assert error.value.errno == errno.EBADF
        assert request.done and lease.pins == 0
        assert request.request_id not in engine.pending
    finally:
        lease.close()
        engine.close()
    assert budget.used == 0


def test_native_cancel_race_accepts_either_result_but_pin_lives_to_original_cqe(tmp_path):
    path = tmp_path / 'cancel-race.bin'
    path.write_bytes(bytes(1024 * 1024))
    budget = MemoryBudget(4 * 1024 * 1024)
    engine = _engine(max_requests=1, budget=budget)
    lease = BufferLease(budget, 1024 * 1024)
    callback_pins = []
    try:
        with open_storage(path, 'rb', backend='buffered') as handle:
            request = engine.submit_read(handle, 0, lease)
            request.add_done_callback(lambda completed: callback_pins.append(lease.pins))
            assert engine.cancel(request)
            deadline = time.monotonic() + 5.0
            while (engine.pending or engine.cancels) and time.monotonic() < deadline:
                engine.poll()
                if not request.done:
                    assert lease.pins == 1
                time.sleep(0.0001)
            assert request.done and not engine.pending and not engine.cancels
            assert callback_pins == [1]
            assert lease.pins == 0
            assert request.error is None or isinstance(request.error, CancelledError)
            if request.error is None:
                assert request.result == 1024 * 1024
    finally:
        lease.close()
        engine.close()
    assert budget.used == 0


def test_native_backpressure_and_budget_are_bounded_and_drain_cleanly(tmp_path):
    budget = MemoryBudget(512 * 1024)
    engine = _engine(max_requests=1, budget=budget)
    path = tmp_path / 'bounded.bin'
    path.write_bytes(bytes(4096))
    try:
        with open_storage(path, 'r+b', backend='buffered') as handle:
            first = engine.submit_flush(handle)
            with pytest.raises(BackpressureError):
                engine.submit_flush(handle)
            assert _wait(engine, first) is None
            assert engine.pending == {}
            assert engine.peak_requests == 1
    finally:
        engine.close()
    assert budget.used == 0
