import threading
from concurrent.futures import CancelledError
import pytest
from svsim.async_io import WorkerIOEngine, BackpressureError
from svsim.async_store_io import AsyncStoreIO
from svsim.cache import BufferLease, MemoryBudget
from svsim.storage import FileBlockStore, StorageConfig
from svsim.transport import open_storage


def test_worker_pin_backpressure_and_release_after_owner_close(tmp_path):
    budget = MemoryBudget(32768)
    engine = WorkerIOEngine(max_requests=1, budget=budget)
    lease = BufferLease(budget, 4096)
    entered, release = threading.Event(), threading.Event()
    class Handle:
        def write_from(self, offset, view):
            entered.set(); assert release.wait(5)
            assert bytes(view[:4]) == b'test'
            return len(view)
    lease.view[:4] = b'test'
    request = engine.submit_write(Handle(), 0, lease)
    assert entered.wait(5)
    lease.close()
    assert not lease.closed and lease.pins == 1
    with pytest.raises(BackpressureError): engine.submit_flush(Handle())
    release.set()
    assert engine.wait(request) == 4096
    assert lease.closed and budget.used == engine.metadata_bytes
    engine.close()
    assert budget.used == 0


def test_cooperative_cancel_short_read_and_single_capability(monkeypatch):
    engine = WorkerIOEngine(engine='auto', mpi_thread_level=0)
    assert engine.identity == 'cooperative'
    class Handle:
        def read_into(self, offset, view): return 2
    request = engine.submit_read(Handle(), 0, bytearray(4))
    assert not request.done
    with pytest.raises(OSError, match='Incomplete read'): engine.wait(request)
    cancelled = engine.submit_read(Handle(), 0, bytearray(4))
    assert engine.cancel(cancelled)
    with pytest.raises(CancelledError): engine.wait(cancelled)
    engine.close()
    with pytest.raises(RuntimeError, match='SINGLE'): WorkerIOEngine(engine='worker', mpi_thread_level=0)
    monkeypatch.setattr('svsim.uring_backend.sys.platform', 'darwin')
    with pytest.raises(NotImplementedError, match='io_uring'): WorkerIOEngine(engine='io_uring')


def test_async_store_publish_after_completion_checksum_and_durability(tmp_path):
    store = FileBlockStore(StorageConfig(str(tmp_path), 2), 4)
    budget = MemoryBudget(65536)
    engine = WorkerIOEngine(engine='cooperative')
    adapter = AsyncStoreIO(store, engine)
    lease = BufferLease(budget, store.slot_bytes)
    lease.view[:] = b'\0' * store.slot_bytes
    lease.view[:4] = b'test'
    version = store.new_version()
    write = adapter.submit_write(version, lease)
    assert store.ref(version).segment is None
    engine.wait(write)
    assert store.ref(version).segment == 0
    adapter.flush()
    lease.view[:] = b'\0' * store.slot_bytes
    engine.wait(adapter.submit_read(version, lease))
    assert bytes(lease.view[:4]) == b'test'
    ref = store.ref(version)
    with open_storage(store.directory/'state'/'segment-000000.bin') as handle:
        handle.write(ref.offset, b'bad!')
    with pytest.raises(OSError, match='checksum'): engine.wait(adapter.submit_read(version, lease))
    assert lease.pins == 0 and not adapter.pending
    lease.close(); engine.close(); store.close()
    assert budget.used == 0


def test_worker_out_of_order_and_running_cancel_preserve_pin():
    engine = WorkerIOEngine(max_requests=2, workers=2)
    budget = MemoryBudget(65536)
    first, second = BufferLease(budget, 4096), BufferLease(budget, 4096)
    entered, release = threading.Event(), threading.Event()
    class Slow:
        def write_from(self, offset, view):
            entered.set(); assert release.wait(5); return len(view)
    class Fast:
        def write_from(self, offset, view): return len(view)
    r1 = engine.submit_write(Slow(), 0, first)
    assert entered.wait(5)
    r2 = engine.submit_write(Fast(), 0, second)
    assert engine.wait(r2) == 4096
    assert not r1.done and first.pins == 1 and second.pins == 0
    assert not engine.cancel(r1)
    release.set(); engine.wait(r1)
    first.close(); second.close(); engine.close()
    assert budget.used == 0


def test_failed_async_write_keeps_version_unpublished_and_releases_disk_credit(tmp_path):
    class FailedFile:
        backend_identity = 'buffered'
        def write_from(self, offset, view): raise OSError('injected write error')
        def close(self): pass
    class Ledger:
        def __init__(self): self.pending = set()
        def reserve(self, amount): self.pending.add('token'); return 'token'
        def release(self, token): self.pending.remove(token)
    store = FileBlockStore(StorageConfig(str(tmp_path), 2), 4,
                           transport_factory=lambda *a, **kw: FailedFile())
    store.disk_budget = Ledger()
    engine = WorkerIOEngine(engine='cooperative')
    adapter = AsyncStoreIO(store, engine)
    budget = MemoryBudget(32768)
    lease = BufferLease(budget, store.slot_bytes)
    version = store.new_version()
    request = adapter.submit_write(version, lease)
    assert store.disk_budget.pending == {'token'}
    with pytest.raises(OSError, match='injected'): engine.wait(request)
    assert not store.disk_budget.pending and store.ref(version).segment is None
    assert lease.pins == 0 and not adapter.pending
    lease.close(); engine.close(); store.close()
