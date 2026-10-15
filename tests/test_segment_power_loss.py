"""Power-loss ordering of extent publication versus segment fsync (P4-009).

A catalog row that publishes an extent must never reference segment bytes that a power loss could still
drop. The test-only ``LossyFile`` layer wraps the real transport, remembers the previous bytes of every write
and restores them at a simulated power loss unless a ``flush`` (fsync) on that file followed the write. The
SQLite catalog itself is real and durable (synchronous=EXTRA). A negative control disables the pre-publication
segment fsync and must lose published bytes, so the layer is shown to be sensitive.
"""
import mmap
import os
from pathlib import Path

import pytest

from svsim.storage import FileBlockStore, StorageConfig
from svsim.transport import StorageFile

BACKEND = os.environ.get('SVSIM_TEST_BACKEND', 'buffered')
SLOT = 4096


class Disk:
    """Per-test model of the page cache: undo records of writes not yet fsynced."""
    def __init__(self): self.undo = {}

    def factory(self):
        disk = self
        class LossyFile:
            def __init__(self, path, mode='r+b', **kw):
                self._inner = StorageFile(path, mode, **kw)
                self._path = str(path)
            def __getattr__(self, name): return getattr(self._inner, name)
            def write_from(self, offset, buffer):
                n = len(memoryview(buffer).cast('B'))
                fd = os.open(self._path, os.O_RDONLY)
                try: old = os.pread(fd, n, offset)
                finally: os.close(fd)
                count = self._inner.write_from(offset, buffer)
                disk.undo.setdefault(self._path, []).append((offset, n, old))
                return count
            def flush(self):
                self._inner.flush()
                disk.undo.pop(self._path, None)
            def close(self): self._inner.close()
        return lambda path, mode='r+b', **kw: LossyFile(path, mode, **kw)

    def power_loss(self):
        for path, records in self.undo.items():
            fd = os.open(path, os.O_RDWR)
            try:
                for offset, n, old in reversed(records):
                    os.pwrite(fd, old, offset)
                    if len(old) < n: os.ftruncate(fd, offset + len(old))
            finally: os.close(fd)
        self.undo.clear()


def _store(tmp_path, disk):
    return FileBlockStore(StorageConfig(str(tmp_path / 's'), 2, backend=BACKEND), 4, transport_factory=disk.factory())


def _region(byte):
    region = mmap.mmap(-1, SLOT); region.write(bytes([byte]) * SLOT); return region


def _published(store):
    return [(v, s) for v, s in store.db.execute('SELECT id,segment FROM versions WHERE segment IS NOT NULL')]


def _lose_power_and_verify(tmp_path, disk, store, expected):
    """Drop every un-fsynced write, reopen, and require each published extent to read back."""
    disk.power_loss()
    store.close()
    again = FileBlockStore(StorageConfig(str(tmp_path / 's'), 2, backend=BACKEND), 4, create=False)
    try:
        region = mmap.mmap(-1, SLOT)
        lost = []
        for version, byte in expected.items():
            if again.ref(version).segment is None: continue
            try:
                again.read_version(version, region)
                if bytes(region) != bytes([byte]) * SLOT: lost.append(version)
            except OSError: lost.append(version)
        return lost, sum(1 for v in expected if again.ref(v).segment is not None)
    finally: again.close()


def _sync_writes(store, count, first=1):
    expected = {}
    for i in range(count):
        v = store.new_version(); store.write_version(v, _region(first + i)); expected[v] = first + i
    return expected


def _async_writes(store, count, first=1):
    from svsim.async_io import WorkerIOEngine
    from svsim.async_store_io import AsyncStoreIO
    from svsim.cache import BufferLease, MemoryBudget
    engine = WorkerIOEngine(engine='cooperative')
    adapter = AsyncStoreIO(store, engine)
    lease = BufferLease(MemoryBudget(1 << 20), store.slot_bytes)
    expected = {}
    for i in range(count):
        v = store.new_version(); lease.view[:] = bytes([first + i]) * store.slot_bytes
        engine.wait(adapter.submit_write(v, lease)); expected[v] = first + i
    return expected


@pytest.mark.parametrize('writer', [_sync_writes, _async_writes], ids=['sync', 'async'])
@pytest.mark.parametrize('batched', [False, True], ids=['single', 'gate-batch'])
def test_published_extents_survive_power_loss(tmp_path, writer, batched):
    disk = Disk()
    store = _store(tmp_path, disk)
    if batched:
        with store.db.batch(): expected = writer(store, 5)
    else:
        expected = writer(store, 5)
    assert len(_published(store)) == 5
    lost, published = _lose_power_and_verify(tmp_path, disk, store, expected)
    assert published == 5 and lost == []


@pytest.mark.parametrize('writer', [_sync_writes, _async_writes], ids=['sync', 'async'])
def test_unpublished_tail_may_be_lost_but_published_prefix_never(tmp_path, writer):
    """Power loss inside an open batch: published extents of earlier batches stay whole."""
    disk = Disk()
    store = _store(tmp_path, disk)
    with store.db.batch(): first = writer(store, 3)
    with pytest.raises(RuntimeError):
        with store.db.batch():
            writer(store, 2, first=50)
            raise RuntimeError('crash inside the second batch')
    lost, published = _lose_power_and_verify(tmp_path, disk, store, first)
    assert lost == [] and published == 3


def test_tile_writer_seal_survives_power_loss(tmp_path):
    from svsim.tile_extent_writer import TileExtentWriter
    disk = Disk()
    store = _store(tmp_path, disk)
    version = store.new_version()
    writer = TileExtentWriter(store, version, tile_bytes=SLOT)
    writer.write(0, _region(7))
    writer.seal(mmap.mmap(-1, SLOT))
    lost, published = _lose_power_and_verify(tmp_path, disk, store, {version: 7})
    assert published == 1 and lost == []


@pytest.mark.parametrize('batched', [False, True], ids=['single', 'gate-batch'])
def test_negative_control_without_segment_fsync_loses_published_bytes(tmp_path, monkeypatch, batched):
    """Without the pre-publication fsync the layer drops bytes of a published row: the model is sensitive."""
    monkeypatch.setattr(FileBlockStore, '_sync_segments', lambda self, segments=None: self._dirty_segments.clear())
    disk = Disk()
    store = _store(tmp_path, disk)
    if batched:
        with store.db.batch(): expected = _sync_writes(store, 3)
    else:
        expected = _sync_writes(store, 3)
    lost, published = _lose_power_and_verify(tmp_path, disk, store, expected)
    assert published == 3 and len(lost) == 3


def test_segment_fsync_runs_once_per_gate_batch(tmp_path):
    disk = Disk()
    store = _store(tmp_path, disk)
    calls = []
    original = StorageFile.flush
    try:
        StorageFile.flush = lambda self: (calls.append(self.path.name), original(self))[1]
        with store.db.batch(): _sync_writes(store, 6)
        batched = len(calls)
        calls.clear()
        _sync_writes(store, 6)
        single = len(calls)
    finally: StorageFile.flush = original  # the lossy layer forwards flush to StorageFile.flush
    store.close()
    assert batched == 1 and single == 6
