import numpy as np
import pytest

from svsim.cache import BlockCache, BufferLease, MemoryBudget
from svsim.storage import FileBlockStore, StorageConfig
from svsim.transport import open_storage


def test_memory_budget_and_buffer_lease_alias_contract():
    cost = BufferLease.allocation_cost(128,64)
    budget = MemoryBudget(cost + 64, metadata_bytes=64)
    lease = BufferLease(budget, 128, alignment=64)
    assert lease.cost == cost
    assert budget.used == cost + 64
    assert lease.view.readonly is False
    array = lease.array(128)
    array[:] = [2+3j] * 8
    assert np.frombuffer(lease.view, dtype='<c16')[0] == 2+3j
    with pytest.raises(MemoryError):
        BufferLease(budget, 64, alignment=64)
    del array
    lease.close()
    assert budget.used == 64
    with pytest.raises(ValueError, match='Closed'):
        lease.array(16)


def make_store(tmp_path, *, budget=None, metadata=64, transport_factory=open_storage):
    budget = budget or metadata + 2*BufferLease.allocation_cost(64,64)
    config = StorageConfig(str(tmp_path), block_bits=2, alignment=64,
                           ram_budget_bytes=budget, metadata_reserve_bytes=metadata)
    store = FileBlockStore(config, 4, transport_factory=transport_factory)
    memory = MemoryBudget(config.ram_budget_bytes, config.metadata_reserve_bytes)
    return config, store, memory, BlockCache(store, memory)


def test_cache_pin_lru_eviction_and_dirty_writeback(tmp_path):
    config, store, budget, cache = make_store(tmp_path)
    # Create a persisted nonzero version outside the cache.
    seed = BufferLease(budget, config.slot_bytes, config.alignment)
    array = seed.array(config.payload_bytes)
    array[:] = np.arange(4) + 0.5j
    del array
    version = store.new_version()
    store.write_version(version, seed.view)
    seed.close()

    with cache.pin('zero'):
        pass
    with cache.pin('initial-one'):
        pass
    # The third miss evicts the least recently used unpinned entry.
    with cache.pin(version) as lease:
        np.testing.assert_allclose(lease.array(config.payload_bytes), np.arange(4) + 0.5j)
    assert cache.evictions == 1
    assert set(cache.entries) == {'initial-one', version}

    with cache.outputs(1) as outputs:
        adopted = outputs[0]
        values = adopted.array(config.payload_bytes)
        values[:] = [1+2j, 3+4j, 5+6j, 7+8j]
        del values
        dirty_version = cache.adopt(adopted)
    assert dirty_version in cache.entries
    assert not adopted.closed
    cache.flush()
    assert not cache.entries[dirty_version].dirty
    cache.clear()
    target = BufferLease(budget, config.slot_bytes, config.alignment)
    store.read_version(dirty_version, target.view)
    np.testing.assert_array_equal(target.array(config.payload_bytes), [1+2j,3+4j,5+6j,7+8j])
    target.close()
    store.close()
    assert budget.used == config.metadata_reserve_bytes


def test_pinned_entries_cannot_be_evicted_and_failed_read_releases_reservation(tmp_path):
    _, store, budget, cache = make_store(tmp_path)
    with cache.pin('zero'):
        with cache.pin('initial-one'):
            with pytest.raises(MemoryError, match='unpinned'):
                with cache.pin('unwritten-version'):
                    pass
    assert budget.used == store.config.metadata_reserve_bytes + 2*cache.buffer_cost
    assert 'unwritten-version' not in cache.entries
    cache.clear()

    before = budget.used
    original_read = store.read_version
    store.read_version = lambda version, buffer: (_ for _ in ()).throw(OSError('injected read failure'))
    with pytest.raises(OSError, match='injected'):
        with cache.pin('broken-version'):
            pass
    assert budget.used == before
    assert 'broken-version' not in cache.entries
    store.read_version = original_read
    store.close()


def test_failed_dirty_write_keeps_cache_buffer_and_unwritten_version(tmp_path):
    class ShortWriteTransport:
        def __init__(self, path, mode, **kwargs):
            self.delegate = open_storage(path, mode, backend='buffered', alignment=kwargs['alignment'])
        def write_from(self, offset, buffer): return len(buffer)-1
        def read_into(self, offset, buffer): return self.delegate.read_into(offset, buffer)
        def flush(self): self.delegate.flush()
        def close(self): self.delegate.close()

    config, store, budget, cache = make_store(tmp_path, transport_factory=ShortWriteTransport)
    with cache.outputs(1) as outputs:
        lease = outputs[0]
        values = lease.array(config.payload_bytes)
        values[:] = np.arange(4) + 9j
        expected = values.copy()
        del values
        version = cache.adopt(lease)
    with pytest.raises(OSError, match='Incomplete Block write'):
        cache.flush()
    assert cache.entries[version].dirty
    assert store.ref(version).segment is None
    result = cache.entries[version].buffer.array(config.payload_bytes)
    np.testing.assert_array_equal(result, expected)
    del result, expected
    cache.discard_unwritten()
    store.close()
    assert budget.used == config.metadata_reserve_bytes
