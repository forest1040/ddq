import numpy as np
import pytest
from svsim.block_store import MemoryBlockStore, ReadOnlyBlockStore
from svsim.cache import BufferLease, MemoryBudget
from svsim.storage import StorageConfig, StoredBlockView, FileBlockStore


def setup_store(tmp_path, buffers=4):
    config = StorageConfig(str(tmp_path),block_bits=2,alignment=64,
                           ram_budget_bytes=1 << 20,metadata_reserve_bytes=0)
    cost = BufferLease.allocation_cost(config.slot_bytes,config.alignment)
    budget = MemoryBudget(buffers*cost)
    return config,budget,MemoryBlockStore(config,4,budget)


def test_memory_extents_are_bounded_immutable_and_explicitly_durable(tmp_path):
    config,budget,store = setup_store(tmp_path,3)
    lease = BufferLease(budget,config.slot_bytes,config.alignment)
    lease.array()[:] = np.arange(4,dtype='<c16')+1j
    version = store.new_version()
    store.write_version(version,lease.view)
    root = store.new_root()
    store.publish(root,[(3,StoredBlockView(version))])
    assert store.resident_bytes == lease.cost
    assert budget.used == 2*lease.cost
    assert not list((tmp_path/'state').iterdir())
    assert not store.durable
    with pytest.raises(ValueError,match='immutable'): store.write_version(version,lease.view)
    with pytest.raises(OSError,match='volatile'): store.flush()
    lease.array()[:] = 0
    store.read_version(version,lease.view)
    np.testing.assert_array_equal(lease.array(),np.arange(4)+1j)
    store.flush_to_file()
    assert store.durable and budget.used == lease.cost
    store.close()
    readonly = ReadOnlyBlockStore(config,4)
    readonly.read_version(version,lease.view)
    np.testing.assert_array_equal(lease.array(),np.arange(4)+1j)
    assert readonly.view(root,3).version_id == version
    with pytest.raises(PermissionError): readonly.new_root()
    with pytest.raises(Exception): readonly.db.execute('DELETE FROM versions')
    readonly.close()
    lease.close()
    assert budget.used == 0


def test_ram_admission_failure_keeps_prior_immutable_extent(tmp_path):
    config,budget,store = setup_store(tmp_path,2)
    lease = BufferLease(budget,config.slot_bytes,config.alignment)
    lease.array()[:] = 3
    first = store.new_version()
    store.write_version(first,lease.view)
    second = store.new_version()
    with pytest.raises(MemoryError): store.write_version(second,lease.view)
    assert store.ref(second).checksum is None
    lease.array()[:] = 0
    store.read_version(first,lease.view)
    assert np.all(lease.array() == 3)
    store.close()
    lease.close()
    assert budget.used == 0
    with pytest.raises(OSError,match='Volatile'): MemoryBlockStore(config,4,budget,create=False)


def test_readonly_open_does_not_change_catalog_or_allocate_files(tmp_path):
    config,budget,store = setup_store(tmp_path)
    store.close()
    path = tmp_path/'metadata/catalog.sqlite3'
    original = path.read_bytes()
    readonly = ReadOnlyBlockStore(config,4)
    lease = BufferLease(budget,config.slot_bytes,config.alignment)
    readonly.read_version('initial-one',lease.view)
    np.testing.assert_array_equal(lease.array(),[1,0,0,0])
    readonly.read_version('zero',lease.view)
    assert np.all(lease.array() == 0)
    readonly.close()
    assert path.read_bytes() == original
    assert not list((tmp_path/'state').iterdir())
    lease.close()


def test_readonly_missing_directory_is_not_created(tmp_path):
    missing = tmp_path/'absent'
    config = StorageConfig(str(missing),block_bits=2)
    with pytest.raises(Exception): ReadOnlyBlockStore(config,4)
    assert not missing.exists()


def test_spill_failure_retains_ram_and_retry_reuses_written_extent(tmp_path, monkeypatch):
    config,budget,store = setup_store(tmp_path)
    lease = BufferLease(budget,config.slot_bytes,config.alignment)
    lease.array()[:] = 2+3j
    version = store.new_version()
    store.write_version(version,lease.view)
    original_flush = FileBlockStore.flush

    def fail_flush(self): raise OSError('injected durability failure')

    monkeypatch.setattr(FileBlockStore,'flush',fail_flush)
    with pytest.raises(OSError,match='durability'): store.flush_to_file()
    address = store.ref(version)
    assert address.segment is not None and not store.durable
    lease.array()[:] = 0
    store.read_version(version,lease.view)
    np.testing.assert_array_equal(lease.array(),[2+3j]*4)
    monkeypatch.setattr(FileBlockStore,'flush',original_flush)
    store.flush_to_file()
    assert store.ref(version) == address and store.durable
    store.close()
    lease.close()
    assert budget.used == 0


def test_reclaim_respects_explicit_live_snapshot_and_cache_versions(tmp_path):
    config,budget,store = setup_store(tmp_path,5)
    lease = BufferLease(budget,config.slot_bytes,config.alignment)
    versions = []
    for value in (1,2,3):
        lease.array()[:] = value
        version = store.new_version()
        store.write_version(version,lease.view)
        versions.append(version)
    # Caller marks both current and snapshot references; superseded payloads
    # can be freed without writing their amplitudes to disk.
    assert store.reclaim_versions({versions[0],versions[2]}) == lease.cost
    assert budget.used == 3*lease.cost
    assert not list((tmp_path/'state').iterdir())
    with pytest.raises(OSError,match='Missing'): store.ref(versions[1])
    for index in (0,2):
        store.read_version(versions[index],lease.view)
        assert np.all(lease.array() == index+1)
    assert store.reclaim_versions({versions[0],versions[2]}) == 0
    store.flush_to_file()
    # This API must not collect already-spilled immutable file versions.
    assert store.reclaim_versions(set()) == 0
    for index in (0,2): assert store.ref(versions[index]).segment is not None
    store.close()
    lease.close()
    assert budget.used == 0
