import numpy as np
import pytest

from svsim.cache import BufferLease, MemoryBudget
from svsim.ir import GateNormalizer
from svsim.storage import FileBlockStore, StorageConfig, StoredBlockView
from svsim.unitary import MatrixAssetRegistry


def make_store(tmp_path, **kwargs):
    config = StorageConfig(str(tmp_path), block_bits=2, alignment=64,
                           ram_budget_bytes=1 << 20, metadata_reserve_bytes=0,
                           **kwargs)
    return config, FileBlockStore(config, 4)


def test_storage_versions_roots_frames_and_reopen(tmp_path):
    config, store = make_store(tmp_path)
    root = store.new_root()
    child = store.new_root(root)
    budget = MemoryBudget(BufferLease.allocation_cost(config.slot_bytes,config.alignment))
    lease = BufferLease(budget, config.slot_bytes, config.alignment)
    expected = np.arange(4, dtype='<c16') + 1j
    lease.array(config.payload_bytes)[:] = expected
    version = store.new_version()
    store.write_version(version, lease.view)
    store.publish(root, [(2, StoredBlockView(version, 3, 5, 0, 0.25+0.5j))])
    assert store.view(root, 2).version_id == version
    assert store.view(child, 2) == store.view(root, 2)
    assert store.view(root, 0).version_id == 'initial-one'
    assert store.view(root, 1).version_id == 'zero'

    assets = MatrixAssetRegistry()
    frame_gate = GateNormalizer(2, assets).gate('H', (0,))
    frame_id = store.intern_frame((frame_gate,))
    assert store.frame(frame_id) == (frame_gate,)
    store.seal(root)
    with pytest.raises(ValueError, match='immutable'):
        store.publish(root, [(3, StoredBlockView('zero'))])
    store.flush()
    lease.close()
    store.close()

    reopened = FileBlockStore(config, 4, create=False)
    out = BufferLease(MemoryBudget(BufferLease.allocation_cost(config.slot_bytes,config.alignment)), config.slot_bytes, config.alignment)
    reopened.read_version(version, out.view)
    np.testing.assert_array_equal(out.array(config.payload_bytes), expected)
    assert reopened.view(child, 2).phase == 0.25+0.5j
    assert reopened.frame(frame_id) == (frame_gate,)
    out.close()
    reopened.close()


@pytest.mark.parametrize('failure', ['checksum', 'short-read'])
def test_read_version_rejects_corrupt_or_short_slots(tmp_path, failure):
    config, store = make_store(tmp_path)
    lease = BufferLease(MemoryBudget(BufferLease.allocation_cost(config.slot_bytes,config.alignment)), config.slot_bytes, config.alignment)
    lease.array(config.payload_bytes)[:] = np.arange(4, dtype='<c16')
    version = store.new_version()
    store.write_version(version, lease.view)
    ref = store.ref(version)
    store.close()
    path = tmp_path / 'state' / f'segment-{ref.segment:06d}.bin'
    if failure == 'checksum':
        with path.open('r+b') as f:
            f.seek(ref.offset)
            f.write(b'corruption')
    else:
        with path.open('r+b') as f: f.truncate(ref.offset + config.slot_bytes - 1)
    reopened = FileBlockStore(config, 4, create=False)
    out = BufferLease(MemoryBudget(BufferLease.allocation_cost(config.slot_bytes,config.alignment)), config.slot_bytes, config.alignment)
    with pytest.raises(OSError, match='checksum|Truncated'):
        reopened.read_version(version, out.view)
    out.close()
    lease.close()
    reopened.close()


def test_write_version_refuses_immutable_extent(tmp_path):
    config, store = make_store(tmp_path)
    lease = BufferLease(MemoryBudget(BufferLease.allocation_cost(config.slot_bytes,config.alignment)), config.slot_bytes, config.alignment)
    version = store.new_version()
    store.write_version(version, lease.view)
    with pytest.raises(ValueError, match='immutable'):
        store.write_version(version, lease.view)
    lease.close()
    store.close()
