import hashlib
from svsim.cache import BufferLease, MemoryBudget

import pytest

from svsim.storage import FileBlockStore, StorageConfig
from svsim.tile_extent_writer import TileExtentWriter


def test_tile_writer_requires_exact_nonoverlapping_coverage_and_seals(tmp_path):
    config = StorageConfig(str(tmp_path), 3, ram_budget_bytes=1 << 16,
                           metadata_reserve_bytes=0, alignment=64)
    store = FileBlockStore(config, 3)
    version = store.new_version()
    try:
        with TileExtentWriter(store, version, tile_bytes=64) as writer:
            writer.write(0, bytes([7])*64)
            writer.write(64, bytes([7])*64)
            with pytest.raises(ValueError, match='Overlapping'):
                writer.write(0, bytes([1])*64)
            budget = MemoryBudget(BufferLease.allocation_cost(64, config.alignment))
            scratch = BufferLease(budget, 64, config.alignment)
            assert writer.seal(scratch.view) == version
            scratch.close()
        ref = store.ref(version)
        assert ref.checksum == hashlib.sha256(bytes([7])*128).hexdigest()
    finally:
        store.close()


def test_tile_writer_rejects_holes(tmp_path):
    config = StorageConfig(str(tmp_path), 3, ram_budget_bytes=1 << 16,
                           metadata_reserve_bytes=0, alignment=64)
    store = FileBlockStore(config, 3)
    version = store.new_version()
    try:
        with TileExtentWriter(store, version, tile_bytes=64) as writer:
            with pytest.raises(ValueError, match='coverage'):
                writer.write(64, bytes(64))
            with pytest.raises(ValueError, match='coverage'):
                writer.seal()
    finally:
        store.close()
