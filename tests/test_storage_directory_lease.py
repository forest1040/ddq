from __future__ import annotations

from pathlib import Path

import pytest

from svsim.dataset_lease import DatasetBusyError, DirectoryLease
from svsim.block_store import ReadOnlyBlockStore
from svsim.storage import FileBlockStore, StorageConfig
from svsim.stored_runtime import StoredSimulator


def _config(path: Path, *, block_bits: int = 2) -> StorageConfig:
    return StorageConfig(str(path), block_bits=block_bits, alignment=64,
                         ram_budget_bytes=1 << 20, metadata_reserve_bytes=65536)


def test_file_store_shared_leases_allow_peers_but_block_exclusive(tmp_path: Path) -> None:
    config = _config(tmp_path)
    first = FileBlockStore(config, 4)
    second = FileBlockStore(config, 4, create=False)
    with pytest.raises(DatasetBusyError):
        DirectoryLease(tmp_path, exclusive=True)
    first.close()
    with pytest.raises(DatasetBusyError):
        DirectoryLease(tmp_path, exclusive=True)
    second.close()
    with DirectoryLease(tmp_path, exclusive=True):
        pass


def test_constructor_failure_releases_directory_lease(tmp_path: Path) -> None:
    FileBlockStore(_config(tmp_path), 4).close()
    with pytest.raises(ValueError, match='dimensions'):
        FileBlockStore(_config(tmp_path, block_bits=1), 4, create=False)
    with DirectoryLease(tmp_path, exclusive=True):
        pass


def test_constructor_cleanup_failure_keeps_lease_until_explicit_release(tmp_path: Path, monkeypatch) -> None:
    import svsim.storage as storage_module

    acquired = []
    real_directory_lease = storage_module.DirectoryLease

    def capture_lease(path, exclusive=False):
        lease = real_directory_lease(path, exclusive=exclusive)
        acquired.append(lease)
        return lease

    class FailingCatalog:
        def __init__(self, *args, **kwargs):
            pass

        def execute(self, *args, **kwargs):
            raise RuntimeError('catalog initialization failed')

        def close(self):
            raise OSError('catalog close failed')

    monkeypatch.setattr(storage_module, 'DirectoryLease', capture_lease)
    monkeypatch.setattr('svsim.catalog_admission.CatalogConnection', FailingCatalog)
    with pytest.raises(RuntimeError, match='catalog initialization failed'):
        FileBlockStore(_config(tmp_path), 4)

    assert len(acquired) == 1
    with pytest.raises(DatasetBusyError):
        DirectoryLease(tmp_path, exclusive=True)
    acquired[0].close()
    with DirectoryLease(tmp_path, exclusive=True):
        pass


def test_close_failure_keeps_shared_lease_until_retry(tmp_path: Path) -> None:
    store = FileBlockStore(_config(tmp_path), 4)

    class FailingHandle:
        fail = True

        def close(self):
            if self.fail:
                raise OSError('flush pending')

    handle = FailingHandle()
    store.files[999] = handle
    with pytest.raises(OSError, match='flush pending'):
        store.close()
    with pytest.raises(DatasetBusyError):
        DirectoryLease(tmp_path, exclusive=True)
    handle.fail = False
    store.close()
    with DirectoryLease(tmp_path, exclusive=True):
        pass


def test_stored_simulator_read_open_keeps_rank_directory_shared_lease(tmp_path: Path) -> None:
    config = _config(tmp_path)
    FileBlockStore(config, 4).close()
    sim = StoredSimulator(4, config, _create=False)
    try:
        with pytest.raises(DatasetBusyError):
            DirectoryLease(tmp_path, exclusive=True)
    finally:
        sim.close()
    with DirectoryLease(tmp_path, exclusive=True):
        pass


def test_standalone_readonly_store_blocks_exclusive_lease(tmp_path: Path) -> None:
    config = _config(tmp_path)
    FileBlockStore(config, 4).close()
    reader = ReadOnlyBlockStore(config, 4)
    with pytest.raises(DatasetBusyError):
        DirectoryLease(tmp_path, exclusive=True)
    reader.close()
    with DirectoryLease(tmp_path, exclusive=True):
        pass


def test_readonly_constructor_close_failure_retains_lease(tmp_path: Path, monkeypatch) -> None:
    import svsim.block_store as block_store_module

    config = _config(tmp_path)
    FileBlockStore(config, 4).close()
    acquired = []
    real_directory_lease = block_store_module.DirectoryLease

    def capture_lease(path, exclusive=False):
        lease = real_directory_lease(path, exclusive=exclusive)
        acquired.append(lease)
        return lease

    class FailingConnection:
        def execute(self, *args, **kwargs):
            raise RuntimeError('catalog read failed')

        def close(self):
            raise OSError('catalog close failed')

    monkeypatch.setattr(block_store_module, 'DirectoryLease', capture_lease)
    monkeypatch.setattr(block_store_module.sqlite3, 'connect', lambda *args, **kwargs: FailingConnection())
    with pytest.raises(RuntimeError, match='catalog read failed'):
        ReadOnlyBlockStore(config, 4)

    with pytest.raises(DatasetBusyError):
        DirectoryLease(tmp_path, exclusive=True)
    acquired[0].close()
    with DirectoryLease(tmp_path, exclusive=True):
        pass
