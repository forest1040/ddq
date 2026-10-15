"""Stable file attribution, including the Linux statx ABI and unavailable capability."""
import ctypes
import os
import stat
import struct
from types import SimpleNamespace

import pytest

import svsim.catalog_admission as module


def test_birth_identity_survives_writes_but_changes_for_replacement(tmp_path):
    path = tmp_path / 'catalog.sqlite3'
    path.write_bytes(b'first')
    birth = module.catalog_birth_identity(path, path.lstat())
    if birth is None:
        pytest.skip('filesystem does not expose a stable birth time')
    owner = module.catalog_file_owner(path)
    path.write_bytes(b'changed')
    assert module.catalog_birth_identity(path, path.lstat()) == birth
    assert module.catalog_file_owner(path) == owner
    path.rename(tmp_path / 'original')
    path.write_bytes(b'changed')
    assert module.catalog_file_owner(path) != owner


@pytest.mark.parametrize('mask,identity_matches,success', [(0x900, True, True), (0x100, True, False),
                                                         (0x900, False, False)])
def test_linux_statx_fixed_abi_and_fail_closed(monkeypatch, mask, identity_matches, success):
    info = SimpleNamespace(st_dev=os.makedev(7, 9), st_ino=123, st_mode=stat.S_IFREG, st_nlink=1)
    raw = bytearray(256)
    struct.pack_into('=I', raw, 0, mask)
    struct.pack_into('=Q', raw, 32, 123 if identity_matches else 999)
    struct.pack_into('=qI', raw, 80, 1234567890, 345678901)
    struct.pack_into('=II', raw, 136, 7, 9)

    class Statx:
        def __call__(self, directory, path, flags, requested, output):
            assert directory == -100 and flags == 0x100
            assert requested & 0x900 == 0x900
            ctypes.memmove(output, bytes(raw), 256)
            return 0

    monkeypatch.setattr(module.sys, 'platform', 'linux')
    monkeypatch.setattr(ctypes, 'CDLL', lambda *a, **k: SimpleNamespace(statx=Statx()))
    value = module.catalog_birth_identity('/catalog', info)
    assert value == ('ns:1234567890345678901' if success else None)


def test_linux_statx_absent_is_unbound(monkeypatch, tmp_path):
    path = tmp_path / 'catalog.sqlite3'
    path.write_bytes(b'')
    monkeypatch.setattr(module, 'catalog_birth_identity', lambda *args: None)
    assert module.catalog_file_owner(path).startswith('catalog-unbound-v3:')


def test_catalog_on_another_real_volume_is_refused_before_open(tmp_path):
    import sys
    import tempfile
    from pathlib import Path
    from svsim.disk_budget import DiskBudget
    if sys.platform != 'linux' or not Path('/dev/shm').is_dir():
        pytest.skip('requires Linux tmpfs on another device')
    with tempfile.TemporaryDirectory(prefix='svsim-catalog-volume-', dir='/dev/shm') as other:
        if Path(other).stat().st_dev == tmp_path.stat().st_dev:
            pytest.skip('tmpfs and test root have the same device')
        path = Path(other) / 'catalog.sqlite3'
        budget = DiskBudget(tmp_path / 'ledger.sqlite3', [tmp_path])
        try:
            with pytest.raises(module.CatalogAdmissionError, match='same volume'):
                module.CatalogConnection(path, budget_getter=lambda: budget)
            assert not path.exists() and budget.reserved_bytes == 0
        finally:
            budget.close()
