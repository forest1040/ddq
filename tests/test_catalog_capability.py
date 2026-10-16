"""Build provenance and same-connection dispatch checks for the native VFS."""
from copy import deepcopy
from hashlib import sha256
import json
import sqlite3

import pytest

from svsim.catalog_admission import CatalogAdmissionError, CatalogConnection, get_sqlite_guard
from svsim import catalog_admission
from svsim.disk_budget import DiskBudget
from svsim.storage import FileBlockStore, StorageConfig


def test_capability_records_actual_build_and_connection_identity(tmp_path):
    guard=get_sqlite_guard()
    capability=guard.capability()
    assert capability['binary_sha256']==sha256(guard.library.read_bytes()).hexdigest()
    assert capability['runtime_version']==sqlite3.sqlite_version
    assert capability['runtime_source_id']==guard.loader.execute('SELECT sqlite_source_id()').fetchone()[0]
    assert capability['runtime_compile_options']==sorted(row[0] for row in guard.loader.execute('PRAGMA compile_options'))
    assert capability['header_version'] and capability['header_source_id']
    assert set(capability['headers'])=={'sqlite3.h','sqlite3ext.h'}
    assert all(len(value)==64 for value in capability['headers'].values())
    assert capability['compiler_version'] and '-fPIC' in capability['build_flags']
    assert not capability['header_runtime_build_match_claimed']
    unsigned={key:value for key,value in capability.items() if key!='fingerprint'}
    assert capability['fingerprint']==sha256(json.dumps(unsigned,sort_keys=True,separators=(',',':')).encode()).hexdigest()
    capability['headers'].clear()
    assert len(guard.capability()['headers'])==2


@pytest.mark.parametrize('field',['runtime_version','runtime_source_id','runtime_compile_options'])
def test_dispatch_mismatch_is_rejected(field):
    guard=get_sqlite_guard()

    class Result:
        def fetchone(self):return ('mismatched',)

    class Mismatch:
        def execute(self,sql,parameters=()):
            if sql=='SELECT svsim_disk_guard_identity(?)' and parameters==(field,):
                return Result()
            if field=='runtime_compile_options' and sql=='SELECT svsim_disk_guard_compile_option(?)' and parameters==(0,):
                return Result()
            return guard.loader.execute(sql,parameters)

    with pytest.raises(CatalogAdmissionError,match='dispatch identity mismatch'):
        guard._dispatch_identity(Mismatch())


def test_binary_mismatch_prevents_load_and_releases_startup_credit(tmp_path,monkeypatch):
    budget=DiskBudget(tmp_path/'ledger.sqlite',[tmp_path],usage=lambda:0,free=lambda:10**9)
    guard=get_sqlite_guard()
    try:
        before=set(tmp_path.iterdir())
        assert budget.snapshot()['ledger_vfs_capability'] is None  # injected accounting
        monkeypatch.setattr(guard,'_binary_hash','0'*64)
        with pytest.raises(CatalogAdmissionError,match='binary changed'):
            CatalogConnection(tmp_path/'catalog.sqlite',budget_getter=lambda:budget,require_native=True)
        assert budget.reserved_bytes==0
        assert set(tmp_path.iterdir())==before
    finally:budget.close()


def test_identity_failure_cannot_downgrade_to_optional_soft_catalog(tmp_path,monkeypatch):
    budget=DiskBudget(tmp_path/'ledger.sqlite',[tmp_path],usage=lambda:0,free=lambda:10**9)

    def failed_guard():raise CatalogAdmissionError('dispatch identity mismatch')

    monkeypatch.setattr(catalog_admission,'get_sqlite_guard',failed_guard)
    try:
        before=set(tmp_path.iterdir())
        with pytest.raises(CatalogAdmissionError,match='dispatch identity mismatch'):
            CatalogConnection(tmp_path/'catalog.sqlite',budget_getter=lambda:budget,require_native=False)
        assert not (tmp_path/'catalog.sqlite').exists()
        assert budget.reserved_bytes==0
        assert set(tmp_path.iterdir())==before
    finally:budget.close()


def test_required_guard_unavailable_refuses_before_admission_or_namespace(tmp_path,monkeypatch):
    budget=DiskBudget(tmp_path/'ledger.sqlite',[tmp_path],usage=lambda:0,free=lambda:10**9)
    before=set(tmp_path.iterdir());calls=[]
    def unavailable():raise NotImplementedError('no compatible native guard')
    def getter():calls.append('admission');return budget
    monkeypatch.setattr(catalog_admission,'get_sqlite_guard',unavailable)
    try:
        with pytest.raises(CatalogAdmissionError,match='no compatible native guard'):
            CatalogConnection(tmp_path/'catalog.sqlite',budget_getter=getter,require_native=True)
        assert calls==[] and budget.reserved_bytes==0 and set(tmp_path.iterdir())==before
    finally:budget.close()


def test_creation_and_open_capability_are_persisted_separately(tmp_path,monkeypatch):
    config=StorageConfig(str(tmp_path/'store'),2,alignment=64)
    store=FileBlockStore(config,4)
    try:
        creation=json.loads(store.db.execute("SELECT data FROM settings WHERE key='catalog_vfs_creation_capability'").fetchone()[0])
        assert store.db.admission_status()['capability']==creation
        assert store.disk_budget.snapshot()['ledger_vfs_capability']==creation
    finally:store.close()
    guard=get_sqlite_guard()
    current=deepcopy(guard.capability())
    current['test_open_marker']='compatible reopening evidence'
    monkeypatch.setattr(guard,'capability',lambda:deepcopy(current))
    store=FileBlockStore(config,4,create=False)
    try:
        assert json.loads(store.db.execute("SELECT data FROM settings WHERE key='catalog_vfs_creation_capability'").fetchone()[0])==creation
        assert json.loads(store.db.execute("SELECT data FROM settings WHERE key='catalog_vfs_open_capability'").fetchone()[0])==current
    finally:store.close()
