"""Sidecar admission, durability barriers and snapshot recovery under faults."""
import json
import os
from pathlib import Path
import stat
import subprocess
import sys

import numpy as np
import pytest

from svsim import StoredSimulator, StorageConfig
from svsim import admitted_files
from svsim.disk_budget import DiskBudget, DiskAdmissionError


@pytest.fixture
def budget(tmp_path):
    result=DiskBudget(tmp_path/'ledger.sqlite',[tmp_path],usage=lambda:0,free=lambda:10**9)
    try:yield result
    finally:result.close()


@pytest.mark.parametrize('fault',['none','short-write','file-fsync','rename','directory-fsync','cleanup'])
def test_sidecar_credit_covers_publication_and_failure_cleanup(tmp_path,monkeypatch,budget,fault):
    path=tmp_path/'asset.bin'
    path.write_bytes(b'old')
    original_open=Path.open
    original_fsync=os.fsync
    original_replace=os.replace
    original_unlink=Path.unlink
    stages=[]

    def assert_live():
        rows=budget.db.execute("SELECT bytes,state FROM reservations WHERE operation='sidecar-write'").fetchall()
        assert rows==[(3,'WRITING')]

    class ShortWriter:
        def __init__(self,stream):self.stream=stream
        def __enter__(self):return self
        def __exit__(self,*args):return self.stream.__exit__(*args)
        def write(self,data):return self.stream.write(data[:-1])

    def checked_open(target,*args,**kwargs):
        stream=original_open(target,*args,**kwargs)
        if target.name.endswith('.tmp'):
            assert_live()
            if fault=='short-write':return ShortWriter(stream)
        return stream

    def checked_fsync(fd):
        assert_live()
        assert budget.reserved_bytes==0
        stage='directory-fsync' if stat.S_ISDIR(os.fstat(fd).st_mode) else 'file-fsync'
        stages.append(stage)
        if fault==stage:raise OSError(stage)
        return original_fsync(fd)

    def checked_replace(src,dst):
        assert_live()
        assert budget.reserved_bytes==0
        stages.append('rename')
        if fault=='rename':raise OSError(fault)
        return original_replace(src,dst)

    def checked_unlink(target,*args,**kwargs):
        if target.name.endswith('.tmp'):
            assert_live()
            stages.append('cleanup')
            if fault=='cleanup':raise OSError(fault)
        return original_unlink(target,*args,**kwargs)

    monkeypatch.setattr(Path,'open',checked_open)
    monkeypatch.setattr(admitted_files.os,'fsync',checked_fsync)
    monkeypatch.setattr(admitted_files.os,'replace',checked_replace)
    monkeypatch.setattr(Path,'unlink',checked_unlink)
    if fault=='none':
        admitted_files.atomic_write_bytes(path,b'new',disk_budget=budget)
        assert stages==['file-fsync','rename','directory-fsync','cleanup']
    else:
        with pytest.raises(OSError):admitted_files.atomic_write_bytes(path,b'new',disk_budget=budget)
    assert path.read_bytes()==(b'new' if fault in ('none','directory-fsync','cleanup') else b'old')
    assert budget.reserved_bytes==0
    assert not list(tmp_path.glob('asset.bin.*.tmp'))


def test_quota_rejection_precedes_sidecar_namespace_changes(tmp_path,budget):
    budget.quota_bytes=2
    target=tmp_path/'missing'/'asset.bin'
    with pytest.raises(DiskAdmissionError):admitted_files.atomic_write_bytes(target,b'new',disk_budget=budget)
    assert not target.parent.exists()
    assert budget.reserved_bytes==0


def test_flushed_sidecar_is_charged_once_for_concurrent_admission(tmp_path,monkeypatch):
    target=tmp_path/'asset.bin'
    target.write_bytes(b'old')
    def actual_usage():
        return sum(path.stat().st_size for path in tmp_path.iterdir()
                   if path.name=='asset.bin' or path.name.endswith('.tmp'))
    budget=DiskBudget(tmp_path/'ledger.sqlite',[tmp_path],quota_bytes=7,usage=actual_usage,free=lambda:10**9)
    original_fsync=os.fsync
    observed=[]

    def concurrent_admission(fd):
        if stat.S_ISREG(os.fstat(fd).st_mode):
            assert actual_usage()==6  # old target plus admitted temporary
            assert budget.reserved_bytes==0
            token=budget.reserve(1)
            observed.append(token)
            budget.release(token)
        return original_fsync(fd)

    monkeypatch.setattr(admitted_files.os,'fsync',concurrent_admission)
    try:
        admitted_files.atomic_write_bytes(target,b'new',disk_budget=budget)
        assert observed and actual_usage()==3 and budget.reserved_bytes==0
    finally:budget.close()


def test_failed_rename_and_cleanup_preserve_live_token_and_temporary(tmp_path,monkeypatch,budget):
    target=tmp_path/'asset.bin'
    target.write_bytes(b'old')
    original_unlink=Path.unlink

    def failed_rename(*args):raise OSError('rename failed')
    def failed_unlink(path,*args,**kwargs):
        if path.name.endswith('.tmp'):raise OSError('cleanup failed')
        return original_unlink(path,*args,**kwargs)

    monkeypatch.setattr(admitted_files.os,'replace',failed_rename)
    monkeypatch.setattr(Path,'unlink',failed_unlink)
    with pytest.raises(OSError,match='cleanup failed'):
        admitted_files.atomic_write_bytes(target,b'new',disk_budget=budget)
    assert target.read_bytes()==b'old'
    temporary=list(tmp_path.glob('asset.bin.*.tmp'))
    assert len(temporary)==1 and temporary[0].read_bytes()==b'new'
    assert budget.db.execute("SELECT bytes,materialized,state FROM reservations WHERE operation='sidecar-write'").fetchall()==[(3,3,'WRITING')]


def test_failed_temporary_inventory_check_retains_token(tmp_path,monkeypatch,budget):
    original=Path.lstat
    def unknown_stat(path,*args,**kwargs):
        if path.name.endswith('.tmp'):raise OSError('inventory unavailable')
        return original(path,*args,**kwargs)
    monkeypatch.setattr(Path,'lstat',unknown_stat)
    with pytest.raises(OSError,match='inventory unavailable'):
        admitted_files.atomic_write_bytes(tmp_path/'asset.bin',b'new',disk_budget=budget)
    assert budget.db.execute("SELECT state FROM reservations WHERE operation='sidecar-write'").fetchall()==[('WRITING',)]


def test_temporary_collision_preserves_foreign_file_and_releases_own_credit(tmp_path,monkeypatch,budget):
    from types import SimpleNamespace
    monkeypatch.setattr(admitted_files,'uuid4',lambda:SimpleNamespace(hex='collision'))
    target=tmp_path/'asset.bin'
    target.write_bytes(b'old')
    temporary=tmp_path/'asset.bin.collision.tmp'
    temporary.write_bytes(b'foreign')
    with pytest.raises(FileExistsError):
        admitted_files.atomic_write_bytes(target,b'new',disk_budget=budget)
    assert target.read_bytes()==b'old'
    assert temporary.read_bytes()==b'foreign'
    assert budget.reserved_bytes==0
    assert budget.db.execute('SELECT count(*) FROM reservations').fetchone()[0]==0


@pytest.mark.parametrize('operation',['matrix-asset','snapshot-export'])
def test_stored_outputs_hold_credit_through_directory_fsync(tmp_path,monkeypatch,operation):
    config=StorageConfig(str(tmp_path/'state'),2,alignment=64)
    with StoredSimulator(4,config) as sim:
        sim.gate('H',(0,))
        sim.materialize()
        seen=[]
        original_fsync=os.fsync
        parent=sim.store.directory/('assets' if operation=='matrix-asset' else 'manifests')
        parent_stat=parent.stat()

        def checked_fsync(fd):
            info=os.fstat(fd)
            if (info.st_dev,info.st_ino)==(parent_stat.st_dev,parent_stat.st_ino):
                rows=sim.store.disk_budget.db.execute('SELECT operation,bytes FROM reservations').fetchall()
                assert any(label==operation and amount>0 for label,amount in rows)
                seen.append(operation)
            return original_fsync(fd)

        monkeypatch.setattr(admitted_files.os,'fsync',checked_fsync)
        if operation=='matrix-asset':
            sim.assets.register(np.array([[0,1],[1,0]],dtype=np.complex128),1)
            sim._persist_assets()
        else:
            manifest=sim.commit_snapshot()
            exported=json.loads((sim.store.directory/'manifests'/f'snapshot-{manifest.snapshot_id}.json').read_text())
            assert exported['snapshot_id']==manifest.snapshot_id
        assert seen and sim.store.disk_budget.reserved_bytes==0


def test_failed_snapshot_sidecar_keeps_previous_checkpoint_resumable(tmp_path,monkeypatch):
    config=StorageConfig(str(tmp_path/'state'),2,alignment=64)
    with StoredSimulator(4,config) as sim:
        sim.gate('H',(0,));sim.materialize()
        original=sim.commit_snapshot()
        expected=sim.state().copy()
        original_fsync=os.fsync
        parent_stat=(sim.store.directory/'manifests').stat()

        def fail_directory(fd):
            info=os.fstat(fd)
            if (info.st_dev,info.st_ino)==(parent_stat.st_dev,parent_stat.st_ino):raise OSError('directory fault')
            return original_fsync(fd)

        with monkeypatch.context() as patch:
            patch.setattr(admitted_files.os,'fsync',fail_directory)
            with pytest.raises(OSError,match='directory fault'):sim.commit_snapshot()
        assert sim.store.disk_budget.reserved_bytes==0
    with StoredSimulator.resume(config,original) as restored:
        np.testing.assert_allclose(restored.state(),expected,atol=1e-14)


@pytest.mark.parametrize('crash',['before-rename','after-rename'])
def test_real_process_crash_keeps_sidecar_credit_and_inventory(tmp_path,crash):
    target=tmp_path/'manifest.bin'
    target.write_bytes(b'old')
    code='''
import os, stat, sys
from pathlib import Path
from svsim.disk_budget import DiskBudget
from svsim.admitted_files import atomic_write_bytes
root=Path(sys.argv[1]); crash=sys.argv[2]
budget=DiskBudget(root/'ledger.sqlite',[root],quota_bytes=32*1024**2)
original_replace=os.replace
original_fsync=os.fsync
def replace(src,dst):
    if crash=='before-rename':os._exit(17)
    return original_replace(src,dst)
def fsync(fd):
    if crash=='after-rename' and stat.S_ISDIR(os.fstat(fd).st_mode):os._exit(17)
    return original_fsync(fd)
os.replace=replace;os.fsync=fsync
atomic_write_bytes(root/'manifest.bin',b'new',disk_budget=budget,operation='crash-sidecar')
'''
    result=subprocess.run([sys.executable,'-c',code,str(tmp_path),crash],capture_output=True,text=True,timeout=60)
    assert result.returncode==17,result.stderr
    recovered=DiskBudget(tmp_path/'ledger.sqlite',[tmp_path],quota_bytes=32*1024**2)
    try:
        rows=recovered.db.execute("SELECT bytes,materialized,state FROM reservations WHERE operation='crash-sidecar'").fetchall()
        assert rows==[(3,3,'WRITING')]
        assert recovered.reserved_bytes==0
        if crash=='before-rename':
            assert target.read_bytes()==b'old'
            partial=list(tmp_path.glob('manifest.bin.*.tmp'))
            assert len(partial)==1 and partial[0].read_bytes()==b'new'
            assert recovered._observed_usage()==6
        else:
            assert target.read_bytes()==b'new'
            assert not list(tmp_path.glob('manifest.bin.*.tmp'))
            assert recovered._observed_usage()==3
        # Neither opening the ledger nor file presence proves an authoritative
        # checkpoint decision. No automatic reconciliation is performed.
        assert recovered.snapshot()['strict_capacity_guarantee'] is False
    finally:recovered.close()
