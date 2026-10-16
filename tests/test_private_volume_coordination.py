"""Owned private stores participate in the same volume claims and explicit recovery."""
from dataclasses import replace
import hashlib
import os
from pathlib import Path
import subprocess
import select
import sys
import textwrap

import numpy as np
import pytest

from svsim.disk_budget import DiskAdmissionError,DiskBudget
from svsim.storage import FileBlockStore,StorageConfig
from svsim.stored_runtime import StoredSimulator
from svsim.uncommitted_recovery import recover_uncommitted_reservations,UncommittedRecoveryRefused,offline_scope
from svsim.volume_coordinator import VolumeCoordinator

ROOT=Path(__file__).resolve().parents[1]
ENV={**os.environ,'PYTHONPATH':str(ROOT/'src'),'OMP_NUM_THREADS':'1','OPENBLAS_NUM_THREADS':'1'}
QUOTA=64*1024**2
BACKEND=os.environ.get('SVSIM_TEST_BACKEND','buffered')


def config(base,name='store'):
    return StorageConfig(str(base/name),2,backend=BACKEND,ram_budget_bytes=512*1024,
        volume_coordinator=str(base/'volume.sqlite3'),disk_quota_bytes=QUOTA)


@pytest.mark.parametrize('extra',[{'volume_coordinator':'relative'}, {'volume_coordinator':7},
    {'disk_quota_bytes':0},{'disk_quota_bytes':True},{'min_free_bytes':-1},{'drain_reserve_bytes':False}])
def test_private_volume_config_rejects_invalid_settings_before_creation(tmp_path,extra):
    with pytest.raises(ValueError):StorageConfig(str(tmp_path/'store'),2,**extra)
    assert not (tmp_path/'store').exists()


def test_independent_private_stores_share_one_volume_quota(tmp_path):
    first=FileBlockStore(config(tmp_path,'first'),4)
    second=FileBlockStore(config(tmp_path,'second'),4)
    try:
        a,b=first.disk_budget,second.disk_budget
        assert a._ledger_path!=b._ledger_path and a._coord.path==b._coord.path
        token=a.reserve(24*1024**2,operation='trace-append',writing=True)
        before=b.reserved_bytes
        with pytest.raises(DiskAdmissionError,match='quota'):
            b.reserve(24*1024**2,operation='trace-append',writing=True)
        assert b.reserved_bytes==before
        assert a._coord.claim(a._ledger_path)['pending']==a.reserved_bytes
        a.release(token)
        token=b.reserve(24*1024**2,operation='trace-append',writing=True)
        b.release(token)
    finally:second.close();first.close()


def test_private_resume_cannot_drop_replace_or_add_a_volume_binding(tmp_path):
    cfg=config(tmp_path)
    store=FileBlockStore(cfg,4);ledger=store.disk_budget._ledger_path;store.close()
    with pytest.raises(DiskAdmissionError,match='bound to a volume coordinator'):
        FileBlockStore(replace(cfg,volume_coordinator=None),4,create=False)
    other=tmp_path/'other.sqlite3'
    coordinator=VolumeCoordinator(other,quota_bytes=QUOTA);coordinator.close()
    with pytest.raises(DiskAdmissionError,match='different volume coordinator'):
        FileBlockStore(replace(cfg,volume_coordinator=str(other)),4,create=False)
    saved=Path(cfg.volume_coordinator).with_suffix('.saved')
    Path(cfg.volume_coordinator).rename(saved)
    with pytest.raises(DiskAdmissionError,match='missing'):
        FileBlockStore(cfg,4,create=False)
    assert not Path(cfg.volume_coordinator).exists()
    saved.rename(cfg.volume_coordinator)
    backup=ledger.with_suffix('.saved');ledger.rename(backup)
    with pytest.raises(DiskAdmissionError,match='ledger is missing'):
        FileBlockStore(cfg,4,create=False)
    assert not ledger.exists()
    backup.rename(ledger)
    plain=replace(config(tmp_path,'plain'),volume_coordinator=None)
    store=FileBlockStore(plain,4);plain_ledger=store.disk_budget._ledger_path;store.close()
    with pytest.raises(DiskAdmissionError,match='new volume binding'):
        FileBlockStore(replace(plain,volume_coordinator=cfg.volume_coordinator),4,create=False)
    assert DiskBudget.read_volume_binding(plain_ledger) is None
    store=FileBlockStore(cfg,4,create=False);store.close()


CHILD='''
import os,sys
from pathlib import Path
import numpy as np
from svsim.storage import StorageConfig
from svsim.stored_runtime import StoredSimulator
base=Path(sys.argv[1]);cfg=StorageConfig(str(base/'store'),2,backend=sys.argv[2],ram_budget_bytes=512*1024,
    volume_coordinator=str(base/'volume.sqlite3'),disk_quota_bytes=64*1024**2)
sim=StoredSimulator(5,cfg)
sim.gate('H',(0,));sim.gate('RY',(4,),angle=.37);sim.materialize()
np.save(base/'expected.npy',sim.state())
snapshot=sim.commit_snapshot();(base/'snapshot.txt').write_text(snapshot.snapshot_id)
sim.store.disk_budget.reserve(24*1024**2,operation='trace-append',writing=True)
os._exit(17)
'''


def test_private_offline_recovery_republishes_bound_claim_and_resumes_oracle(tmp_path):
    child=subprocess.run([sys.executable,'-c',textwrap.dedent(CHILD),str(tmp_path),BACKEND],cwd=ROOT,env=ENV,
                         capture_output=True,text=True,timeout=120)
    assert child.returncode==17,child.stdout+'\n'+child.stderr
    cfg=config(tmp_path);ledger=tmp_path/'store/metadata/disk-ledger.sqlite3'
    coordinator=VolumeCoordinator.open_bound(cfg.volume_coordinator)
    before=coordinator.claim(ledger)['pending']
    assert before>=24*1024**2
    report=recover_uncommitted_reservations(ledger)
    assert report['status']=='dry-run' and report['volume_claim']['published_pending_bytes']==before
    assert coordinator.claim(ledger)['pending']==before
    applied=recover_uncommitted_reservations(ledger,apply=True,confirm_plan=report['plan_sha256'])
    assert applied['volume_claim']['status']=='republished' and applied['volume_claim']['published_pending_bytes']==0
    assert coordinator.claim(ledger)['pending']==0
    with offline_scope(ledger,writer=True) as scope:
        assert scope['private_ledger'] and scope['budget']._coord.path==coordinator.path
    coordinator.close()
    snapshot=(tmp_path/'snapshot.txt').read_text()
    with StoredSimulator.resume(cfg,snapshot) as sim:
        np.testing.assert_allclose(sim.state(),np.load(tmp_path/'expected.npy'),atol=1e-12)
        sim.commit_snapshot()


def test_private_recovery_missing_coordinator_changes_neither_token_nor_artifact(tmp_path):
    child=subprocess.run([sys.executable,'-c',textwrap.dedent(CHILD),str(tmp_path),BACKEND],cwd=ROOT,env=ENV,
                         capture_output=True,text=True,timeout=120)
    assert child.returncode==17,child.stderr
    ledger=tmp_path/'store/metadata/disk-ledger.sqlite3'
    (tmp_path/'volume.sqlite3').rename(tmp_path/'volume.saved')
    before=hashlib.sha256(ledger.read_bytes()).hexdigest()
    with pytest.raises(DiskAdmissionError,match='missing'):
        recover_uncommitted_reservations(ledger)
    assert hashlib.sha256(ledger.read_bytes()).hexdigest()==before
    assert not (tmp_path/'volume.sqlite3').exists()


PARALLEL='''
import sys
from pathlib import Path
from svsim.storage import FileBlockStore,StorageConfig
from svsim.disk_budget import DiskAdmissionError
base,name,backend=Path(sys.argv[1]),sys.argv[2],sys.argv[3]
cfg=StorageConfig(str(base/name),2,backend=backend,volume_coordinator=str(base/'volume.sqlite3'),disk_quota_bytes=64*1024**2)
store=FileBlockStore(cfg,4);print('ready',flush=True);key=None
for command in sys.stdin:
    if command.strip()=='reserve':
        try:
            key=store.disk_budget.reserve(24*1024**2,operation='trace-append',writing=True)
            print('reserved',flush=True)
        except DiskAdmissionError:print('refused',flush=True)
    else:break
if key:store.disk_budget.release(key)
store.close()
'''


def test_real_private_processes_joint_quota_and_offline_settlement_while_other_owner_lives(tmp_path):
    children=[]
    def read(child):
        assert select.select([child.stdout],[],[],30)[0], 'private store worker timed out'
        return child.stdout.readline().strip()
    try:
        for name in ('first','second'):
            child=subprocess.Popen([sys.executable,'-c',textwrap.dedent(PARALLEL),str(tmp_path),name,BACKEND],
                cwd=ROOT,env=ENV,stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
            children.append(child)
            assert read(child)=='ready',child.stderr.read() if child.poll() is not None else ''
        first,second=children
        first.stdin.write('reserve\n');first.stdin.flush()
        assert read(first)=='reserved'
        second.stdin.write('reserve\n');second.stdin.flush()
        assert read(second)=='refused'
        first.kill();first.wait(timeout=30)
        ledger=tmp_path/'first/metadata/disk-ledger.sqlite3'
        plan=recover_uncommitted_reservations(ledger)
        assert plan['status']=='dry-run',plan['refusals']
        applied=recover_uncommitted_reservations(ledger,apply=True,confirm_plan=plan['plan_sha256'])
        assert applied['volume_claim']['published_pending_bytes']==0
        second.stdin.write('reserve\n');second.stdin.flush()
        assert read(second)=='reserved'
        second.stdin.write('close\n');second.stdin.flush();second.wait(timeout=30)
        assert second.returncode==0,second.stderr.read()
    finally:
        for child in children:
            if child.poll() is None:child.kill();child.wait(timeout=30)


@pytest.mark.parametrize('case',['missing','path','budget-policy','coordinator-policy'])
def test_explicit_storage_coordinator_cannot_be_ignored_by_an_injected_budget(tmp_path,case):
    cfg=config(tmp_path);root=Path(cfg.directory);root.mkdir()
    locator=tmp_path/('other.sqlite3' if case=='path' else 'volume.sqlite3')
    coordinator=None if case=='missing' else VolumeCoordinator(locator,quota_bytes=None if case=='coordinator-policy' else QUOTA)
    budget=DiskBudget(tmp_path/'ledger.sqlite3',[root],shared_policy=True,
        quota_bytes=None if case=='budget-policy' else QUOTA,volume_coordinator=coordinator)
    try:
        pending=budget.reserved_bytes
        with pytest.raises(DiskAdmissionError,match='coordinator'):
            FileBlockStore(cfg,4,disk_budget=budget)
        assert not (root/'metadata/catalog.sqlite3').exists() and budget.reserved_bytes==pending
    finally:
        budget.close()
        if coordinator is not None:coordinator.close()


def test_matching_injected_coordinator_is_reused_without_reopening_or_ownership_transfer(tmp_path,monkeypatch):
    cfg=config(tmp_path);root=Path(cfg.directory);root.mkdir()
    coordinator=VolumeCoordinator(cfg.volume_coordinator,quota_bytes=QUOTA)
    budget=DiskBudget(tmp_path/'ledger.sqlite3',[root],shared_policy=True,quota_bytes=QUOTA,volume_coordinator=coordinator)
    def reopened(*args,**kwargs):raise AssertionError('injected coordinator was reopened')
    monkeypatch.setattr(VolumeCoordinator,'__init__',reopened)
    try:
        store=FileBlockStore(cfg,4,disk_budget=budget)
        assert store.disk_budget is budget and store.disk_budget._coord is coordinator
        store.close()
        key=budget.reserve(1,operation='trace-append');budget.release(key)
        assert coordinator.claim(budget._ledger_path)['pending']==0
    finally:budget.close();coordinator.close()


@pytest.mark.skipif(sys.platform!='linux',reason='requires a real second Linux volume')
def test_injected_coordinator_cannot_admit_a_store_on_another_real_volume(tmp_path):
    import tempfile
    if not Path('/dev/shm').is_dir():pytest.skip('tmpfs is unavailable')
    with tempfile.TemporaryDirectory(prefix='svsim-private-coord-',dir='/dev/shm') as directory:
        other=Path(directory)
        if other.stat().st_dev==tmp_path.stat().st_dev:pytest.skip('tmpfs is not another device')
        managed=tmp_path/'managed';managed.mkdir()
        coordinator=VolumeCoordinator(tmp_path/'volume.sqlite3',quota_bytes=QUOTA)
        budget=DiskBudget(tmp_path/'ledger.sqlite3',[managed],quota_bytes=QUOTA,shared_policy=True,volume_coordinator=coordinator)
        try:
            cfg=StorageConfig(str(other/'store'),2,volume_coordinator=str(coordinator.path),disk_quota_bytes=QUOTA)
            with pytest.raises(DiskAdmissionError,match='another volume'):
                FileBlockStore(cfg,4,disk_budget=budget)
            assert not (other/'store/metadata/catalog.sqlite3').exists() and budget.reserved_bytes==0
        finally:budget.close();coordinator.close()
