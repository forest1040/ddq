"""Bootstrap pool rejection cannot create an unadmitted namespace."""
import os
from pathlib import Path
import signal
import subprocess
import sys

import pytest

from svsim.disk_budget import DiskBudget,DiskAdmissionError
from svsim.storage import FileBlockStore,StorageConfig
from svsim.volume_coordinator import VolumeCoordinator
from svsim.uncommitted_recovery import recover_uncommitted_reservations


@pytest.mark.parametrize('reason',['quota','min-free'])
def test_ledger_pool_rejects_before_creating_parent(tmp_path,reason):
    target=tmp_path/'missing/nested/ledger.sqlite3'
    options={'quota_bytes':8*1024**2-1} if reason=='quota' else {'min_free_bytes':1<<100}
    with pytest.raises(DiskAdmissionError,match='bootstrap'):
        DiskBudget(target,[tmp_path],**options)
    assert not (tmp_path/'missing').exists()


@pytest.mark.parametrize('reason',['quota','min-free','filesystem'])
def test_coordinator_pool_or_capability_rejects_before_creating_parent(tmp_path,reason):
    target=tmp_path/'missing/nested/volume.sqlite3'
    options={'quota_bytes':8191,'bootstrap_bytes':8192} if reason=='quota' else (
        {'min_free_bytes':1<<100} if reason=='min-free' else {'filesystem_probe':lambda p:'unaccepted-fs'})
    with pytest.raises(DiskAdmissionError):VolumeCoordinator(target,**options)
    assert not (tmp_path/'missing').exists()


def test_distributed_quota_refusal_precedes_root_creation(tmp_path):
    from mpi4py import MPI
    from svsim import DistributedSimulator,DistributedConfig
    target=tmp_path/'missing/state'
    config=DistributedConfig(str(target),2,disk_quota_bytes=8*1024**2-1,storage_scope='node-local')
    with pytest.raises(Exception,match='bootstrap'):DistributedSimulator(4,config,comm=MPI.COMM_SELF)
    assert not (tmp_path/'missing').exists()


def test_supplied_budget_refuses_store_namespace_before_root_or_subdirectories(tmp_path):
    budget=DiskBudget(tmp_path/'ledger.sqlite',[tmp_path],usage=lambda:0,free=lambda:10**9)
    try:
        budget.quota_bytes=0
        target=tmp_path/'missing/state'
        with pytest.raises(DiskAdmissionError):FileBlockStore(StorageConfig(str(target),2),4,disk_budget=budget)
        assert not (tmp_path/'missing').exists() and budget.reserved_bytes==0
    finally:budget.close()


def test_private_store_free_pool_refusal_precedes_root_creation(tmp_path,monkeypatch):
    import svsim.bootstrap_preflight as bootstrap
    monkeypatch.setattr(bootstrap.os,'statvfs',lambda p:type('Free',(),{'f_bavail':1,'f_frsize':1})())
    with pytest.raises(DiskAdmissionError,match='minimum free'):
        FileBlockStore(StorageConfig(str(tmp_path/'missing/state'),2),4)
    assert not (tmp_path/'missing').exists()


@pytest.mark.parametrize('stage',['reserved','root','subdirectory'])
def test_supplied_ledger_store_bootstrap_kill_keeps_inventory_and_recoverable_namespace(tmp_path,stage):
    script=r'''
import os,signal,sys
from pathlib import Path
from svsim.disk_budget import DiskBudget
from svsim.storage import FileBlockStore,StorageConfig
root=Path(sys.argv[1]);stage=sys.argv[2]
budget=DiskBudget(root/'ledger.sqlite3',[root],shared_policy=True)
reserve=budget.reserve

def reserve_hook(amount,**kw):
 key=reserve(amount,**kw)
 if kw.get('operation')=='store-open' and stage=='reserved':os.kill(os.getpid(),signal.SIGKILL)
 return key
budget.reserve=reserve_hook
mkdir=Path.mkdir

def mkdir_hook(path,*args,**kw):
 result=mkdir(path,*args,**kw)
 if (stage=='root' and path==root/'store') or (stage=='subdirectory' and path==root/'store/state'):
  os.kill(os.getpid(),signal.SIGKILL)
 return result
Path.mkdir=mkdir_hook
FileBlockStore(StorageConfig(str(root/'store'),2),4,disk_budget=budget)
raise AssertionError('kill hook missed')
'''
    result=subprocess.run([sys.executable,'-c',script,str(tmp_path),stage],
        env={**os.environ,'PYTHONPATH':'src'},capture_output=True,timeout=60)
    assert result.returncode==-signal.SIGKILL,result.stderr.decode()
    ledger=(tmp_path/'ledger.sqlite3').resolve()
    dry=recover_uncommitted_reservations(ledger)
    assert not dry['refusals']
    token=next(row for row in dry['tokens'] if row['operation']=='store-open')
    assert token['class']=='namespace' and token['release_pending_bytes']==1
    before=sorted(str(p.relative_to(tmp_path)) for p in tmp_path.rglob('*'))
    applied=recover_uncommitted_reservations(ledger,apply=True,confirm_plan=dry['plan_sha256'])
    assert applied['status']=='applied'
    assert sorted(str(p.relative_to(tmp_path)) for p in tmp_path.rglob('*'))==before
    assert recover_uncommitted_reservations(ledger)['status']=='nothing-to-do'
