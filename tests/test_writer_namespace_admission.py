"""First namespace mutation and journal repair remain behind admission."""
from contextlib import contextmanager
import os
from pathlib import Path
import signal
import subprocess
import sys

import numpy as np
import pytest

from svsim import StoredSimulator,StorageConfig
from svsim.disk_budget import DiskBudget,DiskAdmissionError
from svsim.journal import Journal,canonical,content_hash
from svsim.telemetry import StreamingExecutionTrace
from svsim.uncommitted_recovery import recover_uncommitted_reservations


@contextmanager
def budget(path):
    disk=DiskBudget(path/'ledger.sqlite',[path],usage=lambda:0,free=lambda:10**9)
    try:yield disk
    finally:disk.close()


@pytest.mark.parametrize('writer',['journal','trace','measurement'])
@pytest.mark.parametrize('aliased',[False,True])
def test_supplied_budget_append_writers_reject_uninventoried_destination(tmp_path,writer,aliased):
    from types import SimpleNamespace
    from svsim.runtime_control import RuntimeControl
    managed=tmp_path/'managed';managed.mkdir()
    outside=tmp_path/'outside';outside.mkdir()
    destination=outside/'new'
    if aliased:
        destination=managed/'alias';destination.symlink_to(outside,target_is_directory=True)
    disk=DiskBudget(managed/'ledger.sqlite',[managed],quota_bytes=32*1024**2)
    try:
        with pytest.raises(DiskAdmissionError):
            if writer=='journal':Journal(destination,disk_budget_getter=lambda:disk)
            elif writer=='trace':
                StreamingExecutionTrace(destination/'trace.jsonl',profile_id='p',plan_id='p',disk_budget_getter=lambda:disk)
            else:
                control=RuntimeControl.__new__(RuntimeControl)
                control.sim=SimpleNamespace(directory=destination,rank=0,run_id='r',disk_budget=disk,store=None)
                control._spill_raw([b'raw\n'])
        assert list(outside.iterdir())==[] and disk.reserved_bytes==0
    finally:disk.close()


@pytest.mark.parametrize('alias',['symlink','hardlink'])
def test_journal_first_append_rejects_foreign_leaf_before_tail_repair(tmp_path,alias):
    managed=tmp_path/'managed';managed.mkdir()
    sentinel=tmp_path/'sentinel';sentinel.write_bytes(b'{"foreign-partial":')
    disk=DiskBudget(managed/'ledger.sqlite',[managed],quota_bytes=32*1024**2)
    journal=Journal(managed/'journal',disk_budget_getter=lambda:disk)
    target=journal.directory/'gates.jsonl'
    if alias=='symlink':target.symlink_to(sentinel)
    else:os.link(sentinel,target)
    try:
        with pytest.raises(DiskAdmissionError):journal.append('gates',{'new':True})
        assert sentinel.read_bytes()==b'{"foreign-partial":' and disk.reserved_bytes==0
    finally:journal.close();disk.close()


def test_trace_budget_attached_after_open_still_requires_inventory(tmp_path):
    managed=tmp_path/'managed';managed.mkdir()
    disk=DiskBudget(managed/'ledger.sqlite',[managed],quota_bytes=32*1024**2)
    holder=[None]
    trace=StreamingExecutionTrace(tmp_path/'trace.jsonl',profile_id='p',plan_id='p',disk_budget_getter=lambda:holder[0])
    try:
        holder[0]=disk
        with pytest.raises(DiskAdmissionError):trace.record('task')
        assert (tmp_path/'trace.jsonl').read_bytes()==b'' and disk.reserved_bytes==0
    finally:trace.close();disk.close()


@pytest.mark.parametrize('writer',['journal','trace'])
@pytest.mark.parametrize('quota',[0,1])
def test_namespace_one_byte_admission_precedes_mkdir_and_open(tmp_path,writer,quota):
    with budget(tmp_path) as disk:
        disk.quota_bytes=quota
        target=tmp_path/'missing'/writer
        if quota==0:
            with pytest.raises(DiskAdmissionError):
                if writer=='journal':Journal(target,disk_budget_getter=lambda:disk)
                else:StreamingExecutionTrace(target/'trace.jsonl',profile_id='p',plan_id='p',disk_budget_getter=lambda:disk)
            assert not target.exists()
        else:
            obj=(Journal(target,disk_budget_getter=lambda:disk) if writer=='journal' else
                 StreamingExecutionTrace(target/'trace.jsonl',profile_id='p',plan_id='p',disk_budget_getter=lambda:disk))
            obj.close();assert target.is_dir()
        assert disk.reserved_bytes==0


def encoded(payload):
    row={'schema_version':1,'payload':payload};row['checksum']=content_hash(row)
    return (canonical(row)+'\n').encode()


@pytest.mark.parametrize('tail',['missing','partial'])
def test_refused_first_journal_append_does_not_create_or_repair_file(tmp_path,tail):
    directory=tmp_path/'journal';directory.mkdir();target=directory/'gates.jsonl'
    if tail=='partial':target.write_bytes(encoded({'value':'committed'})+b'{"partial":')
    before=target.read_bytes() if target.exists() else None
    with budget(tmp_path) as disk:
        journal=Journal(directory,disk_budget_getter=lambda:disk)
        disk.quota_bytes=len(encoded({'value':'new'}))-1
        with pytest.raises(DiskAdmissionError):journal.append('gates',{'value':'new'})
        assert (target.read_bytes() if target.exists() else None)==before
        assert journal.streams=={} and disk.reserved_bytes==0
        journal.close()


def test_invalid_journal_serialization_cannot_mutate_a_partial_tail(tmp_path):
    target=tmp_path/'journal/gates.jsonl';target.parent.mkdir()
    target.write_bytes(encoded({'value':'committed'})+b'{"partial":')
    before=target.read_bytes()
    with budget(tmp_path) as disk:
        journal=Journal(target.parent,disk_budget_getter=lambda:disk)
        with pytest.raises(ValueError):journal.append('gates',{'invalid':float('nan')})
        assert target.read_bytes()==before and journal.streams=={}
        journal.close()


def test_exact_record_credit_precedes_repair_and_preserves_committed_cursor(tmp_path):
    directory=tmp_path/'journal';directory.mkdir();target=directory/'gates.jsonl'
    old=encoded({'value':'committed'});target.write_bytes(old+b'{"partial":')
    with budget(tmp_path) as disk:
        journal=Journal(directory,disk_budget_getter=lambda:disk)
        journal.protected_cursor=lambda kind:len(old)
        disk.quota_bytes=len(encoded({'value':'next'}))
        journal.append('gates',{'value':'next'});journal.flush()
        assert list(journal.replay('gates',len(old)))==[{'value':'committed'}]
        assert list(journal.replay('gates'))==[{'value':'committed'},{'value':'next'}]
        assert disk.reserved_bytes==len(encoded({'value':'next'}))
        journal.close();assert disk.reserved_bytes==0


CHILD=r'''
import os,signal,sys
from pathlib import Path
from svsim import StoredSimulator,StorageConfig
from svsim.journal import Journal
from svsim.telemetry import StreamingExecutionTrace
root=Path(sys.argv[1]);writer=sys.argv[2];stage=sys.argv[3]
cfg=StorageConfig(str(root/'store'),2,alignment=64,ram_budget_bytes=98304,metadata_reserve_bytes=32768)
with StoredSimulator(4,cfg) as sim:
 sim.gate('H',(0,));sim.materialize();snap=sim.commit_snapshot();print(snap.snapshot_id,flush=True)
 disk=sim.store.disk_budget;operation=writer if writer=='dataset-lease-json' else writer+'-open';target=sim.store.directory/('extra-'+writer)
 original_reserve=disk.reserve
 def reserve(amount,**kwargs):
  key=original_reserve(amount,**kwargs)
  if kwargs.get('operation')==operation and stage=='reserved':os.kill(os.getpid(),signal.SIGKILL)
  return key
 disk.reserve=reserve
 original_mkdir=Path.mkdir
 def mkdir(path,*args,**kwargs):
  result=original_mkdir(path,*args,**kwargs)
  if path==target and stage=='mkdir':os.kill(os.getpid(),signal.SIGKILL)
  return result
 Path.mkdir=mkdir
 original_open=Path.open
 def open_path(path,*args,**kwargs):
  result=original_open(path,*args,**kwargs)
  if (path==target/'trace.jsonl' and stage=='open') or (writer=='dataset-lease-json' and path.name.endswith('.tmp') and stage=='temp-open'):os.kill(os.getpid(),signal.SIGKILL)
  return result
 Path.open=open_path
 import builtins
 original_builtin=builtins.open
 def builtin_open(path,*args,**kwargs):
  result=original_builtin(path,*args,**kwargs)
  if writer=='dataset-lease-json' and str(path).endswith('.tmp') and stage=='temp-open':os.kill(os.getpid(),signal.SIGKILL)
  return result
 builtins.open=builtin_open
 if writer=='dataset-lease-json':
  from svsim.distributed_checkpoint import atomic_json
  atomic_json(sim.store.directory/'manifests/extra-lease.json',{'lease':'test'},disk_budget=disk,operation=writer)
 elif writer=='journal':Journal(target,disk_budget_getter=lambda:disk)
 else:StreamingExecutionTrace(target/'trace.jsonl',profile_id='p',plan_id='p',disk_budget_getter=lambda:disk)
 raise AssertionError('kill hook did not run')
'''


@pytest.mark.parametrize('writer,stage',[('journal','reserved'),('journal','mkdir'),
                                        ('trace','reserved'),('trace','mkdir'),('trace','open'),
                                        ('dataset-lease-json','reserved'),('dataset-lease-json','temp-open')])
def test_namespace_kill_tokens_recover_after_owner_death_and_preserve_authoritative_cut(tmp_path,writer,stage):
    result=subprocess.run([sys.executable,'-c',CHILD,str(tmp_path),writer,stage],
        env={**os.environ,'PYTHONPATH':'src'},capture_output=True,timeout=60)
    assert result.returncode==-signal.SIGKILL,result.stderr.decode()
    snapshot=result.stdout.decode().strip().splitlines()[-1]
    ledger=(tmp_path/'store/metadata/disk-ledger.sqlite3').resolve()
    dry=recover_uncommitted_reservations(ledger)
    assert not dry['refusals'],dry['refusals']
    operation=writer if writer=='dataset-lease-json' else writer+'-open'
    tokens=[row for row in dry['tokens'] if row['operation']==operation]
    assert len(tokens)==1 and tokens[0]['class']==('sidecar' if writer=='dataset-lease-json' else 'namespace')
    assert tokens[0]['bytes']==tokens[0]['release_pending_bytes']>0
    before={str(p.relative_to(tmp_path/'store')):p.read_bytes() for p in (tmp_path/'store').rglob('*')
            if p.is_file() and 'disk-ledger.sqlite3' not in p.name}
    applied=recover_uncommitted_reservations(ledger,apply=True,confirm_plan=dry['plan_sha256'])
    assert applied['status']=='applied'
    after={str(p.relative_to(tmp_path/'store')):p.read_bytes() for p in (tmp_path/'store').rglob('*')
           if p.is_file() and 'disk-ledger.sqlite3' not in p.name}
    assert after==before
    assert recover_uncommitted_reservations(ledger)['status']=='nothing-to-do'
    cfg=StorageConfig(str(tmp_path/'store'),2,alignment=64,ram_budget_bytes=98304,metadata_reserve_bytes=32768)
    with StoredSimulator.resume(cfg,snapshot) as sim:
        expected=np.zeros(16,dtype='<c16');expected[:2]=1/np.sqrt(2)
        np.testing.assert_allclose(sim.state(),expected)
