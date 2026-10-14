"""Offline checkpoint settlement must prove authority, ownership and inventory."""
from dataclasses import replace
from hashlib import sha256
from pathlib import Path
import sqlite3
import subprocess
import sys
from uuid import uuid4

import numpy as np
import pytest

MPI=pytest.importorskip('mpi4py.MPI')
from svsim import DistributedConfig, DistributedSimulator
from svsim.checkpoint_payload import decision_record
from svsim.checkpoint_recovery import recover_checkpoint_reservations
from svsim.dataset_lease import DirectoryLease
from svsim.disk_budget import DiskAdmissionError,DiskBudget
from svsim.journal import canonical,content_hash
from svsim.mpi_transport import CollectiveError


@pytest.fixture
def saved(tmp_path):
    config=DistributedConfig(str(tmp_path/'state'),2,storage_scope='node-local',
        metadata_reserve_bytes=256*1024,ram_budget_bytes_per_rank=2*1024**2,
        disk_quota_bytes=32*1024**2,drain_reserve_bytes=1024**2)
    with DistributedSimulator(6,config,comm=MPI.COMM_SELF) as sim:
        sim.gate('H',(0,));sim.gate('RY',(5,),angle=.31)
        sim.materialize(binding=sim.bind())
        snapshot=sim.commit_snapshot()
        expected=sim.state(allow_gather=True).copy()
        config=replace(config,disk_admission_ledger=str(sim.disk_budget._ledger_path))
    return config,snapshot,expected


def _reserve(saved,stage='decision-copy',*,partial=False,target_missing=False):
    config,snapshot,_=saved;root=Path(config.directory)
    if stage=='prepare':target=root/'rank-0000/manifests'/f'prepare-{snapshot["snapshot_id"]}.json'
    elif stage=='decision':target=root/'global/manifests'/f'{snapshot["snapshot_id"]}.json'
    else:target=root/'rank-0000/manifests'/f'decision-{snapshot["snapshot_id"]}.json'
    raw=target.read_bytes()
    if target_missing:target.unlink()
    nonce=uuid4().hex
    temporary=target.with_name('.'+target.name+'.'+nonce+'.tmp')
    budget=DiskBudget(config.disk_admission_ledger,[root],quota_bytes=config.disk_quota_bytes,
                      drain_reserve_bytes=config.drain_reserve_bytes,shared_policy=True)
    try:
        key=budget.reserve(len(raw),drain=True,writing=True,operation='atomic-json',owner=uuid4().hex,generation=nonce,
            checkpoint_link={'dataset_root':str(root),'dataset_id':snapshot['dataset_id'],'snapshot_id':snapshot['snapshot_id'],
                'rank':0,'stage':stage,'target':str(target),'temporary':str(temporary),'digest':sha256(raw).hexdigest()})
        if partial:
            temporary.write_bytes(raw[:13])
            budget.materialize(key,13)
    finally:budget.close()
    return key,temporary,raw


def _budget(config):
    return DiskBudget(config.disk_admission_ledger,[Path(config.directory)],quota_bytes=config.disk_quota_bytes,
                      drain_reserve_bytes=config.drain_reserve_bytes,shared_policy=True)


@pytest.mark.parametrize('stage',['prepare','decision','decision-copy'])
def test_dry_run_and_apply_keep_cut_and_store_audit_receipt(saved,stage):
    config,snapshot,expected=saved
    key,temporary,raw=_reserve(saved,stage)
    report=recover_checkpoint_reservations(config,comm=MPI.COMM_SELF)
    assert report['apply'] is False and report['ranks'][0]['receipts'][0]['status']=='would-settle'
    budget=_budget(config)
    try:assert budget.reservation(key)['state']=='WRITING'
    finally:budget.close()
    report=recover_checkpoint_reservations(config,comm=MPI.COMM_SELF,apply=True)
    assert report['snapshot_id']==snapshot['snapshot_id'] and report['artifacts_preserved']
    budget=_budget(config)
    try:
        assert budget.reservation(key)['state']=='DURABLE'
        receipt=budget.recovery_receipt(key)
        assert receipt['metadata_only'] and receipt['snapshot_id']==snapshot['snapshot_id']
        assert budget.reservation(key)['evidence'].endswith(':'+content_hash(receipt))
    finally:budget.close()
    again=recover_checkpoint_reservations(config,comm=MPI.COMM_SELF,apply=True)
    assert again['ranks'][0]['already_terminal']==1 and not again['ranks'][0]['receipts']
    with DistributedSimulator.resume(config,snapshot,comm=MPI.COMM_SELF) as restored:
        np.testing.assert_allclose(restored.state(allow_gather=True),expected,atol=1e-12)


def test_missing_copy_and_partial_temporary_are_preserved_then_normal_resume_repairs(saved):
    config,snapshot,expected=saved
    key,temporary,raw=_reserve(saved,partial=True,target_missing=True)
    before=temporary.read_bytes()
    report=recover_checkpoint_reservations(config,comm=MPI.COMM_SELF,apply=True)
    assert report['ranks'][0]['receipts'][0]['released_pending_bytes']==len(raw)-13
    assert temporary.read_bytes()==before
    budget=_budget(config)
    try:
        assert budget.reservation(key)['state']=='RECONCILED'
        assert budget.reservation(key)['materialized_bytes']==13
        assert budget.reserved_bytes==0
    finally:budget.close()
    with DistributedSimulator.resume(config,snapshot,comm=MPI.COMM_SELF) as restored:
        np.testing.assert_allclose(restored.state(allow_gather=True),expected,atol=1e-12)
    assert temporary.read_bytes()==before


def test_active_runtime_prevents_recovery_and_constructor_failure_releases_lease(saved):
    config,snapshot,_=saved
    key,_,_=_reserve(saved)
    with DistributedSimulator.resume(config,snapshot,comm=MPI.COMM_SELF):
        with pytest.raises(CollectiveError,match='Resource temporarily unavailable|unavailable|busy'):
            recover_checkpoint_reservations(config,comm=MPI.COMM_SELF,apply=True)
    with pytest.raises(CollectiveError):
        DistributedSimulator(6,config,comm=MPI.COMM_SELF)  # existing dataset
    with DirectoryLease(config.directory,exclusive=True):pass
    budget=_budget(config)
    try:assert budget.reservation(key)['state']=='WRITING'
    finally:budget.close()


@pytest.mark.parametrize('fault',['decision','prepare','journal','target','symlink','root-identity','hot-journal','generation'])
def test_unknown_or_conflicting_evidence_is_rejected_without_settlement(saved,fault):
    config,snapshot,_=saved
    key,temporary,raw=_reserve(saved)
    root=Path(config.directory)
    if fault=='decision':(root/'global/manifests'/f'{snapshot["snapshot_id"]}.json').write_bytes(b'broken')
    elif fault=='prepare':(root/'rank-0000/manifests'/f'prepare-{snapshot["snapshot_id"]}.json').write_bytes(b'broken')
    elif fault=='journal':
        journal=root/'rank-0000/journal/gates.jsonl'
        assert journal.stat().st_size>0
        journal.write_bytes(b'broken\n')
    elif fault=='target':(root/'rank-0000/manifests'/f'decision-{snapshot["snapshot_id"]}.json').write_bytes(b'broken')
    elif fault=='symlink':temporary.symlink_to(root/'rank-0000/manifests'/f'prepare-{snapshot["snapshot_id"]}.json')
    elif fault=='hot-journal':(root/'rank-0000/metadata/catalog.sqlite3-journal').write_bytes(b'uncertain')
    else:
        budget=_budget(config)
        try:
            with budget._ledger_transaction():
                if fault=='generation':budget.db.execute("UPDATE reservations SET generation=? WHERE id=?",('f'*32,key))
                else:
                    import json
                    link=json.loads(budget.db.execute('SELECT link FROM checkpoint_reservations WHERE id=?',(key,)).fetchone()[0])
                    link['dataset_inode']+=1
                    budget.db.execute('UPDATE checkpoint_reservations SET link=? WHERE id=?',(canonical(link),key))
        finally:budget.close()
    with pytest.raises(CollectiveError):recover_checkpoint_reservations(config,comm=MPI.COMM_SELF,apply=True)
    budget=_budget(config)
    try:
        assert budget.reservation(key)['state']=='WRITING'
        assert budget.recovery_receipt(key) is None
    finally:budget.close()


def test_newest_bad_cut_is_not_replaced_by_older_valid_cut(saved):
    config,old,_=saved
    with DistributedSimulator.resume(config,old,comm=MPI.COMM_SELF) as sim:new=sim.commit_snapshot()
    key,_,_=_reserve((config,new,None))
    payload=Path(config.directory)/'rank-0000/manifests'/new['metadata_payload']['name']
    payload.write_bytes(b'broken')
    with pytest.raises(CollectiveError):recover_checkpoint_reservations(config,comm=MPI.COMM_SELF,apply=True)
    budget=_budget(config)
    try:assert budget.reservation(key)['state']=='WRITING'
    finally:budget.close()


def test_explicit_older_cut_and_metadata_limit_are_refused(saved):
    config,old,_=saved
    with DistributedSimulator.resume(config,old,comm=MPI.COMM_SELF) as sim:new=sim.commit_snapshot()
    _reserve((config,new,None))
    with pytest.raises(CollectiveError,match='older'):recover_checkpoint_reservations(config,comm=MPI.COMM_SELF,snapshot=old['snapshot_id'],apply=True)
    with pytest.raises(CollectiveError,match='limit'):recover_checkpoint_reservations(config,comm=MPI.COMM_SELF,max_metadata_bytes=1,apply=True)


def test_wrong_existing_ledger_is_refused_without_settlement(saved,tmp_path):
    config,_,_=saved
    key,_,_=_reserve(saved)
    alternate=tmp_path/'alternate.sqlite3'
    budget=DiskBudget(alternate,[Path(config.directory)],quota_bytes=config.disk_quota_bytes,
                      drain_reserve_bytes=config.drain_reserve_bytes,shared_policy=True)
    budget.close()
    with pytest.raises(CollectiveError,match='persisted locator'):
        recover_checkpoint_reservations(replace(config,disk_admission_ledger=str(alternate)),comm=MPI.COMM_SELF,apply=True)
    budget=_budget(config)
    try:
        assert budget.reservation(key)['state']=='WRITING'
        assert budget.recovery_receipt(key) is None
    finally:budget.close()


def test_unlinked_token_is_preserved(saved):
    config,_,_=saved
    budget=_budget(config)
    try:key=budget.reserve(4096,writing=True,operation='unknown-writer')
    finally:budget.close()
    report=recover_checkpoint_reservations(config,comm=MPI.COMM_SELF,apply=True)
    assert not report['ranks'][0]['receipts']
    budget=_budget(config)
    try:
        assert budget.reservation(key)['state']=='WRITING'
        assert budget.recovery_receipt(key) is None
    finally:budget.close()


def test_standalone_shard_reader_prevents_recovery(saved):
    from svsim.storage import FileBlockStore,StorageConfig
    config,_,_=saved
    key,_,_=_reserve(saved)
    store=FileBlockStore(StorageConfig(str(Path(config.directory)/'rank-0000'),2),6,create=False)
    try:
        with pytest.raises(CollectiveError,match='unavailable|busy'):
            recover_checkpoint_reservations(config,comm=MPI.COMM_SELF,apply=True)
    finally:store.close()
    budget=_budget(config)
    try:assert budget.reservation(key)['state']=='WRITING'
    finally:budget.close()


def test_checkpoint_writer_and_recovery_accept_root_external_symlink_parent_traversal(saved):
    config,snapshot,expected=saved
    root=Path(config.directory)
    child=root.parent/'child';child.mkdir()
    alias=root.parent/'alias';alias.symlink_to(child,target_is_directory=True)
    aliased=replace(config,directory=str(alias/'..'/root.name))
    with DistributedSimulator.resume(aliased,snapshot,comm=MPI.COMM_SELF) as sim:
        new=sim.commit_snapshot()
    key,_,_=_reserve((aliased,new,expected))
    report=recover_checkpoint_reservations(aliased,comm=MPI.COMM_SELF,apply=True)
    assert report['ranks'][0]['receipts'][0]['reservation_id']==key
    with DistributedSimulator.resume(aliased,new,comm=MPI.COMM_SELF) as sim:
        np.testing.assert_allclose(sim.state(allow_gather=True),expected,atol=1e-12)


def test_checkpoint_settlement_can_spend_precharged_pool_at_pending_quota_boundary(saved):
    config,snapshot,expected=saved
    key,temporary,raw=_reserve(saved,target_missing=True)
    temporary.write_bytes(raw[:13])  # crash window before materialize()
    budget=_budget(config)
    try:
        pressure_bytes=config.disk_quota_bytes-budget.bootstrap_bytes-config.drain_reserve_bytes-budget._usage()-budget.reserved_bytes+1
        assert pressure_bytes>0
    finally:budget.close()
    pressure=Path(config.directory)/'pressure.bin'
    with pressure.open('wb') as stream:stream.truncate(pressure_bytes)
    with pytest.raises(DiskAdmissionError,match='quota'):_budget(config)
    ledger=Path(config.disk_admission_ledger)
    before=sha256(ledger.read_bytes()).hexdigest()
    recover_checkpoint_reservations(config,comm=MPI.COMM_SELF)
    assert sha256(ledger.read_bytes()).hexdigest()==before
    report=recover_checkpoint_reservations(config,comm=MPI.COMM_SELF,apply=True)
    assert report['ranks'][0]['receipts'][0]['released_pending_bytes']==len(raw)
    assert temporary.read_bytes()==raw[:13] and pressure.stat().st_size==pressure_bytes
    budget=_budget(config)
    try:
        assert budget.reservation(key)['state']=='RECONCILED'
        assert budget.reservation(key)['materialized_bytes']==13
        assert budget.recovery_receipt(key) is not None
    finally:budget.close()
    pressure.unlink()  # remove test pressure before ordinary runtime admission
    with DistributedSimulator.resume(config,snapshot,comm=MPI.COMM_SELF) as restored:
        np.testing.assert_allclose(restored.state(allow_gather=True),expected,atol=1e-12)


@pytest.mark.parametrize('crash',['reserved','partial','renamed'])
def test_real_crashed_checkpoint_writer_is_settled_then_resumes(saved,crash):
    config,snapshot,expected=saved
    target=Path(config.directory)/'rank-0000/manifests'/f'decision-{snapshot["snapshot_id"]}.json'
    target.unlink()
    code='''
import builtins,os,stat,sys
from pathlib import Path
from uuid import uuid4
from svsim.disk_budget import DiskBudget
from svsim.distributed_checkpoint import atomic_json,load_global
from svsim.checkpoint_payload import decision_record
root=Path(sys.argv[1]);ledger=sys.argv[2];crash=sys.argv[3]
data=load_global(root);decision=decision_record(data)
budget=DiskBudget(ledger,[root],quota_bytes=32*1024**2,drain_reserve_bytes=1024**2,shared_policy=True)
original_open=builtins.open;original_fsync=os.fsync
class Partial:
    def __init__(self,stream):self.stream=stream
    def __enter__(self):return self
    def __exit__(self,*args):return self.stream.__exit__(*args)
    def write(self,raw):
        self.stream.write(raw[:13]);self.stream.flush();original_fsync(self.stream.fileno());os._exit(17)
def fault_open(path,mode='r',*args,**kwargs):
    if mode=='xb' and crash=='reserved':os._exit(17)
    stream=original_open(path,mode,*args,**kwargs)
    return Partial(stream) if mode=='xb' and crash=='partial' else stream
def fault_fsync(fd):
    if crash=='renamed' and stat.S_ISDIR(os.fstat(fd).st_mode):os._exit(17)
    return original_fsync(fd)
builtins.open=fault_open;os.fsync=fault_fsync
atomic_json(root/'rank-0000/manifests'/f'decision-{data["snapshot_id"]}.json',
    {'manifest':decision,'checksum':__import__('svsim.journal',fromlist=['content_hash']).content_hash(decision)},
    disk_budget=budget,owner=uuid4().hex,checkpoint_context={'dataset_root':str(root),'dataset_id':data['dataset_id'],
      'snapshot_id':data['snapshot_id'],'rank':0,'stage':'decision-copy'})
'''
    completed=subprocess.run([sys.executable,'-c',code,config.directory,config.disk_admission_ledger,crash],
                             capture_output=True,text=True,timeout=60)
    assert completed.returncode==17,completed.stderr
    before={p.name:p.read_bytes() for p in target.parent.glob('.*.tmp')}
    report=recover_checkpoint_reservations(config,comm=MPI.COMM_SELF,apply=True)
    receipt=report['ranks'][0]['receipts'][0]
    assert receipt['terminal_state']==('DURABLE' if crash=='renamed' else 'RECONCILED')
    assert before=={p.name:p.read_bytes() for p in target.parent.glob('.*.tmp')}
    with DistributedSimulator.resume(config,snapshot,comm=MPI.COMM_SELF) as restored:
        np.testing.assert_allclose(restored.state(allow_gather=True),expected,atol=1e-12)
