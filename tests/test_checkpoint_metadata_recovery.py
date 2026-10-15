"""Payload/chunk intents survive real writer death and preserve authoritative cuts."""
import hashlib
import os
from pathlib import Path
import subprocess
import sys

import numpy as np
import pytest

pytest.importorskip('mpi4py')
from mpi4py import MPI
from svsim import DistributedConfig, DistributedSimulator
from svsim.disk_budget import DiskBudget
from svsim.uncommitted_recovery import recover_uncommitted_reservations, UncommittedRecoveryRefused
from svsim.checkpoint_recovery import recover_checkpoint_reservations

ROOT=Path(__file__).resolve().parents[1]
WORKER=Path(__file__).with_name('worker_checkpoint_metadata_recovery.py')
ENV={**os.environ,'PYTHONPATH':str(ROOT/'src'),'OMP_NUM_THREADS':'1','OPENBLAS_NUM_THREADS':'1'}


def _killed(base,stage,point):
    child=subprocess.run([sys.executable,str(WORKER),str(base),stage,point],cwd=ROOT,env=ENV,
                         capture_output=True,text=True,timeout=120)
    assert child.returncode == 17, child.stdout+'\n'+child.stderr
    assert (base/'fault.txt').read_text() == point
    return Path((base/'ledger.txt').read_text())


def _artifacts(base):
    return {str(p.relative_to(base)):hashlib.sha256(p.read_bytes()).hexdigest()
            for p in (base/'dataset').rglob('*') if p.is_file() and '.disk-admission' not in p.parts}


def _config(base,ledger):
    return DistributedConfig(str(base/'dataset'),2,storage_scope='node-local',
        metadata_reserve_bytes=256*1024,ram_budget_bytes_per_rank=2*1024**2,
        disk_quota_bytes=32*1024**2,disk_admission_ledger=str(ledger),
        backend=os.environ.get('SVSIM_TEST_BACKEND','buffered'))


@pytest.mark.parametrize('stage',['metadata-payload','metadata-chunk'])
@pytest.mark.parametrize('point',['reserved','partial','pre-fsync','post-fsync','pre-rename','post-rename','committed'])
def test_metadata_kill_recovers_once_preserves_files_and_resumes_oracle(tmp_path,stage,point):
    ledger=_killed(tmp_path,stage,point)
    before=_artifacts(tmp_path)
    ledger_before=hashlib.sha256(ledger.read_bytes()).hexdigest()
    report=recover_uncommitted_reservations(ledger)
    assert report['status']=='dry-run',report['refusals']
    metadata=[t for t in report['tokens'] if t['class']=='checkpoint-metadata']
    assert len(metadata)==1 and metadata[0]['operation']=='checkpoint-'+stage
    attribution=metadata[0]['attribution']
    assert attribution['authority']['status']==('committed' if point=='committed' else 'uncommitted')
    assert attribution['stage']==stage and attribution['dataset_birth_identity']
    assert _artifacts(tmp_path)==before and hashlib.sha256(ledger.read_bytes()).hexdigest()==ledger_before
    with pytest.raises(UncommittedRecoveryRefused,match='Plan changed'):
        recover_uncommitted_reservations(ledger,apply=True,confirm_plan='0'*64)
    applied=recover_uncommitted_reservations(ledger,apply=True,confirm_plan=report['plan_sha256'])
    assert applied['status']=='applied' and applied['artifacts_preserved']
    assert _artifacts(tmp_path)==before
    config=_config(tmp_path,ledger)
    recover_checkpoint_reservations(config,comm=MPI.COMM_SELF,apply=True)
    budget=DiskBudget(ledger,[tmp_path/'dataset'],shared_policy=True,quota_bytes=32*1024**2)
    try:
        assert budget.reserved_bytes==0
        receipt=budget.recovery_receipt(metadata[0]['id'])
        assert receipt['attribution']==attribution
    finally: budget.close()
    with pytest.raises(UncommittedRecoveryRefused):
        recover_uncommitted_reservations(ledger,apply=True,confirm_plan=report['plan_sha256'])
    want=np.load(tmp_path/('new.npy' if point=='committed' else 'old.npy'))
    with DistributedSimulator.resume(config,None,comm=MPI.COMM_SELF) as sim:
        np.testing.assert_allclose(sim.state(allow_gather=True),want,atol=1e-12)
        sim.commit_snapshot()


@pytest.mark.parametrize('stage',['metadata-payload','metadata-chunk'])
def test_committed_metadata_also_uses_collective_checkpoint_recovery(tmp_path,stage):
    ledger=_killed(tmp_path,stage,'committed')
    config=_config(tmp_path,ledger)
    before=_artifacts(tmp_path)
    dry=recover_checkpoint_reservations(config,comm=MPI.COMM_SELF)
    receipts=dry['ranks'][0]['receipts']
    metadata=[r for r in receipts if r['proof'].get('class')=='checkpoint-metadata']
    assert len(metadata)==1
    assert metadata[0]['proof']['attribution']['authority']['status']=='committed'
    assert _artifacts(tmp_path)==before
    recover_checkpoint_reservations(config,comm=MPI.COMM_SELF,apply=True)
    assert _artifacts(tmp_path)==before
    plan=recover_uncommitted_reservations(ledger)
    if plan['status']=='dry-run':
        recover_uncommitted_reservations(ledger,apply=True,confirm_plan=plan['plan_sha256'])
    with DistributedSimulator.resume(config,None,comm=MPI.COMM_SELF) as sim:
        np.testing.assert_allclose(sim.state(allow_gather=True),np.load(tmp_path/'new.npy'),atol=1e-12)


@pytest.mark.parametrize('tamper',['generation','final-content','temporary-symlink','dataset-birth','legacy-unspecified'])
def test_unknown_metadata_generation_or_content_refuses_without_changing_tokens(tmp_path,tamper):
    import json
    import sqlite3
    ledger=_killed(tmp_path,'metadata-payload','partial')
    con=sqlite3.connect(ledger)
    key,raw=con.execute("SELECT c.id,c.link FROM checkpoint_reservations c JOIN reservations r ON r.id=c.id "
                        "WHERE r.operation='checkpoint-metadata-payload'").fetchone()
    link=json.loads(raw)
    root=Path(link['dataset_root']);temporary=root/link['temporary'];target=root/link['target']
    if tamper=='generation':
        con.execute('UPDATE reservations SET generation=? WHERE id=?',('f'*32,key))
    elif tamper=='final-content':
        temporary.rename(target)
        target.write_bytes(b'x'*link['expected_bytes'])
    elif tamper=='temporary-symlink':
        temporary.unlink();temporary.symlink_to(root/'rank-0000/metadata/catalog.sqlite3')
    elif tamper=='dataset-birth':
        link['dataset_birth_identity']='ns:unknown'
        con.execute('UPDATE checkpoint_reservations SET link=? WHERE id=?',(json.dumps(link),key))
    else:
        con.execute("UPDATE reservations SET operation='unspecified' WHERE id=?",(key,))
    con.commit();con.close()
    before=hashlib.sha256(ledger.read_bytes()).hexdigest(),_artifacts(tmp_path)
    report=recover_uncommitted_reservations(ledger)
    assert report['status']=='refused' and report['refusals']
    with pytest.raises(UncommittedRecoveryRefused):
        recover_uncommitted_reservations(ledger,apply=True,confirm_plan=report['plan_sha256'])
    assert (hashlib.sha256(ledger.read_bytes()).hexdigest(),_artifacts(tmp_path))==before
