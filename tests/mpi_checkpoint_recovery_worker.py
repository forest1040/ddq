"""Owner-local recovery agrees before settlement and preserves numeric state."""
from dataclasses import replace
from hashlib import sha256
from pathlib import Path
import sys
from uuid import uuid4

import numpy as np
from mpi4py import MPI
from svsim import DistributedConfig,DistributedSimulator
from svsim.checkpoint_recovery import recover_checkpoint_reservations
from svsim.disk_budget import DiskAdmissionError,DiskBudget
from svsim.mpi_transport import CollectiveError
import svsim.checkpoint_recovery as recovery

comm=MPI.COMM_WORLD;rank=comm.Get_rank()
base=Path(sys.argv[1]);shared='--same-root' in sys.argv;pressure='--pressure' in sys.argv
root=base/('same-root' if shared else f'owner-{rank}')
config=DistributedConfig(str(root),2,storage_scope='node-local',metadata_reserve_bytes=256*1024,
    ram_budget_bytes_per_rank=2*1024**2,disk_quota_bytes=32*1024**2,drain_reserve_bytes=1024**2)
with DistributedSimulator(6,config,comm=comm) as sim:
    sim.gate('H',(0,));sim.gate('RY',(5,),angle=.31);sim.materialize(binding=sim.bind())
    snapshot=sim.commit_snapshot();expected=sim.state(allow_gather=True)
    config=replace(config,disk_admission_ledger=str(sim.disk_budget._ledger_path))
    try:recover_checkpoint_reservations(config,comm=comm,apply=True)
    except CollectiveError:pass
    else:raise AssertionError('Active runtime was accepted for recovery')

original=recovery._read_bytes
def own_read(dataset,path,*args,**kwargs):
    assert dataset==root.resolve(),f'Peer data path inspected: {dataset}'
    return original(dataset,path,*args,**kwargs)
recovery._read_bytes=own_read
target=root/f'rank-{rank:04d}/manifests'/f'decision-{snapshot["snapshot_id"]}.json'
raw=target.read_bytes();target.unlink();nonce=uuid4().hex
temporary=target.with_name('.'+target.name+'.'+nonce+'.tmp')
budget=DiskBudget(config.disk_admission_ledger,[root],quota_bytes=config.disk_quota_bytes,
                  drain_reserve_bytes=config.drain_reserve_bytes,shared_policy=True)
key=budget.reserve(len(raw),writing=True,drain=True,operation='atomic-json',owner=uuid4().hex,generation=nonce,
    checkpoint_link={'dataset_root':str(root.resolve()),'dataset_id':snapshot['dataset_id'],
        'snapshot_id':snapshot['snapshot_id'],'rank':rank,'stage':'decision-copy',
        'target':str(target.resolve()),'temporary':str(temporary.resolve()),'digest':sha256(raw).hexdigest()})
temporary.write_bytes(raw[:13]);budget.materialize(key,13);budget.close()
comm.Barrier()
if pressure:
    if rank==0:
        budget=DiskBudget(config.disk_admission_ledger,[root],quota_bytes=config.disk_quota_bytes,
            drain_reserve_bytes=config.drain_reserve_bytes,shared_policy=True)
        amount=config.disk_quota_bytes-budget.bootstrap_bytes-config.drain_reserve_bytes-budget._usage()-budget.reserved_bytes+1
        assert amount>0
        budget.close()
        with (root/'pressure.bin').open('wb') as stream:stream.truncate(amount)
    comm.Barrier()
    try:
        candidate=DiskBudget(config.disk_admission_ledger,[root],quota_bytes=config.disk_quota_bytes,
            drain_reserve_bytes=config.drain_reserve_bytes,shared_policy=True)
    except DiskAdmissionError:pass
    else:
        candidate.close();raise AssertionError('Ordinary admission accepted pressure fixture')
report=recover_checkpoint_reservations(config,comm=comm)
assert len(report['ranks'])==comm.Get_size()
assert all(r['receipts'][0]['status']=='would-settle' for r in report['ranks'])
# One owner is corrupt: every owner refuses before changing any reservation.
if rank==comm.Get_size()-1:target.write_bytes(b'corrupt')
comm.Barrier()
try:recover_checkpoint_reservations(config,comm=comm,apply=True)
except CollectiveError:pass
else:raise AssertionError('Conflicting owner artifact accepted')
budget=DiskBudget(config.disk_admission_ledger,[root],quota_bytes=config.disk_quota_bytes,
                  drain_reserve_bytes=config.drain_reserve_bytes,shared_policy=True,recovery_only=pressure)
assert budget.reservation(key)['state']=='WRITING'
assert budget.recovery_receipt(key) is None
budget.close()
if rank==comm.Get_size()-1:target.unlink()
comm.Barrier()
report=recover_checkpoint_reservations(config,comm=comm,apply=True)
assert all(r['receipts'][0]['status']=='settled' for r in report['ranks'])
assert temporary.read_bytes()==raw[:13]
if pressure:
    if rank==0:(root/'pressure.bin').unlink()
    comm.Barrier()
with DistributedSimulator.resume(config,snapshot,comm=comm) as restored:
    observed=restored.state(allow_gather=True)
    if rank==0:np.testing.assert_allclose(observed,expected,atol=1e-12)
if rank==0:print('checkpoint reservation recovery PASS',comm.Get_size(),'shared' if shared else 'distinct','pressure' if pressure else 'ordinary',flush=True)
