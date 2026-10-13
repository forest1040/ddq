"""Use rank-distinct directories and prohibit peer manifest reads."""
from pathlib import Path
import sys
import numpy as np
from mpi4py import MPI
from svsim.distributed import DistributedConfig, DistributedSimulator
from svsim.mpi_transport import CollectiveError
from svsim.parameters import ParameterSymbol
import svsim.distributed_checkpoint as checkpoints

comm=MPI.COMM_WORLD; rank=comm.Get_rank()
root=Path(sys.argv[1])/f'local-owner-{rank}'
config=DistributedConfig(str(root),2,ram_budget_bytes_per_rank=2*1024**2,
    metadata_reserve_bytes=256*1024,storage_scope='node-local',
    disk_quota_bytes=32*1024**2,drain_reserve_bytes=1024**2)
read=checkpoints._read
allowed=[root]
def own_read(path):
    assert any(Path(path).is_relative_to(p) for p in allowed), f'Peer path opened: {path}'
    return read(path)
checkpoints._read=own_read
symbol=ParameterSymbol('theta',parameter_id='node-local-theta')
with DistributedSimulator(6,config,comm=comm) as sim:
    sim.gate('H',(0,)); sim.gate('RX',(5,),angle=symbol)
    binding=sim.bind({symbol.parameter_id:.2})
    # Unicode metadata exceeds one control packet; the durable decision is small.
    sim._checkpoint_state=lambda: {'diagnostic':'λ'*(sim.transport.metadata_limit//2+100)}
    snap=sim.commit_prefix([symbol],binding=binding)
    assert snap['schema_version'] == 4
    assert snap['metadata_payload']['bytes'] > sim.transport.metadata_limit
    assert (root/'global/manifests'/f'{snap["snapshot_id"]}.json').stat().st_size < sim.transport.metadata_limit if rank == 0 else True
    assert len(snap['registry'][sim.store_id]) == comm.Get_size()
    assert (root/f'rank-{rank:04d}/manifests/decision-{snap["snapshot_id"]}.json').is_file()
    childroot=Path(sys.argv[1])/f'suffix-owner-{rank}'
    allowed.append(childroot)
    with sim.fork(snap,binding=binding,directory=str(childroot)) as child:
        child.materialize(binding=binding)
        result=child.state(allow_gather=True)
        if rank == 0: assert np.isclose(np.linalg.norm(result),1)
        # Only one owner advertises a lease: all ranks must refuse GC.
        if rank != comm.Get_size()-1: child.lease_path.unlink()
        try: sim.collect_garbage()
        except CollectiveError: pass
        else: raise AssertionError('Remote lease was ignored')
    sim.collect_garbage()
    # A COMMIT survives coordinator notification failure: repair absent copy.
    (root/f'rank-{rank:04d}/manifests/decision-{snap["snapshot_id"]}.json').unlink()
with DistributedSimulator.resume(config,snap,comm=comm) as resumed:
    result=resumed.state(allow_gather=True)
    if rank == 0: assert np.isclose(np.linalg.norm(result),1)
    assert (root/f'rank-{rank:04d}/manifests/decision-{snap["snapshot_id"]}.json').is_file()
# Fail after the authoritative decision: do not fall back to the previous cut.
with DistributedSimulator.resume(config,snap,comm=comm) as resumed:
    save=checkpoints.atomic_json
    def fail_copy(path,data,**kwargs):
        if rank == comm.Get_size()-1 and Path(path).name.startswith('decision-'):
            raise OSError('injected decision copy failure')
        return save(path,data,**kwargs)
    checkpoints.atomic_json=fail_copy
    try:
        try: resumed.commit_snapshot()
        except CollectiveError: pass
        else: raise AssertionError('Copy failure ignored')
        try: resumed._open()
        except RuntimeError: pass
        else: raise AssertionError('Execution continued after committed copy failure')
    finally: checkpoints.atomic_json=save
with DistributedSimulator.resume(config,comm=comm) as resumed:
    assert resumed.root == snap['root']
# A corrupt newest committed payload must not silently select an older cut.
latest=checkpoints.load_global(root) if rank == 0 else None
latest_id=comm.bcast(latest['snapshot_id'] if rank == 0 else None,root=0)
if rank == 0:
    payload=root/'rank-0000/manifests'/f'payload-{latest_id}.json'
    saved_payload=payload.read_bytes(); payload.write_bytes(b'broken')
comm.Barrier()
try: DistributedSimulator.resume(config,comm=comm)
except CollectiveError: pass
else: raise AssertionError('Newest authoritative decision fell back to old cut')
if rank == 0: payload.write_bytes(saved_payload)
comm.Barrier()
# Prepare-only candidates have no authoritative coordinator decision.
if rank == comm.Get_size()-1:
    prepare=root/f'rank-{rank:04d}/manifests/prepare-{snap["snapshot_id"]}.json'
    prepare.write_text('{}')
comm.Barrier()
try: DistributedSimulator.resume(config,snap,comm=comm)
except CollectiveError: pass
else: raise AssertionError('Corrupt owner prepare accepted')
# A v3 dataset needs a coherent shared tree. A same-path local directory on
# different hosts is not that tree; offline cross-volume migration is outside
# this adapter. Keep the v3 upgrade regression on actual same-host allocations.
if len(set(comm.allgather(MPI.Get_processor_name()))) == 1:
    from dataclasses import replace
    legacy=Path(sys.argv[1])/'legacy-local-volume'
    allowed.append(legacy)
    legacy_config=replace(config,directory=str(legacy),storage_scope='shared')
    with DistributedSimulator(6,legacy_config,comm=comm) as old:
        old.gate('H',(5,)); old.materialize(binding=old.bind({}))
        legacy_snapshot=old.commit_snapshot()
        assert legacy_snapshot['schema_version'] == 3
    local_config=replace(legacy_config,storage_scope='node-local')
    with DistributedSimulator.resume(local_config,legacy_snapshot,comm=comm) as upgraded:
        result=upgraded.state(allow_gather=True)
        if rank == 0: assert np.isclose(np.linalg.norm(result),1)
        assert isinstance(upgraded.registry[upgraded.store_id],list)
        upgrade=upgraded.commit_snapshot()
        assert upgrade['schema_version'] == 4
    with DistributedSimulator.resume(local_config,upgrade,comm=comm) as upgraded:
        assert upgraded.root == upgrade['root']
elif rank == 0:
    print('v3 same-host upgrade SKIP: cross-volume migration requires offline copying', flush=True)
if rank == 0:
    print('phase4 node-local lifecycle PASS',comm.Get_size(),flush=True)
