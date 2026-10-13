"""Mac-local shared-volume admission across independent datasets and a fork."""
import argparse
from pathlib import Path
import platform
import numpy as np
from mpi4py import MPI

from svsim import AsyncDistributedSimulator, AsyncPolicy, DistributedConfig, ParameterSymbol, ObservableIR, PauliTerm
from svsim.disk_budget import DiskAdmissionError
from svsim.kernels import eager_reference


parser=argparse.ArgumentParser()
parser.add_argument('scratch')
parser.add_argument('--backend',default='buffered')
parser.add_argument('--mismatched-rank-ledger',action='store_true')
args=parser.parse_args()
comm=MPI.COMM_WORLD;rank=comm.Get_rank()
if len(set(comm.allgather(platform.node())))!=1:
    raise ValueError('This validation requires a same-host communicator')
base=Path(args.scratch).resolve()
ledger=base/'host-ledger.sqlite3'
if args.mismatched_rank_ledger:
    ledger=base/f'host-ledger-{rank}.sqlite3'
policy=AsyncPolicy(io_engine='cooperative',io_workers=1,prefetch_blocks=0,swap_policy='S2')
def config(name):
    return DistributedConfig(str(base/f'{name}-owner-{rank}'),2,backend=args.backend,
        ram_budget_bytes_per_rank=2*1024**2,tile_bytes=4096,frame_policy='materialize',
        storage_scope='node-local',disk_quota_bytes=64*1024**2,
        disk_admission_ledger=str(ledger))

if args.mismatched_rank_ledger:
    from svsim.mpi_transport import CollectiveError
    try:
        with AsyncDistributedSimulator(4,config('refused'),comm=comm,policy=policy):
            raise AssertionError('different same-volume ledgers were accepted')
    except CollectiveError as exc:
        assert 'same disk admission ledger' in str(exc)
    assert not list(base.glob('host-ledger-*.sqlite3'))
else:
    theta=ParameterSymbol('coordinator-theta','coordinator-theta')
    with AsyncDistributedSimulator(4,config('a'),comm=comm,policy=policy) as a:
        a.gate('H',(3,));a.gate('RY',(0,),angle=theta)
        binding=a.bind({theta:.37})
        prefix=a.commit_prefix([theta],binding=binding)
        with AsyncDistributedSimulator(4,config('b'),comm=comm,policy=policy) as b:
            b.gate('X',(0,));b.materialize(binding=b.bind())
            with a.fork(prefix,binding=binding) as child:
                assert child.disk_budget._ledger_path==a.disk_budget._ledger_path==b.disk_budget._ledger_path
                assert child.config.disk_admission_ledger==str(ledger)
                assert child.disk_budget.snapshot()['shared_policy'] is True
                child.materialize(binding=binding)
                state=child.state(allow_gather=True)
                error=None
                if rank==0:
                    try:
                        expected=eager_reference(a.sequencer.operations,4,a.assets,binding)
                        np.testing.assert_allclose(state,expected,atol=1e-10,rtol=1e-10)
                    except Exception as exc:error=str(exc)
                child.transport.vote(error)
                observable=ObservableIR((PauliTerm(0,1),))
                assert abs(child.expectation(observable)-np.cos(.37))<1e-10
                gradient=child.gradient(observable,parameters=[theta],method='adjoint')
                assert abs(gradient[theta.parameter_id]+np.sin(.37))<1e-9
                # A caller fork has no captured full-circuit origin. Exercise
                # parameter-shift through the original runtime, whose internally
                # created shift forks must also use this same coordinator.
                a.materialize(binding=binding)
                shifted=a.gradient(observable,parameters=[theta],method='parameter-shift')
                assert abs(shifted[theta.parameter_id]+np.sin(.37))<1e-9
                assert a.disk_budget.snapshot()['registered_root_count']==2*comm.Get_size()
                child.drain();a.drain();b.drain();comm.Barrier()
                token=None
                if rank==0:token=a.disk_budget.reserve(8*1024**2,operation='coordinator-validation')
                token=comm.bcast(token,root=0)
                assert b.disk_budget.reservation(token)['bytes']==8*1024**2
                assert child.disk_budget.reserved_bytes==a.disk_budget.reserved_bytes
                try:
                    if rank==0:
                        room=(a.config.disk_quota_bytes-a.disk_budget._usage()-a.disk_budget.bootstrap_bytes
                              -a.disk_budget.reserved_bytes-a.config.drain_reserve_bytes)
                        assert room>0
                        try:b.disk_budget.reserve(room+1)
                        except DiskAdmissionError:pass
                        else:raise AssertionError('combined quota was exceeded by one byte')
                finally:
                    if rank==0:a.disk_budget.release(token)
                    comm.Barrier()
                assert child.disk_budget.reservation(token) is None
                assert not child.disk_budget.strict_capacity_guarantee

if rank==0:print('shared-volume coordinator validation passed',flush=True)
