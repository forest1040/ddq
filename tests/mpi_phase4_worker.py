"""Single-host integrated async runtime oracle and owner-local lifecycle checks."""
import argparse
import math
from pathlib import Path
import numpy as np
from mpi4py import MPI
from svsim import DistributedConfig, ObservableIR, PauliTerm, ParameterSymbol
from svsim.async_scheduler import AsyncDistributedSimulator, AsyncPolicy
from svsim.kernels import eager_reference
from svsim.journal import gate_data
from svsim.mpi_transport import CollectiveError
import svsim.distributed_checkpoint as checkpoints

comm = MPI.COMM_WORLD
rank = comm.Get_rank()


def assert_state(sim, expected):
    actual = sim.state(allow_gather=True)
    error = None
    if rank == 0:
        try: np.testing.assert_allclose(actual, expected, atol=7e-11, rtol=7e-11)
        except Exception as exc: error = str(exc)
    sim.transport.vote(error)


def assert_requested_engine(sim, requested):
    identities=comm.allgather(getattr(sim.io_engine,'identity',None))
    assert all(identity==requested for identity in identities), (
        f'io engine request {requested!r} resolved unexpectedly on ranks: {identities!r}')


def configuration(directory, backend, *, nsmall=False):
    return DistributedConfig(str(directory), 6 if nsmall else 2,
                ram_budget_bytes_per_rank=192*1024 if nsmall else 2*1024**2,
                metadata_reserve_bytes=32*1024 if nsmall else 128*1024,
                tile_bytes=4096, backend=backend, disk_quota_bytes=64*1024**2,
                storage_scope='node-local')


def run_state(base, backend, policy):
    directory=base/f'state-owner-{rank}'
    config=configuration(directory, backend)
    rng=np.random.default_rng(409)
    initial=rng.normal(size=64)+1j*rng.normal(size=64)
    initial/=np.linalg.norm(initial)
    matrix=np.array([[1, 0, 0, 0], [0, 0, 1j, 0], [0, 1, 0, 0], [0, 0, 0, -1j]], complex)
    with AsyncDistributedSimulator(6,config,comm=comm,initial_state=initial,policy=policy) as sim:
        assert_requested_engine(sim,policy.io_engine)
        sim.gate('H',(0,)); sim.gate('T',(1,))
        sim.gate('RX',(4,),angle=.27)
        sim.gate('RY',(0,),angle=-.13)
        sim.gate('RZ',(0,),angle=.21)
        sim.gate('MCX',(5,),controls=(0,2,4),control_values=(0,1,0))
        sim.gate('PAULI_ROT',(1,3,5),paulis=('Y','Z','X'),angle=-.31)
        sim.gate('DENSE_UNITARY',(0,5),matrix=matrix)
        # Four partners admit several outstanding sends to one coordinator.
        sim.gate('DENSE_UNITARY',(4,5),matrix=matrix)
        sim.gate('H',(3,)); sim.gate('SWAP',(4,5))
        binding=sim.bind()
        source=tuple(gate_data(g) for g in sim.sequencer.operations)
        expected=eager_reference(sim.sequencer.operations,6,sim.assets,binding,initial_state=initial)
        sim.materialize(binding=binding)
        assert tuple(gate_data(g) for g in sim.sequencer.operations)==source
        assert_state(sim,expected)
        assert sim.io_engine.completed > 0
        if comm.Get_size() > 1:
            assert sim.transport.peak_requests >= 2
        assert sim.budget.peak <= config.ram_budget_bytes_per_rank
        snapshot=sim.commit_snapshot()
        assert snapshot['schema_version']==4
    with AsyncDistributedSimulator.resume(config,snapshot,comm=comm) as sim:
        assert_requested_engine(sim,policy.io_engine)
        assert_state(sim,expected)
        assert sim.policy==policy


def run_prefix_and_gradient(base, backend, policy):
    directory=base/f'prefix-owner-{rank}'
    config=configuration(directory,backend)
    theta=ParameterSymbol('theta',parameter_id='phase4-real-mpi-theta')
    with AsyncDistributedSimulator(6,config,comm=comm,policy=policy) as sim:
        assert_requested_engine(sim,policy.io_engine)
        sim.gate('H',(0,)); sim.gate('RY',(5,),angle=theta)
        binding=sim.bind({theta.parameter_id:.37})
        snapshot=sim.commit_prefix([theta],binding=binding)
        childdirectory=base/f'suffix-owner-{rank}'
        allowed.append(childdirectory)
        with sim.fork(snapshot,binding=binding,directory=str(childdirectory)) as child:
            assert_requested_engine(child,policy.io_engine)
            child.materialize(binding=binding)
            expected=eager_reference(child.sequencer.operations,6,child.assets,binding)
            assert_state(child,expected)
            try: sim.collect_garbage()
            except CollectiveError: pass
            else: raise AssertionError('Active remote prefix lease ignored')
        sim.collect_garbage()
    directory=base/f'gradient-owner-{rank}'
    allowed.append(directory)
    config=configuration(directory,backend)
    with AsyncDistributedSimulator(6,config,comm=comm,policy=policy) as sim:
        sim.gate('H',(0,)); sim.gate('RY',(5,),angle=theta)
        binding=sim.bind({theta.parameter_id:.37})
        sim.materialize(binding=binding)
        expected=eager_reference(sim.sequencer.operations,6,sim.assets,binding)
        observable=ObservableIR((PauliTerm(0,1<<5),))
        np.testing.assert_allclose(sim.expectation(observable),math.cos(.37),atol=1e-10)
        sim.fused_index_bit_swap(((0,5),(1,4)))
        assert_state(sim,expected)
        sim.fused_index_bit_swap(((0,3),(3,5)))
        assert_state(sim,expected)
        assert_requested_engine(sim,policy.io_engine)
        result=sim.gradient(observable,parameters=[theta],method='adjoint')
        assert_requested_engine(sim,policy.io_engine)
        np.testing.assert_allclose(result[theta.parameter_id],-math.sin(.37),atol=2e-9)
        assert_state(sim,expected)


def run_bounded(base, backend, policy):
    directory=base/f'bounded-owner-{rank}'
    config=configuration(directory,backend,nsmall=True)
    n=14 + int(math.log2(comm.Get_size()))
    assert 16*(1<<n)//comm.Get_size()>config.ram_budget_bytes_per_rank
    with AsyncDistributedSimulator(n,config,comm=comm,policy=policy) as sim:
        assert_requested_engine(sim,policy.io_engine)
        sim.gate('RY',(0,),angle=.21)
        sim.gate('RY',(n-1,),angle=.43)
        sim.gate('MCX',(n-2,),controls=(0,n-1),control_values=(0,1))
        sim.materialize()
        observables=[ObservableIR((PauliTerm(0,mask),)) for mask in
                     (1,1<<(n-1),1<<(n-2),0)]
        values=sim.expectation(observables)
        expected=[math.cos(.21),math.cos(.43),
                  1-2*math.cos(.21/2)**2*math.sin(.43/2)**2,1]
        np.testing.assert_allclose(values,expected,atol=2e-10)
        assert sim.budget.peak<=config.ram_budget_bytes_per_rank
        assert sim.io_engine.completed>0
        sim.commit_snapshot()


parser=argparse.ArgumentParser()
parser.add_argument('directory');parser.add_argument('--backend',default='buffered')
parser.add_argument('--io-engine',choices=('worker','cooperative','io_uring'),default='worker')
parser.add_argument('--policy',default='S2');parser.add_argument('--extended',action='store_true')
parser.add_argument('--bounded',action='store_true')
args=parser.parse_args()
base=Path(args.directory)
policy=AsyncPolicy(swap_policy=args.policy,io_engine=args.io_engine,lookahead_gates=8)
if args.io_engine == 'io_uring':
    thread_levels=comm.allgather(MPI.Query_thread())
    assert all(level==MPI.THREAD_SINGLE for level in thread_levels), (
        f'native io_uring MPI requires MPI_THREAD_SINGLE, got {thread_levels!r}')
allowed=[base/f'state-owner-{rank}',base/f'prefix-owner-{rank}',base/f'bounded-owner-{rank}']
original_read=checkpoints._read

def owner_read(path):
    assert any(Path(path).is_relative_to(p) for p in allowed), f'Foreign rank manifest opened: {path}'
    return original_read(path)
checkpoints._read=owner_read
if args.bounded: run_bounded(base,args.backend,policy)
else:
    run_state(base,args.backend,policy)
    if args.extended: run_prefix_and_gradient(base,args.backend,policy)
if rank==0: print('phase4 local MPI PASS',comm.Get_size(),args.policy,args.backend,flush=True)
