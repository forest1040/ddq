"""Real MPI tile-transpose fragment-communication oracle and bounded check."""
import sys
import numpy as np
from mpi4py import MPI
from svsim import DistributedConfig, ObservableIR, PauliTerm
from svsim.async_scheduler import AsyncDistributedSimulator, AsyncPolicy
from svsim.cache import BufferLease

comm = MPI.COMM_WORLD
rank = comm.Get_rank()


def assert_state(sim, expected):
    actual = sim.state(allow_gather=True)
    error = None
    if rank == 0:
        try: np.testing.assert_allclose(actual, expected, atol=7e-11, rtol=7e-11)
        except Exception as exc: error = str(exc)
    sim.transport.vote(error)


def run_pairs(base, pairs):
    directory = f'{base}/rank-{rank}'
    config = DistributedConfig(directory, 2, ram_budget_bytes_per_rank=2*1024**2,
        metadata_reserve_bytes=128*1024, tile_bytes=64, alignment=64, storage_scope='node-local')
    rng = np.random.default_rng(818)
    initial = rng.normal(size=16) + 1j*rng.normal(size=16)
    initial /= np.linalg.norm(initial)
    policy = AsyncPolicy(io_engine='cooperative', prefetch_blocks=0, lookahead_gates=2)
    with AsyncDistributedSimulator(4, config, comm=comm, initial_state=initial, policy=policy) as sim:
        before_layout = sim.layout.logical_to_physical
        expectation = sim.expectation(ObservableIR((PauliTerm(0, 1),)))
        sim.tile_index_bit_swap(pairs, tile_bytes=64)
        assert_state(sim, initial)
        after = sim.expectation(ObservableIR((PauliTerm(0, 1),)))
        error = None
        if rank == 0:
            try: assert abs(after - expectation) < 1e-9
            except Exception as exc: error = str(exc)
        sim.transport.vote(error)
        sim.tile_index_bit_swap(list(reversed(pairs)), tile_bytes=64)
        assert_state(sim, initial)
        error = None
        if rank == 0:
            try: assert sim.layout.logical_to_physical == before_layout
            except Exception as exc: error = str(exc)
        sim.transport.vote(error)


def run_bounded(base):
    directory = f'{base}/rank-{rank}'
    config = DistributedConfig(directory, 13, ram_budget_bytes_per_rank=1 << 20,
        metadata_reserve_bytes=128*1024, tile_bytes=4096, alignment=64, storage_scope='node-local')
    initial = np.ones(1 << 14, dtype=np.complex128)
    policy = AsyncPolicy(io_engine='cooperative', prefetch_blocks=0, lookahead_gates=2)
    with AsyncDistributedSimulator(14, config, comm=comm, initial_state=initial, policy=policy) as sim:
        sim.cache.flush()
        sim.cache.clear()
        tile_charge = (BufferLease.allocation_cost(4096, config.alignment)
                       + BufferLease.allocation_cost(4096, config.alignment))
        full_block_cost = BufferLease.allocation_cost(sim.store.slot_bytes, config.alignment)
        sim.budget.limit = sim.budget.used + 2*tile_charge
        assert 2*tile_charge < full_block_cost
        sim.tile_index_bit_swap([(13, 0)], tile_bytes=4096)
        sim.budget.limit = 1 << 20
        assert_state(sim, initial)


def main():
    base = sys.argv[1]
    mode = sys.argv[2]
    if mode == 'bounded':
        run_bounded(base)
    else:
        run_pairs(base, eval(mode))
    print(f'OK rank={rank} mode={mode}', flush=True)


main()
