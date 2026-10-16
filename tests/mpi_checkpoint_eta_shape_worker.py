"""Verify that rank-local checkpoint observations use the same payload shape."""
import sys
from pathlib import Path

from mpi4py import MPI

from svsim import AsyncDistributedSimulator, AsyncPolicy, DistributedConfig
from svsim.remaining_work import checkpoint_condition
from svsim.runtime_observed_costs import _class

comm = MPI.COMM_WORLD
root = Path(sys.argv[1]) / 'checkpoint-shape'
config = DistributedConfig(str(root), 2, ram_budget_bytes_per_rank=8 * 1024**2,
                           storage_scope='node-local', disk_quota_bytes=64 * 1024**2)
policy = AsyncPolicy(io_engine='cooperative', swap_policy='S2', lookahead_gates=2,
                     prefetch_blocks=0)
with AsyncDistributedSimulator(4, config, comm=comm, policy=policy) as sim:
    sim.runtime.configure_measurements(max_bytes=512 * 1024)
    sim.gate('H', (0,))
    sim.materialize(frontier=0, binding=sim.bind())
    sim.commit_snapshot()
    sim.commit_snapshot()
    sim.commit_snapshot()

    current = checkpoint_condition(sim)
    assert all(shape == current for shape in comm.allgather(current))
    shapes = {_class(row.condition) for row in sim.runtime.measurements.cost_observations()
              if row.stage == 'checkpoint-commit'}
    assert all(other == shapes for other in comm.allgather(shapes))
if comm.rank == 0:
    print({'kind': 'checkpoint-eta-payload-shape', 'ranks': comm.size, 'pass': True}, flush=True)
