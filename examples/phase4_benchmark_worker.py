"""Identical small circuit for synchronous/async/S2 local MPI comparison."""
import argparse
import json
import os
from pathlib import Path
from time import perf_counter

from mpi4py import MPI
import numpy as np
from svsim import AsyncDistributedSimulator, AsyncPolicy, DistributedConfig, DistributedSimulator, ObservableIR, PauliTerm


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--runtime', choices=('sync', 'async'), required=True)
    parser.add_argument('--policy', choices=('S0', 'S1', 'S2'), default='S0')
    parser.add_argument('--backend', choices=('buffered', 'direct'), default='buffered')
    parser.add_argument('--directory')
    args = parser.parse_args()
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    base = Path(args.directory or os.environ['SVSIM_JOB_SCRATCH'])
    config = DistributedConfig(str(base/f'owner-{rank}'), 4,
                     ram_budget_bytes_per_rank=256*1024, metadata_reserve_bytes=64*1024,
                     tile_bytes=16*1024, backend=args.backend, frame_policy='materialize',
                     storage_scope='node-local', disk_quota_bytes=64*1024**2,
                     drain_reserve_bytes=1024**2)
    cls = AsyncDistributedSimulator if args.runtime == 'async' else DistributedSimulator
    kwargs = {'policy': AsyncPolicy(swap_policy=args.policy)} if args.runtime == 'async' else {}
    begin = perf_counter()
    with cls(8, config, comm=comm, **kwargs) as sim:
        sim.gate('H', (7,))
        for _ in range(6):
            sim.gate('RY', (0,), angle=.37/6)
            sim.gate('RZ', (1,), angle=.12)
        started = perf_counter()
        sim.materialize(binding=sim.bind())
        execute = sim.transport.max_value(perf_counter()-started)
        value = sim.expectation(ObservableIR((PauliTerm(0, 1),)))
        np.testing.assert_allclose(value, np.cos(.37), atol=1e-10)
        snapshot = sim.commit_snapshot()
        rows = comm.gather({'rank': rank, 'peak': sim.budget.peak,
                           'read_bytes': sim.trace.read_bytes, 'write_bytes': sim.trace.write_bytes,
                           'send_bytes': sim.transport.send_bytes,
                           'io_requests': getattr(getattr(sim, 'io_engine', None), 'submitted', 0)}, root=0)
    with cls.resume(config, snapshot, comm=comm) as resumed:
        np.testing.assert_allclose(resumed.expectation(ObservableIR((PauliTerm(0, 1),))), value, atol=1e-10)
    elapsed = comm.allreduce(perf_counter()-begin, op=MPI.MAX)
    if rank == 0:
        print(json.dumps({'runtime': args.runtime, 'policy': args.policy, 'backend': args.backend,
               'ranks': comm.Get_size(), 'execute_seconds': execute, 'end_to_end_seconds': elapsed,
               'expectation': value, 'rank_metrics': rows, 'state_bytes': 4096,
               'num_qubits': 8, 'block_bits': 4, 'managed_budget_per_rank': 256*1024,
               'scope': 'same-host', 'frame_policy': 'materialize'}))


if __name__ == '__main__':
    main()
