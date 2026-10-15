"""Pre-calibrate the executor-class profile that prices P4-004 layout migration candidates.

Run under MPI with the same rank count, I/O policy and backend as the campaign that will use it:

    mpiexec -n 2 python examples/p4_004_layout_migration.py --output PROFILE_DIR

Each rank runs the campaign's MPI recipe (RX on the rank-bit qubit q3) under
the current layout and under every layout the candidate generator would
propose for it, and repeats the physical bit swap itself. Only classes and swaps
that pass the rolling held-out certification are exported, one
``executor-profile-rank<r>.json`` per rank (costs are rank-local).
"""
import argparse
import json
import shutil
import tempfile
from pathlib import Path

from mpi4py import MPI

from svsim import AsyncDistributedSimulator, AsyncPolicy, DistributedConfig
from svsim.executor_profile import export_profile, merge


def config(path, args):
    return DistributedConfig(str(path), 2, ram_budget_bytes_per_rank=args.ram_bytes, metadata_reserve_bytes=128 * 1024,
                             tile_bytes=4096, backend=args.backend, frame_policy='materialize',
                             disk_quota_bytes=args.disk_bytes, storage_scope='node-local')


def policy():
    # Must equal the campaign's policy: classes carry the swap policy and engine.
    return AsyncPolicy(io_engine='cooperative', io_workers=1, prefetch_blocks=0, lookahead_gates=4, cooldown_seconds=0)


def run(comm, directory, args, *, swap=None, gates=0, swaps=0):
    with AsyncDistributedSimulator(4, config(directory, args), comm=comm, policy=policy()) as sim:
        sim.runtime.configure_measurements(max_bytes=args.collector_bytes)
        if swap is not None and gates:
            sim.physical_index_bit_swap(*swap)
        for _ in range(gates): sim.gate('RX', (3,), angle=.025)
        if gates: sim.materialize(binding=sim.bind())
        for _ in range(swaps): sim.physical_index_bit_swap(*swap)
        return export_profile(sim)


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument('--output', required=True)
    parser.add_argument('--gates', type=int, default=40)
    parser.add_argument('--swaps', type=int, default=16)
    parser.add_argument('--local-bit', type=int, default=0, help='local physical bit the candidate swaps with q3')
    parser.add_argument('--backend', default='buffered', choices=('buffered', 'direct'))
    parser.add_argument('--ram-bytes', type=int, default=8 * 1024**2)
    parser.add_argument('--disk-bytes', type=int, default=256 * 1024**2)
    parser.add_argument("--collector-bytes", type=int, default=256 * 1024)
    parser.add_argument("--scratch", default=None)
    args = parser.parse_args()
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    scratch = Path(comm.bcast(args.scratch or tempfile.mkdtemp(prefix='p4004-') if rank == 0 else None, root=0))
    pair = (3, args.local_bit)
    try:
        documents = [run(comm, scratch / f'current-{rank}', args, gates=args.gates),
                     run(comm, scratch / f'migrated-{rank}', args, swap=pair, gates=args.gates),
                     run(comm, scratch / f'swap-{rank}', args, swap=pair, swaps=args.swaps)]
    finally:
        comm.Barrier()
        if rank == 0 and args.scratch is None: shutil.rmtree(scratch, ignore_errors=True)
    profile = merge(documents)
    output = Path(args.output)
    output.mkdir(parents=True, exist_ok=True)
    (output / f'executor-profile-rank{rank}.json').write_text(json.dumps(profile, indent=1, sort_keys=True))
    summary = comm.gather({'rank': rank, 'profile_id': profile['profile_id'], 'rows': len(profile['rows']),
                           'operations': sorted({json.loads(r['class_key']).get('operation') for r in profile['rows']})})
    if rank == 0: print(json.dumps({'kind': 'executor-profile-calibration', 'ranks': summary}, indent=1))


if __name__ == '__main__':
    main()
