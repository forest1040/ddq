"""Run the exact Phase 4 runtime on local Mac MPI, never submitting a job."""
import argparse
from dataclasses import asdict
import json
from pathlib import Path
from time import perf_counter

import numpy as np
from mpi4py import MPI
from svsim import (AsyncDistributedSimulator, AsyncPolicy, DistributedConfig,
                   ObservableIR, PauliTerm, ParameterSymbol)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--directory', required=True)
    parser.add_argument('--num-qubits', type=int, default=8)
    parser.add_argument('--block-bits', type=int, default=4)
    parser.add_argument('--budget-kib', type=int, default=256)
    parser.add_argument('--backend', choices=('buffered', 'direct'), default='buffered')
    parser.add_argument('--policy', choices=('S0', 'S1', 'S2'), default='S2')
    parser.add_argument('--gradient', choices=('adjoint', 'recompute', 'parameter-shift'))
    parser.add_argument('--output')
    args = parser.parse_args()
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    root = Path(args.directory)/f'owner-{rank}'
    config = DistributedConfig(str(root), args.block_bits,
                     ram_budget_bytes_per_rank=args.budget_kib*1024,
                     metadata_reserve_bytes=64*1024, tile_bytes=16*1024,
                     backend=args.backend, frame_policy='materialize',
                     disk_quota_bytes=128*1024**2, drain_reserve_bytes=1024**2)
    theta = ParameterSymbol('theta', parameter_id='phase4-example-theta')
    observable = ObservableIR((PauliTerm(0, 1),))
    begin = perf_counter()
    with AsyncDistributedSimulator(args.num_qubits, config, comm=comm,
              policy=AsyncPolicy(swap_policy=args.policy)) as sim:
        sim.gate('RY', (0,), angle=theta)
        sim.gate('RZ', (1,), angle=.12)
        sim.gate('H', (args.num_qubits-1,))
        sim.gate('RZ', (1,), angle=-.12)
        binding = sim.bind({theta: .37})
        started = perf_counter()
        sim.materialize(binding=binding)
        execute = sim.transport.max_value(perf_counter()-started)
        value = sim.expectation(observable)
        np.testing.assert_allclose(value, np.cos(.37), atol=1e-10)
        derivative = sim.gradient(observable, method=args.gradient) if args.gradient else None
        if derivative:
            np.testing.assert_allclose(derivative[theta.parameter_id], -np.sin(.37), atol=1e-9)
        snapshot = sim.commit_snapshot()
        rows = comm.gather({'rank': rank, 'managed_peak_bytes': sim.budget.peak,
                         'io_submitted': sim.io_engine.submitted,
                         'io_peak_requests': sim.io_engine.peak_requests,
                         'mpi_peak_requests': sim.transport.peak_requests,
                         'send_payload_bytes': sim.transport.send_bytes,
                         'storage_read_bytes': sim.trace.read_bytes,
                         'storage_write_bytes': sim.trace.write_bytes,
                         'prefetch_hits': getattr(sim.cache, 'prefetch_hits', 0)}, root=0)
        environment = sim.environment
        engine = sim.io_engine.identity
    with AsyncDistributedSimulator.resume(config, snapshot, comm=comm) as restored:
        np.testing.assert_allclose(restored.expectation(observable), value, atol=1e-10)
    elapsed = comm.allreduce(perf_counter()-begin, op=MPI.MAX)
    if rank == 0:
        report = {'schema_version': 1, 'scope': 'same-host' if len({r['host'] for r in environment}) == 1 else 'multi-host',
                  'num_qubits': args.num_qubits, 'block_bits': args.block_bits,
                  'state_bytes': 16*(1 << args.num_qubits), 'ranks': comm.Get_size(),
                  'backend': args.backend, 'io_engine': engine, 'policy': args.policy,
                  'execution_seconds_max': execute, 'end_to_end_seconds_max': elapsed,
                  'expectation': value, 'gradient': derivative, 'rank_metrics': rows,
                  'environment': environment, 'checkpoint_schema': snapshot['schema_version']}
        raw = json.dumps(report, ensure_ascii=False, indent=2)+'\n'
        if args.output:
            Path(args.output).write_text(raw)
        print(raw)


if __name__ == '__main__':
    main()
