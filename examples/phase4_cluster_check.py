"""Small exact MPI checks and JSON report; no SSH or job submission."""
import argparse
import ctypes.util
import json
import math
from pathlib import Path
import resource
import socket
import statistics
import sys
from time import perf_counter

from mpi4py import MPI
import numpy as np
from svsim import (AsyncDistributedSimulator, AsyncPolicy, DistributedConfig,
                   ObservableIR, PauliTerm, ParameterBatchExecutor, ParameterSymbol)
from svsim.cache import BufferLease, MemoryBudget
from svsim.kernels import eager_reference
from svsim.mpi_progress import MPIProgressEngine


def observable_value(state, observable):
    total = 0j
    for term in observable.terms:
        phase = 1j ** (term.x_mask & term.z_mask).bit_count()
        for i, value in enumerate(state):
            total += term.coefficient*state[i ^ term.x_mask].conjugate()*phase*(
                -1 if (i & term.z_mask).bit_count() & 1 else 1)*value
    return float(total.real)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--directory', required=True)
    parser.add_argument('--output', required=True)
    parser.add_argument('--expected-hosts', type=int, required=True)
    parser.add_argument('--backend', choices=('buffered', 'direct'), default='direct')
    args = parser.parse_args()
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    hosts = comm.allgather(socket.gethostname())
    if len(set(hosts)) != args.expected_hosts:
        raise RuntimeError(f'Expected {args.expected_hosts} hosts, got {sorted(set(hosts))}')
    directory = Path(args.directory)/f'owner-{rank}'
    config = DistributedConfig(str(directory/'state'), 2,
        ram_budget_bytes_per_rank=8*1024**2, metadata_reserve_bytes=256*1024,
        tile_bytes=4096, backend=args.backend, frame_policy='materialize',
        disk_quota_bytes=1024**3, min_free_bytes=100*1024**3, drain_reserve_bytes=1024**2)
    theta = ParameterSymbol('theta', parameter_id='phase4-cluster-theta')
    observable = ObservableIR((PauliTerm(0, 1<<5), PauliTerm(1, 1, .3)))
    rng = np.random.default_rng(20261013)
    initial = rng.normal(size=64)+1j*rng.normal(size=64)
    initial /= np.linalg.norm(initial)
    begin = perf_counter()
    with AsyncDistributedSimulator(6, config, policy=AsyncPolicy(swap_policy='S2'),
                                   comm=comm, initial_state=initial) as sim:
        sim.gate('H', (0,))
        sim.gate('RY', (5,), angle=theta)
        sim.gate('RY', (3,), controls=(0,2), control_values=(0,1), angle=-theta)
        sim.gate('PAULI_ROT', (1,4), paulis=('Y','X'), angle=theta/2)
        binding = sim.bind({theta: .37})
        expected = eager_reference(sim.sequencer.operations, 6, sim.assets, binding, initial_state=initial)
        expected_value = observable_value(expected, observable)
        epsilon = 2e-6
        derivative = (observable_value(eager_reference(sim.sequencer.operations, 6, sim.assets,
                      binding.shifted(theta.parameter_id, epsilon), initial_state=initial), observable)-
                      observable_value(eager_reference(sim.sequencer.operations, 6, sim.assets,
                      binding.shifted(theta.parameter_id, -epsilon), initial_state=initial), observable))/(2*epsilon)
        actual = sim.expectation(observable, binding=binding)
        np.testing.assert_allclose(actual, expected_value, atol=1e-10)
        sim.fused_index_bit_swap(((0,5),(5,3)))
        np.testing.assert_allclose(sim.expectation(observable), expected_value, atol=1e-10)
        gradients = {}
        for method in ('adjoint', 'recompute', 'parameter-shift'):
            gradients[method] = sim.gradient(observable, parameters=[theta], method=method)[theta.parameter_id]
            np.testing.assert_allclose(gradients[method], derivative, atol=1e-7, rtol=1e-7)
        snapshot = sim.commit_snapshot()
        metrics = comm.gather({'rank':rank, 'host':hosts[rank], 'managed_peak_bytes':sim.budget.peak,
                     'io_peak_requests':sim.io_engine.peak_requests,
                     'mpi_peak_requests':sim.transport.peak_requests,
                     'read_bytes':sim.trace.read_bytes,'write_bytes':sim.trace.write_bytes,
                     'send_bytes':sim.transport.send_bytes}, root=0)
    with AsyncDistributedSimulator.resume(config, snapshot, comm=comm) as restored:
        np.testing.assert_allclose(restored.expectation(observable), expected_value, atol=1e-10)
    batch_config = DistributedConfig(str(directory/'batch'), 2,
        ram_budget_bytes_per_rank=8*1024**2, metadata_reserve_bytes=256*1024,
        backend=args.backend, disk_quota_bytes=1024**3, min_free_bytes=100*1024**3)
    with AsyncDistributedSimulator(4, batch_config, policy=AsyncPolicy(max_active_jobs=2), comm=comm) as template:
        template.gate('H', (3,)); template.gate('RY', (0,), angle=theta)
        bindings = [template.bind({theta:x}) for x in (.2,.7)]
        prefix = template.commit_prefix([theta], binding=bindings[0])
        with ParameterBatchExecutor(template, prefix) as executor:
            batch = executor.run(bindings, observable=ObservableIR((PauliTerm(0,1),)), gradient='parameter-shift')
        for result, x in zip(batch, (.2,.7)):
            np.testing.assert_allclose(result.expectation, math.cos(x), atol=1e-10)
            np.testing.assert_allclose(result.gradient[theta.parameter_id], -math.sin(x), atol=1e-9)
        template.collect_garbage()
    budget = MemoryBudget(8*1024**2)
    transfer_seconds = []
    with MPIProgressEngine(comm, max_requests=2) as engine:
        source, destination = 0, comm.Get_size()-1
        send = BufferLease(budget, 1024**2) if rank == source else None
        recv = BufferLease(budget, 1024**2) if rank == destination else None
        try:
            if send is not None:
                np.frombuffer(send.view, dtype='u1').fill(73)
            for _ in range(3):
                start = perf_counter()
                engine.transfer(send, recv, source, destination, metadata={'case':'1MiB-bounded-transfer'})
                transfer_seconds.append(comm.allreduce(perf_counter()-start, op=MPI.MAX))
                if recv is not None:
                    assert np.frombuffer(recv.view, dtype='u1').min() == 73
        finally:
            for lease in (send, recv):
                if lease is not None:
                    lease.close()
    elapsed = comm.allreduce(perf_counter()-begin, op=MPI.MAX)
    rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss*(1024 if sys.platform.startswith('linux') else 1)
    rss_rows = comm.gather(rss, root=0)
    if rank == 0:
        report = {'schema_version':1, 'scope':'multi-host' if len(set(hosts))>1 else 'same-host',
                  'hosts':hosts, 'ranks':comm.Get_size(), 'backend':args.backend,
                  'elapsed_seconds_max':elapsed, 'expectation':actual, 'expected_expectation':expected_value,
                  'gradients':gradients, 'finite_difference':derivative,
                  'checkpoint_schema':snapshot['schema_version'], 'rank_metrics':metrics,
                  'rss_peak_bytes':rss_rows, 'transfer_bytes':1024**2,
                  'transfer_seconds_max':transfer_seconds,
                  'transfer_median_seconds':statistics.median(transfer_seconds),
                  'mpi_library':MPI.Get_library_version().strip().rstrip('\0'),
                  'mpi_component_request':__import__('os').environ.get('OMPI_MCA_mtl') or __import__('os').environ.get('OMPI_MCA_btl'),
                  'native_liburing_found':bool(ctypes.util.find_library('uring'))}
        output = Path(args.output)
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(report, indent=2)+'\n')
        print(json.dumps(report), flush=True)


if __name__ == '__main__':
    main()
