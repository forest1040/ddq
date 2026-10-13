"""Measure Qulacs native Pauli gates and a controlled symmetric Trotter slice.

uv run --locked --extra mpi --with qulacs==0.6.14 python \
  docs/slides/benchmark_qulacs_ram.py --output docs/slides/qulacs_ram_measurement.json

Each thread count runs in a fresh process. All quantum states stay in RAM.
The eight synthetic Pauli terms are not molecular integrals or an error certificate.
"""
import argparse
from datetime import datetime, timezone
from hashlib import sha256
import json
import os
from pathlib import Path
import statistics
import subprocess
import sys
import threading
from time import perf_counter, process_time


def verify():
    import numpy as np
    from qulacs import QuantumCircuit, QuantumState
    from qulacs.gate import PauliRotation
    from svsim.pauli_rotation import SignedPauli, apply_pauli_rotation
    rng = np.random.default_rng(20261013)
    vector = rng.normal(size=16)+1j*rng.normal(size=16)
    vector /= np.linalg.norm(vector)
    expected = vector.copy()
    apply_pauli_rotation(expected, SignedPauli(3, 5), .13)
    state = QuantumState(4); state.load(vector)
    PauliRotation([0, 1, 2], [2, 1, 3], -.13).update_quantum_state(state)
    np.testing.assert_allclose(state.get_vector(), expected, atol=1e-12)
    # exp(-i theta |1><1| P/2) = exp(-i theta P/4) exp(+i theta Za P/4).
    state.load(vector); circuit = QuantumCircuit(4)
    circuit.add_gate(PauliRotation([0, 1, 2], [2, 1, 3], -.13/2))
    circuit.add_gate(PauliRotation([0, 1, 2, 3], [2, 1, 3, 3], .13/2))
    circuit.update_quantum_state(state)
    expected = vector.copy(); expected[8:] = 0
    branch = vector.copy(); branch[:8] = 0
    apply_pauli_rotation(branch, SignedPauli(3, 5), .13)
    expected += branch
    np.testing.assert_allclose(state.get_vector(), expected, atol=1e-12)


def child(threads, repetitions, sizes):
    import platform
    import resource
    import numpy as np
    import qulacs
    from qulacs import QuantumCircuit, QuantumState
    from qulacs.gate import PauliRotation
    verify(); rows = []
    for qubits in sizes:
        state = QuantumState(qubits)
        state.set_computational_basis(0)
        terms = [([0], [3]), ([1], [3]), ([qubits-2], [3]),
                 ([0, 1], [1, 1]), ([1, 2], [2, 2]),
                 ([0, qubits-2], [1, 3]),
                 ([0, 2, 4, qubits-2], [1, 2, 2, 1]),
                 ([1, 3, 5, qubits-3], [2, 1, 1, 2])]
        circuit = QuantumCircuit(qubits)
        # Second order: term list followed by reverse, half-angle factors.
        # Each controlled factor uses two native Pauli rotations, 32 gates total.
        indexed_terms = list(enumerate(terms))
        for i, (targets, paulis) in indexed_terms+list(reversed(indexed_terms)):
            angle = .001*(i+1)
            circuit.add_gate(PauliRotation(targets, paulis, -angle/2))
            circuit.add_gate(PauliRotation(targets+[qubits-1], paulis+[3], angle/2))
        local = PauliRotation([0, 1, 2], [2, 1, 3], -.13)
        diagonal = PauliRotation([0, qubits-2], [3, 3], -.13)
        for name, operation, count in (
                ('rotation_local', lambda: local.update_quantum_state(state), 1),
                ('rotation_diagonal', lambda: diagonal.update_quantum_state(state), 1),
                ('controlled_symmetric_trotter_slice', lambda: circuit.update_quantum_state(state), 32)):
            operation(); samples = []
            for _ in range(repetitions):
                cpu = process_time(); start = perf_counter()
                operation()
                samples.append({'wall_seconds': perf_counter()-start,
                                'cpu_seconds': process_time()-cpu})
            norm = state.get_squared_norm()
            if abs(norm-1) > 1e-9: raise RuntimeError(f'norm error {norm}')
            elapsed = statistics.median(x['wall_seconds'] for x in samples)
            rows.append({'qubits': qubits, 'state_bytes': 16*(1 << qubits),
                'threads_requested': threads, 'kernel': name, 'native_rotations_per_call': count,
                'median_seconds': elapsed, 'seconds_per_native_rotation': elapsed/count,
                'amplitude_updates_per_second': count*(1 << qubits)/elapsed,
                'cpu_wall_ratio_median': statistics.median(x['cpu_seconds']/x['wall_seconds'] for x in samples),
                'samples': samples, 'norm': norm})
            print(f'Qulacs {threads} threads {qubits}Q {name}: {elapsed:.6f}s',
                  file=sys.stderr, flush=True)
        del state
    return {'threads_requested': threads, 'rows': rows, 'qulacs': qulacs.__version__,
            'numpy': np.__version__, 'python': platform.python_version(),
            'rss_peak_bytes': resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
            'verification': 'Pauli sign convention and controlled lowering match independent svsim oracle'}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output')
    parser.add_argument('--repetitions', type=int, default=5)
    parser.add_argument('--child-threads', type=int)
    parser.add_argument('--qubits', type=int, nargs='+', default=[20, 25, 29])
    parser.add_argument('--threads', type=int, nargs='+', default=[1, 8, 16, 24])
    args = parser.parse_args()
    if args.repetitions < 3: parser.error('at least three measured trials')
    if any(q < 4 for q in args.qubits): parser.error('at least four qubits')
    if any(t < 1 for t in args.threads): parser.error('positive thread counts required')
    if args.child_threads:
        print(json.dumps(child(args.child_threads, args.repetitions, args.qubits))); return
    if not args.output: parser.error('--output is required')
    from recalibrate_performance import load_sample
    import qulacs_core
    root = Path(__file__).resolve().parents[2]
    script_hash = sha256(Path(__file__).read_bytes()).hexdigest()
    linked = subprocess.check_output(['otool', '-L', qulacs_core.__file__], text=True)
    before = load_sample(); results = []; samples = []
    during = []; stop = threading.Event()
    def monitor():
        while not stop.wait(5): during.append(load_sample())
    thread = threading.Thread(target=monitor, daemon=True); thread.start()
    for threads in args.threads:
        env = dict(os.environ, OMP_NUM_THREADS=str(threads), QULACS_NUM_THREADS=str(threads),
                   OMP_DYNAMIC='FALSE', OPENBLAS_NUM_THREADS='1', MKL_NUM_THREADS='1')
        output = subprocess.check_output([sys.executable, str(Path(__file__).resolve()),
            '--child-threads', str(threads), '--repetitions', str(args.repetitions),
            '--qubits', *map(str,args.qubits)], env=env, text=True)
        result = json.loads(output); results.append(result)
        for row in result['rows']:
            print(threads, row['qubits'], row['kernel'], round(row['median_seconds'], 6),
                  'cpu/wall', round(row['cpu_wall_ratio_median'], 2), flush=True)
        samples.append(load_sample())
    stop.set(); thread.join()
    report = {'schema_version': 1, 'time_utc': datetime.now(timezone.utc).isoformat(),
        'scope': 'one Mac / RAM only / Qulacs native CPU; synthetic terms, not molecular QPE',
        'repetitions': args.repetitions, 'warmup_calls': 1, 'rows': [r for x in results for r in x['rows']],
        'environments': [{k:v for k,v in x.items() if k != 'rows'} for x in results],
        'load_before': before, 'load_between_thread_counts': samples, 'load_after': load_sample(),
        'load_during': during, 'threads_requested': args.threads, 'qubits_measured': args.qubits,
        'openmp_linked': 'libomp' in linked,
        'extension_sha256': sha256(Path(qulacs_core.__file__).read_bytes()).hexdigest(),
        'revision': subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=root, text=True).strip(),
        'script_sha256': script_hash}
    Path(args.output).write_text(json.dumps(report, ensure_ascii=False, indent=2)+'\n')


if __name__ == '__main__': main()
