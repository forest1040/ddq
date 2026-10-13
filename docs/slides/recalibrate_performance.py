"""Mac-local kernel recalibration; no SSH, PBS, or chemistry simulation.

Run through uv with BLAS/OMP thread counts set to one. Load samples use macOS
top/sysctl and contain aggregate counters, not other users' process details.
"""
import argparse
from datetime import datetime, timezone
from hashlib import sha256
import json
import math
import multiprocessing as mp
from pathlib import Path
import platform
import re
import statistics
import subprocess
import threading
from time import perf_counter, process_time

import numpy as np
from svsim.distributed_kernels import pauli_inner
from svsim.pauli_rotation import SignedPauli, apply_pauli_rotation, apply_pauli_pair


def load_sample():
    output = subprocess.check_output(
        ['top', '-l', '2', '-s', '1', '-n', '0'], text=True)
    matches = re.findall(r'CPU usage: ([\d.]+)% user, ([\d.]+)% sys, ([\d.]+)% idle', output)
    user, system, idle = map(float, matches[-1])
    uptime = subprocess.check_output(['uptime'], text=True).strip()
    return {'time_utc': datetime.now(timezone.utc).isoformat(),
            'user_percent': user, 'system_percent': system, 'idle_percent': idle,
            'load_average': uptime.split('load averages:')[-1].strip()}


def worker(bits, kind, repetitions, barrier, queue):
    try:
        n = 1 << bits
        a = np.ones(n, np.complex128); partner = a.copy()
        oa = np.empty_like(a); ob = np.empty_like(a)
        pauli = SignedPauli(3, 5)
        if kind == 'pauli_inner':
            operation = lambda: pauli_inner(a, partner, 3, 5, 0, bits)
            updates = n
        elif kind == 'rotation_local':
            operation = lambda: apply_pauli_rotation(a, pauli, .01, block_bits=bits)
            updates = n
        elif kind == 'rotation_diagonal':
            operation = lambda: apply_pauli_rotation(a, SignedPauli(0, 5), .01, block_bits=bits)
            updates = n
        else:
            operation = lambda: apply_pauli_pair(a, partner, oa, ob,
                SignedPauli((1 << bits) | 3, 5), .01, 0, 1, bits)
            updates = 2*n
        operation()  # One untimed warmup, then matched simultaneous trials.
        samples = []
        for _ in range(repetitions):
            barrier.wait(timeout=120)
            cpu = process_time(); start = perf_counter()
            value = operation()
            elapsed = perf_counter()-start; cpu_elapsed = process_time()-cpu
            samples.append({'wall_seconds': elapsed, 'cpu_seconds': cpu_elapsed})
        if kind == 'pauli_inner':
            np.testing.assert_allclose(value, 0, atol=1e-9)
        elif kind.startswith('rotation_') and kind != 'rotation_pair':
            p = SignedPauli(0, 5) if kind == 'rotation_diagonal' else pauli
            apply_pauli_rotation(a, p, -.01*(repetitions+1), block_bits=bits)
            np.testing.assert_allclose(a, 1, atol=1e-10)
        else:
            indices = np.arange(n, dtype=np.int64)
            parity = np.array([((int(i) ^ 3) & 5).bit_count() for i in indices])
            # Both all-one inputs: one Y gives phase i*(-1)^parity.
            expected = math.cos(.005)+math.sin(.005)*np.where(parity & 1, -1, 1)
            np.testing.assert_allclose(oa, expected, atol=1e-10)
            np.testing.assert_allclose(ob, expected, atol=1e-10)
        queue.put({'updates_per_call': updates, 'samples': samples})
    except BaseException as exc:
        barrier.abort()
        queue.put({'error': repr(exc)})


def measure(bits, kind, processes, repetitions):
    context = mp.get_context('spawn')
    barrier = context.Barrier(processes); queue = context.Queue()
    children = [context.Process(target=worker,
        args=(bits, kind, repetitions, barrier, queue)) for _ in range(processes)]
    for child in children: child.start()
    results = [queue.get(timeout=180) for _ in children]
    for child in children:
        child.join(timeout=30)
        if child.is_alive(): child.terminate(); child.join(); raise RuntimeError('worker timeout')
        if child.exitcode: raise RuntimeError(f'worker exit {child.exitcode}')
    if any('error' in result for result in results): raise RuntimeError(str(results))
    elapsed = [max(result['samples'][i]['wall_seconds'] for result in results)
               for i in range(repetitions)]
    rates = [sum(result['updates_per_call'] for result in results)/seconds for seconds in elapsed]
    ratios = [sample['cpu_seconds']/sample['wall_seconds']
              for result in results for sample in result['samples']]
    return {'block_bits': bits, 'kernel': kind, 'processes': processes,
            'aggregate_updates_per_second_median': statistics.median(rates),
            'aggregate_updates_per_second_min': min(rates),
            'aggregate_updates_per_second_max': max(rates),
            'wall_seconds_rank_max': elapsed, 'cpu_wall_ratio_median': statistics.median(ratios),
            'cpu_wall_ratio_min': min(ratios), 'workers': results}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', required=True)
    parser.add_argument('--repetitions', type=int, default=7)
    args = parser.parse_args()
    if args.repetitions < 5: parser.error('at least five measured trials')
    root = Path(__file__).resolve().parents[2]
    hardware = {key: subprocess.check_output(['sysctl', '-n', key], text=True).strip()
                for key in ('hw.model', 'hw.ncpu', 'hw.memsize',
                            'hw.perflevel0.physicalcpu', 'hw.perflevel1.physicalcpu')}
    before = load_sample(); during = []; stop = threading.Event()
    def monitor():
        while not stop.wait(5): during.append(load_sample())
    thread = threading.Thread(target=monitor); thread.start()
    rows = []
    try:
        for bits in (16, 20):
            for kind in ('pauli_inner', 'rotation_local', 'rotation_diagonal', 'rotation_pair'):
                row = measure(bits, kind, 1, args.repetitions); rows.append(row)
                print(bits, kind, 1, round(row['aggregate_updates_per_second_median']), flush=True)
        for processes in (2, 4, 8, 16):
            for kind in ('pauli_inner', 'rotation_local', 'rotation_pair'):
                row = measure(16, kind, processes, args.repetitions); rows.append(row)
                print(16, kind, processes, round(row['aggregate_updates_per_second_median']), flush=True)
    finally:
        stop.set(); thread.join()
    report = {'schema_version': 1, 'time_utc': datetime.now(timezone.utc).isoformat(),
              'scope': 'Mac local kernels; excludes simulator/MPI/SSD/Frame/chemistry',
              'hardware': hardware, 'platform': platform.platform(), 'python': platform.python_version(),
              'numpy': np.__version__, 'revision': subprocess.check_output(
                  ['git', 'rev-parse', 'HEAD'], cwd=root, text=True).strip(),
              'lock_sha256': sha256((root/'uv.lock').read_bytes()).hexdigest(),
              'script_sha256': sha256(Path(__file__).read_bytes()).hexdigest(),
              'repetitions': args.repetitions, 'warmup_calls_per_worker': 1,
              'thread_limit': 1, 'load_before': before, 'load_during': during,
              'load_after': load_sample(), 'rows': rows}
    destination = Path(args.output); destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(json.dumps(report, ensure_ascii=False, indent=2)+'\n')


if __name__ == '__main__': main()
