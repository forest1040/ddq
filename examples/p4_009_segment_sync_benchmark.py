"""Overhead of the pre-publication segment fsync (P4-009), measured in-process.

Workload: 14 qubits, block_bits 8, 2 MiB RAM budget, ``materialize`` + cache flush + store flush, timed with
and without ``FileBlockStore._sync_segments``. The "without" arm monkeypatches that method to a no-op inside this
script only; production code has no switch, and the "without" arm is NOT crash-safe (benchmark use only).

    uv run python examples/p4_009_segment_sync_benchmark.py --repeats 5 --backend buffered
    uv run python examples/p4_009_segment_sync_benchmark.py --repeats 5 --backend direct

``direct`` is O_DIRECT on Linux and F_NOCACHE on macOS. The arms alternate in order to spread drift.
"""
from __future__ import annotations

import argparse
import json
import statistics
import tempfile
import time
from pathlib import Path

from svsim.storage import FileBlockStore, StorageConfig
from svsim.stored_runtime import StoredSimulator


def run_once(directory: Path, *, backend: str, qubits: int, block_bits: int, rounds: int) -> float:
    config = StorageConfig(str(directory), block_bits, ram_budget_bytes=2 * 1024**2, backend=backend,
                           alignment=4096 if backend != 'buffered' else 64, metadata_reserve_bytes=256 * 1024)
    sim = StoredSimulator(qubits, config)
    try:
        start = time.perf_counter()
        for layer in range(rounds):
            for q in range(qubits):
                if (q + layer) % 2:
                    sim.gate('H', (q,))
                else:
                    sim.gate('RX', (q,), angle=0.3 + 0.01 * layer)
            sim.materialize()
            sim.cache.flush()
            sim.store.flush()
        return time.perf_counter() - start
    finally:
        sim.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--repeats', type=int, default=3)
    parser.add_argument('--backend', choices=('buffered', 'direct'), default='buffered')
    parser.add_argument('--qubits', type=int, default=14)
    parser.add_argument('--block-bits', type=int, default=8)
    parser.add_argument('--rounds', type=int, default=2)
    parser.add_argument('--work-dir', default=None, help='directory on the volume to test (default: system temp)')
    args = parser.parse_args()
    original = FileBlockStore._sync_segments
    results: dict[str, list[float]] = {'with_sync': [], 'without_sync': []}
    for repeat in range(args.repeats):
        for arm in ('without_sync', 'with_sync') if repeat % 2 == 0 else ('with_sync', 'without_sync'):
            FileBlockStore._sync_segments = original if arm == 'with_sync' else (lambda self, segments=None: None)
            try:
                with tempfile.TemporaryDirectory(dir=args.work_dir) as tmp:
                    seconds = run_once(Path(tmp) / 'run', backend=args.backend, qubits=args.qubits,
                                       block_bits=args.block_bits, rounds=args.rounds)
            finally:
                FileBlockStore._sync_segments = original
            results[arm].append(seconds)
    median = {arm: statistics.median(values) for arm, values in results.items()}
    print(json.dumps({'backend': args.backend, 'qubits': args.qubits, 'block_bits': args.block_bits,
                      'rounds': args.rounds, 'samples_seconds': results, 'median_seconds': median,
                      'overhead_percent': 100 * (median['with_sync'] / median['without_sync'] - 1)}, indent=2))


if __name__ == '__main__':
    main()
