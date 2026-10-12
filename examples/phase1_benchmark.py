"""Reproducible Phase 1 runtime comparisons and JSON reporting.

Run with ``uv run python examples/phase1_benchmark.py --output phase1.json``.
The report includes input import, execution, durable snapshot, streamed export,
and eager-reference evaluation for every scenario.
"""

from __future__ import annotations

import argparse
from collections import Counter
from dataclasses import asdict
import hashlib
import json
import os
from pathlib import Path
import platform
import resource
import shutil
import tempfile
import time
from typing import Any

import numpy as np

from svsim.benchmark import run_benchmarks
from svsim.kernels import eager_reference
from svsim.storage import StorageConfig
from svsim.stored_runtime import StoredSimulator


SCHEMA_VERSION = 1
ERROR_TOLERANCE = 1e-8


def _random_state(num_qubits: int, seed: int) -> np.ndarray:
    rng = np.random.default_rng(seed)
    state = rng.normal(size=1 << num_qubits) + 1j * rng.normal(size=1 << num_qubits)
    state /= np.linalg.norm(state)
    return np.asarray(state, dtype=np.complex128)


def _random_unitary(width: int, seed: int) -> np.ndarray:
    rng = np.random.default_rng(seed)
    matrix = rng.normal(size=(1 << width, 1 << width)) + 1j * rng.normal(size=(1 << width, 1 << width))
    q, r = np.linalg.qr(matrix)
    diagonal = np.diag(r)
    phases = np.ones_like(diagonal)
    nonzero = np.abs(diagonal) > 0
    phases[nonzero] = diagonal[nonzero] / np.abs(diagonal[nonzero])
    return q * phases.conj()[None, :]


def _named_circuit(sim: StoredSimulator) -> None:
    n = sim.num_qubits
    sim.gate("H", (0,))
    sim.gate("RY", (1,), angle=0.37)
    sim.gate("CX", (0, n - 1))
    sim.gate("CZ", (1, n - 1))
    sim.gate("RZ", (2,), angle=-0.21)
    sim.gate("RX", (3,), angle=0.17)
    sim.gate("SWAP", (0, 1))


def _rss_bytes() -> int | None:
    try:
        value = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    except (AttributeError, OSError):
        return None
    return int(value if platform.system() == "Darwin" else value * 1024)


def _trace_metrics(trace) -> dict[str, Any]:
    read_bytes = write_bytes = read_count = write_count = 0
    io_seconds = frame_seconds = compute_seconds = 0.0
    paths: Counter[str] = Counter()
    event_count = 0
    lowered_count = fused_count = execution_gate_count = 0
    approximation_error = 0.0
    for row in trace.iter_events():
        event_count += 1
        metadata = row.get("metadata") or {}
        lowered_count += int(metadata.get('lowered_count',0))
        fused_count += int(metadata.get('fused_count',0))
        execution_gate_count += int(metadata.get('execution_gate_count',0))
        approximation_error += float(metadata.get('approximation_error',0))
        path = metadata.get("path")
        if path:
            paths[str(path)] += 1
        operation = metadata.get("operation")
        nbytes = metadata.get("transfer_bytes", row.get("bytes"))
        if operation == "read":
            read_count += 1
            read_bytes += int(nbytes or 0)
        elif operation == "write":
            write_count += 1
            write_bytes += int(nbytes or 0)
        seconds = float(row.get("transfer_ms") or 0) / 1000
        if str(row.get("performance_class") or "").startswith("storage-"):
            io_seconds += seconds
        elif row.get("performance_class") == "frame":
            frame_seconds += seconds
        elif row.get("performance_class") == "compute":
            compute_seconds += seconds
    return {
        "trace_events": event_count,
        "read_bytes": read_bytes,
        "write_bytes": write_bytes,
        "read_count": read_count,
        "write_count": write_count,
        "io_seconds": io_seconds,
        "frame_seconds": frame_seconds,
        "compute_seconds": compute_seconds,
        "compute_seconds_includes_io_and_frame":True,
        "lowered_count":lowered_count,"fused_count":fused_count,
        "execution_gate_count":execution_gate_count,"approximation_error":approximation_error,
        "path_counts": dict(sorted(paths.items())),
    }


def _run_scenario(*, root: Path, scenario_id: str, comparison: str, num_qubits: int,
                  block_bits: int, ram_budget: int, backend: str, alignment: int,
                  initial_state: np.ndarray, mode: str, execution: str,
                  circuit_kind: str, dense_width: int | None = None,
                  dense_matrix: np.ndarray | None = None,
                  retained_trace_path: Path | None = None, profile=None) -> dict[str, Any]:
    state_path = root / "run"
    export_path = root / "export.state"
    config = StorageConfig(str(state_path), block_bits=block_bits,
        ram_budget_bytes=ram_budget, metadata_reserve_bytes=64 * 1024,
        backend=backend, alignment=alignment)
    cpu_start = time.process_time()
    wall_start = time.perf_counter()
    sim = StoredSimulator(num_qubits, config, initial_state=initial_state,
                          execution=execution, mode=mode,profile=profile)
    try:
        if circuit_kind == "named":
            _named_circuit(sim)
        elif circuit_kind == "dense":
            if dense_width is None or dense_matrix is None:
                raise ValueError("dense scenario needs a width and matrix")
            sim.gate("DENSE_UNITARY", tuple(range(dense_width)), matrix=dense_matrix)
        else:
            raise ValueError(f"unknown circuit kind: {circuit_kind}")

        access = sim.access_plan()
        io_before = sim.trace.read_bytes,sim.trace.write_bytes
        execute_start = time.perf_counter()
        sim.materialize()
        execute_seconds = time.perf_counter()-execute_start
        snapshot = sim.commit_snapshot()
        observed_reads = sim.trace.read_bytes-io_before[0]
        observed_writes = sim.trace.write_bytes-io_before[1]
        sim.export_state(export_path, order="logical")
        actual = np.fromfile(export_path, dtype=np.complex128)
        # The oracle is an independent, eager scalar-indexing reference. The
        # exported runtime state is read from disk after the streaming export.
        expected = eager_reference(sim.sequencer.operations, num_qubits, sim.assets,
                                   sim.binding, initial_state)
        max_error = float(np.max(np.abs(actual - expected), initial=0.0))
        paths = _trace_metrics(sim.trace)
        cache_hits, cache_misses = sim.cache.hits, sim.cache.misses
        managed_peak = sim.budget.peak
        budget_limit = sim.budget.limit
        plan_id = sim.plan.plan_id if sim.plan else None
        plan_version = sim.plan.version if sim.plan else None
        trace_path = str(sim.trace.path)
        circuit_hash = sim.circuit_hash
    finally:
        sim.close()
    if retained_trace_path is not None:
        retained_trace_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(trace_path, retained_trace_path)
        trace_path = str(retained_trace_path)
    wall_seconds = time.perf_counter() - wall_start
    cpu_seconds = time.process_time() - cpu_start
    export_path.unlink(missing_ok=True)
    return {
        "scenario_id": scenario_id,
        "comparison": comparison,
        "mode": mode,
        "execution": execution,
        "circuit_kind": circuit_kind,
        "dense_width": dense_width,
        "num_qubits": num_qubits,
        "block_bits": block_bits,
        "snapshot_id": snapshot.snapshot_id,
        "circuit_hash": circuit_hash,
        "initial_state_sha256": hashlib.sha256(initial_state.tobytes()).hexdigest(),
        "plan_id": plan_id,
        "plan_version": plan_version,
        "profile_id":sim.profile.profile_id,
        "trace_path": trace_path,
        "accuracy": {"max_abs_error": max_error,
                     "tolerance": ERROR_TOLERANCE,
                     "within_budget": max_error <= ERROR_TOLERANCE},
        "metrics": {
            **paths,
            "cache_hits": cache_hits,
            "cache_misses": cache_misses,
            "managed_peak_bytes": managed_peak,
            "managed_budget_bytes": budget_limit,
            "managed_peak_within_budget": managed_peak <= budget_limit,
            "process_peak_rss_bytes": _rss_bytes(),
            "cpu_seconds": cpu_seconds,
            "wall_seconds": wall_seconds,
            "execution_wall_seconds":execute_seconds,
            "access_plan_comparison":{
                "scope":"materialization + snapshot, amplitude I/O only",
                "predicted_read_bytes_lower":access.read_bytes_lower,
                "predicted_read_bytes_upper":access.read_bytes_upper,
                "predicted_write_bytes_lower":access.write_bytes_lower,
                "predicted_write_bytes_upper":access.write_bytes_upper,
                "observed_read_bytes":observed_reads,"observed_write_bytes":observed_writes,
                "within_byte_bounds":observed_reads <= access.read_bytes_upper and observed_writes <= access.write_bytes_upper,
            },
            "includes": ["initial_import", "execution", "snapshot_commit", "streamed_export", "eager_oracle", "error_evaluation"],
        },
    }


def run_comparison(output_path: str | os.PathLike[str], num_qubits: int = 10,
                   block_bits: int = 4, ram_budget: int = 131072,
                   backend: str = "buffered", alignment: int = 64) -> dict[str, Any]:
    """Run same-input named-gate, execution-path, and dense-lowering comparisons."""
    if num_qubits < 4: raise ValueError("num_qubits must be at least 4")
    if not 1 <= block_bits <= num_qubits: raise ValueError("block_bits must be in [1, num_qubits]")
    if backend == "direct" and alignment < 1: raise ValueError("alignment must be positive")
    if ram_budget <= 64 * 1024: raise ValueError("ram_budget must exceed the 64 KiB metadata reserve")
    if backend not in {"buffered", "direct"}: raise ValueError("backend must be buffered or direct")
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    initial_state = _random_state(num_qubits, 20261012)
    scenarios: list[tuple[str, str, str, str, int | None, np.ndarray | None]] = []
    for mode in ("eager", "batching", "deferred-no-frame", "per-block-frame"):
        scenarios.append((f"named-{mode}", "named-modes", mode, "auto", None, None))
    for execution in ("auto", "fallback"):
        scenarios.append((f"named-{execution}", "named-execution", "per-block-frame", execution, None, None))
    dense_matrices = {width: _random_unitary(width, 7000 + width) for width in (3, 4)}
    for width in (3, 4):
        for execution in ("auto", "structured-exact"):
            scenarios.append((f"dense-{width}q-{execution}", f"dense-{width}q",
                              "per-block-frame", execution, width, dense_matrices[width]))

    slot_bytes = ((16 * (1 << block_bits) + alignment - 1) // alignment) * alignment
    with tempfile.TemporaryDirectory(prefix=f".{output.stem}-calibration-", dir=output.parent) as calibration_dir:
        calibration = run_benchmarks(calibration_dir, block_bytes=slot_bytes, samples=2,
                                     backend=backend, alignment=alignment, warmup=1)
    results = []
    with tempfile.TemporaryDirectory(prefix=f"{output.stem}-runs-", dir=output.parent) as scratch:
        scratch_root = Path(scratch)
        for scenario_id, comparison, mode, execution, dense_width, matrix in scenarios:
            scenario_root = scratch_root / scenario_id
            scenario_root.mkdir()
            circuit_kind = "dense" if dense_width is not None else "named"
            results.append(_run_scenario(root=scenario_root, scenario_id=scenario_id,
                comparison=comparison, num_qubits=num_qubits, block_bits=block_bits,
                ram_budget=ram_budget, backend=backend, alignment=alignment,
                initial_state=initial_state, mode=mode, execution=execution,
                circuit_kind=circuit_kind, dense_width=dense_width, dense_matrix=matrix,
                retained_trace_path=output.parent / f"{output.stem}-traces" / f"{scenario_id}.jsonl",profile=calibration.profile))

    report = {
        "schema_version": SCHEMA_VERSION,
        "configuration": {"num_qubits": num_qubits, "block_bits": block_bits,
            "ram_budget_bytes": ram_budget, "backend": backend, "alignment": alignment,
            "precision": "complex128", "initial_state_seed": 20261012,
            "dense_seeds": {"3": 7003, "4": 7004}, "error_tolerance": ERROR_TOLERANCE},
        "calibration": {"profile_id": calibration.profile.profile_id,
            "profile_version": calibration.profile.version,
            "warmup_count": calibration.warmup_count,
            "measurements": [asdict(sample) for sample in calibration.measurements]},
        "scenarios": results,
    }
    output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return report


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, help="JSON report output path")
    parser.add_argument("--num-qubits", type=int, default=10)
    parser.add_argument("--block-bits", type=int, default=4)
    parser.add_argument("--ram-budget", type=int, default=131072)
    parser.add_argument("--backend", choices=("buffered", "direct"), default="buffered")
    parser.add_argument("--alignment", type=int)
    args = parser.parse_args(argv)
    alignment = args.alignment if args.alignment is not None else (4096 if args.backend == "direct" else 64)
    report = run_comparison(args.output, args.num_qubits, args.block_bits,
                            args.ram_budget, args.backend, alignment)
    failures = [row["scenario_id"] for row in report["scenarios"]
                if not row["accuracy"]["within_budget"] or not row["metrics"]["managed_peak_within_budget"]]
    print(json.dumps({"output": str(Path(args.output)), "scenarios": len(report["scenarios"]),
                      "failed_scenarios": failures}, sort_keys=True))
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
