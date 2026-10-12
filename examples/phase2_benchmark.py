"""Compare Phase 2 frame policies against a complex eager reference.

Example: ``uv run python examples/phase2_benchmark.py --output phase2.json``.
Each policy uses a fresh temporary catalog; export/readback is timed and
counted separately from execution.
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
import shutil
import sys
import tempfile
from time import perf_counter
from typing import Any

import numpy as np

try:
    import resource
except ImportError:  # pragma: no cover - Windows has no resource module
    resource = None

from svsim.frame_benchmark import FrameCalibration, calibrate_frames
from svsim.kernels import eager_reference
from svsim.parameters import ExactPi
from svsim.storage import StorageConfig
from svsim.stored_runtime import StoredSimulator


POLICIES = (
    ("materialize", "materialize", "identity"),
    ("keep-input-a", "keep", "input-a"),
    ("keep-input-b", "keep", "input-b"),
    ("auto", "auto", "auto"),
)


def _random_state(num_qubits: int, seed: int) -> np.ndarray:
    rng = np.random.default_rng(seed)
    state = rng.normal(size=1 << num_qubits) + 1j * rng.normal(size=1 << num_qubits)
    state /= np.linalg.norm(state)
    return np.asarray(state, dtype=np.complex128)


def _add_circuit(sim: StoredSimulator) -> None:
    n = sim.num_qubits
    sim.gate("H", (0,))
    sim.gate("H", (1,))
    sim.gate("H", (2,), controls=(n - 1,))
    sim.gate("T", (1,))
    sim.gate("RZ", (2,), angle=ExactPi(1, 4))
    sim.gate("H", (n - 1,))
    sim.gate("PAULI_ROT", (0, 1), paulis=("Y", "Z"), angle=ExactPi(1, 4))
    sim.gate("PAULI_ROT", (0, n - 1), paulis=("Y", "X"), angle=ExactPi(1, 3))
    # Upper-only involutions exercise symbolic permutation roots.
    sim.gate("X", (n - 2,))
    sim.gate("CX", (n - 2, n - 1))
    sim.gate("SWAP", (n - 2, n - 1))


def _execution_counters(trace) -> tuple[int, int]:
    return trace.read_bytes, trace.write_bytes


def _frame_decisions(trace) -> list[dict[str, Any]]:
    decisions = []
    for event in trace.iter_events():
        metadata = event.get("metadata") or {}
        if metadata.get("decision"):
            decisions.append(metadata["decision"])
    return decisions


def _rss_high_water_bytes() -> int | None:
    if resource is None:
        return None
    try:
        value = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    except (AttributeError, OSError):
        return None
    return int(value if platform.system() == "Darwin" else value * 1024)


def _environment() -> dict[str, Any]:
    try:
        page_size = int(os.sysconf("SC_PAGE_SIZE"))
    except (AttributeError, OSError, ValueError):
        page_size = None
    return {
        "platform": platform.platform(),
        "machine": platform.machine(),
        "processor": platform.processor(),
        "cpu_count": os.cpu_count(),
        "python_version": sys.version.split()[0],
        "numpy_version": np.__version__,
        "page_size_bytes": page_size,
        "process_rss_high_water_bytes": _rss_high_water_bytes(),
        "process_rss_scope": "process-global maximum resident set size; not per-case",
    }


def _run_case(name: str, frame_policy: str, alignment_policy: str, *,
              num_qubits: int, backend: str, initial_state: np.ndarray,
              calibration: FrameCalibration, workdir: Path,
              trace_directory: Path | None = None) -> dict[str, Any]:
    temp = tempfile.TemporaryDirectory(prefix=f"phase2-{name}-", dir=workdir)
    try:
        alignment = 4096 if backend == "direct" else 64
        block_bits = calibration.block_bits
        ram_budget = 256 << 10 if num_qubits == 16 else 1 << 20
        metadata_reserve = 128 << 10
        config = StorageConfig(str(Path(temp.name) / "catalog"), block_bits=block_bits,
                               ram_budget_bytes=ram_budget, metadata_reserve_bytes=metadata_reserve,
                               backend=backend, alignment=alignment)
        end_to_end_start = perf_counter()
        import_start = perf_counter()
        sim = StoredSimulator(num_qubits, config, initial_state=initial_state,
                              frame_policy=frame_policy, alignment_policy=alignment_policy,
                              frame_profile=calibration)
        import_seconds = perf_counter() - import_start
        trace_source = sim.trace.path
        try:
            flush_import_start = perf_counter()
            sim.cache.flush()
            sim.cache.clear()
            import_flush_seconds = perf_counter() - flush_import_start
            import_counters = _execution_counters(sim.trace)
            _add_circuit(sim)
            binding = sim.bind()
            compile_seconds = perf_counter() - import_start - import_seconds - import_flush_seconds
            reference_start = perf_counter()
            expected = eager_reference(sim.sequencer.operations, num_qubits, sim.assets,
                                       binding, initial_state=initial_state)
            reference_seconds = perf_counter() - reference_start
            plan = sim.access_plan(binding=binding)
            counter_start = _execution_counters(sim.trace)
            start = perf_counter()
            sim.materialize(binding=binding)
            execution_seconds = perf_counter() - start
            counter_after_execution = _execution_counters(sim.trace)
            execution_reads = counter_after_execution[0] - counter_start[0]
            execution_writes = counter_after_execution[1] - counter_start[1]

            snapshot_start = perf_counter()
            sim.commit_snapshot()
            snapshot_seconds = perf_counter() - snapshot_start
            counter_after_snapshot = _execution_counters(sim.trace)
            snapshot_reads = counter_after_snapshot[0] - counter_after_execution[0]
            snapshot_writes = counter_after_snapshot[1] - counter_after_execution[1]

            # Export and readback are deliberately outside the execution interval.
            exported_path = Path(temp.name) / "logical-state.bin"
            export_start = perf_counter()
            sim.export_state(exported_path, binding=binding)
            counter_after_export = _execution_counters(sim.trace)
            export_reads = counter_after_export[0] - counter_after_snapshot[0]
            export_writes = counter_after_export[1] - counter_after_snapshot[1]
            actual = np.fromfile(exported_path, dtype="<c16")
            export_seconds = perf_counter() - export_start
            error = float(np.max(np.abs(actual - expected)))
            norm = float(np.vdot(actual, actual).real)
            decisions = _frame_decisions(sim.trace)
            state_bytes = (1 << num_qubits) * 16
            initial_hash = hashlib.sha256(initial_state.astype("<c16", copy=False).tobytes()).hexdigest()
            return {
                "policy": name,
                "frame_policy": frame_policy,
                "alignment_policy": alignment_policy,
                "bound_profile_id": calibration.profile_id,
                "circuit_hash": sim.circuit_hash,
                "initial_state_sha256": initial_hash,
                "seed": 20261013,
                "state_bytes": state_bytes,
                "caller_owned_reference_arrays": ["initial_state", "eager_expected", "exported_actual"],
                "caller_owned_reference_bytes": 3 * state_bytes,
                "reference_arrays_outside_managed_budget": True,
                "initial_import_seconds": import_seconds,
                "import_cache_flush_seconds": import_flush_seconds,
                "gate_registration_seconds": compile_seconds,
                "compile_seconds": compile_seconds,
                "eager_reference_seconds": reference_seconds,
                "execution_seconds": execution_seconds,
                "snapshot_seconds": snapshot_seconds,
                "export_readback_seconds": export_seconds,
                "end_to_end_seconds": perf_counter() - end_to_end_start,
                "managed_peak_bytes": sim.budget.peak,
                "metadata_frame_cache_bytes": sim.phase2.memo_bytes,
                "metadata_aux_cache_bytes": sim.phase2.aux_bytes,
                "ram_budget_bytes": ram_budget,
                "metadata_reserve_bytes": metadata_reserve,
                "import_read_bytes": import_counters[0],
                "import_write_bytes": import_counters[1],
                "execution_read_bytes": execution_reads,
                "execution_write_bytes": execution_writes,
                "snapshot_read_bytes": snapshot_reads,
                "snapshot_write_bytes": snapshot_writes,
                "export_read_bytes": export_reads,
                "export_write_bytes": export_writes,
                "export_bytes": exported_path.stat().st_size,
                "norm": norm,
                "norm_error": abs(norm - 1.0),
                "max_complex_error": error,
                "frame_plan_decisions": decisions,
                "frame_plan_count": len(decisions),
                "planned_read_bytes_upper": plan.read_bytes_upper,
                "planned_write_bytes_upper": plan.write_bytes_upper,
                "trace_path": str(trace_directory / f"{name}.jsonl") if trace_directory else None,
                "process_rss_high_water_bytes": _rss_high_water_bytes(),
                "frame_gate_seconds": calibration.frame_gate_seconds,
                "pauli_rotation_seconds": calibration.pauli_rotation_seconds,
                "conjugation_seconds": calibration.conjugation_seconds,
                "synthesis_seconds": calibration.synthesis_seconds,
                "tableau_update_seconds": calibration.tableau_update_seconds,
            }
        finally:
            sim.close()
            if trace_directory is not None:
                trace_directory.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(trace_source, trace_directory / f"{name}.jsonl")
    finally:
        temp.cleanup()


def run_phase2_benchmark(*, num_qubits: int = 6, backend: str = "buffered",
                         calibration: FrameCalibration | None = None,
                         repetitions: int = 3, workdir: str | Path | None = None,
                         trace_directory: str | Path | None = None) -> dict[str, Any]:
    if num_qubits not in (6, 8, 16):
        raise ValueError("num_qubits must be 6, 8, or 16")
    if backend not in {"buffered", "direct"}:
        raise ValueError("backend must be buffered or direct")
    block_bits = 6 if num_qubits == 16 else 3
    calibration = calibration or calibrate_frames(block_bits=block_bits, repetitions=repetitions)
    if calibration.block_bits != block_bits:
        raise ValueError(f"calibration must match the benchmark's block_bits={block_bits}")
    base = Path(workdir) if workdir is not None else Path(tempfile.gettempdir())
    base.mkdir(parents=True, exist_ok=True)
    initial = _random_state(num_qubits, seed=20261013)
    cases = [_run_case(name, frame_policy, alignment_policy, num_qubits=num_qubits,
                       backend=backend, initial_state=initial, calibration=calibration,
                       workdir=base, trace_directory=Path(trace_directory) if trace_directory else None)
             for name, frame_policy, alignment_policy in POLICIES]
    return {
        "schema_version": 2,
        "num_qubits": num_qubits,
        "block_bits": calibration.block_bits,
        "seed": 20261013,
        "circuit_hash": cases[0]["circuit_hash"],
        "initial_state_sha256": cases[0]["initial_state_sha256"],
        "state_bytes": cases[0]["state_bytes"],
        "backend": backend,
        "ram_budget_bytes": 256 << 10 if num_qubits == 16 else 1 << 20,
        "metadata_reserve_bytes": 128 << 10,
        "caller_owned_reference_arrays_outside_managed_budget": True,
        "caller_owned_reference_arrays": ["initial_state", "eager_expected", "exported_actual"],
        "caller_owned_reference_bytes": cases[0]["caller_owned_reference_bytes"],
        "environment": _environment(),
        "comparison": "direct complex amplitude comparison; no phase alignment",
        "calibration": asdict(calibration),
        "cases": cases,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True, help="JSON report destination")
    parser.add_argument("--num-qubits", type=int, choices=(6, 8, 16), default=6)
    parser.add_argument("--backend", choices=("buffered", "direct"), default="buffered")
    parser.add_argument("--calibration-repetitions", type=int, default=3)
    args = parser.parse_args(argv)
    block_bits = 6 if args.num_qubits == 16 else 3
    calibration = calibrate_frames(block_bits=block_bits, repetitions=args.calibration_repetitions)
    report = run_phase2_benchmark(num_qubits=args.num_qubits, backend=args.backend,
                                  calibration=calibration,
                                  trace_directory=args.output.parent / f"{args.output.stem}.traces")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    print(f"Wrote {args.output}; max error={max(row['max_complex_error'] for row in report['cases']):.3g}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
