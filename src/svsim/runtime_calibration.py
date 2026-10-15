"""Finite calibration campaigns using the Phase 4 distributed runtime."""
from __future__ import annotations

from dataclasses import asdict, dataclass
import hashlib
import json
import math
from pathlib import Path
import platform
import shutil
import time
import uuid

import numpy as np

MAX_REPORT_BYTES = 16 * 1024**2


@dataclass(frozen=True)
class RuntimeCalibrationSpec:
    scratch: str
    num_qubits: int = 8
    block_bits: int = 4
    ranks: tuple[int, ...] = (1,)
    seed: int = 1
    ram_budget_bytes: int = 256 * 1024**2
    disk_budget_bytes: int = 1024**3
    walltime_seconds: float = 3600
    repetitions: int = 5
    warmup: int = 1
    backend: str = "buffered"
    engine: str = "worker"
    max_io_requests: int = 1
    suites: tuple[str, ...] = ("kernel-storage",)

    def __post_init__(self):
        if not self.scratch:
            raise ValueError("scratch path is required")
        if type(self.num_qubits) is not int or not 1 <= self.num_qubits <= 16:
            raise ValueError("num_qubits must be between 1 and 16")
        if type(self.block_bits) is not int or not 1 <= self.block_bits <= self.num_qubits:
            raise ValueError("block_bits must be between 1 and num_qubits")
        if not self.ranks or any(type(n) is not int or n < 1 for n in self.ranks):
            raise ValueError("rank counts must be positive integers")
        if any(n & (n - 1) for n in self.ranks):
            raise ValueError("rank counts must be powers of two")
        if len(set(self.ranks)) != len(self.ranks):
            raise ValueError("rank counts must be unique")
        if max(self.ranks) > (1 << (self.num_qubits - self.block_bits)):
            raise ValueError("rank count exceeds available shard blocks")
        if type(self.seed) is not int:
            raise ValueError("seed must be an integer")
        if type(self.ram_budget_bytes) is not int or self.ram_budget_bytes < 1:
            raise ValueError("RAM budget must be positive")
        if type(self.disk_budget_bytes) is not int or self.disk_budget_bytes < 1:
            raise ValueError("disk budget must be positive")
        if not math.isfinite(self.walltime_seconds) or self.walltime_seconds <= 0:
            raise ValueError("walltime must be finite and positive")
        if type(self.repetitions) is not int or not 1 <= self.repetitions <= 100:
            raise ValueError("repetitions must be between 1 and 100")
        if type(self.warmup) is not int or not 1 <= self.warmup <= 100:
            raise ValueError("warmup must be between 1 and 100")
        if self.backend not in {"buffered", "direct"}:
            raise ValueError("backend must be buffered or direct")
        if self.engine not in {"worker", "cooperative"}:
            raise ValueError("engine must be worker or cooperative")
        if type(self.max_io_requests) is not int or self.max_io_requests not in {1, 2}:
            raise ValueError("max_io_requests must be 1 or 2")
        allowed = {"kernel-storage", "memory-mpi", "storage-mpi"}
        if not self.suites or len(set(self.suites)) != len(self.suites) or set(self.suites) - allowed:
            raise ValueError("suites must be a unique nonempty subset of supported suites")


def _circuit(spec, suite, ranks):
    """Build one reproducible local or communicating gate recipe."""
    from .parameters import ParameterSymbol
    theta = ParameterSymbol("theta", parameter_id=f"runtime-calibration-{spec.seed}")
    n = spec.num_qubits
    local_bits = min(spec.block_bits, n)
    recipe = [("H", (i,), {}) for i in range(local_bits)]
    recipe += [("RX", (i,), {"angle": theta}) for i in range(local_bits)]
    if local_bits > 1:
        recipe.append(("PAULI_ROT", (0, local_bits - 1), {"paulis": ("X", "Z"), "angle": theta}))
    if suite in {"memory-mpi", "storage-mpi"} and ranks > 1:
        shard_bits = max(0, spec.num_qubits - spec.block_bits)
        if shard_bits < 1 or ranks > (1 << shard_bits):
            raise ValueError("MPI suite requires enough shard bits for the requested ranks")
        remote_target = spec.num_qubits - (ranks.bit_length() - 1)
        recipe.extend([("H", (remote_target,), {}), ("RX", (remote_target,), {"angle": theta})])
    np.random.default_rng(spec.seed).shuffle(recipe)
    return recipe, theta


def _observation_dict(observation):
    if isinstance(observation, dict):
        result=dict(observation)
    elif hasattr(observation, "to_dict"):
        result=observation.to_dict()
    else: result=asdict(observation)
    # Shared wire codec reserves `warmup` for the repetition count.
    result['is_warmup']=result.pop('warmup',False)
    return result


def _resolved_storage_backends(sim):
    """Read identities emitted by the handles that actually completed storage I/O."""
    identities = set()
    for event in sim.trace.iter_events():
        metadata = event.get("metadata") or {}
        backend = metadata.get("backend")
        if backend:
            identities.add(str(backend))
    return sorted(identities)


ENVELOPE_MIN_HELD_OUT = 5
ENVELOPE_MIN_COVERAGE = .8
# Five training trials alone bound only ~67% of i.i.d. repeats; widen by the ETA tolerance.
ENVELOPE_TOLERANCE = .1


def _held_out_envelope(values, fingerprint, ranks):
    """Train [min, max] (widened by ENVELOPE_TOLERANCE) on the older half of validated trials; certify on the newer half (>=5, coverage >= 0.8)."""
    half = len(values) // 2
    train, held = values[:half], values[half:]
    if len(values) < 2 * ENVELOPE_MIN_HELD_OUT:
        return None, f'INSUFFICIENT_TRIALS:{len(values)}<{2 * ENVELOPE_MIN_HELD_OUT}'
    lower, upper = min(train) * (1 - ENVELOPE_TOLERANCE), max(train) * (1 + ENVELOPE_TOLERANCE)
    inside = sum(lower <= v <= upper for v in held)
    coverage = inside / len(held)
    if coverage < ENVELOPE_MIN_COVERAGE:
        return None, f'LOW_HELD_OUT_COVERAGE:{coverage:.2f}'
    return {'method': 'train-min-max-widened-held-out', 'relative_widening': ENVELOPE_TOLERANCE, 'interval_kind': 'conditional-prediction',
            'validation': 'held-out', 'units': 'seconds/suite-execute', 'lower_seconds': lower,
            'upper_seconds': upper, 'held_out_count': len(held), 'held_out_success_count': inside,
            'held_out_coverage': coverage, 'training_count': len(train), 'ranks': ranks,
            'configuration_fingerprint': fingerprint, 'decision': 'CERTIFIED'}, 'CERTIFIED'


def calibrate_runtime(spec: RuntimeCalibrationSpec, *, comm=None) -> dict:
    """Run bounded actual-runtime trials. Every rank must call with the same spec."""
    if not isinstance(spec, RuntimeCalibrationSpec):
        raise TypeError("spec must be RuntimeCalibrationSpec")
    # Reject impossible campaigns before initializing or querying MPI.
    state_bytes = 16 * (1 << spec.num_qubits)
    # Include eager oracle, returned state, simulator work buffers, bounded observations,
    # and report metadata in the declared campaign budget.
    minimum_ram = (3 * state_bytes + 2 * MAX_REPORT_BYTES +
                   max(128 * 1024, 4096 * (1 << max(0, spec.num_qubits-spec.block_bits))))
    if spec.ram_budget_bytes < minimum_ram:
        raise MemoryError(f"RAM budget {spec.ram_budget_bytes} is below estimated working set {minimum_ram}")
    from .async_scheduler import AsyncDistributedSimulator, AsyncPolicy
    from .distributed import DistributedConfig
    from .kernels import eager_reference

    if comm is None:
        from mpi4py import MPI
        comm = MPI.COMM_WORLD
    rank, size = comm.Get_rank(), comm.Get_size()
    if size not in spec.ranks:
        raise ValueError(f"communicator has {size} ranks, outside requested ranks {spec.ranks}")
    if spec.disk_budget_bytes // size < 10*1024**2:
        raise ValueError("disk budget must provide at least 10 MiB per rank including the bounded metadata pool")
    # Account for per-rank simulator metadata before creating any runtime files.
    runtime_ram_pool = spec.ram_budget_bytes - 2 * MAX_REPORT_BYTES - 3 * state_bytes
    per_rank_ram = runtime_ram_pool // size
    if per_rank_ram < 128 * 1024:
        raise MemoryError("per-rank RAM budget cannot hold runtime metadata")

    root_dir = None
    root_setup_error = None
    if rank == 0:
        try:
            Path(spec.scratch).mkdir(parents=True, exist_ok=True)
            root_dir = str(Path(spec.scratch) / f"runtime-calibration-{uuid.uuid4().hex}")
        except BaseException as exc:
            root_setup_error = f"{type(exc).__name__}: {exc}"
    root_dir, root_setup_error = comm.bcast((root_dir, root_setup_error), root=0)
    if root_setup_error:
        if rank == 0:
            return {"kind": "runtime-calibration", "schema_version": 1,
                    "scope": "runtime-calibration", "status": "failed",
                    "failure": root_setup_error, "trials": []}
        return {"kind": "runtime-calibration", "schema_version": 1, "status": "failed"}
    from .profile_document import probe_profile
    nodes=len(set(comm.allgather(platform.node())))
    profile=None;profile_error=None
    if rank==0:
        try:
            profile=probe_profile({'scope':'local','ranks':size,'nodes':nodes,
                'threads':2 if spec.engine=='worker' else 1},(spec.scratch,))
        except Exception as exc:profile_error=f'{type(exc).__name__}: {exc}'
    profile_error=comm.bcast(profile_error,root=0)
    if profile_error:
        return {'kind':'runtime-calibration','schema_version':1,'scope':'runtime-calibration',
                'status':'failed','failure':profile_error,'trials':[],'summary':{'count':0,'by_suite':{}}}
    rank_dir = Path(root_dir) / f"rank-{rank:04d}"
    recipe, theta = None, None
    source_identities = {}
    trials = []
    started_campaign = time.monotonic()
    trial_index = 0
    error = None
    try:
        for suite in spec.suites:
            if suite in {"memory-mpi", "storage-mpi"} and size == 1:
                if rank == 0:
                    trials.append({"suite": suite, "status": "not-applicable-single-rank",
                                   "reason": "MPI suite requires at least two ranks"})
                continue
            recipe, theta = _circuit(spec, suite, size)
            source_identities[suite] = hashlib.sha256(json.dumps(
                [(k, t, {x: str(y) for x, y in kw.items()}) for k,t,kw in recipe],
                sort_keys=True).encode()).hexdigest()
            amplitude_store = "ram" if suite == "memory-mpi" else "file"
            # Each suite exercises its named path with the same fixed seed and recipe over repeats.
            for repetition in range(-spec.warmup, spec.repetitions):
                expired = (time.monotonic() - started_campaign >= spec.walltime_seconds) if rank == 0 else None
                expired = comm.bcast(expired, root=0)
                if expired:
                    if rank == 0:
                        trials.append({"suite": suite, "repetition": repetition, "status": "omitted", "reason": "walltime"})
                    break
                trial_index += 1
                trial_dir = rank_dir / f"trial-{trial_index:04d}"
                config = DistributedConfig(
                    str(trial_dir), spec.block_bits,
                    ram_budget_bytes_per_rank=per_rank_ram,
                    metadata_reserve_bytes=64 * 1024, tile_bytes=16 * 1024,
                    backend=spec.backend, frame_policy="materialize", storage_scope="node-local",
                    amplitude_store=amplitude_store,
                    disk_quota_bytes=max(1024**2, spec.disk_budget_bytes // size),
                    drain_reserve_bytes=min(1024**2, max(0, spec.disk_budget_bytes // (size * 8))))
                policy = AsyncPolicy(mode="observe", io_engine=spec.engine,
                                     max_io_requests=spec.max_io_requests,
                                     io_workers=2 if spec.engine == "worker" else 1,
                                     swap_policy="S0", prefetch_blocks=0)
                all_start = time.perf_counter()
                failure = None
                elapsed = None
                norm_error = None
                validation_seconds = None
                measured_io = {"send_bytes_max": 0, "storage_read_bytes_max": 0,
                               "storage_write_bytes_max": 0, "io_requests_max": 0,
                               "managed_peak_bytes_max": 0, "scratch_bytes_max": 0,
                               "resolved_backends": []}
                sim = None
                disk_admission=None
                startup = 0.0
                try:
                    constructed = time.perf_counter()
                    sim = AsyncDistributedSimulator(spec.num_qubits, config, comm=comm, policy=policy)
                    startup = time.perf_counter() - constructed
                    # One materialize has few epoch boundaries: give pending raw lines (not the summary) more room.
                    sim.runtime.configure_measurements(max_bytes=min(128*1024,max(1024,per_rank_ram//64)),
                                                       raw_max_bytes=min(1024*1024,max(1024,per_rank_ram//16)))
                    sim._measurement_warmup = repetition < 0
                    for kind, targets, kwargs in recipe:
                        sim.gate(kind, targets, **kwargs)
                    binding = sim.bind({theta: 0.37})
                    gates = tuple(sim.sequencer.operations)
                    oracle = eager_reference(gates, spec.num_qubits, sim.assets, binding)
                    before_send = sim.transport.send_bytes
                    before_read = sim.trace.read_bytes
                    before_write = sim.trace.write_bytes
                    before_io_submitted = sim.io_engine.submitted
                    execute_start = time.perf_counter()
                    sim.materialize(binding=binding)
                    elapsed = sim.transport.max_value(time.perf_counter() - execute_start)
                    if amplitude_store == "ram":
                        local_backends = ["memory"]
                    else:
                        local_backends = _resolved_storage_backends(sim) or ["unknown"]
                    backend_rows = comm.allgather(local_backends)
                    resolved_backends = sorted({backend for rows in backend_rows for backend in rows})
                    send_delta = max(0, sim.transport.send_bytes - before_send)
                    read_delta = max(0, sim.trace.read_bytes - before_read)
                    write_delta = max(0, sim.trace.write_bytes - before_write)
                    measured_io = {"send_bytes_max": comm.allreduce(send_delta, op=_mpi_max(comm)),
                                   "storage_read_bytes_max": comm.allreduce(read_delta, op=_mpi_max(comm)),
                                   "storage_write_bytes_max": comm.allreduce(write_delta, op=_mpi_max(comm)),
                                   "io_requests_max": comm.allreduce(max(0, sim.io_engine.submitted-before_io_submitted), op=_mpi_max(comm)),
                                   "managed_peak_bytes_max": 0, "scratch_bytes_max": 0,
                                   "resolved_backends": resolved_backends if rank == 0 else []}
                    observed = sim.state(binding=binding, allow_gather=True)
                    check_start = time.perf_counter()
                    validation_error = None
                    if rank == 0:
                        try:
                            np.testing.assert_allclose(observed, oracle, rtol=1e-10, atol=1e-10)
                            norm_error = abs(float(np.vdot(observed, observed).real) - 1.0)
                            if norm_error > 1e-10:
                                raise AssertionError(f"norm error {norm_error}")
                        except BaseException as exc:
                            validation_error = f"{type(exc).__name__}: {exc}"
                        validation_seconds = time.perf_counter() - check_start
                    validation_error = comm.bcast(validation_error, root=0)
                    norm_error = comm.bcast(norm_error if rank == 0 else None, root=0)
                    if validation_error:
                        raise AssertionError(validation_error)
                    snap = sim.runtime.measurements.snapshot()
                    disk_admission={'catalog':sim.store.db.admission_status(),
                        'ledger':sim.store.disk_budget.snapshot()}
                    collector_rows = {
                        "observations": [_observation_dict(o) for o in snap["observations"]],
                        "failed_observations": [_observation_dict(o) for o in snap["failed_observations"]],
                        "dropped_bytes": snap["dropped_bytes"],
                        "dropped_conditions": snap["dropped_conditions"],
                        "dropped_raw": snap.get("dropped_raw", 0),
                    }
                except BaseException as exc:
                    failure = f"{type(exc).__name__}: {exc}"
                    validation_seconds = None
                    collector_rows = None
                finally:
                    local_managed_peak = 0
                    if sim is not None:
                        if collector_rows is None:
                            try:
                                snap = sim.runtime.measurements.snapshot()
                                collector_rows = {
                                    "observations": [_observation_dict(o) for o in snap["observations"]],
                                    "failed_observations": [_observation_dict(o) for o in snap["failed_observations"]],
                                    "dropped_bytes": snap["dropped_bytes"],
                                    "dropped_conditions": snap["dropped_conditions"],
                                    "dropped_raw": snap.get("dropped_raw", 0),
                                }
                            except BaseException:
                                collector_rows = {"observations": [], "failed_observations": [],
                                                  "dropped_bytes": 0, "dropped_conditions": 0}
                        try:
                            sim.close()
                        except BaseException as exc:
                            failure = failure or f"{type(exc).__name__} during close: {exc}"
                        local_managed_peak = sim.budget.peak
                    else:
                        collector_rows = collector_rows or {"observations": [], "failed_observations": [],
                                                            "dropped_bytes": 0, "dropped_conditions": 0}
                    measured_io["managed_peak_bytes_max"] = comm.allreduce(local_managed_peak, op=_mpi_max(comm))
                collector_rows = comm.gather(collector_rows, root=0)
                if rank == 0:
                    rank_rows = collector_rows
                    stage_counts = {}
                    for r in rank_rows:
                        for observation in r["observations"]:
                            stage = observation.get("stage", "unknown")
                            stage_counts[stage] = stage_counts.get(stage, 0) + 1
                    collector_rows = {
                        "observations": [o for r in rank_rows for o in r["observations"]],
                        "failed_observations": [o for r in rank_rows for o in r["failed_observations"]],
                        "dropped_bytes": sum(r["dropped_bytes"] for r in rank_rows),
                        "dropped_conditions": sum(r["dropped_conditions"] for r in rank_rows),
                        "dropped_raw": sum(r.get("dropped_raw", 0) for r in rank_rows),
                        "scheduler_completion_counts_by_stage": stage_counts,
                    }
                local_scratch_bytes = (sum(p.stat().st_size for p in trial_dir.rglob("*") if p.is_file())
                                       if trial_dir.exists() else 0)
                measured_io["scratch_bytes_max"] = comm.allreduce(local_scratch_bytes, op=_mpi_max(comm))
                shutil.rmtree(trial_dir, ignore_errors=True)
                total = comm.allreduce(time.perf_counter() - all_start, op=_mpi_max(comm))
                failures = comm.allgather(failure)
                failure = next((f for f in failures if f), None)
                row = {"suite": suite, "repetition": repetition, "is_warmup": repetition < 0,
                       "status": "failed" if failure else "validated", "failure": failure,
                       "startup_seconds_max": comm.allreduce(startup, op=_mpi_max(comm)),
                       "execute_seconds_max": elapsed,
                       "validation_seconds": validation_seconds if rank == 0 else None,
                       "whole_group_seconds_max": total, "norm_error": norm_error if not failure else None,
                       "rank_count": size, "storage": amplitude_store,
                       "measurements": measured_io,
                       "disk_admission":disk_admission if rank==0 else None,
                       "overlap_evidence": "serial-materialize-no-overlap-claim",
                       "collector": collector_rows if rank == 0 else {}}
                if rank == 0:
                    trials.append(row)
                # Keep parent-owned metadata and measured data inside the explicit output cap.
                too_large = len(json.dumps(trials, default=str)) > MAX_REPORT_BYTES if rank == 0 else None
                too_large = comm.bcast(too_large, root=0)
                if too_large:
                    raise MemoryError("campaign report exceeds 16 MiB limit")
    except BaseException as exc:
        error = f"{type(exc).__name__}: {exc}"
    errors = comm.allgather(error)
    root_error = next((e for e in errors if e), None)
    if rank != 0:
        return {"kind": "runtime-calibration", "schema_version": 1, "status": "failed" if root_error else "complete"}
    # Preserve raw trials, excluding warmups from statistics. Variation is diagnostic only.
    measured = [t["execute_seconds_max"] for t in trials if t.get("status") == "validated" and not t.get("is_warmup") and t.get("execute_seconds_max") is not None]
    has_trial_failures = any(t.get("status") == "failed" for t in trials)
    has_omissions = any(t.get("status") == "omitted" for t in trials)
    # Raw completions are spilled to disk at epoch boundaries; only lost raw lines are monitor loss.
    # RAM summary-ring overflow (dropped_bytes/conditions) stays reported but does not lose evidence.
    has_monitor_loss=any(t.get('collector',{}).get('dropped_raw',0) for t in trials)
    campaign_status = ("failed" if root_error or not measured else
                       "partial" if has_trial_failures or has_omissions or has_monitor_loss else "complete")
    by_suite = {}
    for suite in spec.suites:
        values = [t["execute_seconds_max"] for t in trials if t.get("suite") == suite and
                  t.get("status") == "validated" and not t.get("is_warmup") and
                  t.get("execute_seconds_max") is not None]
        by_suite[suite] = {"count": len(values),
                           "min_seconds": min(values) if values else None,
                           "median_seconds": float(np.median(values)) if values else None,
                           "max_seconds": max(values) if values else None,
                           "finite": all(math.isfinite(v) for v in values)}
    summary = {"count": len(measured), "by_suite": by_suite}
    envelopes, reasons = {}, {}
    for suite in spec.suites:
        values = [t["execute_seconds_max"] for t in trials if t.get("suite") == suite and
                  t.get("status") == "validated" and not t.get("is_warmup") and
                  t.get("execute_seconds_max") is not None]
        envelopes[suite], reasons[suite] = ((None, 'NOT_APPLICABLE_SINGLE_RANK') if size == 1 and suite in {"memory-mpi", "storage-mpi"}
                                            else _held_out_envelope(values, profile.configuration_fingerprint, size))
    case_rows = []
    for suite in spec.suites:
        not_applicable = suite in {"memory-mpi", "storage-mpi"} and size == 1
        validated = [t for t in trials if t.get("suite") == suite and t.get("status") == "validated" and not t.get("is_warmup")]
        mpi_observed = any(t.get("measurements", {}).get("send_bytes_max", 0) > 0 for t in validated)
        storage_observed = any(t.get("measurements", {}).get("storage_read_bytes_max", 0) > 0 or
                               t.get("measurements", {}).get("storage_write_bytes_max", 0) > 0 for t in validated)
        expected_mpi = suite in {"memory-mpi", "storage-mpi"}
        expected_storage = suite in {"kernel-storage", "storage-mpi"}
        status = ("not-applicable-single-rank" if not_applicable else
                  "no-validated-trials" if not validated else
                  "no-mpi-payload-observed" if expected_mpi and not mpi_observed else
                  "no-storage-activity-observed" if expected_storage and not storage_observed else "measured")
        if status in {"no-validated-trials", "no-mpi-payload-observed", "no-storage-activity-observed"}:
            campaign_status = "partial" if campaign_status == "complete" else campaign_status
        case_rows.append({"suite": suite, "ranks": size, "backend": spec.backend, "engine": spec.engine,
                          "max_io_requests": spec.max_io_requests, "status": status,
                          "mpi_payload_observed": mpi_observed,
                          "storage_activity_observed": storage_observed,
                          "resolved_backends": sorted({backend for t in validated
                              for backend in t.get("measurements", {}).get("resolved_backends", [])})})
    return {"kind": "runtime-calibration", "schema_version": 1,
            "scope": "runtime-calibration", "status": campaign_status,
            "failure": root_error, "platform": platform.platform(), "spec": asdict(spec),
            "rank_count": size, "source_identities": source_identities,
            "provenance":profile.data['provenance'],
            "discovered_configuration_fingerprint":profile.configuration_fingerprint,
            "software":profile.data['software'],
            "cases": case_rows,
            "quality": {"correctness": "validated" if measured else "no-validated-trials",
                        "statistical": "held-out-envelope" if any(envelopes.values()) else "finite-diagnostics-only",
                        "confidence_interval": None,
                        "candidate_envelope": {suite: e for suite, e in envelopes.items() if e} or None,
                        "envelope_decisions": {suite: (e['decision'] if e else reasons[suite]) for suite, e in envelopes.items()}},
            "summary": summary, "trials": trials,
            "collector": {"observations": sum(len(t.get("collector", {}).get("observations", ())) for t in trials),
                          "failed_observations": sum(len(t.get("collector", {}).get("failed_observations", ())) for t in trials),
                          "dropped_bytes": sum(t.get("collector", {}).get("dropped_bytes", 0) for t in trials),
                          "dropped_conditions": sum(t.get("collector", {}).get("dropped_conditions", 0) for t in trials)},
            "elapsed_seconds": time.monotonic()-started_campaign}


def split_observations(report, path):
    """Move per-trial collector observations to a JSONL sidecar so the report stays within node limits.

    Each trial keeps counts plus ``observations_line_range`` into the sidecar; the report records its sha256.
    """
    from .journal import canonical
    from .profile_document import wire_encode
    lines = []
    for index, trial in enumerate(report.get("trials", ())):
        collector = trial.get("collector")
        if not isinstance(collector, dict): continue
        start = len(lines)
        for kind in ("observations", "failed_observations"):
            rows = collector.pop(kind, None) or []
            collector[kind + "_count"] = len(rows)
            lines.extend(canonical(wire_encode({"trial": index, "kind": kind, "row": row})) for row in rows)
        collector["observations_line_range"] = [start, len(lines)]
    payload = "".join(line + "\n" for line in lines).encode()
    path = Path(path)
    with path.open("wb") as stream:
        stream.write(payload)
    report["observations_file"] = {"name": path.name, "lines": len(lines),
                                   "sha256": hashlib.sha256(payload).hexdigest()}
    return report


def _mpi_max(comm):
    from mpi4py import MPI
    return MPI.MAX
