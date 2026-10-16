#!/usr/bin/env python3
"""P4-002: Held-out evaluation of runtime ETA 'observed' view vs real remaining wall time.

Runs layered circuits across configurations (workloads, swap policies, amplitude stores,
I/O engines), evaluates runtime ETA at cutoff frontiers, compares against actual
remaining execution time, measures monitoring overhead in paired comparisons,
and summarizes accuracy, coverage, and reasons.
"""

from __future__ import annotations

import argparse
import json
import math
import os
from pathlib import Path
import platform
import random
import shutil
import statistics
import subprocess
import sys
from time import perf_counter
from types import MethodType
from uuid import uuid4

from mpi4py import MPI
from svsim import AsyncDistributedSimulator, AsyncPolicy, DistributedConfig

WORKLOADS = [
    {
        "name": "rx-ry-low",
        "n": 6,
        "block_bits": 2,
        "layers": 30,
        "recipe": [
            ("RX", (0,), 0.3),
            ("RY", (1,), 0.5),
            ("RX", (1,), 0.7),
            ("RY", (0,), 0.9),
        ],
    },
    {
        "name": "upper",
        "n": 7,
        "block_bits": 3,
        "layers": 30,
        "recipe": [
            ("RX", (3,), 0.2),
            ("RZ", (4,), 0.4),
            ("RX", (5,), 0.6),
            ("RZ", (6,), 0.8),
        ],
    },
    {
        "name": "cz",
        "n": 6,
        "block_bits": 2,
        "layers": 30,
        "recipe": [
            ("H", (0,), None),
            ("RZ", (1,), 0.35),
            ("CZ", (0, 1), None),
            ("CZ", (1, 5), None),
        ],
    },
    {
        "name": "mixed",
        "n": 8,
        "block_bits": 3,
        "layers": 20,
        "recipe": [
            ("H", (0,), None),
            ("RX", (1,), 0.25),
            ("RY", (3,), 0.45),
            ("RZ", (7,), 0.65),
            ("CX", (0, 3), None),
            ("CX", (1, 7), None),
        ],
    },
]


def varying_recipe(seed, layers, spec):
    """Per-layer recipes whose rotation angles differ on every layer (fixed seed, reproducible)."""
    rng = random.Random(seed)
    return [[(kind, targets, (round(rng.uniform(0.1, 3.0), 6) if angle == "rand" else angle))
             for kind, targets, angle in spec] for _ in range(layers)]


for _wl in (
    {"name": "rx-ry-var", "n": 6, "block_bits": 2, "layers": 30, "seed": 11,
     "spec": [("RX", (0,), "rand"), ("RY", (1,), "rand"), ("RX", (1,), "rand"), ("RY", (0,), "rand")]},
    {"name": "mixed-var", "n": 7, "block_bits": 3, "layers": 24, "seed": 23,
     "spec": [("H", (0,), None), ("RX", (3,), "rand"), ("CZ", (0, 1), None), ("RZ", (5,), "rand"), ("RY", (2,), "rand")]},
):
    _wl["recipe"] = varying_recipe(_wl["seed"], _wl["layers"], _wl["spec"])
    WORKLOADS.append(_wl)


def apply_recipe(sim, recipe, layers):
    """Append circuit operations: a fixed layer repeated, or (varying workloads) one explicit recipe per layer."""
    layer_list = recipe if recipe and isinstance(recipe[0], list) else [recipe] * layers
    for layer in layer_list:
        for kind, targets, angle in layer:
            if angle is not None:
                sim.gate(kind, targets, angle=angle)
            else:
                sim.gate(kind, targets)


def percentile(values: list[float], p: float) -> float | None:
    """Calculate percentile using linear interpolation."""
    if not values:
        return None
    sorted_v = sorted(values)
    k = (len(sorted_v) - 1) * (p / 100.0)
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return sorted_v[int(k)]
    d0 = sorted_v[int(f)] * (c - k)
    d1 = sorted_v[int(c)] * (k - f)
    return d0 + d1


def get_git_rev(root_dir: Path) -> str | None:
    """Return the current git revision hash if available."""
    try:
        out = subprocess.check_output(
            ["git", "rev-parse", "HEAD"],
            cwd=str(root_dir),
            stderr=subprocess.DEVNULL,
            text=True,
        ).strip()
        return out if out else None
    except Exception:
        return None


def get_dirty_src(root_dir: Path) -> str | None:
    """Uncommitted changes under src/ (None when clean or git is unavailable, e.g. inside the Docker image)."""
    try:
        out = subprocess.check_output(["git", "status", "--porcelain", "src"], cwd=str(root_dir),
                                      stderr=subprocess.DEVNULL, text=True).strip()
        return out or None
    except Exception:
        return None


def compute_slice_summary(rows: list[dict]) -> dict:
    """Compute summary statistics for a subset of trial rows."""
    n_checks = len(rows)
    error_rows = [r for r in rows if r.get("status") == "error"]
    ok_rows = [r for r in rows if r.get("status") == "ok"]
    answered_rows = [r for r in ok_rows if r.get("eta") is not None and r.get("relative_error") is not None]
    unanswered_rows = [r for r in ok_rows if r.get("eta") is None]

    n_answered = len(answered_rows)
    answered_fraction = (n_answered / n_checks) if n_checks > 0 else 0.0

    if answered_rows:
        abs_errors = [abs(r["relative_error"]) for r in answered_rows]
        med_err = float(statistics.median(abs_errors))
        p95_err = float(percentile(abs_errors, 95))
        worst_err = float(max(abs_errors))
        covered_count = sum(1 for r in answered_rows if r.get("covered") is True)
        coverage = float(covered_count / n_answered)
    else:
        med_err = None
        p95_err = None
        worst_err = None
        coverage = None

    unknown_tally = {}
    for r in unanswered_rows:
        for reason in r.get("unknown_reasons", []):
            unknown_tally[reason] = unknown_tally.get(reason, 0) + 1

    exact_c = pooled_c = missing_c = 0
    for r in rows:
        for d in r.get("class_diag") or []:
            exact_c += d.get("exact", 0); pooled_c += d.get("pooled", 0); missing_c += d.get("missing", 0)
    return {
        "class_resolution": {"exact": exact_c, "pooled": pooled_c, "missing": missing_c},
        "n_checks": n_checks,
        "n_errors": len(error_rows),
        "n_answered": n_answered,
        "answered_fraction": answered_fraction,
        "median_abs_relative_error": med_err,
        "p95_abs_relative_error": p95_err,
        "worst_abs_relative_error": worst_err,
        "coverage": coverage,
        "unknown_reason_tally": unknown_tally,
    }


def certification_diagnostic(collector, condition, policy_epoch, plan_version):
    """Explain the exact/pooled sample threshold without changing certification."""
    from svsim import runtime_observed_costs as roc
    stages = roc._stages(condition)
    index = roc._by_class(collector, stages)
    executor = stages is roc.EXECUTOR_STAGES
    keys = {'exact': (executor, roc._class(condition))}
    pooled = roc._pooled(condition)
    if pooled is not None:
        keys['pooled_angle'] = (executor, 'pooled', pooled)
    result = {'required_post_burn_in_samples': roc.CERTIFY_SAMPLES,
              'burn_in_samples': roc.BURN_IN, 'minimum_held_out_coverage': roc.MIN_COVERAGE}
    for name, key in keys.items():
        reason, values, _, _, coverage = roc._assess(index.get(key, []), policy_epoch, plan_version, roc.BURN_IN)
        result[name] = {'reason': reason, 'post_burn_in_window_samples': len(values),
                        'held_out_samples': len(values) - len(values) // 2 if values else 0,
                        'held_out_coverage': coverage}
    return result


def run_trial(comm, scratch_dir: Path, wl: dict, frac: float, trial_idx: int,
              policy: str, store: str, engine: str, backend: str) -> dict:
    """Execute a single ETA evaluation trial and return row metrics."""
    rank = comm.Get_rank()
    size = comm.Get_size()

    trial_tag = f"trial_{wl['name']}_{policy}_{store}_{engine}_{backend}_f{int(frac*100)}_{trial_idx}_{uuid4().hex[:8]}"
    trial_dir = scratch_dir / trial_tag

    mkdir_err = None
    if rank == 0:
        try:
            trial_dir.mkdir(parents=True, exist_ok=True)
        except Exception as exc:
            mkdir_err = str(exc)
    mkdir_errs = comm.allgather(mkdir_err)
    if any(e is not None for e in mkdir_errs):
        comm.Barrier()
        return {
            "status": "error",
            "error": "mkdir failed: " + "; ".join(filter(None, mkdir_errs)),
            "workload": wl["name"],
            "n": wl["n"],
            "block_bits": wl["block_bits"],
            "total_gates": None,
            "policy": policy,
            "store": store,
            "engine": engine,
            "backend": backend,
            "frac": frac,
            "k": None,
            "last": None,
            "trial": trial_idx,
            "ranks": size,
            "eta": None,
            "lower": None,
            "upper": None,
            "actual": None,
            "relative_error": None,
            "covered": None,
            "unknown_reasons": [],
            "future_complete": None,
            "epochs": None,
        }
    comm.Barrier()

    trial_err = None
    row_data = None
    total_gates = None
    k = None
    last = None

    try:
        cfg = DistributedConfig(
            str(trial_dir),
            wl["block_bits"],
            ram_budget_bytes_per_rank=64 * 1024**2,
            metadata_reserve_bytes=8 * 1024**2,
            amplitude_store=store,
            backend=backend,
            storage_scope="node-local",
            frame_policy="materialize",
        )
        pol = AsyncPolicy(
            io_engine=engine,
            io_workers=1,
            prefetch_blocks=0,
            lookahead_gates=4,
            swap_policy=policy,
            cooldown_seconds=0,
        )

        with AsyncDistributedSimulator(wl["n"], cfg, comm=comm, policy=pol) as sim:
            sim.runtime.configure_measurements(max_bytes=2 * 1024**2)
            apply_recipe(sim, wl["recipe"], wl["layers"])

            total_gates = len(sim.sequencer.operations)
            last = total_gates - 1
            binding = sim.bind()
            k = int(frac * last)

            sim.materialize(frontier=k, binding=binding)
            comm.Barrier()

            eta_dict = sim.runtime.update_eta(endpoint_frontier=last, collective=True)
            views = eta_dict.get("views", {}) if isinstance(eta_dict, dict) else {}
            obs_view = views.get("observed", {}) if isinstance(views, dict) else {}

            local_lower = obs_view.get("lower_seconds")
            local_upper = obs_view.get("upper_seconds")
            local_reasons = tuple(obs_view.get("unknown_reasons") or ())

            fc = None
            try:  # exposed only on the snapshot; capture again (cheap, incremental) right after the check
                fc = bool(sim.runtime.remaining_extractor.capture(sim, endpoint_frontier=last).future_complete)
            except Exception:
                fc = None

            diag = {}
            try:
                from svsim import runtime_observed_costs as roc
                snap = sim.runtime.remaining_extractor.capture(sim, endpoint_frontier=last)
                tasks = tuple(snap.window) + tuple(snap.future if (snap.future_complete and not snap.future_reasons) else snap.tail)
                exact = pooled = 0
                missing = {}
                missing_classes = []
                for t in tasks:
                    if not t.count:
                        continue
                    res = sim.runtime.costs.resolve('observed-task-seconds', t.context, t.condition, purpose='production')
                    if res.value is not None:
                        if str(getattr(res.value, "condition", "")).startswith("observed-pooled-angle"):
                            pooled += 1
                        else:
                            exact += 1
                    else:
                        detail = certification_diagnostic(sim.runtime.measurements, t.condition,
                                                          sim.runtime.policy_epoch, sim.runtime.plan_version)
                        assessment = detail['exact']
                        if assessment['reason'] == roc.INSUFFICIENT:
                            assessment = detail.get('pooled_angle', assessment)
                        why = assessment['reason']
                        key = t.task_id.split(":")[0] + ":" + (t.task_id.split(":")[-1] if t.task_id.endswith("boundary") or t.task_id.endswith("publish") else "") + ":" + str(why)
                        missing[key] = missing.get(key, 0) + 1
                        missing_classes.append({'task_id': t.task_id, **detail})
                diag = {"classes": sum(1 for t in tasks if t.count), "exact": exact, "pooled": pooled,
                        "missing": sum(missing.values()), "missing_detail": missing, "missing_classes": missing_classes,
                        "collector_dropped_conditions": sim.runtime.measurements.dropped_conditions,
                        "collector_dropped_bytes": sim.runtime.measurements.dropped_bytes}
            except Exception as exc:
                diag = {"error": repr(exc)}

            gathered_info = comm.allgather({
                "lower": local_lower,
                "upper": local_upper,
                "reasons": local_reasons,
                "fc": fc,
                "diag": diag,
            })

            rank_max_dict = eta_dict.get("rank_max_expected_seconds") if isinstance(eta_dict, dict) else None
            observed_expected = rank_max_dict.get("observed") if isinstance(rank_max_dict, dict) else None

            all_lowers = [g["lower"] for g in gathered_info]
            lower = max(all_lowers) if all(l is not None for l in all_lowers) else None

            all_uppers = [g["upper"] for g in gathered_info]
            upper = max(all_uppers) if all(u is not None for u in all_uppers) else None

            unknown_reasons = sorted(set().union(*(g["reasons"] for g in gathered_info)))
            future_complete = all(g["fc"] is True for g in gathered_info)

            epochs = math.ceil(total_gates / 4)  # lookahead_gates=4 gates per epoch window

            comm.Barrier()
            t_start = perf_counter()
            sim.materialize()
            t_end = perf_counter()
            local_actual = t_end - t_start
            actual = max(comm.allgather(local_actual))

            rel_err = (
                (observed_expected - actual) / actual
                if (observed_expected is not None and actual is not None and actual > 0)
                else None
            )
            covered = (
                (lower <= actual <= upper)
                if (lower is not None and upper is not None and actual is not None)
                else None
            )

            row_data = {
                "status": "ok",
                "error": None,
                "workload": wl["name"],
                "n": wl["n"],
                "block_bits": wl["block_bits"],
                "total_gates": total_gates,
                "policy": policy,
                "store": store,
                "engine": engine,
                "backend": backend,
                "frac": frac,
                "k": k,
                "last": last,
                "trial": trial_idx,
                "ranks": size,
                "eta": observed_expected,
                "lower": lower,
                "upper": upper,
                "actual": actual,
                "relative_error": rel_err,
                "covered": covered,
                "unknown_reasons": unknown_reasons,
                "future_complete": future_complete,
                "epochs": epochs,
                "class_diag": [g["diag"] for g in gathered_info],
            }
    except Exception as exc:
        trial_err = str(exc)

    errs = comm.allgather(trial_err)
    if any(e is not None for e in errs):
        combined_err = "; ".join(filter(None, errs))
        row_data = {
            "status": "error",
            "error": combined_err,
            "workload": wl["name"],
            "n": wl["n"],
            "block_bits": wl["block_bits"],
            "total_gates": total_gates,
            "policy": policy,
            "store": store,
            "engine": engine,
            "backend": backend,
            "frac": frac,
            "k": k,
            "last": last,
            "trial": trial_idx,
            "ranks": size,
            "eta": None,
            "lower": None,
            "upper": None,
            "actual": None,
            "relative_error": None,
            "covered": None,
            "unknown_reasons": [],
            "future_complete": None,
            "epochs": None,
        }

    comm.Barrier()
    if rank == 0:
        try:
            shutil.rmtree(trial_dir, ignore_errors=True)
        except Exception:
            pass
    comm.Barrier()

    return row_data


def run_overhead_circuit(comm, scratch_dir: Path, wl: dict, backend: str,
                         eta_flag: bool, run_tag: str, *, return_metrics: bool = False):
    """Run a full-circuit execution under specified monitoring ETA setting."""
    rank = comm.Get_rank()
    run_dir = scratch_dir / f"overhead_{run_tag}_{uuid4().hex[:8]}"

    mkdir_err = None
    if rank == 0:
        try:
            run_dir.mkdir(parents=True, exist_ok=True)
        except Exception as exc:
            mkdir_err = str(exc)
    mkdir_errs = comm.allgather(mkdir_err)
    if any(e is not None for e in mkdir_errs):
        comm.Barrier()
        raise RuntimeError("mkdir failed: " + "; ".join(filter(None, mkdir_errs)))
    comm.Barrier()

    wall_time = None
    local_metrics = {"capture_count": 0, "capture_wall_seconds": 0.0,
                     "reported_extraction_seconds": 0.0,
                     "eta_evaluation_count": 0, "eta_evaluation_seconds": 0.0}
    restore_eta_class = None
    restore_eta_evaluate = None
    err = None
    try:
        cfg = DistributedConfig(
            str(run_dir),
            wl["block_bits"],
            ram_budget_bytes_per_rank=64 * 1024**2,
            metadata_reserve_bytes=8 * 1024**2,
            amplitude_store="file",
            backend=backend,
            storage_scope="node-local",
            frame_policy="materialize",
        )
        pol = AsyncPolicy(
            io_engine="cooperative",
            io_workers=1,
            prefetch_blocks=0,
            lookahead_gates=4,
            swap_policy="S2",
            cooldown_seconds=0,
        )
        with AsyncDistributedSimulator(wl["n"], cfg, comm=comm, policy=pol) as sim:
            sim.runtime.configure_measurements(max_bytes=2 * 1024**2)
            sim.runtime.configure_monitoring(eta=eta_flag)
            if eta_flag and return_metrics:
                from svsim.runtime_eta import RuntimeETA
                restore_eta_class = RuntimeETA
                restore_eta_evaluate = RuntimeETA.evaluate

                extractor = sim.runtime.remaining_extractor
                capture_original = extractor.capture

                def timed_capture(self, simulator, **kwargs):
                    started = perf_counter()
                    try:
                        snapshot = capture_original(simulator, **kwargs)
                        local_metrics["reported_extraction_seconds"] += snapshot.extraction_seconds
                        return snapshot
                    finally:
                        local_metrics["capture_wall_seconds"] += perf_counter() - started
                        local_metrics["capture_count"] += 1

                extractor.capture = MethodType(timed_capture, extractor)
                def timed_evaluate(instance, *args, **kwargs):
                    started = perf_counter()
                    try:
                        return restore_eta_evaluate(instance, *args, **kwargs)
                    finally:
                        local_metrics["eta_evaluation_seconds"] += perf_counter() - started
                        local_metrics["eta_evaluation_count"] += 1

                RuntimeETA.evaluate = timed_evaluate
            apply_recipe(sim, wl["recipe"], wl["layers"])
            binding = sim.bind()
            sim.runtime.update_eta(endpoint_frontier=len(sim.sequencer.operations) - 1, collective=True)  # arms progress_eta

            comm.Barrier()
            local_metrics = {"capture_count": 0, "capture_wall_seconds": 0.0,
                             "reported_extraction_seconds": 0.0,
                             "eta_evaluation_count": 0, "eta_evaluation_seconds": 0.0}
            storage_io_start = {
                "trace_event_count": sim.trace.event_count,
                "read_bytes": sim.trace.read_bytes,
                "write_bytes": sim.trace.write_bytes,
                "io_submitted": sim.io_engine.submitted,
                "io_completed": sim.io_engine.completed,
            }
            t0 = perf_counter()
            sim.materialize(binding=binding)
            t1 = perf_counter()
            local_wall = t1 - t0
            wall_time = max(comm.allgather(local_wall))
            if eta_flag and return_metrics:
                rank_metrics = comm.allgather(local_metrics)
                max_metrics = {}
                for key in ("capture_wall_seconds", "reported_extraction_seconds", "eta_evaluation_seconds"):
                    max_metrics[key] = max(row[key] for row in rank_metrics)
                for key in ("capture_count", "eta_evaluation_count"):
                    max_metrics[key] = max(row[key] for row in rank_metrics)
                local_metrics = max_metrics
            # The async store opens short-lived handles and closes them in each
            # completion callback, so store.files only describes currently
            # cached synchronous handles. Read completed I/O evidence from the
            # trace after stopping the timed materialize interval. Do not drain
            # pending requests here: record the exact observation window and
            # mark its backend list incomplete if requests remain outstanding.
            local_io_evidence = {
                "backend_identities": [],
                "read_bytes": max(0, sim.trace.read_bytes - storage_io_start["read_bytes"]),
                "write_bytes": max(0, sim.trace.write_bytes - storage_io_start["write_bytes"]),
                "read_events": 0,
                "write_events": 0,
                "read_event_bytes": 0,
                "write_event_bytes": 0,
                "submitted_requests": max(0, sim.io_engine.submitted - storage_io_start["io_submitted"]),
                "completed_requests": max(0, sim.io_engine.completed - storage_io_start["io_completed"]),
            }
            for event_index, event in enumerate(sim.trace.iter_events()):
                if event_index < storage_io_start["trace_event_count"]:
                    continue
                metadata = event.get("metadata") or {}
                operation = metadata.get("operation")
                if event.get("performance_class") not in {"storage-read", "storage-write"}:
                    continue
                if operation == "read":
                    local_io_evidence["read_events"] += 1
                    local_io_evidence["read_event_bytes"] += event.get("bytes") or 0
                elif operation == "write":
                    local_io_evidence["write_events"] += 1
                    local_io_evidence["write_event_bytes"] += event.get("bytes") or 0
                else:
                    continue
                resolved = metadata.get("backend")
                if isinstance(resolved, str) and resolved:
                    local_io_evidence["backend_identities"].append(resolved)
            local_io_evidence["backend_identities"] = sorted(set(local_io_evidence["backend_identities"]))
            local_io_evidence["outstanding_requests_at_observation"] = max(
                0, local_io_evidence["submitted_requests"] - local_io_evidence["completed_requests"])
            local_io_evidence["trace_event_bytes_match_counters"] = (
                local_io_evidence["read_event_bytes"] == local_io_evidence["read_bytes"] and
                local_io_evidence["write_event_bytes"] == local_io_evidence["write_bytes"])
            if local_io_evidence["outstanding_requests_at_observation"]:
                observation_status = "incomplete-pending-requests"
            elif not local_io_evidence["trace_event_bytes_match_counters"]:
                observation_status = "incomplete-trace-counter-mismatch"
            elif ((local_io_evidence["read_events"] + local_io_evidence["write_events"]) and
                  not local_io_evidence["backend_identities"]):
                observation_status = "incomplete-missing-backend-identity"
            elif not (local_io_evidence["read_events"] + local_io_evidence["write_events"]):
                observation_status = "complete-no-storage-io"
            else:
                observation_status = "complete"
            local_io_evidence["backend_observation_status"] = observation_status
            local_io_evidence["backend_observation_complete"] = observation_status in {
                "complete", "complete-no-storage-io"}
            local_io_evidence["observation_window"] = "completed storage events during materialize; sampled after wall stop"
            local_io_evidence["byte_counter_scope"] = "exact trace counter delta during materialize"
            local_io_evidence["event_identity_scope"] = "trace events since pre-materialize event count"
            local_io_evidence["cached_handle_backend_identities"] = sorted(
                {handle.backend_identity for handle in sim.store.files.values()})
            local_io_evidence["cached_handle_scope"] = "currently cached FileBlockStore handles; not async handles"
            local_metrics["storage_io_local"] = local_io_evidence
            local_metrics["storage_io_by_rank"] = comm.allgather(local_io_evidence)
            # Preserve the historical field but make its narrower meaning
            # explicit. Actual completion identities are in storage_io_by_rank.
            local_metrics["backend_resolved_by_rank"] = comm.allgather(
                local_io_evidence["cached_handle_backend_identities"])
            local_metrics["backend_resolved_by_rank_scope"] = (
                "currently cached FileBlockStore handles only; may be empty for AsyncStoreIO")
    except Exception as exc:
        err = str(exc)
    finally:
        if restore_eta_class is not None:
            restore_eta_class.evaluate = restore_eta_evaluate

    errs = comm.allgather(err)
    if any(e is not None for e in errs):
        comm.Barrier()
        if rank == 0:
            shutil.rmtree(run_dir, ignore_errors=True)
        comm.Barrier()
        raise RuntimeError("Overhead run failed: " + "; ".join(filter(None, errs)))

    comm.Barrier()
    if rank == 0:
        shutil.rmtree(run_dir, ignore_errors=True)
    comm.Barrier()

    if return_metrics:
        return {"actual_rank_max_seconds": wall_time, **local_metrics}
    return wall_time


def measure_monitor_overhead(comm, scratch_dir: Path, workloads: list[dict],
                             backend: str, num_pairs: int) -> dict:
    """Perform paired full-circuit runs with alternating monitoring switches."""
    results = {}
    for wl in workloads:
        warmup_record = None
        measured_pairs = []

        for pair_idx in range(1 + num_pairs):
            is_warmup = (pair_idx == 0)
            first_is_true = (pair_idx % 2 == 0)

            tag_a = f"{wl['name']}_p{pair_idx}_A"
            tag_b = f"{wl['name']}_p{pair_idx}_B"

            if first_is_true:
                wall_true = run_overhead_circuit(comm, scratch_dir, wl, backend, True, tag_a)
                wall_false = run_overhead_circuit(comm, scratch_dir, wl, backend, False, tag_b)
            else:
                wall_false = run_overhead_circuit(comm, scratch_dir, wl, backend, False, tag_a)
                wall_true = run_overhead_circuit(comm, scratch_dir, wl, backend, True, tag_b)

            diff = wall_true - wall_false
            pair_data = {
                "pair_index": pair_idx,
                "warmup": is_warmup,
                "order": ["eta=True", "eta=False"] if first_is_true else ["eta=False", "eta=True"],
                "wall_eta_true_seconds": wall_true,
                "wall_eta_false_seconds": wall_false,
                "difference_seconds": diff,
            }

            if is_warmup:
                warmup_record = pair_data
            else:
                measured_pairs.append(pair_data)

        true_walls = [p["wall_eta_true_seconds"] for p in measured_pairs]
        false_walls = [p["wall_eta_false_seconds"] for p in measured_pairs]
        diffs = [p["difference_seconds"] for p in measured_pairs]

        med_true = float(statistics.median(true_walls)) if true_walls else 0.0
        med_false = float(statistics.median(false_walls)) if false_walls else 0.0
        med_diff = float(statistics.median(diffs)) if diffs else 0.0
        wall_diff_of_medians = med_true - med_false
        ratio = float(med_true / med_false) if med_false > 0 else None

        results[wl["name"]] = {
            "workload": wl["name"],
            "n": wl["n"],
            "block_bits": wl["block_bits"],
            "measured_pair_count": len(measured_pairs),
            "warmup_pair": warmup_record,
            "measured_pairs": measured_pairs,
            "median_eta_true_seconds": med_true,
            "median_eta_false_seconds": med_false,
            "median_wall_difference_seconds": med_diff,
            "difference_of_medians_seconds": wall_diff_of_medians,
            "ratio_of_medians": ratio,
        }

    return results


def run_mid_epoch(comm, scratch_dir: Path, wl: dict, policy: str, store: str, backend: str, frac: float,
                  trial_idx: int, total_calls: int | None) -> dict:
    """One mid-epoch check: non-collective update_eta() from inside the after_executor hook (private epoch).

    total_calls=None counts the executor calls of an unchecked run. Otherwise the check fires once, on call
    int(frac*total_calls); actual remaining = wall from check start to the end minus the check's own duration.
    Single-rank only (the hook is not collective).
    """
    trial_dir = scratch_dir / f"mid_{wl['name']}_{policy}_{store}_{int(frac*100)}_{trial_idx}_{uuid4().hex[:8]}"
    trial_dir.mkdir(parents=True, exist_ok=True)
    out = {"status": "ok", "workload": wl["name"], "policy": policy, "store": store, "backend": backend,
           "frac": frac, "trial": trial_idx, "ranks": 1, "mid_epoch": True}
    try:
        cfg = DistributedConfig(str(trial_dir), wl["block_bits"], ram_budget_bytes_per_rank=64 * 1024**2,
                                metadata_reserve_bytes=8 * 1024**2, amplitude_store=store, backend=backend,
                                storage_scope="node-local", frame_policy="materialize")
        pol = AsyncPolicy(io_engine="cooperative", io_workers=1, prefetch_blocks=0, lookahead_gates=4,
                          swap_policy=policy, cooldown_seconds=0)
        with AsyncDistributedSimulator(wl["n"], cfg, comm=comm, policy=pol) as sim:
            sim.runtime.configure_measurements(max_bytes=2 * 1024**2)
            apply_recipe(sim, wl["recipe"], wl["layers"])
            last = len(sim.sequencer.operations) - 1
            binding = sim.bind()
            sim.runtime.update_eta(endpoint_frontier=last)  # sets the endpoint
            state = {"calls": 0, "fired": None}
            original = sim.runtime.after_executor

            def hook(*a, **k):
                result = original(*a, **k)
                state["calls"] += 1
                if total_calls and state["fired"] is None and state["calls"] >= int(frac * total_calls):
                    t0 = perf_counter()
                    eta = sim.runtime.update_eta()
                    d = perf_counter() - t0
                    view = eta["views"]["observed"]
                    state["fired"] = {"t0": t0, "check_seconds": d, "eta": view["expected_seconds"],
                                      "lower": view["lower_seconds"], "upper": view["upper_seconds"],
                                      "unknown_reasons": list(view.get("unknown_reasons") or ()),
                                      "private": eta.get("private"), "call": state["calls"]}
                return result

            sim.runtime.after_executor = hook
            start = perf_counter()
            sim.materialize(binding=binding)
            end = perf_counter()
            out["total_calls"] = state["calls"]
            f = state["fired"]
            if total_calls is not None:
                if f is None:
                    out["status"] = "error"; out["error"] = "check never fired"
                else:
                    actual = (end - f["t0"]) - f["check_seconds"]
                    out.update({k: f[k] for k in ("eta", "lower", "upper", "unknown_reasons", "private", "call", "check_seconds")})
                    out["actual"] = actual
                    out["relative_error"] = (f["eta"] - actual) / actual if f["eta"] is not None and actual > 0 else None
                    out["covered"] = (f["lower"] <= actual <= f["upper"]) if f["lower"] is not None and f["upper"] is not None else None
    except Exception as exc:
        out["status"] = "error"; out["error"] = repr(exc)
    shutil.rmtree(trial_dir, ignore_errors=True)
    return out


def run_mid_epoch_campaign(comm, scratch_dir: Path, workloads: list[dict], backend: str, trials: int) -> list[dict]:
    rows = []
    if comm.Get_size() != 1:
        return rows
    for wl in workloads:
        for policy in ("S0", "S2"):
            count = run_mid_epoch(comm, scratch_dir, wl, policy, "ram", backend, 0.0, 0, None).get("total_calls")
            for frac in (0.6, 0.8):
                for t in range(trials):
                    rows.append(run_mid_epoch(comm, scratch_dir, wl, policy, "ram", backend, frac, t, count))
    return rows


def print_results_table(summary: dict, overhead_results: dict, env_info: dict,
                        output_path: str):
    """Print ASCII summary table to stdout from rank 0."""
    print("=" * 96)
    print("                Held-out Runtime ETA Evaluation (Observed View)")
    print("=" * 96)
    print("Environment:")
    print(f"  Platform:       {env_info.get('platform')}")
    print(f"  Python:         {env_info.get('python')}")
    print(f"  MPI Ranks:      {env_info.get('ranks')}")
    print(f"  Backend:        {env_info.get('backend')}")
    print(f"  Git Revision:   {env_info.get('git_rev') or 'unknown'}")
    print("-" * 96)

    header = f"{'Slice':<22} {'Checks':>7} {'Answered':>9} {'Answer%':>9} {'Coverage':>9} {'Med Err':>10} {'P95 Err':>10} {'Worst Err':>10}"
    print(header)
    print("-" * 96)

    def format_row(name: str, s: dict) -> str:
        checks = s.get("n_checks", 0)
        ans = s.get("n_answered", 0)
        ans_pct = f"{s.get('answered_fraction', 0.0) * 100:6.1f}%" if checks > 0 else "N/A"
        cov = f"{s.get('coverage', 0.0) * 100:6.1f}%" if s.get("coverage") is not None else "N/A"
        med = f"{s.get('median_abs_relative_error', 0.0):10.4f}" if s.get("median_abs_relative_error") is not None else "N/A"
        p95 = f"{s.get('p95_abs_relative_error', 0.0):10.4f}" if s.get("p95_abs_relative_error") is not None else "N/A"
        worst = f"{s.get('worst_abs_relative_error', 0.0):10.4f}" if s.get("worst_abs_relative_error") is not None else "N/A"
        return f"{name:<22} {checks:>7} {ans:>9} {ans_pct:>9} {cov:>9} {med:>10} {p95:>10} {worst:>10}"

    print(format_row("Overall", summary.get("overall", {})))

    for cat_name, key in [
        ("Policy", "by_policy"),
        ("Store", "by_store"),
        ("Engine", "by_engine"),
        ("Ranks", "by_ranks"),
        ("Workload", "by_workload"),
    ]:
        cat_dict = summary.get(key, {})
        for sub_name, s in cat_dict.items():
            print(format_row(f"{cat_name}: {sub_name}", s))

    print("-" * 96)

    overall_unanswered = summary.get("overall", {}).get("unknown_reason_tally", {})
    if overall_unanswered:
        print("Unanswered Unknown Reason Tally:")
        for reason, count in sorted(overall_unanswered.items()):
            print(f"  {reason:<45}: {count}")
        print("-" * 96)

    if overhead_results:
        print("Monitor Overhead Summary (S2, file store, cooperative):")
        ov_hdr = f"{'Workload':<16} {'Pairs':>6} {'Median True (s)':>16} {'Median False (s)':>17} {'Median Diff (s)':>16} {'Ratio (T/F)':>13}"
        print(ov_hdr)
        print("-" * 96)
        for wl_name, r in overhead_results.items():
            diff_sign = "+" if r["median_wall_difference_seconds"] >= 0 else ""
            diff_str = f"{diff_sign}{r['median_wall_difference_seconds']:.4f}"
            ratio_str = f"{r['ratio_of_medians']:.4f}" if r["ratio_of_medians"] is not None else "N/A"
            print(
                f"{wl_name:<16} {r['measured_pair_count']:>6} {r['median_eta_true_seconds']:>16.4f} "
                f"{r['median_eta_false_seconds']:>17.4f} {diff_str:>16} {ratio_str:>13}"
            )
        print("-" * 96)

    print(f"Results written to: {output_path}")
    print("=" * 96)


def main():
    parser = argparse.ArgumentParser(
        description="Held-out evaluation of runtime ETA 'observed' view vs real remaining wall time."
    )
    parser.add_argument("--output", type=str, default="observed_eta_results.json", help="Path to write output JSON")
    parser.add_argument("--scratch", type=str, default="./scratch_p4_002", help="Scratch directory for trials")
    parser.add_argument("--backend", type=str, default="buffered", choices=["buffered", "direct"], help="Storage backend")
    parser.add_argument("--engines", type=str, default="cooperative", help="Comma-separated I/O engines (e.g. cooperative,worker)")
    parser.add_argument("--trials", type=int, default=3, help="Number of trials per (workload, frac) configuration")
    parser.add_argument("--quick", action="store_true", help="Quick smoke test with fewer trials/configurations")
    parser.add_argument("--overhead-pairs", type=int, default=5, help="Number of measured overhead pairs")
    parser.add_argument("--mid-epoch", action="store_true", help="also run mid-epoch (private epoch) checks, 1 rank only")
    parser.add_argument("--workloads", type=str, default="", help="Comma-separated workload names (default: all)")
    parser.add_argument("--skip-overhead", action="store_true", help="Skip the monitor-overhead pairs")
    parser.add_argument("--policies", type=str, default="S0,S2", help="Comma-separated swap policies (e.g. S0,S2)")
    parser.add_argument("--stores", type=str, default="file,ram", help="Comma-separated amplitude stores (e.g. file,ram)")

    args = parser.parse_args()

    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()

    scratch_dir = Path(args.scratch).resolve()
    if rank == 0:
        scratch_dir.mkdir(parents=True, exist_ok=True)
    comm.Barrier()

    policies = [p.strip() for p in args.policies.split(",") if p.strip()]
    stores = [s.strip() for s in args.stores.split(",") if s.strip()]
    engines = [e.strip() for e in args.engines.split(",") if e.strip()]

    if args.quick:
        trials = 1
        fractions = (0.6,)
        overhead_pairs = min(args.overhead_pairs, 1)
    else:
        trials = args.trials
        fractions = (0.4, 0.6, 0.8)
        overhead_pairs = args.overhead_pairs

    workloads = [w for w in WORKLOADS if not args.workloads or w["name"] in args.workloads.split(",")]
    rows = []

    # Run ETA evaluation campaign across parameter grid
    for wl in workloads:
        for policy in policies:
            for store in stores:
                for engine in engines:
                    for frac in fractions:
                        for trial_idx in range(trials):
                            row = run_trial(
                                comm,
                                scratch_dir,
                                wl,
                                frac,
                                trial_idx,
                                policy,
                                store,
                                engine,
                                args.backend,
                            )
                            rows.append(row)
                            if rank == 0:  # crash-safe progress record
                                with open(str(Path(args.output)) + ".rows.jsonl", "a", encoding="utf-8") as pf:
                                    pf.write(json.dumps(row) + "\n")

    # Monitor overhead evaluation on 2 workloads (S2, file store, cooperative)
    overhead_workload_candidates = [w for w in workloads if w["name"] in ("rx-ry-low", "upper")]
    if len(overhead_workload_candidates) < 2:
        overhead_workload_candidates = workloads[:2]

    overhead_results = {} if args.skip_overhead else measure_monitor_overhead(
        comm, scratch_dir, overhead_workload_candidates, args.backend, overhead_pairs
    )

    mid_rows = run_mid_epoch_campaign(comm, scratch_dir, workloads, args.backend, 2) if args.mid_epoch else []

    # Clean up top-level scratch directory if empty
    comm.Barrier()
    if rank == 0:
        try:
            if scratch_dir.exists() and not any(scratch_dir.iterdir()):
                scratch_dir.rmdir()
        except Exception:
            pass
    comm.Barrier()

    # Compile environment information
    project_root = Path(__file__).resolve().parent.parent
    env_info = {
        "platform": platform.platform(),
        "python": platform.python_version(),
        "ranks": size,
        "backend": args.backend,
        "git_rev": os.environ.get("DDQ_GIT_REV") or get_git_rev(project_root),
        "git_dirty_src": os.environ.get("DDQ_GIT_DIRTY_SRC") or get_dirty_src(project_root),
    }

    # Summary breakdowns
    workload_names = [w["name"] for w in workloads]
    summary = {
        "overall": compute_slice_summary(rows),
        "by_policy": {p: compute_slice_summary([r for r in rows if r.get("policy") == p]) for p in policies},
        "by_store": {s: compute_slice_summary([r for r in rows if r.get("store") == s]) for s in stores},
        "by_engine": {e: compute_slice_summary([r for r in rows if r.get("engine") == e]) for e in engines},
        "by_ranks": {str(size): compute_slice_summary([r for r in rows if r.get("ranks") == size])},
        "by_workload": {w: compute_slice_summary([r for r in rows if r.get("workload") == w]) for w in workload_names},
    }

    output_data = {
        "schema_version": 1,
        "kind": "runtime-eta-accuracy",
        "env": env_info,
        "summary": summary,
        "overall": summary["overall"],
        "by_policy": summary["by_policy"],
        "by_store": summary["by_store"],
        "by_engine": summary["by_engine"],
        "by_ranks": summary["by_ranks"],
        "by_workload": summary["by_workload"],
        "monitor_overhead": overhead_results,
        "mid_epoch_summary": compute_slice_summary(mid_rows) if mid_rows else None,
        "mid_epoch_rows": mid_rows,
        "rows": rows,
    }

    if rank == 0:
        out_path = Path(args.output).resolve()
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(output_data, f, indent=2)

        print_results_table(summary, overhead_results, env_info, str(out_path))


if __name__ == "__main__":
    main()
