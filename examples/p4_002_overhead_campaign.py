#!/usr/bin/env python3
"""Finite paired ETA-monitoring overhead campaign for P4-002.

The timing path is delegated to ``p4_002_observed_eta.run_overhead_circuit``.
This script records failures as data and never applies an acceptance threshold:
confidence intervals that cross zero (or a ratio of one) are inconclusive.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import random
import statistics
import subprocess
import sys

from mpi4py import MPI

import p4_002_observed_eta as observed


DEFAULT_WORKLOADS = ("rx-ry-low", "mixed")
BOOTSTRAP_METHOD = "paired percentile bootstrap of median, 95% CI"


def percentile(values, p):
    if not values:
        return None
    ordered = sorted(values)
    k = (len(ordered) - 1) * p / 100.0
    lo, hi = math.floor(k), math.ceil(k)
    if lo == hi:
        return float(ordered[lo])
    return float(ordered[lo] * (hi - k) + ordered[hi] * (k - lo))


def bootstrap_median_ci(values, *, seed, replicates=5000):
    """Percentile interval from bootstrap resampling paired observations."""
    if len(values) < 2:
        return {"method": BOOTSTRAP_METHOD, "confidence": 0.95, "replicates": replicates,
                "seed": seed, "lower": None, "upper": None, "status": "insufficient-pairs"}
    rng = random.Random(seed)
    n = len(values)
    estimates = [statistics.median(values[rng.randrange(n)] for _ in range(n))
                 for _ in range(replicates)]
    return {"method": BOOTSTRAP_METHOD, "confidence": 0.95, "replicates": replicates,
            "seed": seed, "lower": percentile(estimates, 2.5), "upper": percentile(estimates, 97.5),
            "status": "computed"}


def summarize_pairs(rows, *, seed, bootstrap_replicates=5000):
    good = [row for row in rows if row.get("status") == "ok"]
    differences = [row["eta_on_seconds"] - row["eta_off_seconds"] for row in good]
    ratios = [row["eta_on_seconds"] / row["eta_off_seconds"] for row in good
              if row["eta_off_seconds"] > 0]
    med_diff = statistics.median(differences) if differences else None
    med_ratio = statistics.median(ratios) if ratios else None
    diff_ci = bootstrap_median_ci(differences, seed=seed, replicates=bootstrap_replicates)
    ratio_ci = bootstrap_median_ci(ratios, seed=seed + 1, replicates=bootstrap_replicates)
    if not differences or diff_ci["lower"] is None or ratio_ci["lower"] is None:
        conclusion = "inconclusive"
    elif diff_ci["lower"] <= 0 <= diff_ci["upper"] or ratio_ci["lower"] <= 1 <= ratio_ci["upper"]:
        conclusion = "inconclusive"
    elif diff_ci["lower"] > 0 and ratio_ci["lower"] > 1:
        conclusion = "eta-on-slower"
    elif diff_ci["upper"] < 0 and ratio_ci["upper"] < 1:
        conclusion = "eta-on-faster"
    else:
        conclusion = "inconclusive"
    eta_on = [row["eta_on_seconds"] for row in good]
    eta_off = [row["eta_off_seconds"] for row in good]

    def distribution(values):
        return {"median_seconds": statistics.median(values) if values else None,
                "p95_seconds": percentile(values, 95),
                "sample_stdev_seconds": statistics.stdev(values) if len(values) > 1 else None,
                "coefficient_of_variation": (statistics.stdev(values) / abs(statistics.mean(values))
                                             if len(values) > 1 and statistics.mean(values) != 0 else None)}
    return {
        "status": "complete" if len(good) == len(rows) else "incomplete",
        "requested_pair_count": len(rows), "successful_pair_count": len(good),
        "failed_pair_count": len(rows) - len(good),
        "median_paired_difference_seconds": med_diff,
        "p95_paired_difference_seconds": percentile(differences, 95),
        "paired_difference_sample_stdev_seconds": statistics.stdev(differences) if len(differences) > 1 else None,
        "paired_difference_coefficient_of_variation": (
            statistics.stdev(differences) / abs(statistics.mean(differences))
            if len(differences) > 1 and statistics.mean(differences) != 0 else None),
        "median_paired_ratio_eta_on_over_eta_off": med_ratio,
        "p95_paired_ratio_eta_on_over_eta_off": percentile(ratios, 95),
        "eta_on_wall": distribution(eta_on), "eta_off_wall": distribution(eta_off),
        "difference_ci95": diff_ci, "ratio_ci95": ratio_ci,
        "interpretation": conclusion,
        "ci_note": "CI crossing zero/one is inconclusive; no minimum effect or response-rate threshold is imposed.",
    }


def _result_seconds(result):
    if isinstance(result, dict):
        return result["actual_rank_max_seconds"]
    return float(result)


def run_campaign(comm, *, workloads, backend, scratch_dir=Path("."), warmups=2, pairs=7, seed=20261016,
                 bootstrap_replicates=5000, run_one=None, progress_path=None):
    """Run seeded alternating ETA on/off pairs; ``run_one`` is injectable for smoke tests."""
    run_one = run_one or observed.run_overhead_circuit
    rng = random.Random(seed)
    rank = comm.Get_rank()
    results = {}
    for wl_index, wl in enumerate(workloads):
        first_eta_on = bool(rng.getrandbits(1))
        pair_rows = []
        for pair_index in range(warmups + pairs):
            warmup = pair_index < warmups
            first_on = first_eta_on if pair_index % 2 == 0 else not first_eta_on
            order = (True, False) if first_on else (False, True)
            run_results = {}
            errors = {}
            for eta_flag in order:
                tag = f"{wl['name']}_p{pair_index}_{'on' if eta_flag else 'off'}"
                try:
                    run_results[eta_flag] = run_one(comm, Path(scratch_dir), wl, backend, eta_flag, tag,
                                                    return_metrics=True)
                except Exception as exc:  # preserve each failed side and continue the finite campaign
                    errors["eta_on" if eta_flag else "eta_off"] = f"{type(exc).__name__}: {exc}"
                # Same seeded alternating order on every rank; no rank-local RNG is used.
            row = {
                "workload": wl["name"], "pair_index": pair_index, "warmup": warmup,
                "order": ["eta-on" if value else "eta-off" for value in order],
                "status": "ok" if not errors and len(run_results) == 2 else "failed",
                "eta_on": run_results.get(True), "eta_off": run_results.get(False),
                "errors": errors,
            }
            if row["status"] == "ok":
                row["eta_on_seconds"] = _result_seconds(run_results[True])
                row["eta_off_seconds"] = _result_seconds(run_results[False])
                row["difference_seconds"] = row["eta_on_seconds"] - row["eta_off_seconds"]
                row["ratio_eta_on_over_eta_off"] = (row["eta_on_seconds"] / row["eta_off_seconds"]
                                                      if row["eta_off_seconds"] > 0 else None)
            pair_rows.append(row)
            if rank == 0 and progress_path:
                with open(progress_path, "a", encoding="utf-8") as stream:
                    stream.write(json.dumps(row, sort_keys=True) + "\n")

        measured = [row for row in pair_rows if not row["warmup"]]
        results[wl["name"]] = {
            "workload": wl["name"], "workload_class": wl.get("campaign_class"),
            "status": "complete" if all(row["status"] == "ok" for row in pair_rows) else "incomplete",
            "warmup_pair_count": warmups, "measured_pair_count": pairs,
            "initial_order_eta_on_first": first_eta_on,
            "warmup_pairs": [row for row in pair_rows if row["warmup"]],
            "measured_pairs": measured,
            "summary": summarize_pairs(measured, seed=seed + wl_index * 17,
                                       bootstrap_replicates=bootstrap_replicates),
        }
    return results


def source_metadata():
    root = Path(__file__).resolve().parent.parent
    files = [Path(__file__).resolve(), Path(observed.__file__).resolve(),
             root / "src/svsim/runtime_eta.py", root / "src/svsim/runtime_control.py",
             root / "src/svsim/remaining_work.py"]
    hashes = {}
    for path in files:
        hashes[str(path.relative_to(root))] = hashlib.sha256(path.read_bytes()).hexdigest()
    try:
        revision = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root,
                                           text=True, stderr=subprocess.DEVNULL).strip()
    except Exception:
        revision = None
    return {"git_revision": revision, "sha256": hashes}


def resolved_conditions(backend, ranks, workloads):
    return {
        "monitoring_delta": "RuntimeControl ETA monitoring enabled versus disabled",
        "shared_conditions": {"amplitude_store": "file", "io_engine": "cooperative", "io_workers": 1,
                              "prefetch_blocks": 0, "lookahead_gates": 4, "swap_policy": "S2",
                              "cooldown_seconds": 0, "storage_scope": "node-local",
                              "frame_policy": "materialize", "ram_budget_bytes_per_rank": 64 * 1024**2,
                              "metadata_reserve_bytes": 8 * 1024**2,
                              "measurement_collector_max_bytes": 2 * 1024**2,
                              "backend_resolved": backend},
        "ranks": ranks,
        "threads": {name: os.environ.get(name) for name in
                    ("OMP_NUM_THREADS", "MKL_NUM_THREADS", "OPENBLAS_NUM_THREADS", "NUMEXPR_NUM_THREADS")},
        "workloads": [{"name": wl["name"], "campaign_class": wl["campaign_class"],
                       "n": wl["n"], "block_bits": wl["block_bits"], "layers": wl["layers"],
                       "recipe_sha256": hashlib.sha256(json.dumps(wl["recipe"], sort_keys=True,
                                                                    separators=(",", ":")).encode()).hexdigest(),
                       "gate_count": sum(len(layer) for layer in
                                         (wl["recipe"] if wl["recipe"] and isinstance(wl["recipe"][0], list)
                                          else [wl["recipe"]] * wl["layers"]))}
                      for wl in workloads],
    }


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", default="p4_002_overhead_results.json")
    parser.add_argument("--scratch", default="./scratch_p4_002_overhead")
    parser.add_argument("--backend", choices=("buffered", "direct"), default="buffered")
    parser.add_argument("--workloads", default=",".join(DEFAULT_WORKLOADS))
    parser.add_argument("--warmups", type=int, default=2)
    parser.add_argument("--pairs", type=int, default=7)
    parser.add_argument("--seed", type=int, default=20261016)
    parser.add_argument("--bootstrap-replicates", type=int, default=5000)
    parser.add_argument("--plan-only", action="store_true", help="validate and emit conditions without running trials")
    args = parser.parse_args(argv)
    if args.warmups < 0 or args.pairs < 1 or args.bootstrap_replicates < 100:
        parser.error("warmups must be nonnegative, pairs >= 1, and bootstrap-replicates >= 100")
    selected = set(args.workloads.split(","))
    definitions = {"rx-ry-low": ("small", 0), "mixed": ("long", 3)}
    missing = selected - definitions.keys()
    if missing:
        parser.error("unknown or unsupported campaign workload(s): " + ",".join(sorted(missing)))
    workloads = []
    for name in ("rx-ry-low", "mixed"):
        if name not in selected:
            continue
        campaign_class, index = definitions[name]
        wl = dict(observed.WORKLOADS[index])
        wl["campaign_class"] = campaign_class
        workloads.append(wl)
    comm = MPI.COMM_WORLD
    rank, ranks = comm.Get_rank(), comm.Get_size()
    output = Path(args.output).resolve()
    progress_path = str(output) + ".rows.jsonl"
    metadata = {
        "schema_version": 1, "kind": "p4-002-monitoring-overhead-campaign",
        "source": source_metadata(), "created_unix_seconds": __import__("time").time(),
        "platform": platform.platform(), "python": sys.version,
        "measurement_clock": {"clock": "time.perf_counter", "scope": "local wall; actual run reported as max across ranks",
                              "capture_and_evaluation": "per-rank accumulated wall; report rank maximum"},
        "campaign_seed": args.seed, "pair_order": "seeded initial side, then alternating side per pair",
        "warmup_pairs_per_workload": args.warmups, "measured_pairs_per_workload": args.pairs,
        "bootstrap_replicates": args.bootstrap_replicates,
        "conditions": resolved_conditions(args.backend, ranks, workloads),
    }
    if args.plan_only:
        if rank == 0:
            output.parent.mkdir(parents=True, exist_ok=True)
            output.write_text(json.dumps({**metadata, "status": "plan-only", "workloads": []}, indent=2),
                              encoding="utf-8")
        return 0
    scratch = Path(args.scratch).resolve()
    if rank == 0:
        scratch.mkdir(parents=True, exist_ok=True)
        output.parent.mkdir(parents=True, exist_ok=True)
        Path(progress_path).write_text("", encoding="utf-8")
    comm.Barrier()
    results = run_campaign(comm, workloads=workloads, backend=args.backend,
                           scratch_dir=scratch,
                           warmups=args.warmups, pairs=args.pairs, seed=args.seed,
                           bootstrap_replicates=args.bootstrap_replicates, progress_path=progress_path)
    if rank == 0:
        payload = {**metadata, "status": "complete" if all(v["status"] == "complete"
                                                             for v in results.values()) else "incomplete",
                   "results": results, "progress_jsonl": str(Path(progress_path).resolve())}
        output.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
