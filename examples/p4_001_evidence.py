#!/usr/bin/env python3
"""P4-001 Reproducible Evidence-Bundle Tool.

Scans evidence directories, computes immutable file digests, classifies artifacts,
summarizes benchmark runs and model calibrations, and produces a reproducible manifest
and markdown report.
"""

from __future__ import annotations

import argparse
import datetime
import hashlib
import json
import math
import os
import subprocess
import sys
from pathlib import Path, PurePosixPath
from typing import Any, Optional

MAX_FILES = 20000

SCANNED_SUBDIRS = [
    "phase5_timing_validation",
    "phase5_crosshost",
    "phase4_monitor_overhead",
    "phase4_ledger_lease_validation",
    "phase4_adaptive_validation",
    "phase4_foundation_validation",
    "phase5_mac_calibration",
]

KNOWN_KINDS = {
    "benchmark-run",
    "time-model",
    "runtime-eta-accuracy",
    "comparison-report",
    "hardware-profile",
    "execution-plan",
    "runtime-calibration",
}


def to_float(val: Any) -> Optional[float]:
    """Defensively convert value to float."""
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def to_int(val: Any) -> Optional[int]:
    """Defensively convert value to int."""
    if val is None:
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


def calculate_file_sha256(filepath: Path) -> str:
    """Compute SHA-256 of a file."""
    h = hashlib.sha256()
    with open(filepath, "rb") as f:
        while chunk := f.read(65536):
            h.update(chunk)
    return h.hexdigest()


def calculate_bundle_sha256(files_list: list[dict[str, Any]]) -> str:
    """Compute bundle SHA-256 over canonical JSON of the files list."""
    canonical = json.dumps(files_list, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def get_git_info(root_dir: Path) -> tuple[Optional[str], bool]:
    """Retrieve git revision and dirty status."""
    try:
        rev = (
            subprocess.check_output(
                ["git", "rev-parse", "HEAD"],
                cwd=root_dir,
                stderr=subprocess.DEVNULL,
                text=True,
            )
            .strip()
        )
    except Exception:
        rev = None

    try:
        status = (
            subprocess.check_output(
                ["git", "status", "--porcelain"],
                cwd=root_dir,
                stderr=subprocess.DEVNULL,
                text=True,
            )
            .strip()
        )
        dirty = bool(status)
    except Exception:
        dirty = False

    return rev, dirty


def calculate_median(sorted_vals: list[float]) -> Optional[float]:
    """Calculate median from a sorted list of numbers."""
    n = len(sorted_vals)
    if n == 0:
        return None
    if n % 2 == 1:
        return sorted_vals[n // 2]
    return (sorted_vals[n // 2 - 1] + sorted_vals[n // 2]) / 2.0


def percentile_p95(sorted_vals: list[float]) -> Optional[float]:
    """Calculate 95th percentile using linear interpolation at (n-1)*0.95."""
    n = len(sorted_vals)
    if n == 0:
        return None
    if n == 1:
        return sorted_vals[0]
    idx = (n - 1) * 0.95
    low = int(math.floor(idx))
    high = int(math.ceil(idx))
    frac = idx - low
    if low == high:
        return sorted_vals[low]
    return sorted_vals[low] + frac * (sorted_vals[high] - sorted_vals[low])


def unwrap_comparison_report(doc: dict[str, Any]) -> Optional[dict[str, Any]]:
    """Unwrap a single top-level dict whose value has kind == 'comparison-report'."""
    if not isinstance(doc, dict):
        return None
    if doc.get("kind") == "comparison-report":
        return doc
    comp_children = [
        v
        for v in doc.values()
        if isinstance(v, dict) and v.get("kind") == "comparison-report"
    ]
    if len(comp_children) == 1:
        return comp_children[0]
    return None


def classify_json_document(data: Any) -> tuple[str, Any]:
    """Classify parsed JSON document and unwrap if needed."""
    if not isinstance(data, dict):
        return "other", data

    # Check comparison-report (including wrapped form)
    unwrapped_comp = unwrap_comparison_report(data)
    if unwrapped_comp is not None:
        return "comparison-report", unwrapped_comp

    top_kind = data.get("kind")
    if top_kind in KNOWN_KINDS:
        return top_kind, data

    return "other", data


def scan_and_collect_evidence(
    root: Path, max_files: int = MAX_FILES
) -> tuple[list[dict[str, Any]], list[str], list[tuple[dict[str, Any], str, Any]]]:
    """Scan target subdirectories, collect file hashes, and classify artifacts."""
    file_records: list[dict[str, Any]] = []
    unparsed: list[str] = []
    classified_entries: list[tuple[dict[str, Any], str, Any]] = []

    total_scanned_count = 0

    for subdir in SCANNED_SUBDIRS:
        subdir_path = root / subdir
        if not subdir_path.is_dir():
            continue

        for dirpath, _, filenames in os.walk(subdir_path):
            for fname in filenames:
                total_scanned_count += 1
                if total_scanned_count > max_files:
                    sys.stderr.write(
                        f"Error: File limit of {max_files} exceeded (found >{max_files} files).\n"
                    )
                    sys.exit(2)

                fpath = Path(dirpath) / fname
                rel_path = fpath.relative_to(root).as_posix()
                size = fpath.stat().st_size
                sha256 = calculate_file_sha256(fpath)

                kind = "other"
                parsed_doc = None

                if fname.endswith(".json"):
                    try:
                        with open(fpath, "r", encoding="utf-8") as f:
                            data = json.load(f)
                        kind, parsed_doc = classify_json_document(data)
                    except Exception:
                        unparsed.append(rel_path)
                        kind = "other"
                        parsed_doc = None

                record = {
                    "path": rel_path,
                    "size": size,
                    "sha256": sha256,
                    "kind": kind,
                }
                file_records.append(record)
                if parsed_doc is not None:
                    classified_entries.append((record, kind, parsed_doc))

    # Deterministic sorting
    file_records.sort(key=lambda r: r["path"])
    unparsed.sort()
    return file_records, unparsed, classified_entries


def build_summaries(
    classified_entries: list[tuple[dict[str, Any], str, Any]]
) -> dict[str, Any]:
    """Build structured summaries for each artifact kind."""
    # 1. Benchmark-run summary
    benchmark_runs: list[tuple[dict[str, Any], dict[str, Any]]] = []
    time_models: list[dict[str, Any]] = []
    eta_accuracies: list[dict[str, Any]] = []
    comparison_reports: list[dict[str, Any]] = []
    hardware_profiles: list[dict[str, Any]] = []
    runtime_calibrations: list[dict[str, Any]] = []

    for file_record, kind, doc in classified_entries:
        rel_path = file_record["path"]

        if kind == "benchmark-run":
            if isinstance(doc, dict) and doc.get("run_type") == "workload-execution":
                benchmark_runs.append((file_record, doc))

        elif kind == "time-model":
            if isinstance(doc, dict):
                sync_lat = doc.get("sync_latency")
                cal_ids = doc.get("calibration_run_ids")
                cal_ids_count = len(cal_ids) if isinstance(cal_ids, list) else 0

                excluded = None
                if isinstance(sync_lat, dict):
                    excluded = sync_lat.get("excluded_unstable_trial_count")
                if excluded is None:
                    excluded = doc.get("excluded_unstable_trial_count")
                excluded_count = to_int(excluded) if excluded is not None else 0

                time_models.append(
                    {
                        "path": rel_path,
                        "model_id": doc.get("model_id"),
                        "condition": doc.get("condition"),
                        "sync_latency": sync_lat,
                        "calibration_run_ids_count": cal_ids_count,
                        "excluded_unstable_trial_count": excluded_count,
                    }
                )

        elif kind == "runtime-eta-accuracy":
            if isinstance(doc, dict):
                eta_accuracies.append(
                    {
                        "path": rel_path,
                        "check_count": to_int(doc.get("check_count")),
                        "predicted_count": to_int(doc.get("predicted_count")),
                        "median_abs_relative_error": to_float(
                            doc.get("median_abs_relative_error")
                        ),
                        "p95_abs_relative_error": to_float(
                            doc.get("p95_abs_relative_error")
                        ),
                        "worst_abs_relative_error": to_float(
                            doc.get("worst_abs_relative_error")
                        ),
                        "interval_coverage": to_float(doc.get("interval_coverage")),
                    }
                )

        elif kind == "comparison-report":
            if isinstance(doc, dict):
                extracted_rows: list[dict[str, Any]] = []
                for row in doc.get("rows") or []:
                    if not isinstance(row, dict):
                        continue
                    v_id = row.get("variant_id")
                    wall_med = None
                    metrics = row.get("metrics")
                    if isinstance(metrics, dict) and isinstance(
                        metrics.get("wall_seconds"), dict
                    ):
                        wall_med = metrics["wall_seconds"].get("median")
                    elif isinstance(row.get("wall_seconds"), dict):
                        wall_med = row["wall_seconds"].get("median")
                    elif "median_wall_seconds" in row:
                        wall_med = row["median_wall_seconds"]
                    extracted_rows.append(
                        {
                            "variant_id": v_id,
                            "median_wall_seconds": to_float(wall_med),
                        }
                    )

                extracted_comps: list[dict[str, Any]] = []
                for comp in doc.get("comparisons") or []:
                    if not isinstance(comp, dict):
                        continue
                    v_target = comp.get("variant")
                    ratio_med = None
                    ratio = comp.get("ratio")
                    if isinstance(ratio, dict):
                        ratio_med = ratio.get("median")
                    elif isinstance(ratio, (int, float, str)):
                        ratio_med = ratio
                    diff_ci95 = comp.get("difference_ci95")
                    extracted_comps.append(
                        {
                            "variant": v_target,
                            "ratio_median": to_float(ratio_med),
                            "difference_ci95": diff_ci95,
                        }
                    )

                comparison_reports.append(
                    {
                        "path": rel_path,
                        "case_id": doc.get("case_id"),
                        "rows": extracted_rows,
                        "comparisons": extracted_comps,
                    }
                )

        elif kind == "hardware-profile":
            if isinstance(doc, dict):
                hardware_profiles.append(
                    {
                        "path": rel_path,
                        "profile_id": doc.get("profile_id"),
                        "configuration_fingerprint": doc.get(
                            "configuration_fingerprint"
                        ),
                    }
                )

        elif kind == "runtime-calibration":
            if isinstance(doc, dict):
                by_suite_medians: dict[str, Optional[float]] = {}
                summary = doc.get("summary")
                if isinstance(summary, dict) and isinstance(
                    summary.get("by_suite"), dict
                ):
                    for suite, sinfo in sorted(summary["by_suite"].items()):
                        if isinstance(sinfo, dict) and "median_seconds" in sinfo:
                            by_suite_medians[suite] = to_float(sinfo["median_seconds"])
                        elif isinstance(sinfo, (int, float, str)):
                            by_suite_medians[suite] = to_float(sinfo)

                runtime_calibrations.append(
                    {
                        "path": rel_path,
                        "status": doc.get("status"),
                        "by_suite_medians": by_suite_medians,
                        "statistical": (doc.get("quality") or {}).get("statistical"),
                        "envelope_decisions": (doc.get("quality") or {}).get("envelope_decisions") or {},
                    }
                )

    # Group benchmark runs by condition class:
    # (case.mode, case.storage, case.backend, case.ranks, case.nodes or 1, evidence_dir)
    grouped_benchmarks: dict[tuple[Any, ...], list[dict[str, Any]]] = {}
    for file_record, doc in benchmark_runs:
        rel_path = file_record["path"]
        case = doc.get("case") or {}
        mode = case.get("mode")
        storage = case.get("storage")
        backend = case.get("backend")
        ranks = to_int(case.get("ranks"))
        nodes = to_int(case.get("nodes")) or 1

        parts = PurePosixPath(rel_path).parts
        evidence_dir = "/".join(parts[:2]) if len(parts) >= 2 else parts[0]
        group_key = (mode, storage, backend, ranks, nodes, evidence_dir)
        grouped_benchmarks.setdefault(group_key, []).append(doc)

    benchmark_groups: list[dict[str, Any]] = []
    for group_key in sorted(grouped_benchmarks.keys(), key=lambda k: tuple(str(x) for x in k)):
        mode, storage, backend, ranks, nodes, evidence_dir = group_key
        runs = grouped_benchmarks[group_key]

        run_count = len(runs)
        completed_count = sum(1 for r in runs if r.get("status") == "completed")
        validation_passed_count = sum(
            1
            for r in runs
            if (
                isinstance(r.get("validation"), dict)
                and r["validation"].get("passed") is True
            )
            or r.get("validation") is True
        )

        per_run_medians: list[float] = []
        trial_spreads: list[float] = []
        for r in runs:
            summary_tot = r.get("summary", {}).get("total", {})
            med_sec = to_float(summary_tot.get("median_seconds"))
            if med_sec is not None:
                per_run_medians.append(med_sec)
                lo, hi = to_float(summary_tot.get("min_seconds")), to_float(summary_tot.get("max_seconds"))
                if lo is not None and hi is not None and med_sec > 0:
                    # Trial-to-trial spread within one workload: the measurement noise of the condition.
                    trial_spreads.append((hi - lo) / med_sec)

        per_run_medians.sort()
        n = len(per_run_medians)
        if n > 0:
            median_val = calculate_median(per_run_medians)
            p95_val = percentile_p95(per_run_medians)
            worst_val = per_run_medians[-1]
            min_val = per_run_medians[0]
            max_val = per_run_medians[-1]
            variation_val = ((max_val - min_val) / median_val) if median_val else 0.0
        else:
            median_val = None
            p95_val = None
            worst_val = None
            variation_val = None

        benchmark_groups.append(
            {
                "condition": {
                    "mode": mode,
                    "storage": storage,
                    "backend": backend,
                    "ranks": ranks,
                    "nodes": nodes,
                    "evidence_dir": evidence_dir,
                },
                "run_count": run_count,
                "completed_count": completed_count,
                "validation_passed_count": validation_passed_count,
                "median_seconds": median_val,
                "p95_seconds": p95_val,
                "worst_seconds": worst_val,
                "variation": variation_val,
                "trial_variation_median": calculate_median(sorted(trial_spreads)) if trial_spreads else None,
            }
        )

    # Sort other summaries deterministically by path
    time_models.sort(key=lambda x: str(x.get("path", "")))
    eta_accuracies.sort(key=lambda x: str(x.get("path", "")))
    comparison_reports.sort(key=lambda x: str(x.get("path", "")))
    hardware_profiles.sort(key=lambda x: str(x.get("path", "")))
    runtime_calibrations.sort(key=lambda x: str(x.get("path", "")))

    return {
        "benchmark-run": benchmark_groups,
        "time-model": time_models,
        "runtime-eta-accuracy": eta_accuracies,
        "comparison-report": comparison_reports,
        "hardware-profile": hardware_profiles,
        "runtime-calibration": runtime_calibrations,
    }


def build_evidence_manifest(
    root: Path, max_files: int = MAX_FILES
) -> dict[str, Any]:
    """Scan root directory, compute file digests, and build manifest structure."""
    file_records, unparsed, classified_entries = scan_and_collect_evidence(
        root=root, max_files=max_files
    )
    bundle_sha256 = calculate_bundle_sha256(file_records)
    git_rev, git_dirty = get_git_info(root)
    summaries = build_summaries(classified_entries)

    manifest: dict[str, Any] = {
        "schema_version": 1,
        "kind": "p4-001-evidence-manifest",
        "git_revision": git_rev,
        "git_dirty": git_dirty,
        "generated_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "files": file_records,
        "unparsed": unparsed,
        "summaries": summaries,
        "bundle_sha256": bundle_sha256,
    }
    return manifest


def generate_markdown(manifest: dict[str, Any]) -> str:
    """Generate Markdown comparison and evidence report."""
    bundle_sha256 = manifest.get("bundle_sha256", "n/a")
    git_rev = manifest.get("git_revision") or "n/a"
    git_dirty = manifest.get("git_dirty", False)
    dirty_str = "dirty" if git_dirty else "clean"
    generated_at = manifest.get("generated_at", "")
    summaries = manifest.get("summaries", {})

    lines: list[str] = [
        "# P4-001 Reproducible Evidence Bundle Report",
        "",
        f"- **Bundle SHA256**: `{bundle_sha256}`",
        f"- **Git Revision**: `{git_rev}` ({dirty_str})",
        f"- **Generated At**: `{generated_at}`",
        f"- **Total Files**: {len(manifest.get('files', []))}",
        f"- **Unparsed JSON**: {len(manifest.get('unparsed', []))}",
        "",
        "---",
        "",
        "## 1. Benchmark Execution Groups (ベンチマーク実行グループ)",
        "",
    ]

    bench_groups = summaries.get("benchmark-run", [])
    if bench_groups:
        lines.append("Variation: (max−min)/median of per-run medians across the group's workloads (workload size spread, not noise). "
                     "Trial variation: median over runs of each run's own (max−min)/median trial spread (measurement noise).")
        lines.append("")
        lines.extend(
            [
                "| Evidence Dir | Mode | Storage | Backend | Ranks | Nodes | Runs | Completed | Validated | Median (s) | P95 (s) | Worst (s) | Variation | Trial variation |",
                "|---|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
            ]
        )
        for g in bench_groups:
            c = g.get("condition", {})
            med = g.get("median_seconds")
            p95 = g.get("p95_seconds")
            worst = g.get("worst_seconds")
            var = g.get("variation")

            med_s = f"{med:.6f}" if med is not None else "n/a"
            p95_s = f"{p95:.6f}" if p95 is not None else "n/a"
            worst_s = f"{worst:.6f}" if worst is not None else "n/a"
            var_s = f"{var:.4f}" if var is not None else "n/a"
            tv = g.get("trial_variation_median")
            tv_s = f"{tv:.4f}" if tv is not None else "n/a"

            lines.append(
                f"| `{c.get('evidence_dir')}` | {c.get('mode')} | {c.get('storage')} | {c.get('backend')} | {c.get('ranks')} | {c.get('nodes')} | {g.get('run_count')} | {g.get('completed_count')} | {g.get('validation_passed_count')} | {med_s} | {p95_s} | {worst_s} | {var_s} | {tv_s} |"
            )
    else:
        lines.append("*(No benchmark runs)*")

    lines.extend(
        [
            "",
            "## 2. Runtime ETA Accuracy (ETA 予測精度)",
            "",
        ]
    )
    eta_list = summaries.get("runtime-eta-accuracy", [])
    if eta_list:
        lines.extend(
            [
                "| Path | Checks | Predicted | Median Abs Rel Err | P95 Abs Rel Err | Worst Abs Rel Err | Interval Coverage |",
                "|---|---:|---:|---:|---:|---:|---:|",
            ]
        )
        for eta in eta_list:
            m_err = eta.get("median_abs_relative_error")
            p95_err = eta.get("p95_abs_relative_error")
            w_err = eta.get("worst_abs_relative_error")
            cov = eta.get("interval_coverage")

            m_s = f"{m_err:.4%}" if m_err is not None else "n/a"
            p_s = f"{p95_err:.4%}" if p95_err is not None else "n/a"
            w_s = f"{w_err:.4%}" if w_err is not None else "n/a"
            cov_s = f"{cov:.2f}" if cov is not None else "n/a"

            lines.append(
                f"| `{eta.get('path')}` | {eta.get('check_count')} | {eta.get('predicted_count')} | {m_s} | {p_s} | {w_s} | {cov_s} |"
            )
    else:
        lines.append("*(No ETA accuracy reports)*")

    lines.extend(
        [
            "",
            "## 3. Time Models (タイムモデル)",
            "",
        ]
    )
    models = summaries.get("time-model", [])
    if models:
        lines.extend(
            [
                "| Path | Model ID | Backend | Storage | Mode | Ranks | Calib Runs | Excluded Unstable | Median Sync Latency (s) |",
                "|---|---|---|---|---|---:|---:|---:|---:|",
            ]
        )
        for tm in models:
            c = tm.get("condition") or {}
            sync_lat = tm.get("sync_latency") or {}
            med_sync = (
                sync_lat.get("median_seconds")
                if isinstance(sync_lat, dict)
                else None
            )
            med_sync_s = f"{to_float(med_sync):.6e}" if med_sync is not None else "n/a"

            lines.append(
                f"| `{tm.get('path')}` | `{str(tm.get('model_id'))[:12]}...` | {c.get('backend')} | {c.get('storage')} | {c.get('mode')} | {c.get('ranks')} | {tm.get('calibration_run_ids_count')} | {tm.get('excluded_unstable_trial_count')} | {med_sync_s} |"
            )
    else:
        lines.append("*(No time models)*")

    lines.extend(
        [
            "",
            "## 4. Comparison Reports & Monitor Overhead (比較レポート / モニターオーバーヘッド)",
            "",
        ]
    )
    comp_reports = summaries.get("comparison-report", [])
    if comp_reports:
        lines.extend(
            [
                "| Path | Case ID | Variant Rows (Variant: Median Wall s) | Comparisons (Variant: Ratio Median, Diff CI95) |",
                "|---|---|---|---|",
            ]
        )
        for cr in comp_reports:
            row_items = []
            for r in cr.get("rows", []):
                v_id = r.get("variant_id")
                w_med = r.get("median_wall_seconds")
                w_str = f"{w_med:.4f}s" if w_med is not None else "n/a"
                row_items.append(f"{v_id}: {w_str}")
            row_cell = "<br>".join(row_items) if row_items else "n/a"

            comp_items = []
            for cp in cr.get("comparisons", []):
                v_name = cp.get("variant")
                r_med = cp.get("ratio_median")
                r_str = f"{r_med:.4f}" if r_med is not None else "n/a"
                ci = cp.get("difference_ci95")
                ci_str = (
                    f"[{ci[0]:.4f}, {ci[1]:.4f}]"
                    if isinstance(ci, list) and len(ci) == 2
                    else str(ci)
                )
                comp_items.append(f"{v_name}: ratio {r_str}, CI95 {ci_str}")
            comp_cell = "<br>".join(comp_items) if comp_items else "n/a"

            lines.append(
                f"| `{cr.get('path')}` | `{cr.get('case_id')}` | {row_cell} | {comp_cell} |"
            )
    else:
        lines.append("*(No comparison reports)*")

    lines.extend(
        [
            "",
            "## 5. Hardware Profiles (ハードウェアプロファイル)",
            "",
        ]
    )
    profiles = summaries.get("hardware-profile", [])
    if profiles:
        lines.extend(
            [
                "| Path | Profile ID | Configuration Fingerprint |",
                "|---|---|---|",
            ]
        )
        for p in profiles:
            p_id = str(p.get("profile_id"))
            p_fp = str(p.get("configuration_fingerprint"))
            lines.append(
                f"| `{p.get('path')}` | `{p_id}` | `{p_fp}` |"
            )
    else:
        lines.append("*(No hardware profiles)*")

    lines.extend(
        [
            "",
            "## 6. Runtime Calibrations (ランタイムキャリブレーション)",
            "",
        ]
    )
    calibs = summaries.get("runtime-calibration", [])
    if calibs:
        lines.extend(
            [
                "| Path | Status | Suite Medians (s) | Quality | Envelope decisions |",
                "|---|---|---|---|---|",
            ]
        )
        for cal in calibs:
            medians = cal.get("by_suite_medians", {})
            m_items = [
                f"{k}: {v:.4f}s" if v is not None else f"{k}: n/a"
                for k, v in medians.items()
            ]
            m_cell = ", ".join(m_items) if m_items else "n/a"
            lines.append(
                f"| `{cal.get('path')}` | {cal.get('status')} | {m_cell} | {cal.get('statistical') or 'n/a'} | "
                + (", ".join(f"{k}: {v}" for k, v in sorted(cal.get("envelope_decisions", {}).items())) or "n/a") + " |"
            )
    else:
        lines.append("*(No runtime calibrations)*")

    lines.append("")
    return "\n".join(lines)


def main(argv: Optional[list[str]] = None) -> None:
    """CLI entrypoint."""
    parser = argparse.ArgumentParser(
        description="P4-001 reproducible evidence-bundle tool"
    )
    parser.add_argument(
        "--root",
        default="docs/internal",
        help="Root directory containing scanned evidence subdirectories",
    )
    parser.add_argument(
        "--output",
        required=True,
        help="Target manifest JSON file path",
    )
    parser.add_argument(
        "--markdown",
        required=True,
        help="Target markdown report file path",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing output files if present",
    )

    args = parser.parse_args(argv)

    root_path = Path(args.root).resolve()
    output_path = Path(args.output).resolve()
    markdown_path = Path(args.markdown).resolve()

    if not args.overwrite:
        if output_path.exists():
            sys.stderr.write(
                f"Error: Output file already exists and --overwrite not specified: {output_path}\n"
            )
            sys.exit(1)
        if markdown_path.exists():
            sys.stderr.write(
                f"Error: Markdown file already exists and --overwrite not specified: {markdown_path}\n"
            )
            sys.exit(1)

    manifest = build_evidence_manifest(root_path, max_files=MAX_FILES)
    markdown_content = generate_markdown(manifest)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2, sort_keys=True)
        f.write("\n")

    with open(markdown_path, "w", encoding="utf-8") as f:
        f.write(markdown_content)

    print(
        f"Manifest written to {output_path} (bundle sha256: {manifest['bundle_sha256']})"
    )
    print(f"Markdown report written to {markdown_path}")


if __name__ == "__main__":
    main()
