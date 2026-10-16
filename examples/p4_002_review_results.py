#!/usr/bin/env python3
"""Audit P4-002 held-out and monitoring-overhead JSON/JSONL artifacts.

This report preserves unanswered/error rows and never treats a partial overhead
campaign as a performance conclusion. Extraction time is reported as a
diagnostic subset of capture wall time; the monitor subtotal uses capture wall
plus ETA evaluation only.
"""
from __future__ import annotations

import argparse
import json
import math
from pathlib import Path
import statistics
import sys


def _number(value):
    return type(value) in (int, float) and math.isfinite(value)


def _percentile(values, p):
    if not values:
        return None
    ordered = sorted(values)
    position = (len(ordered) - 1) * p / 100
    lo, hi = math.floor(position), math.ceil(position)
    if lo == hi:
        return float(ordered[lo])
    return float(ordered[lo] * (hi - position) + ordered[hi] * (position - lo))


def _distribution(values):
    return {
        "count": len(values),
        "median": statistics.median(values) if values else None,
        "p95": _percentile(values, 95),
        "worst": max(values) if values else None,
    }


def _read_json(path):
    return json.loads(Path(path).read_text(encoding="utf-8"))


def _read_jsonl(path):
    rows, errors = [], []
    for line_number, line in enumerate(Path(path).read_text(encoding="utf-8").splitlines(), 1):
        if not line.strip():
            continue
        try:
            rows.append(json.loads(line))
        except json.JSONDecodeError as exc:
            errors.append({"line": line_number, "error": str(exc), "raw": line})
    return rows, errors


def summarize_heldout(document):
    rows = document.get("rows")
    if not isinstance(rows, list):
        raise ValueError("held-out input must contain a rows array")

    def summarize(subset):
        successful = [row for row in subset if row.get("status") == "ok"]
        errors = [row for row in subset if row.get("status") != "ok"]
        missing = [row for row in successful if not _number(row.get("eta"))]
        answered = [row for row in successful if _number(row.get("eta")) and _number(row.get("actual"))]
        abs_seconds = [abs(row["eta"] - row["actual"]) for row in answered]
        abs_relative = [abs(row["relative_error"]) for row in answered
                        if _number(row.get("relative_error"))]
        interval_rows = [row for row in answered if _number(row.get("lower")) and
                         _number(row.get("upper")) and row["lower"] <= row["upper"]]
        covered = sum(row["lower"] <= row["actual"] <= row["upper"] for row in interval_rows)
        reasons = {}
        for row in missing:
            for reason in row.get("unknown_reasons") or ():
                reasons[reason] = reasons.get(reason, 0) + 1
        return {
            "n_checks": len(subset),
            "n_errors": len(errors),
            "n_unanswered": len(missing),
            "n_answered": len(answered),
            "answered_fraction_of_all_checks": len(answered) / len(subset) if subset else None,
            "absolute_seconds_error": _distribution(abs_seconds),
            "absolute_relative_error": _distribution(abs_relative),
            "interval": {"n_with_interval": len(interval_rows), "n_covered": covered,
                         "coverage": covered / len(interval_rows) if interval_rows else None},
            "unknown_reason_tally": reasons,
            "error_rows": [{"workload": row.get("workload"), "policy": row.get("policy"),
                            "store": row.get("store"), "frac": row.get("frac"),
                            "error": row.get("error")} for row in errors],
            "unanswered_rows": [{"workload": row.get("workload"), "policy": row.get("policy"),
                                 "store": row.get("store"), "frac": row.get("frac"),
                                 "unknown_reasons": row.get("unknown_reasons") or (),
                                 "class_diag": row.get("class_diag")} for row in missing],
        }

    by_workload = {}
    for name in sorted({row.get("workload", "unknown") for row in rows}):
        by_workload[name] = summarize([row for row in rows if row.get("workload", "unknown") == name])
    return {"overall": summarize(rows), "by_workload": by_workload}


def _monitor_cost(row):
    metrics = row.get("eta_on")
    if not isinstance(metrics, dict):
        return None
    capture = metrics.get("capture_wall_seconds")
    evaluation = metrics.get("eta_evaluation_seconds")
    if not (_number(capture) and _number(evaluation)) or capture < 0 or evaluation < 0:
        return None
    return capture + evaluation


def _backend_identities(rows):
    cached_values = set()
    actual_values = set()
    empty = 0
    actual_observations = 0
    incomplete_actual_observations = 0
    read_bytes = write_bytes = read_events = write_events = outstanding = 0
    windows = set()
    for row in rows:
        for side in ("eta_on", "eta_off"):
            metrics = row.get(side)
            if not isinstance(metrics, dict):
                continue
            per_rank = metrics.get("backend_resolved_by_rank")
            if not isinstance(per_rank, list):
                continue
            if not any(per_rank):
                empty += 1
            for identities in per_rank:
                if isinstance(identities, list):
                    cached_values.update(value for value in identities if isinstance(value, str))
            actual_by_rank = metrics.get("storage_io_by_rank")
            if not isinstance(actual_by_rank, list):
                continue
            actual_observations += 1
            for evidence in actual_by_rank:
                if not isinstance(evidence, dict):
                    incomplete_actual_observations += 1
                    continue
                identities = evidence.get("backend_identities", [])
                if isinstance(identities, list):
                    actual_values.update(value for value in identities if isinstance(value, str))
                read_bytes += evidence.get("read_bytes", 0) if _number(evidence.get("read_bytes")) else 0
                write_bytes += evidence.get("write_bytes", 0) if _number(evidence.get("write_bytes")) else 0
                read_events += evidence.get("read_events", 0) if _number(evidence.get("read_events")) else 0
                write_events += evidence.get("write_events", 0) if _number(evidence.get("write_events")) else 0
                outstanding += evidence.get("outstanding_requests_at_observation", 0) if _number(
                    evidence.get("outstanding_requests_at_observation")) else 0
                if evidence.get("backend_observation_complete") is not True:
                    incomplete_actual_observations += 1
                if isinstance(evidence.get("observation_window"), str):
                    windows.add(evidence["observation_window"])
    return {
        "observed_identities": sorted(cached_values),
        "cached_handle_scope": "currently cached FileBlockStore handles only; may be empty for AsyncStoreIO",
        "empty_backend_observation_count": empty,
        "empty_means_backend_unverified": True,
        "actual_completed_storage_identities_from_trace": sorted(actual_values),
        "actual_storage_observation_records": actual_observations,
        "incomplete_actual_storage_observation_count": incomplete_actual_observations,
        "completed_storage_read_bytes": read_bytes,
        "completed_storage_write_bytes": write_bytes,
        "completed_storage_read_events": read_events,
        "completed_storage_write_events": write_events,
        "outstanding_io_requests_at_observation_sum": outstanding,
        "actual_storage_observation_windows": sorted(windows),
        "actual_identity_claim_scope": "completed storage trace events only; empty remains unverified",
    }


def _safe_workload_summary(name, rows, *, expected_pairs, reported=None, final_document=False):
    measured = [row for row in rows if row.get("warmup") is False]
    successful = [row for row in measured if row.get("status") == "ok"]
    failed = [row for row in measured if row.get("status") != "ok"]
    costs = [(row, _monitor_cost(row)) for row in successful]
    missing_cost = [row for row, cost in costs if cost is None]
    valid_costs = [cost for _, cost in costs if cost is not None]
    summary = (reported or {}).get("summary") or {}
    diff_ci = summary.get("difference_ci95") or {}
    ratio_ci = summary.get("ratio_ci95") or {}
    complete_pairs = (len(measured) == expected_pairs and len(successful) == expected_pairs and not failed)
    ci_complete = (diff_ci.get("status") == "computed" and ratio_ci.get("status") == "computed" and
                   _number(diff_ci.get("lower")) and _number(diff_ci.get("upper")) and
                   _number(ratio_ci.get("lower")) and _number(ratio_ci.get("upper")))
    can_report_conclusion = final_document and complete_pairs and ci_complete and summary.get("status") == "complete"
    return {
        "workload": name,
        "expected_measured_pairs": expected_pairs,
        "observed_measured_pairs": len(measured),
        "successful_measured_pairs": len(successful),
        "failed_or_error_pairs": [{"pair_index": row.get("pair_index"), "errors": row.get("errors"),
                                    "status": row.get("status")} for row in failed],
        "missing_monitor_cost_pairs": [row.get("pair_index") for row in missing_cost],
        "monitor_cost_eta_on_seconds_capture_plus_eval": _distribution(valid_costs),
        "monitor_cost_sum_seconds_capture_plus_eval": sum(valid_costs) if valid_costs else None,
        "reported_extraction_seconds_is_nested_in_capture_wall": True,
        "backend_evidence": _backend_identities(measured),
        "campaign_status": "complete" if can_report_conclusion else "incomplete-or-summary-unavailable",
        "reported_interpretation": summary.get("interpretation"),
        "safe_interpretation": summary.get("interpretation") if can_report_conclusion else "inconclusive",
        "reported_difference_ci95": diff_ci or None,
        "reported_ratio_ci95": ratio_ci or None,
    }


def summarize_overhead(path, *, expected_pairs):
    source = Path(path)
    if source.suffix.lower() == ".jsonl":
        rows, parse_errors = _read_jsonl(source)
        grouped = {}
        for row in rows:
            grouped.setdefault(row.get("workload", "unknown"), []).append(row)
        return {
            "source": str(source),
            "input_kind": "progress-jsonl",
            "parse_errors": parse_errors,
            "by_workload": {name: _safe_workload_summary(name, subset, expected_pairs=expected_pairs)
                            for name, subset in sorted(grouped.items())},
        }

    document = _read_json(source)
    results = document.get("results")
    if not isinstance(results, dict):
        raise ValueError("overhead JSON must contain a results mapping")
    expected = document.get("measured_pairs_per_workload", expected_pairs)
    by_workload = {}
    for name, result in sorted(results.items()):
        pairs = result.get("measured_pairs") or []
        by_workload[name] = _safe_workload_summary(name, pairs, expected_pairs=expected,
                                                   reported=result, final_document=True)
    return {"source": str(source), "input_kind": "final-json",
            "document_status": document.get("status"), "by_workload": by_workload}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--heldout", required=True, help="p4_002_observed_eta result JSON")
    parser.add_argument("--overhead", help="overhead final JSON or progress JSONL")
    parser.add_argument("--expected-pairs", type=int, default=7,
                        help="required measured pairs per workload; warmups are excluded")
    parser.add_argument("--output", help="write report JSON here (default: stdout)")
    args = parser.parse_args(argv)
    if args.expected_pairs < 1:
        parser.error("expected-pairs must be positive")
    report = {"schema_version": 1, "kind": "p4-002-results-review",
              "heldout": summarize_heldout(_read_json(args.heldout))}
    if args.overhead:
        report["overhead"] = summarize_overhead(args.overhead, expected_pairs=args.expected_pairs)
    encoded = json.dumps(report, indent=2, sort_keys=True, allow_nan=False) + "\n"
    if args.output:
        Path(args.output).write_text(encoded, encoding="utf-8")
    else:
        sys.stdout.write(encoded)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
