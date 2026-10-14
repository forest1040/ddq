"""Comparison evaluation reports comparing ExecutionPlan and BenchmarkRun."""
from __future__ import annotations

import hashlib
import json
import math
import os
from pathlib import Path
import statistics
from typing import Any

from .journal import content_hash
from .profile_document import (
    _pairs_no_duplicates,
    _parse_float,
    _reject_constant,
    wire_decode,
    wire_encode,
    write_document,
)

SCHEMA_VERSION = 1
DOCUMENT_KIND = "comparison-report"
CAMPAIGN_KIND = "comparison-campaign"
MODEL_NAME = "static-vs-observed-v1"
MAX_CASES = 256
MAX_FILE_BYTES = 4 * 1024 * 1024
DEFAULT_SMALL_TIME_THRESHOLD_SECONDS = 0.05

NOTES = (
    "relative error uses the median of measured trials; sample extrema are not confidence intervals",
    "held-out cases exclude workloads used to fit the time model",
    "phase times are serial wall components on one main thread; no overlap is assumed",
    "the 30% median target is an evaluation goal, not a guarantee",
)


def _p95(vals: list[float]) -> float | None:
    if not vals: return None
    index = (len(vals) - 1) * 0.95
    low, high = math.floor(index), math.ceil(index)
    return vals[low] + (vals[high] - vals[low]) * (index - low)


def _winner(ratio: float | None) -> str | None:
    if ratio is None: return None
    if abs(ratio - 1.0) < 0.02: return "tie"
    return "variant" if ratio < 1.0 else "baseline"


def _read_json_and_sha(path: str | os.PathLike[str], max_bytes: int = MAX_FILE_BYTES) -> tuple[dict[str, Any], str]:
    source = Path(path)
    if source.stat().st_size > max_bytes:
        raise ValueError(f"document exceeds byte limit: {path}")
    with source.open("rb") as stream:
        raw = stream.read(max_bytes + 1)
    if len(raw) > max_bytes:
        raise ValueError(f"document exceeds byte limit: {path}")
    try:
        value = json.loads(raw.decode("utf-8"), object_pairs_hook=_pairs_no_duplicates,
                           parse_constant=_reject_constant, parse_float=_parse_float)
    except RecursionError as exc:
        raise ValueError("document nesting exceeds parser limit") from exc
    if not isinstance(value, dict):
        raise ValueError("document root must be an object")
    return wire_decode(value), hashlib.sha256(raw).hexdigest()


def _wall_range(run_doc: dict[str, Any]) -> tuple[float, float] | None:
    totals = []
    for t in run_doc.get("trials") or []:
        if isinstance(t, dict):
            ws = t.get("wall_seconds")
            val = ws.get("total") if isinstance(ws, dict) else None
            if isinstance(val, (int, float)) and not isinstance(val, bool) and math.isfinite(val):
                totals.append(float(val))
    if totals: return min(totals), max(totals)
    tot = (run_doc.get("summary") or {}).get("total") or {}
    mn, mx = tot.get("min_seconds"), tot.get("max_seconds")
    if (isinstance(mn, (int, float)) and not isinstance(mn, bool) and math.isfinite(mn) and
            isinstance(mx, (int, float)) and not isinstance(mx, bool) and math.isfinite(mx)):
        return float(mn), float(mx)
    return None


def _load_campaign(path: str | os.PathLike[str]) -> tuple[dict[str, Any], str]:
    campaign_path = Path(path).resolve()
    raw, sha = _read_json_and_sha(campaign_path)
    if raw.get("kind") != CAMPAIGN_KIND: raise ValueError(f"unsupported campaign kind: {raw.get('kind')}")
    schema_ver = raw.get("schema_version")
    if isinstance(schema_ver, bool) or not isinstance(schema_ver, int) or schema_ver != SCHEMA_VERSION:
        raise ValueError(f"unsupported schema_version: {schema_ver}")
    allowed = {"schema_version", "kind", "cases", "calibration_workload_ids",
               "baseline_variant", "small_time_threshold_seconds"}
    unknown = set(raw.keys()) - allowed
    if unknown: raise ValueError(f"unknown campaign fields: {', '.join(sorted(unknown))}")
    cases = raw.get("cases")
    if not isinstance(cases, list): raise ValueError("campaign cases must be a list")
    if len(cases) > MAX_CASES: raise ValueError(f"campaign exceeds {MAX_CASES} cases: {len(cases)}")
    seen_ids = set()
    case_keys = {"case_id", "plan", "run", "variant", "group"}
    resolved = []
    for idx, c in enumerate(cases):
        if not isinstance(c, dict) or set(c.keys()) != case_keys:
            raise ValueError(f"case {idx} has missing or unknown fields")
        for key in case_keys:
            if not isinstance(c[key], str) or not c[key]:
                raise ValueError(f"case {idx} field {key} must be a non-empty string")
        if c["case_id"] in seen_ids: raise ValueError(f"duplicate case_id: {c['case_id']}")
        seen_ids.add(c["case_id"])
        resolved.append({**c, "plan": str((campaign_path.parent / c["plan"]).resolve()),
                              "run": str((campaign_path.parent / c["run"]).resolve())})
    cal_ids = raw.get("calibration_workload_ids", [])
    if not isinstance(cal_ids, list) or any(not isinstance(w, str) for w in cal_ids):
        raise ValueError("calibration_workload_ids must be a list of strings")
    base_var = raw.get("baseline_variant")
    if base_var is not None and not isinstance(base_var, str):
        raise ValueError("baseline_variant must be a string or None")
    threshold = raw.get("small_time_threshold_seconds", DEFAULT_SMALL_TIME_THRESHOLD_SECONDS)
    if (isinstance(threshold, bool) or not isinstance(threshold, (int, float)) or
            not math.isfinite(threshold) or threshold <= 0):
        raise ValueError("small_time_threshold_seconds must be a positive finite number")
    return {"schema_version": SCHEMA_VERSION, "kind": CAMPAIGN_KIND, "cases": resolved,
            "calibration_workload_ids": list(cal_ids), "baseline_variant": base_var,
            "small_time_threshold_seconds": float(threshold)}, sha


def load_campaign(path: str | os.PathLike[str]) -> dict[str, Any]:
    """Validate campaign document and return it with resolved absolute paths."""
    return _load_campaign(path)[0]


def _calc_stats(cases: list[dict[str, Any]]) -> dict[str, Any]:
    if not cases:
        return {"case_count": 0, "median_abs_relative_error": None, "p95_abs_relative_error": None,
                "worst_abs_relative_error": None, "worst_case_id": None, "interval_coverage": None,
                "interval_case_count": 0}
    rel = [c for c in cases if c["relative_error"] is not None]
    vals = sorted(abs(c["relative_error"]) for c in rel) if rel else []
    worst = max(rel, key=lambda c: abs(c["relative_error"])) if rel else None
    ints = [c for c in cases if c["within_interval"] is not None]
    return {
        "case_count": len(cases),
        "median_abs_relative_error": statistics.median(vals) if vals else None,
        "p95_abs_relative_error": _p95(vals),
        "worst_abs_relative_error": abs(worst["relative_error"]) if worst else None,
        "worst_case_id": worst["case_id"] if worst else None,
        "interval_coverage": (sum(1 for c in ints if c["within_interval"] is True) / len(ints)) if ints else None,
        "interval_case_count": len(ints),
    }


def _calc_abs_stats(cases: list[dict[str, Any]]) -> dict[str, Any]:
    if not cases:
        return {"case_count": 0, "median_abs_error_seconds": None, "worst_abs_error_seconds": None, "worst_case_id": None}
    errs = [c for c in cases if c["absolute_error_seconds"] is not None]
    vals = sorted(abs(c["absolute_error_seconds"]) for c in errs) if errs else []
    worst = max(errs, key=lambda c: abs(c["absolute_error_seconds"])) if errs else None
    return {
        "case_count": len(cases),
        "median_abs_error_seconds": statistics.median(vals) if vals else None,
        "worst_abs_error_seconds": abs(worst["absolute_error_seconds"]) if worst else None,
        "worst_case_id": worst["case_id"] if worst else None,
    }


def build_report(campaign_path: str | os.PathLike[str]) -> dict[str, Any]:
    """Read campaign, validate plan and run documents, and produce wire-encoded comparison report."""
    campaign, campaign_sha256 = _load_campaign(campaign_path)
    file_cache: dict[str, tuple[dict[str, Any], str]] = {}

    def _read_doc(p_str: str) -> tuple[dict[str, Any], str]:
        if p_str not in file_cache:
            file_cache[p_str] = _read_json_and_sha(p_str)
        return file_cache[p_str]

    report_cases: list[dict[str, Any]] = []
    participating_ab_cases: list[dict[str, Any]] = []
    case_wall_ranges: dict[str, tuple[float, float] | None] = {}

    for c in campaign["cases"]:
        plan_doc, plan_sha256 = _read_doc(c["plan"])
        run_doc, run_sha256 = _read_doc(c["run"])

        if not isinstance(plan_doc, dict) or plan_doc.get("kind") != "execution-plan":
            raise ValueError(f"plan document {c['plan']} kind must be 'execution-plan'")
        if (not isinstance(run_doc, dict) or run_doc.get("kind") != "benchmark-run" or
                run_doc.get("run_type") != "workload-execution"):
            raise ValueError(f"run document {c['run']} kind must be 'benchmark-run' and run_type 'workload-execution'")

        run_completed = run_doc.get("status") == "completed"
        validation_passed = (run_doc.get("validation") or {}).get("passed") is True

        reason = None
        if not run_completed: reason = "RUN_NOT_COMPLETED"
        elif not validation_passed: reason = "RUN_VALIDATION_FAILED"
        elif run_doc.get("workload_id") != plan_doc.get("workload_id"): reason = "WORKLOAD_MISMATCH"
        elif run_doc.get("plan_id") is not None and run_doc.get("plan_id") != plan_doc.get("plan_id"): reason = "PLAN_MISMATCH"
        elif run_doc.get("configuration_fingerprint") != plan_doc.get("configuration_fingerprint"): reason = "CONFIGURATION_MISMATCH"
        elif (plan_doc.get("time") or {}).get("expected_seconds") is None: reason = "NO_TIME_PREDICTION"

        case_status = "compared" if reason is None else "not-compared"
        workload_id = plan_doc.get("workload_id") or run_doc.get("workload_id")
        held_out = bool(workload_id not in campaign["calibration_workload_ids"])

        plan_time = plan_doc.get("time") or {}
        predicted = {
            "lower_seconds": plan_time.get("lower_seconds"),
            "expected_seconds": plan_time.get("expected_seconds"),
            "conditional_upper_seconds": plan_time.get("conditional_upper_seconds"),
            "evidence": plan_time.get("evidence"),
        }

        total_summary = (run_doc.get("summary") or {}).get("total") or {}
        sample_count_val = run_doc.get("sample_count")
        observed = {
            "median_seconds": total_summary.get("median_seconds"),
            "min_seconds": total_summary.get("min_seconds"),
            "max_seconds": total_summary.get("max_seconds"),
            "p95_seconds": total_summary.get("p95_seconds"),
            "sample_count": int(sample_count_val) if sample_count_val is not None else None,
        }

        expected = predicted["expected_seconds"]
        obs_median = observed["median_seconds"]
        lower, upper = predicted["lower_seconds"], predicted["conditional_upper_seconds"]

        if case_status == "compared":
            abs_err = (expected - obs_median) if (expected is not None and obs_median is not None) else None
            rel_err = (abs_err / obs_median) if (abs_err is not None and obs_median and obs_median > 0) else None
            within = bool(lower <= obs_median <= upper) if (lower is not None and upper is not None and obs_median is not None) else None
        else:
            abs_err, rel_err, within = None, None, None

        small_time = bool(obs_median < campaign["small_time_threshold_seconds"]) if obs_median is not None else None

        plan_phases = plan_time.get("phases")
        run_phases = run_doc.get("phases") or ["setup", "execute", "flush", "cleanup"]
        run_summary = run_doc.get("summary") or {}
        phase_medians = {p: (run_summary.get(p) or {}).get("median_seconds") for p in run_phases}
        valid_medians = [m for m in phase_medians.values() if m is not None and m >= 0]
        sum_phase_medians = sum(valid_medians) if valid_medians else None

        case_phases = {}
        for p in run_phases:
            pred_p = (plan_phases.get(p) or {}).get("expected_seconds") if plan_phases else None
            obs_p = phase_medians.get(p)
            p_rel_err = ((pred_p - obs_p) / obs_p) if (case_status == "compared" and pred_p is not None and obs_p and obs_p > 0) else None
            p_share = (obs_p / sum_phase_medians) if (sum_phase_medians and sum_phase_medians > 0 and obs_p is not None) else None
            case_phases[p] = {"predicted_expected_seconds": pred_p, "observed_median_seconds": obs_p,
                              "relative_error": p_rel_err, "observed_share": p_share}

        obs_cands = [(p, m) for p, m in phase_medians.items() if m is not None]
        observed_bottleneck = max(obs_cands, key=lambda item: item[1])[0] if obs_cands else None
        if plan_phases:
            pred_cands = [(p, (plan_phases.get(p) or {}).get("expected_seconds")) for p in plan_phases
                          if (plan_phases.get(p) or {}).get("expected_seconds") is not None]
            predicted_bottleneck = max(pred_cands, key=lambda item: item[1])[0] if pred_cands else None
        else:
            predicted_bottleneck = None

        case_entry = {
            "case_id": c["case_id"], "variant": c["variant"], "group": c["group"],
            "workload_id": workload_id, "plan_id": plan_doc.get("plan_id"), "run_id": run_doc.get("run_id"),
            "plan_sha256": plan_sha256, "run_sha256": run_sha256, "held_out": held_out,
            "status": case_status, "reason": reason, "predicted": predicted, "observed": observed,
            "absolute_error_seconds": abs_err, "relative_error": rel_err, "within_interval": within,
            "small_time": small_time, "phases": case_phases, "observed_bottleneck": observed_bottleneck,
            "predicted_bottleneck": predicted_bottleneck,
        }
        report_cases.append(case_entry)
        case_wall_ranges[c["case_id"]] = _wall_range(run_doc)
        if run_completed and validation_passed: participating_ab_cases.append(case_entry)

    held_out_pool = [c for c in report_cases if c["status"] == "compared" and c["small_time"] is False and c["held_out"]]
    calibration_pool = [c for c in report_cases if c["status"] == "compared" and c["small_time"] is False and not c["held_out"]]
    small_time_pool = [c for c in report_cases if c["status"] == "compared" and c["small_time"] is True]

    held_out_stats = _calc_stats(held_out_pool)
    meets_target = (bool(held_out_stats["median_abs_relative_error"] <= 0.30)
                    if (held_out_stats["case_count"] > 0 and held_out_stats["median_abs_relative_error"] is not None) else None)
    accuracy = {
        "held_out": held_out_stats,
        "calibration": _calc_stats(calibration_pool),
        "small_time": _calc_abs_stats(small_time_pool),
        "target": {"statistic": "held_out median |relative_error|", "threshold": 0.30, "meets_target": meets_target},
    }

    ab_comparisons = []
    baseline_variant = campaign["baseline_variant"]
    if baseline_variant is not None:
        by_group: dict[str, list[dict[str, Any]]] = {}
        for c in participating_ab_cases: by_group.setdefault(c["group"], []).append(c)
        for group, group_cases in by_group.items():
            baseline_candidates = [c for c in group_cases if c["variant"] == baseline_variant]
            if not baseline_candidates: continue
            baseline_case = min(baseline_candidates, key=lambda c: c["case_id"])
            base_med = baseline_case["observed"]["median_seconds"]
            base_exp = baseline_case["predicted"]["expected_seconds"]
            base_range = case_wall_ranges.get(baseline_case["case_id"])

            for c in group_cases:
                if c["variant"] == baseline_variant: continue
                obs_med = c["observed"]["median_seconds"]
                pred_exp = c["predicted"]["expected_seconds"]
                obs_ratio = (obs_med / base_med) if (obs_med is not None and base_med is not None and base_med > 0) else None
                obs_diff = (obs_med - base_med) if (obs_med is not None and base_med is not None) else None
                pred_ratio = (pred_exp / base_exp) if (pred_exp is not None and base_exp is not None and base_exp > 0) else None
                obs_w, pred_w = _winner(obs_ratio), _winner(pred_ratio)
                agrees = bool(obs_w == pred_w) if (obs_w is not None and pred_w is not None) else None
                var_range = case_wall_ranges.get(c["case_id"])
                overlap = bool(max(var_range[0], base_range[0]) <= min(var_range[1], base_range[1])) if (var_range and base_range) else None

                ab_comparisons.append({
                    "group": group, "baseline_case_id": baseline_case["case_id"], "case_id": c["case_id"],
                    "baseline_variant": baseline_variant, "variant": c["variant"],
                    "observed_ratio": obs_ratio, "observed_difference_seconds": obs_diff,
                    "predicted_ratio": pred_ratio, "observed_winner": obs_w, "predicted_winner": pred_w,
                    "winner_agrees": agrees, "trial_ranges_overlap": overlap,
                })

    not_compared = [{"case_id": c["case_id"], "reason": c["reason"]} for c in report_cases if c["status"] == "not-compared"]
    report_dict = {
        "schema_version": SCHEMA_VERSION, "kind": DOCUMENT_KIND, "model": MODEL_NAME,
        "campaign_sha256": campaign_sha256, "cases": report_cases, "accuracy": accuracy,
        "ab_comparisons": ab_comparisons, "not_compared": not_compared, "notes": list(NOTES),
    }
    encoded = wire_encode(report_dict)
    encoded.pop("report_id", None)
    encoded["report_id"] = content_hash(encoded)
    return encoded


def save_report(report: dict[str, Any], path: str | os.PathLike[str], overwrite: bool = False) -> None:
    """Save comparison report to file using write_document."""
    write_document(path, report, overwrite=overwrite)


def _fmt_val(val: Any) -> str: return "n/a" if val is None else str(val)


def _fmt_seconds(val: Any) -> str:
    if val is None: return "n/a"
    try: return f"{float(val):.4g}"
    except (ValueError, TypeError): return "n/a"


def _fmt_pct(val: Any) -> str:
    if val is None: return "n/a"
    try: return f"{float(val) * 100:+.4g}%"
    except (ValueError, TypeError): return "n/a"


def render_markdown(report: dict[str, Any]) -> str:
    """Render comparison report in plain Markdown formatting."""
    lines = [f"# Comparison Report ({report.get('model', 'unknown')})", "",
             f"- **Report ID**: `{_fmt_val(report.get('report_id'))}`",
             f"- **Campaign SHA-256**: `{_fmt_val(report.get('campaign_sha256'))}`", "",
             "## Cases", "",
             "| Case ID | Variant | Held-Out | Predicted Expected [Lower, Upper] | Observed Median | Rel. Error % | Within Interval | Observed Bottleneck |",
             "|---|---|---|---|---|---|---|---|"]
    for c in report.get("cases", []):
        pred, obs = c.get("predicted") or {}, c.get("observed") or {}
        pred_col = f"{_fmt_seconds(pred.get('expected_seconds'))} [{_fmt_seconds(pred.get('lower_seconds'))}, {_fmt_seconds(pred.get('conditional_upper_seconds'))}]"
        lines.append(f"| {_fmt_val(c.get('case_id'))} | {_fmt_val(c.get('variant'))} | {_fmt_val(c.get('held_out'))} | "
                     f"{pred_col} | {_fmt_seconds(obs.get('median_seconds'))} | {_fmt_pct(c.get('relative_error'))} | "
                     f"{_fmt_val(c.get('within_interval'))} | {_fmt_val(c.get('observed_bottleneck'))} |")

    lines += ["", "## Accuracy", "", "### Target Evaluation"]
    tgt = (report.get("accuracy") or {}).get("target") or {}
    lines += [f"- **Statistic**: {_fmt_val(tgt.get('statistic'))}",
              f"- **Threshold**: {_fmt_val(tgt.get('threshold'))}",
              f"- **Meets Target**: {_fmt_val(tgt.get('meets_target'))}", ""]

    for scope in ("held_out", "calibration"):
        st = (report.get("accuracy") or {}).get(scope) or {}
        scope_title = "Held-Out Evaluation" if scope == "held_out" else "Calibration Evaluation"
        lines += [f"### {scope_title}",
                  f"- **Case Count**: {_fmt_val(st.get('case_count'))}",
                  f"- **Median |Relative Error|**: {_fmt_seconds(st.get('median_abs_relative_error'))}",
                  f"- **p95 |Relative Error|**: {_fmt_seconds(st.get('p95_abs_relative_error'))}",
                  f"- **Worst |Relative Error|**: {_fmt_seconds(st.get('worst_abs_relative_error'))}",
                  f"- **Worst Case ID**: {_fmt_val(st.get('worst_case_id'))}",
                  f"- **Interval Coverage**: {_fmt_seconds(st.get('interval_coverage'))}",
                  f"- **Interval Case Count**: {_fmt_val(st.get('interval_case_count'))}", ""]

    st_small = (report.get("accuracy") or {}).get("small_time") or {}
    lines += ["### Small-Time Evaluation",
              f"- **Case Count**: {_fmt_val(st_small.get('case_count'))}",
              f"- **Median |Error| (s)**: {_fmt_seconds(st_small.get('median_abs_error_seconds'))}",
              f"- **Worst |Error| (s)**: {_fmt_seconds(st_small.get('worst_abs_error_seconds'))}",
              f"- **Worst Case ID**: {_fmt_val(st_small.get('worst_case_id'))}", "",
              "## A/B Comparisons", ""]

    ab_list = report.get("ab_comparisons", [])
    if ab_list:
        lines += ["| Group | Baseline Case | Case ID | Baseline Variant | Variant | Obs. Ratio | Obs. Diff (s) | Pred. Ratio | Obs. Winner | Pred. Winner | Winner Agrees | Ranges Overlap |",
                  "|---|---|---|---|---|---|---|---|---|---|---|---|"]
        for ab in ab_list:
            lines.append(f"| {_fmt_val(ab.get('group'))} | {_fmt_val(ab.get('baseline_case_id'))} | {_fmt_val(ab.get('case_id'))} | "
                         f"{_fmt_val(ab.get('baseline_variant'))} | {_fmt_val(ab.get('variant'))} | {_fmt_seconds(ab.get('observed_ratio'))} | "
                         f"{_fmt_seconds(ab.get('observed_difference_seconds'))} | {_fmt_seconds(ab.get('predicted_ratio'))} | "
                         f"{_fmt_val(ab.get('observed_winner'))} | {_fmt_val(ab.get('predicted_winner'))} | "
                         f"{_fmt_val(ab.get('winner_agrees'))} | {_fmt_val(ab.get('trial_ranges_overlap'))} |")
    else:
        lines.append("No A/B comparisons performed.")

    lines += ["", "## Not Compared", ""]
    not_comp = report.get("not_compared", [])
    if not_comp:
        lines += ["| Case ID | Reason |", "|---|---|"]
        for nc in not_comp: lines.append(f"| {_fmt_val(nc.get('case_id'))} | {_fmt_val(nc.get('reason'))} |")
    else:
        lines.append("All cases were compared.")

    lines += ["", "## Notes", ""]
    for idx, note in enumerate(report.get("notes", NOTES), 1): lines.append(f"{idx}. {note}")
    lines.append("")
    return "\n".join(lines)
