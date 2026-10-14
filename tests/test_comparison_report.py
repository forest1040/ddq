"""Tests for svsim.comparison_report."""
from __future__ import annotations

import math
from pathlib import Path
import pytest

from svsim.comparison_report import (
    DEFAULT_SMALL_TIME_THRESHOLD_SECONDS,
    NOTES,
    build_report,
    load_campaign,
    render_markdown,
    save_report,
)
from svsim.journal import content_hash
from svsim.profile_document import read_document, wire_decode, write_document


def make_plan(
    *,
    plan_id: str = "plan-1",
    workload_id: str = "wl-1",
    profile_id: str = "prof-1",
    configuration_fingerprint: str = "cfg-1",
    lower_seconds: float | None = 1.0,
    expected_seconds: float | None = 2.0,
    conditional_upper_seconds: float | None = 3.0,
    evidence: str = "derived",
    phases: dict | None = None,
    reason: str = "estimated",
) -> dict:
    time_data: dict = {
        "lower_seconds": lower_seconds,
        "expected_seconds": expected_seconds,
        "conditional_upper_seconds": conditional_upper_seconds,
        "evidence": evidence,
        "reason": reason,
    }
    if phases is not None: time_data["phases"] = phases
    return {
        "schema_version": 1, "kind": "execution-plan", "plan_id": plan_id,
        "workload_id": workload_id, "profile_id": profile_id,
        "configuration_fingerprint": configuration_fingerprint, "time": time_data,
    }


def make_run(
    *,
    run_id: str = "run-1",
    workload_id: str = "wl-1",
    plan_id: str | None = "plan-1",
    profile_id: str = "prof-1",
    configuration_fingerprint: str = "cfg-1",
    median_seconds: float = 2.0,
    min_seconds: float = 1.8,
    max_seconds: float = 2.2,
    p95_seconds: float = 2.18,
    sample_count: int = 5,
    warmup_count: int = 1,
    status: str = "completed",
    validation_passed: bool = True,
    phases: tuple[str, ...] = ("setup", "execute", "flush", "cleanup"),
    phase_medians: dict | None = None,
    include_trials: bool = True,
) -> dict:
    if phase_medians is None:
        exec_med = median_seconds - 0.2 if median_seconds > 0.2 else median_seconds * 0.8
        phase_medians = {"setup": 0.1, "execute": exec_med, "flush": 0.05, "cleanup": 0.05}
    summary: dict = {
        "total": {"median_seconds": median_seconds, "mean_seconds": median_seconds,
                  "min_seconds": min_seconds, "max_seconds": max_seconds, "p95_seconds": p95_seconds}
    }
    for p, med in phase_medians.items():
        summary[p] = {"median_seconds": med, "mean_seconds": med, "min_seconds": med,
                      "max_seconds": med, "p95_seconds": med}
    trials = [
        {"order_index": i,
         "wall_seconds": {"setup": 0.1, "execute": phase_medians.get("execute", 1.0),
                          "flush": 0.05, "cleanup": 0.05,
                          "total": min_seconds + (max_seconds - min_seconds) * (i / max(1, sample_count - 1))},
         "cpu_seconds": median_seconds, "managed_peak_bytes": 1024, "disk_payload_bytes": 0,
         "timestamp": "2026-10-14T00:00:00Z"}
        for i in range(sample_count)
    ] if include_trials else []

    return {
        "schema_version": 1, "kind": "benchmark-run", "run_type": "workload-execution",
        "run_id": run_id, "workload_id": workload_id, "plan_id": plan_id,
        "profile_id": profile_id, "configuration_fingerprint": configuration_fingerprint,
        "case": {"label": "c1", "variant": "v1", "qubits": 4, "block_bits": 2, "ranks": 1,
                 "threads": 1, "storage": "ram", "backend": "buffered", "mode": "eager", "frame": "global"},
        "work": {"lowered_gate_count": 10, "block_count": 1, "block_pass_count": 1},
        "phases": list(phases), "trials": trials, "summary": summary,
        "warmup_count": warmup_count, "sample_count": sample_count,
        "validation": {"oracle": "eager-reference", "max_abs_error": 1e-15, "passed": validation_passed},
        "status": status, "omitted": [], "seed": 42, "provenance": {"tool": "test"},
    }


def make_campaign(
    cases: list[dict],
    *,
    calibration_workload_ids: tuple[str, ...] = (),
    baseline_variant: str | None = None,
    small_time_threshold_seconds: float = DEFAULT_SMALL_TIME_THRESHOLD_SECONDS,
) -> dict:
    return {
        "schema_version": 1, "kind": "comparison-campaign", "cases": cases,
        "calibration_workload_ids": list(calibration_workload_ids),
        "baseline_variant": baseline_variant,
        "small_time_threshold_seconds": small_time_threshold_seconds,
    }


def test_exact_error_math(tmp_path: Path):
    write_document(tmp_path / "plan_a.json", make_plan(plan_id="pA", lower_seconds=6.0, expected_seconds=10.0, conditional_upper_seconds=12.0))
    write_document(tmp_path / "run_a.json", make_run(run_id="rA", plan_id="pA", median_seconds=8.0))
    write_document(tmp_path / "plan_b.json", make_plan(plan_id="pB", lower_seconds=5.0, expected_seconds=6.0, conditional_upper_seconds=7.0))
    write_document(tmp_path / "run_b.json", make_run(run_id="rB", plan_id="pB", median_seconds=8.0))

    camp_file = tmp_path / "campaign.json"
    write_document(camp_file, make_campaign([
        {"case_id": "cA", "plan": "plan_a.json", "run": "run_a.json", "variant": "v1", "group": "g1"},
        {"case_id": "cB", "plan": "plan_b.json", "run": "run_b.json", "variant": "v2", "group": "g1"},
    ]))

    report = wire_decode(build_report(camp_file))
    case_map = {c["case_id"]: c for c in report["cases"]}
    ca, cb = case_map["cA"], case_map["cB"]

    assert ca["absolute_error_seconds"] == pytest.approx(2.0)
    assert ca["relative_error"] == pytest.approx(0.25)
    assert ca["within_interval"] is True

    assert cb["absolute_error_seconds"] == pytest.approx(-2.0)
    assert cb["relative_error"] == pytest.approx(-0.25)
    assert cb["within_interval"] is False


def test_held_out_vs_calibration_split(tmp_path: Path):
    write_document(tmp_path / "plan_ho.json", make_plan(plan_id="pHO", workload_id="wl-heldout", expected_seconds=10.0))
    write_document(tmp_path / "run_ho.json", make_run(run_id="rHO", workload_id="wl-heldout", plan_id="pHO", median_seconds=8.0))
    write_document(tmp_path / "plan_cal.json", make_plan(plan_id="pCAL", workload_id="wl-cal", expected_seconds=10.0))
    write_document(tmp_path / "run_cal.json", make_run(run_id="rCAL", workload_id="wl-cal", plan_id="pCAL", median_seconds=5.0))

    camp_file = tmp_path / "campaign.json"
    write_document(camp_file, make_campaign(
        [
            {"case_id": "cHO", "plan": "plan_ho.json", "run": "run_ho.json", "variant": "v1", "group": "g1"},
            {"case_id": "cCAL", "plan": "plan_cal.json", "run": "run_cal.json", "variant": "v2", "group": "g1"},
        ],
        calibration_workload_ids=("wl-cal",),
    ))

    report = wire_decode(build_report(camp_file))
    case_map = {c["case_id"]: c for c in report["cases"]}
    assert case_map["cHO"]["held_out"] is True
    assert case_map["cCAL"]["held_out"] is False

    acc = report["accuracy"]
    assert acc["held_out"]["case_count"] == 1
    assert acc["held_out"]["median_abs_relative_error"] == pytest.approx(0.25)
    assert acc["held_out"]["worst_case_id"] == "cHO"

    assert acc["calibration"]["case_count"] == 1
    assert acc["calibration"]["median_abs_relative_error"] == pytest.approx(1.0)
    assert acc["calibration"]["worst_case_id"] == "cCAL"
    assert acc["target"]["meets_target"] is True


def test_small_time_separate_aggregation(tmp_path: Path):
    write_document(tmp_path / "plan_st1.json", make_plan(plan_id="pST1", workload_id="wl-ho", expected_seconds=0.03))
    write_document(tmp_path / "run_st1.json", make_run(run_id="rST1", workload_id="wl-ho", plan_id="pST1", median_seconds=0.02))
    write_document(tmp_path / "plan_st2.json", make_plan(plan_id="pST2", workload_id="wl-cal", expected_seconds=0.015))
    write_document(tmp_path / "run_st2.json", make_run(run_id="rST2", workload_id="wl-cal", plan_id="pST2", median_seconds=0.01))
    write_document(tmp_path / "plan_norm.json", make_plan(plan_id="pNORM", workload_id="wl-ho", expected_seconds=1.2))
    write_document(tmp_path / "run_norm.json", make_run(run_id="rNORM", workload_id="wl-ho", plan_id="pNORM", median_seconds=1.0))

    camp_file = tmp_path / "campaign.json"
    write_document(camp_file, make_campaign(
        [
            {"case_id": "cST1", "plan": "plan_st1.json", "run": "run_st1.json", "variant": "v1", "group": "g1"},
            {"case_id": "cST2", "plan": "plan_st2.json", "run": "run_st2.json", "variant": "v2", "group": "g1"},
            {"case_id": "cNORM", "plan": "plan_norm.json", "run": "run_norm.json", "variant": "v3", "group": "g1"},
        ],
        calibration_workload_ids=("wl-cal",),
        small_time_threshold_seconds=0.05,
    ))

    report = wire_decode(build_report(camp_file))
    case_map = {c["case_id"]: c for c in report["cases"]}

    assert case_map["cST1"]["small_time"] is True
    assert case_map["cST2"]["small_time"] is True
    assert case_map["cNORM"]["small_time"] is False

    acc = report["accuracy"]
    assert acc["held_out"]["case_count"] == 1
    assert acc["held_out"]["worst_case_id"] == "cNORM"
    assert acc["calibration"]["case_count"] == 0
    assert acc["small_time"]["case_count"] == 2
    assert acc["small_time"]["worst_abs_error_seconds"] == pytest.approx(0.01)
    assert acc["small_time"]["worst_case_id"] == "cST1"
    assert acc["small_time"]["median_abs_error_seconds"] == pytest.approx(0.0075)


def test_interval_coverage_incl_none_bounds(tmp_path: Path):
    cases = []
    write_document(tmp_path / "p1.json", make_plan(plan_id="p1", lower_seconds=1.0, conditional_upper_seconds=3.0, expected_seconds=2.0))
    write_document(tmp_path / "r1.json", make_run(run_id="r1", plan_id="p1", median_seconds=2.0))
    cases.append({"case_id": "c1", "plan": "p1.json", "run": "r1.json", "variant": "v1", "group": "g1"})

    write_document(tmp_path / "p2.json", make_plan(plan_id="p2", lower_seconds=1.0, conditional_upper_seconds=3.0, expected_seconds=2.0))
    write_document(tmp_path / "r2.json", make_run(run_id="r2", plan_id="p2", median_seconds=4.0))
    cases.append({"case_id": "c2", "plan": "p2.json", "run": "r2.json", "variant": "v2", "group": "g1"})

    write_document(tmp_path / "p3.json", make_plan(plan_id="p3", lower_seconds=None, conditional_upper_seconds=3.0, expected_seconds=2.0))
    write_document(tmp_path / "r3.json", make_run(run_id="r3", plan_id="p3", median_seconds=2.0))
    cases.append({"case_id": "c3", "plan": "p3.json", "run": "r3.json", "variant": "v3", "group": "g1"})

    write_document(tmp_path / "p4.json", make_plan(plan_id="p4", lower_seconds=1.0, conditional_upper_seconds=None, expected_seconds=2.0))
    write_document(tmp_path / "r4.json", make_run(run_id="r4", plan_id="p4", median_seconds=2.0))
    cases.append({"case_id": "c4", "plan": "p4.json", "run": "r4.json", "variant": "v4", "group": "g1"})

    write_document(tmp_path / "p5.json", make_plan(plan_id="p5", lower_seconds=None, conditional_upper_seconds=None, expected_seconds=2.0))
    write_document(tmp_path / "r5.json", make_run(run_id="r5", plan_id="p5", median_seconds=2.0))
    cases.append({"case_id": "c5", "plan": "p5.json", "run": "r5.json", "variant": "v5", "group": "g1"})

    camp_file = tmp_path / "campaign.json"
    write_document(camp_file, make_campaign(cases))
    report = wire_decode(build_report(camp_file))
    case_map = {c["case_id"]: c for c in report["cases"]}

    assert case_map["c1"]["within_interval"] is True
    assert case_map["c2"]["within_interval"] is False
    assert case_map["c3"]["within_interval"] is None
    assert case_map["c4"]["within_interval"] is None
    assert case_map["c5"]["within_interval"] is None

    stats = report["accuracy"]["held_out"]
    assert stats["case_count"] == 5
    assert stats["interval_case_count"] == 2
    assert stats["interval_coverage"] == pytest.approx(0.5)


def test_mismatch_reasons_and_failed_validation(tmp_path: Path):
    cases = []
    write_document(tmp_path / "p_wm.json", make_plan(plan_id="pwm", workload_id="wl-A"))
    write_document(tmp_path / "r_wm.json", make_run(run_id="rwm", plan_id="pwm", workload_id="wl-B"))
    cases.append({"case_id": "c_wm", "plan": "p_wm.json", "run": "r_wm.json", "variant": "v1", "group": "g1"})

    write_document(tmp_path / "p_pm.json", make_plan(plan_id="ppm-1"))
    write_document(tmp_path / "r_pm.json", make_run(run_id="rpm", plan_id="ppm-DIFF"))
    cases.append({"case_id": "c_pm", "plan": "p_pm.json", "run": "r_pm.json", "variant": "v1", "group": "g1"})

    write_document(tmp_path / "p_cm.json", make_plan(plan_id="pcm", configuration_fingerprint="cfg-X"))
    write_document(tmp_path / "r_cm.json", make_run(run_id="rcm", plan_id="pcm", configuration_fingerprint="cfg-Y"))
    cases.append({"case_id": "c_cm", "plan": "p_cm.json", "run": "r_cm.json", "variant": "v1", "group": "g1"})

    write_document(tmp_path / "p_nc.json", make_plan(plan_id="pnc"))
    write_document(tmp_path / "r_nc.json", make_run(run_id="rnc", plan_id="pnc", status="failed"))
    cases.append({"case_id": "c_nc", "plan": "p_nc.json", "run": "r_nc.json", "variant": "v1", "group": "g1"})

    write_document(tmp_path / "p_vf.json", make_plan(plan_id="pvf"))
    write_document(tmp_path / "r_vf.json", make_run(run_id="rvf", plan_id="pvf", status="completed", validation_passed=False))
    cases.append({"case_id": "c_vf", "plan": "p_vf.json", "run": "r_vf.json", "variant": "v1", "group": "g1"})

    write_document(tmp_path / "p_np.json", make_plan(plan_id="pnp", expected_seconds=None))
    write_document(tmp_path / "r_np.json", make_run(run_id="rnp", plan_id="pnp"))
    cases.append({"case_id": "c_np", "plan": "p_np.json", "run": "r_np.json", "variant": "v1", "group": "g1"})

    camp_file = tmp_path / "campaign.json"
    write_document(camp_file, make_campaign(cases))
    report = wire_decode(build_report(camp_file))
    not_comp_map = {item["case_id"]: item["reason"] for item in report["not_compared"]}

    assert not_comp_map["c_wm"] == "WORKLOAD_MISMATCH"
    assert not_comp_map["c_pm"] == "PLAN_MISMATCH"
    assert not_comp_map["c_cm"] == "CONFIGURATION_MISMATCH"
    assert not_comp_map["c_nc"] == "RUN_NOT_COMPLETED"
    assert not_comp_map["c_vf"] == "RUN_VALIDATION_FAILED"
    assert not_comp_map["c_np"] == "NO_TIME_PREDICTION"

    assert report["accuracy"]["held_out"]["case_count"] == 0
    assert report["accuracy"]["calibration"]["case_count"] == 0
    assert report["accuracy"]["small_time"]["case_count"] == 0


def test_no_time_prediction_preserves_observed_values(tmp_path: Path):
    write_document(tmp_path / "p_np.json", make_plan(plan_id="pnp", expected_seconds=None))
    write_document(
        tmp_path / "r_np.json",
        make_run(
            run_id="rnp", plan_id="pnp", median_seconds=2.5, min_seconds=2.0, max_seconds=3.0,
            p95_seconds=2.95, sample_count=7,
            phase_medians={"setup": 0.2, "execute": 2.1, "flush": 0.1, "cleanup": 0.1},
        ),
    )
    camp_file = tmp_path / "campaign.json"
    write_document(camp_file, make_campaign([
        {"case_id": "c_np", "plan": "p_np.json", "run": "r_np.json", "variant": "v1", "group": "g1"}
    ]))

    report = wire_decode(build_report(camp_file))
    case = report["cases"][0]
    assert case["status"] == "not-compared"
    assert case["reason"] == "NO_TIME_PREDICTION"
    assert case["predicted"]["expected_seconds"] is None

    # Observed values are fully populated
    obs = case["observed"]
    assert obs["median_seconds"] == pytest.approx(2.5)
    assert obs["min_seconds"] == pytest.approx(2.0)
    assert obs["max_seconds"] == pytest.approx(3.0)
    assert obs["p95_seconds"] == pytest.approx(2.95)
    assert obs["sample_count"] == 7
    assert case["observed_bottleneck"] == "execute"


def test_ab_comparisons_ratio_winner_and_overlap(tmp_path: Path):
    write_document(tmp_path / "p_base.json", make_plan(plan_id="p_base", expected_seconds=10.0))
    write_document(tmp_path / "r_base.json", make_run(run_id="r_base", plan_id="p_base", median_seconds=10.0, min_seconds=9.0, max_seconds=11.0))

    write_document(tmp_path / "p_fast.json", make_plan(plan_id="p_fast", expected_seconds=5.0))
    write_document(tmp_path / "r_fast.json", make_run(run_id="r_fast", plan_id="p_fast", median_seconds=5.0, min_seconds=4.0, max_seconds=6.0))

    write_document(tmp_path / "p_slow.json", make_plan(plan_id="p_slow", expected_seconds=15.0))
    write_document(tmp_path / "r_slow.json", make_run(run_id="r_slow", plan_id="p_slow", median_seconds=15.0, min_seconds=10.5, max_seconds=16.0))

    write_document(tmp_path / "p_tie.json", make_plan(plan_id="p_tie", expected_seconds=9.9))
    write_document(tmp_path / "r_tie.json", make_run(run_id="r_tie", plan_id="p_tie", median_seconds=10.1, min_seconds=9.5, max_seconds=10.5))

    write_document(tmp_path / "p_broken.json", make_plan(plan_id="p_broken", expected_seconds=5.0))
    write_document(tmp_path / "r_broken.json", make_run(run_id="r_broken", plan_id="p_broken", status="failed"))

    write_document(tmp_path / "p_g2_b.json", make_plan(plan_id="p_g2_b", expected_seconds=10.0))
    write_document(tmp_path / "r_g2_b.json", make_run(run_id="r_g2_b", plan_id="p_g2_b", median_seconds=10.0))
    write_document(tmp_path / "p_g2_a.json", make_plan(plan_id="p_g2_a", expected_seconds=10.0))
    write_document(tmp_path / "r_g2_a.json", make_run(run_id="r_g2_a", plan_id="p_g2_a", median_seconds=10.0))
    write_document(tmp_path / "p_g2_v.json", make_plan(plan_id="p_g2_v", expected_seconds=8.0))
    write_document(tmp_path / "r_g2_v.json", make_run(run_id="r_g2_v", plan_id="p_g2_v", median_seconds=8.0))

    # A/B case where trials are empty and summary has no min/max -> trial_ranges_overlap is None
    write_document(tmp_path / "p_g3_base.json", make_plan(plan_id="p_g3_base", expected_seconds=10.0))
    run_no_range_base = make_run(run_id="r_g3_base", plan_id="p_g3_base", median_seconds=10.0, include_trials=False)
    run_no_range_base["summary"]["total"]["min_seconds"] = None
    run_no_range_base["summary"]["total"]["max_seconds"] = None
    write_document(tmp_path / "r_g3_base.json", run_no_range_base)

    write_document(tmp_path / "p_g3_v.json", make_plan(plan_id="p_g3_v", expected_seconds=5.0))
    run_no_range_v = make_run(run_id="r_g3_v", plan_id="p_g3_v", median_seconds=5.0, include_trials=False)
    run_no_range_v["summary"]["total"]["min_seconds"] = None
    run_no_range_v["summary"]["total"]["max_seconds"] = None
    write_document(tmp_path / "r_g3_v.json", run_no_range_v)

    camp_file = tmp_path / "campaign.json"
    write_document(camp_file, make_campaign(
        [
            {"case_id": "c_base", "plan": "p_base.json", "run": "r_base.json", "variant": "baseline", "group": "g1"},
            {"case_id": "c_fast", "plan": "p_fast.json", "run": "r_fast.json", "variant": "fast", "group": "g1"},
            {"case_id": "c_slow", "plan": "p_slow.json", "run": "r_slow.json", "variant": "slow", "group": "g1"},
            {"case_id": "c_tie", "plan": "p_tie.json", "run": "r_tie.json", "variant": "tie_var", "group": "g1"},
            {"case_id": "c_broken", "plan": "p_broken.json", "run": "r_broken.json", "variant": "broken", "group": "g1"},
            {"case_id": "c_g2_b", "plan": "p_g2_b.json", "run": "r_g2_b.json", "variant": "baseline", "group": "g2"},
            {"case_id": "c_g2_a", "plan": "p_g2_a.json", "run": "r_g2_a.json", "variant": "baseline", "group": "g2"},
            {"case_id": "c_g2_v", "plan": "p_g2_v.json", "run": "r_g2_v.json", "variant": "var2", "group": "g2"},
            {"case_id": "c_g3_b", "plan": "p_g3_base.json", "run": "r_g3_base.json", "variant": "baseline", "group": "g3"},
            {"case_id": "c_g3_v", "plan": "p_g3_v.json", "run": "r_g3_v.json", "variant": "var3", "group": "g3"},
        ],
        baseline_variant="baseline",
    ))

    report = wire_decode(build_report(camp_file))
    ab_map = {item["case_id"]: item for item in report["ab_comparisons"]}

    fast_entry = ab_map["c_fast"]
    assert fast_entry["observed_ratio"] == pytest.approx(0.5)
    assert fast_entry["observed_winner"] == "variant"
    assert fast_entry["predicted_winner"] == "variant"
    assert fast_entry["winner_agrees"] is True
    assert fast_entry["trial_ranges_overlap"] is False

    slow_entry = ab_map["c_slow"]
    assert slow_entry["observed_ratio"] == pytest.approx(1.5)
    assert slow_entry["observed_winner"] == "baseline"
    assert slow_entry["predicted_winner"] == "baseline"
    assert slow_entry["winner_agrees"] is True
    assert slow_entry["trial_ranges_overlap"] is True

    tie_entry = ab_map["c_tie"]
    assert tie_entry["observed_winner"] == "tie"
    assert tie_entry["predicted_winner"] == "tie"
    assert tie_entry["winner_agrees"] is True

    assert "c_broken" not in ab_map
    assert ab_map["c_g2_v"]["baseline_case_id"] == "c_g2_a"

    # In g3, range cannot be determined -> trial_ranges_overlap is None (not fabricated False)
    assert ab_map["c_g3_v"]["trial_ranges_overlap"] is None


def test_phases_share_and_bottleneck_when_plan_phases_none(tmp_path: Path):
    # Plan with time.phases is None
    write_document(tmp_path / "p_nophases.json", make_plan(plan_id="pnp", expected_seconds=2.0, phases=None))
    write_document(
        tmp_path / "r_phases.json",
        make_run(
            run_id="rp", plan_id="pnp", median_seconds=2.0,
            phase_medians={"setup": 0.2, "execute": 1.6, "flush": 0.1, "cleanup": 0.1},
        ),
    )
    camp_file = tmp_path / "campaign.json"
    write_document(camp_file, make_campaign([
        {"case_id": "c_ph", "plan": "p_nophases.json", "run": "r_phases.json", "variant": "v1", "group": "g1"}
    ]))

    report = wire_decode(build_report(camp_file))
    case = report["cases"][0]

    # predicted_bottleneck is None when plan has no phases
    assert case["predicted_bottleneck"] is None
    assert case["observed_bottleneck"] == "execute"

    # observed_share across phases sums to 1.0
    shares = [phase_data["observed_share"] for phase_data in case["phases"].values()]
    assert all(s is not None for s in shares)
    assert sum(shares) == pytest.approx(1.0)
    assert case["phases"]["execute"]["observed_share"] == pytest.approx(1.6 / 2.0)


def test_campaign_validation_rejects_invalids(tmp_path: Path):
    plan_path = tmp_path / "p.json"
    run_path = tmp_path / "r.json"
    write_document(plan_path, make_plan())
    write_document(run_path, make_run())

    valid_case = {"case_id": "c1", "plan": "p.json", "run": "r.json", "variant": "v1", "group": "g1"}

    # Duplicate case_id
    f = tmp_path / "camp_dup.json"
    write_document(f, make_campaign([valid_case, valid_case]))
    with pytest.raises(ValueError, match="duplicate case_id"): load_campaign(f)

    # Unknown campaign field
    bad_camp = make_campaign([valid_case])
    bad_camp["unexpected_field"] = "bad"
    f = tmp_path / "camp_unknown.json"
    write_document(f, bad_camp)
    with pytest.raises(ValueError, match="unknown campaign fields"): load_campaign(f)

    # Wrong kind
    bad_camp = make_campaign([valid_case])
    bad_camp["kind"] = "wrong-kind"
    f = tmp_path / "camp_kind.json"
    write_document(f, bad_camp)
    with pytest.raises(ValueError, match="unsupported campaign kind"): load_campaign(f)

    # Wrong schema version
    bad_camp = make_campaign([valid_case])
    bad_camp["schema_version"] = 2
    f = tmp_path / "camp_ver.json"
    write_document(f, bad_camp)
    with pytest.raises(ValueError, match="unsupported schema_version"): load_campaign(f)

    # Non-string id in case
    f = tmp_path / "camp_nonstr.json"
    write_document(f, make_campaign([{"case_id": 123, "plan": "p.json", "run": "r.json", "variant": "v1", "group": "g1"}]))
    with pytest.raises(ValueError, match="must be a non-empty string"): load_campaign(f)

    # Exceeds 256 cases
    too_many = [{"case_id": f"c{i}", "plan": "p.json", "run": "r.json", "variant": "v", "group": "g"} for i in range(257)]
    f = tmp_path / "camp_toomany.json"
    write_document(f, make_campaign(too_many))
    with pytest.raises(ValueError, match="exceeds 256 cases"): load_campaign(f)

    # Missing plan path reference on disk
    f = tmp_path / "camp_missing_plan.json"
    write_document(f, make_campaign([{"case_id": "c1", "plan": "non_existent_plan.json", "run": "r.json", "variant": "v", "group": "g"}]))
    with pytest.raises(OSError): build_report(f)

    # Invalid plan kind
    bad_plan = make_plan()
    bad_plan["kind"] = "not-a-plan"
    write_document(tmp_path / "bad_plan.json", bad_plan)
    f = tmp_path / "camp_badplan.json"
    write_document(f, make_campaign([{"case_id": "c1", "plan": "bad_plan.json", "run": "r.json", "variant": "v", "group": "g"}]))
    with pytest.raises(ValueError, match="kind must be 'execution-plan'"): build_report(f)

    # Invalid run kind
    bad_run = make_run()
    bad_run["kind"] = "not-a-run"
    write_document(tmp_path / "bad_run.json", bad_run)
    f = tmp_path / "camp_badrun.json"
    write_document(f, make_campaign([{"case_id": "c1", "plan": "p.json", "run": "bad_run.json", "variant": "v", "group": "g"}]))
    with pytest.raises(ValueError, match="kind must be 'benchmark-run'"): build_report(f)


def test_report_roundtrip_preserves_wire_counts_and_content_hash(tmp_path: Path):
    write_document(tmp_path / "p.json", make_plan(expected_seconds=2.0))
    write_document(tmp_path / "r.json", make_run(median_seconds=2.0, sample_count=7))
    camp_file = tmp_path / "campaign.json"
    write_document(camp_file, make_campaign([{"case_id": "c1", "plan": "p.json", "run": "r.json", "variant": "v1", "group": "g1"}]))

    report_encoded = build_report(camp_file)
    rep_path = tmp_path / "report.json"
    save_report(report_encoded, rep_path)

    raw_doc = read_document(rep_path)
    assert isinstance(raw_doc["accuracy"]["held_out"]["case_count"], str)
    assert raw_doc["accuracy"]["held_out"]["case_count"] == "1"
    assert isinstance(raw_doc["accuracy"]["held_out"]["interval_case_count"], str)
    assert raw_doc["accuracy"]["held_out"]["interval_case_count"] == "1"
    assert isinstance(raw_doc["cases"][0]["observed"]["sample_count"], str)
    assert raw_doc["cases"][0]["observed"]["sample_count"] == "7"

    payload = {k: v for k, v in raw_doc.items() if k != "report_id"}
    assert raw_doc["report_id"] == content_hash(payload)

    decoded = wire_decode(raw_doc)
    assert decoded["accuracy"]["held_out"]["case_count"] == 1
    assert decoded["accuracy"]["held_out"]["interval_case_count"] == 1
    assert decoded["cases"][0]["observed"]["sample_count"] == 7

    with pytest.raises(FileExistsError): save_report(report_encoded, rep_path, overwrite=False)
    save_report(report_encoded, rep_path, overwrite=True)


def test_markdown_rendering(tmp_path: Path):
    write_document(
        tmp_path / "p1.json",
        make_plan(plan_id="p1", expected_seconds=2.0, lower_seconds=1.5, conditional_upper_seconds=2.5,
                  phases={"setup": {"expected_seconds": 0.1}, "execute": {"expected_seconds": 1.9}}),
    )
    write_document(
        tmp_path / "r1.json",
        make_run(run_id="r1", plan_id="p1", median_seconds=2.0,
                 phase_medians={"setup": 0.1, "execute": 1.8, "flush": 0.05, "cleanup": 0.05}),
    )
    write_document(tmp_path / "p2.json", make_plan(plan_id="p2", expected_seconds=None))
    write_document(tmp_path / "r2.json", make_run(run_id="r2", plan_id="p2", median_seconds=3.0))

    camp_file = tmp_path / "campaign.json"
    write_document(camp_file, make_campaign(
        [
            {"case_id": "case_full", "plan": "p1.json", "run": "r1.json", "variant": "v1", "group": "g1"},
            {"case_id": "case_missing", "plan": "p2.json", "run": "r2.json", "variant": "v2", "group": "g1"},
        ],
        baseline_variant="v1",
    ))

    report = build_report(camp_file)
    md = render_markdown(report)

    assert "case_full" in md
    assert "case_missing" in md
    assert "n/a" in md

    for note in NOTES: assert note in md
    note_indices = [md.index(note) for note in NOTES]
    assert note_indices == sorted(note_indices)

    assert "execute" in md
    assert "NO_TIME_PREDICTION" in md


def test_p95_rule(tmp_path: Path):
    cases = []
    vals = [1.0, 2.0, 3.0, 4.0, 5.0]
    for i, v in enumerate(vals):
        p_name, r_name = f"p_{i}.json", f"r_{i}.json"
        write_document(tmp_path / p_name, make_plan(plan_id=f"p_{i}", expected_seconds=1.0 + v))
        write_document(tmp_path / r_name, make_run(run_id=f"r_{i}", plan_id=f"p_{i}", median_seconds=1.0))
        cases.append({"case_id": f"c_{i}", "plan": p_name, "run": r_name, "variant": f"v_{i}", "group": "g"})

    camp_file = tmp_path / "campaign.json"
    write_document(camp_file, make_campaign(cases))
    report = wire_decode(build_report(camp_file))
    assert report["accuracy"]["held_out"]["p95_abs_relative_error"] == pytest.approx(4.8)
