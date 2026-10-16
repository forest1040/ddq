import json
from pathlib import Path
import sys

# The console pytest entry point does not add the repository root to sys.path.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
try:
    from examples.p4_002_overhead_campaign import main, run_campaign, summarize_pairs
finally:
    sys.path.pop(0)


class SerialComm:
    def Get_rank(self):
        return 0


def test_campaign_uses_two_warmups_seeded_alternation_and_keeps_rank_max():
    calls = []

    def fake_run(comm, scratch, workload, backend, eta_flag, run_tag, *, return_metrics=False):
        calls.append((workload["name"], eta_flag, run_tag, return_metrics, str(scratch)))
        return {"actual_rank_max_seconds": (12.0 if eta_flag else 10.0),
                "capture_count": 3 if eta_flag else 0,
                "capture_wall_seconds": .03 if eta_flag else 0.0,
                "reported_extraction_seconds": .02 if eta_flag else 0.0,
                "eta_evaluation_count": 9 if eta_flag else 0,
                "eta_evaluation_seconds": .01 if eta_flag else 0.0}

    wl = {"name": "small", "campaign_class": "small"}
    first = run_campaign(SerialComm(), workloads=[wl], backend="buffered", scratch_dir="scratch",
                         warmups=2, pairs=7, seed=4, bootstrap_replicates=100, run_one=fake_run)
    second = run_campaign(SerialComm(), workloads=[wl], backend="buffered", scratch_dir="scratch",
                          warmups=2, pairs=7, seed=4, bootstrap_replicates=100, run_one=fake_run)
    result = first["small"]
    assert result["warmup_pair_count"] == 2
    assert result["measured_pair_count"] == 7
    assert [p["order"] for p in result["warmup_pairs"] + result["measured_pairs"]] == [
        p["order"] for p in second["small"]["warmup_pairs"] + second["small"]["measured_pairs"]]
    assert all(row["status"] == "ok" for row in result["measured_pairs"])
    assert result["measured_pairs"][0]["eta_on"]["actual_rank_max_seconds"] == 12.0
    assert result["measured_pairs"][0]["eta_on"]["capture_wall_seconds"] == .03
    assert result["summary"]["difference_ci95"]["method"] == \
        "paired percentile bootstrap of median, 95% CI"
    assert result["summary"]["interpretation"] == "eta-on-slower"
    assert all(call[3] is True for call in calls)
    assert calls[0][4] == "scratch"


def test_failed_pair_remains_visible_and_bootstrap_reports_inconclusive():
    def fake_run(comm, scratch, workload, backend, eta_flag, run_tag, *, return_metrics=False):
        if eta_flag:
            raise RuntimeError("synthetic test failure")
        return 5.0

    result = run_campaign(SerialComm(), workloads=[{"name": "long", "campaign_class": "long"}],
                          backend="direct", warmups=0, pairs=2, seed=3,
                          bootstrap_replicates=100, run_one=fake_run)["long"]
    assert [row["status"] for row in result["measured_pairs"]] == ["failed", "failed"]
    assert all("synthetic test failure" in row["errors"]["eta_on"]
               for row in result["measured_pairs"])
    assert result["summary"]["status"] == "incomplete"
    assert result["summary"]["interpretation"] == "inconclusive"
    assert result["summary"]["difference_ci95"]["status"] == "insufficient-pairs"


def test_empty_or_identical_noise_has_no_invented_effect_threshold():
    rows = [{"status": "ok", "eta_on_seconds": 3.0, "eta_off_seconds": 3.0} for _ in range(4)]
    summary = summarize_pairs(rows, seed=10, bootstrap_replicates=100)
    assert summary["interpretation"] == "inconclusive"
    assert summary["median_paired_difference_seconds"] == 0.0
    assert summary["difference_ci95"]["lower"] == summary["difference_ci95"]["upper"] == 0.0


def test_incomplete_campaign_never_claims_a_performance_effect():
    rows = [{"status": "ok", "eta_on_seconds": 6.0, "eta_off_seconds": 3.0} for _ in range(7)]
    rows.append({"status": "failed", "errors": {"eta_off": "I/O failed"}})
    result = summarize_pairs(rows, seed=4, bootstrap_replicates=100)
    assert result["difference_ci95"]["lower"] > 0
    assert result["status"] == "incomplete"
    assert result["interpretation"] == "inconclusive"


def test_plan_only_smoke_records_resolved_workloads_and_sources(tmp_path):
    output = tmp_path / "campaign-plan.json"
    assert main(["--plan-only", "--output", str(output), "--backend", "direct"]) == 0
    plan = json.loads(output.read_text(encoding="utf-8"))
    assert plan["status"] == "plan-only"
    assert plan["conditions"]["shared_conditions"]["backend_requested"] == "direct"
    assert plan["conditions"]["shared_conditions"]["backend_resolved"] is None
    assert [(row["name"], row["campaign_class"]) for row in plan["conditions"]["workloads"]] == [
        ("rx-ry-low", "small"), ("mixed", "long")]
    assert len(plan["source"]["sha256"]) >= 5
