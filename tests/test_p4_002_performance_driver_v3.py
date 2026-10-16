import json

from examples.p4_002_performance_driver_v3 import (
    REPRESENTATIVE_HELDOUT,
    classify_artifact,
    manifest,
)


def test_manifest_records_selected_container_image(tmp_path):
    result = manifest("heldout-direct-p2", tmp_path, 10, "hash", image="custom:test")
    assert result["image"] == "custom:test"
    assert result["expected_checks"] == 54
    assert result["omitted_workloads"] == ["upper", "cz", "mixed-var"]


def _heldout_rows():
    return [
        {"workload": workload, "policy": policy, "store": store, "frac": fraction,
         "trial": 0, "engine": "cooperative", "backend": "buffered", "ranks": 1,
         # Unknown/error outcomes are still valid measured conditions.
         "status": "ok", "eta": None, "unknown_reasons": ["COST_UNCERTIFIED"]}
        for workload in REPRESENTATIVE_HELDOUT
        for policy in ("S0", "S1", "S2")
        for store in ("file", "ram")
        for fraction in (0.4, 0.6, 0.8)
    ]


def test_heldout_artifact_requires_full_unique_condition_matrix(tmp_path):
    artifact = tmp_path / "heldout-heldout-buffered-p1.json"
    rows = _heldout_rows()
    artifact.write_text(json.dumps({"kind": "runtime-eta-accuracy",
                                    "summary": {"overall": {"n_checks": 54}},
                                    "rows": rows}))
    assert classify_artifact("heldout-buffered-p1", tmp_path, 7) == (
        "complete", "runtime-eta-accuracy")

    # Keep the row count and workload set constant while losing one matrix cell.
    rows[-1] = dict(rows[0])
    artifact.write_text(json.dumps({"kind": "runtime-eta-accuracy",
                                    "summary": {"overall": {"n_checks": 54}},
                                    "rows": rows}))
    assert classify_artifact("heldout-buffered-p1", tmp_path, 7) == (
        "artifact-incomplete", "runtime-eta-accuracy")


def test_overhead_artifact_requires_distinct_measured_pair_indices(tmp_path):
    artifact = tmp_path / "overhead-overhead-buffered-p1.json"
    results = {}
    for workload in ("rx-ry-low", "mixed"):
        results[workload] = {
            "warmup_pairs": [{"pair_index": i} for i in range(2)],
            "measured_pairs": [{"pair_index": i} for i in range(2, 9)],
        }
    artifact.write_text(json.dumps({"status": "incomplete", "results": results}))
    assert classify_artifact("overhead-buffered-p1", tmp_path, 7) == (
        "complete-with-failed-pairs", "incomplete")

    results["mixed"]["measured_pairs"][-1]["pair_index"] = 2
    artifact.write_text(json.dumps({"status": "incomplete", "results": results}))
    assert classify_artifact("overhead-buffered-p1", tmp_path, 7) == (
        "artifact-incomplete", "incomplete")
