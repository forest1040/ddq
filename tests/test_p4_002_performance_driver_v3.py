import json
from types import SimpleNamespace

import examples.p4_002_performance_driver_v3 as driver
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


def test_timeout_removes_only_its_named_container_and_records_cleanup(tmp_path, monkeypatch, capsys):
    root = tmp_path / "campaign"
    context = root / "wave3-performance-context-v3"
    for area in ("src", "tests", "examples"):
        (context / area).mkdir(parents=True)
    (context / "examples" / "probe.py").write_text("pass\n", encoding="utf-8")
    name = "ddq-p4-v3-overhead-buffered-p1-a01"
    active = {"unrelated-container"}
    seen_run = []

    def fake_run(command, **kwargs):
        if command[0] == "timeout":
            seen_run.append(command)
            assert command[command.index("--name") + 1] == name
            assert "--rm" in command
            assert "ddq-linux@sha256:fixed" in command
            # Simulate outer timeout ending the Docker CLI while its container survives.
            active.add(name)
            return SimpleNamespace(returncode=124)
        if command[:4] == ["docker", "ps", "--all", "--quiet"]:
            assert command[-1] == f"name=^/{name}$"
            return SimpleNamespace(returncode=0, stdout=("container-id\n" if name in active else ""), stderr="")
        if command[:3] == ["docker", "rm", "--force"]:
            assert command[3:] == ["container-id"]
            active.discard(name)
            return SimpleNamespace(returncode=0, stdout="container-id\n", stderr="")
        raise AssertionError(f"unexpected subprocess command: {command}")

    monkeypatch.setattr(driver.subprocess, "run", fake_run)
    assert driver.main(["--case", "overhead-buffered-p1", "--root", str(root),
                        "--timeout", "1", "--image", "ddq-linux@sha256:fixed"]) == 1
    capsys.readouterr()
    status = json.loads((root / "wave3-performance-output-v3" / "overhead-buffered-p1" /
                         "attempt-01" / "status.json").read_text(encoding="utf-8"))
    assert len(seen_run) == 1
    assert status["status"] == "timeout"
    assert status["container_cleanup"]["status"] == "removed"
    assert status["container_cleanup"]["verified_absent"] is True
    assert active == {"unrelated-container"}
    assert status["manifest"]["image"] == "ddq-linux@sha256:fixed"


def test_normal_rm_already_absent_is_a_successful_cleanup(monkeypatch, tmp_path):
    calls = []

    def fake_run(command, **kwargs):
        calls.append(command)
        if command[0] == "timeout":
            return SimpleNamespace(returncode=0)
        assert command[:4] == ["docker", "ps", "--all", "--quiet"]
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    monkeypatch.setattr(driver.subprocess, "run", fake_run)
    with (tmp_path / "run.log").open("w", encoding="utf-8") as log:
        code, launch_error, cleanup = driver.run_with_container_cleanup(
            ["timeout", "1", "docker", "run"], container_name="case-attempt", log=log)
    assert code == 0 and launch_error is None
    assert cleanup["status"] == "already-absent" and cleanup["verified_absent"] is True
    assert len(calls) == 2


def test_unverifiable_cleanup_cannot_leave_driver_status_complete(monkeypatch, tmp_path, capsys):
    root = tmp_path / "campaign"
    context = root / "wave3-performance-context-v3"
    for area in ("src", "tests", "examples"):
        (context / area).mkdir(parents=True)
    (context / "examples" / "probe.py").write_text("pass\n", encoding="utf-8")

    def fake_run(command, **kwargs):
        if command[0] == "timeout":
            return SimpleNamespace(returncode=0)
        return SimpleNamespace(returncode=1, stdout="", stderr="docker daemon unavailable")

    monkeypatch.setattr(driver.subprocess, "run", fake_run)
    assert driver.main(["--case", "heldout-buffered-p1", "--root", str(root), "--image", "fixed"]) == 1
    capsys.readouterr()
    status = json.loads((root / "wave3-performance-output-v3" / "heldout-buffered-p1" /
                         "attempt-01" / "status.json").read_text(encoding="utf-8"))
    assert status["status"] == "cleanup-failed"
    assert status["execution_status"] == "artifact-missing"
    assert status["container_cleanup"]["status"] == "container-list-failed"
    assert status["container_cleanup"]["verified_absent"] is False
