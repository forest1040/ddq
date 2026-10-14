"""Tests for P4-001 reproducible evidence-bundle tool."""

from __future__ import annotations

import hashlib
import importlib.util
import json
import tarfile
from pathlib import Path

import pytest

# Load examples/p4_001_evidence.py via importlib
EXAMPLE_SCRIPT_PATH = (
    Path(__file__).resolve().parent.parent / "examples" / "p4_001_evidence.py"
)
spec = importlib.util.spec_from_file_location("p4_001_evidence", EXAMPLE_SCRIPT_PATH)
if spec is None or spec.loader is None:
    raise ImportError(f"Cannot load module from {EXAMPLE_SCRIPT_PATH}")
evidence_mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(evidence_mod)


@pytest.fixture
def synthetic_tree(tmp_path: Path) -> Path:
    """Create a synthetic directory tree with required evidence artifacts."""
    root = tmp_path / "evidence_root"
    root.mkdir(parents=True, exist_ok=True)

    # 1. Benchmark runs with known values for math verification: [1.0, 2.0, 3.0]
    bench_dir = root / "phase5_timing_validation" / "eta-mac" / "held"
    bench_dir.mkdir(parents=True, exist_ok=True)

    for i, med_sec in enumerate(["1.0", "2.0", "3.0"], start=1):
        run_data = {
            "schema_version": 1,
            "kind": "benchmark-run",
            "run_type": "workload-execution",
            "status": "completed",
            "case": {
                "mode": "distributed-async",
                "storage": "ram",
                "backend": "buffered",
                "ranks": "1",
                "nodes": 1,
            },
            "summary": {
                "total": {
                    "median_seconds": med_sec,
                }
            },
            "validation": {
                "passed": True,
                "oracle": "eager-reference",
            },
        }
        with open(bench_dir / f"run{i}.run.json", "w", encoding="utf-8") as f:
            json.dump(run_data, f)

    # 2. Time model
    model_dir = root / "phase5_timing_validation" / "eta-mac"
    model_data = {
        "schema_version": 1,
        "kind": "time-model",
        "model_id": "time-model-test-123",
        "condition": {
            "mode": "distributed-async",
            "storage": "ram",
            "backend": "buffered",
            "ranks": 1,
            "nodes": 1,
        },
        "sync_latency": {
            "median_seconds": 2.5e-05,
            "excluded_unstable_trial_count": "2",
        },
        "calibration_run_ids": ["cal-1", "cal-2", "cal-3", "cal-4"],
    }
    with open(model_dir / "model.json", "w", encoding="utf-8") as f:
        json.dump(model_data, f)

    # 3. Runtime ETA accuracy
    eta_data = {
        "schema_version": 1,
        "kind": "runtime-eta-accuracy",
        "check_count": "40",
        "predicted_count": "40",
        "median_abs_relative_error": 0.05,
        "p95_abs_relative_error": 0.11,
        "worst_abs_relative_error": 0.28,
        "interval_coverage": 1.0,
    }
    with open(model_dir / "eta-report.json", "w", encoding="utf-8") as f:
        json.dump(eta_data, f)

    # 4. Hardware profile
    probe_data = {
        "schema_version": 1,
        "kind": "hardware-profile",
        "profile_id": "hw-prof-abc",
        "configuration_fingerprint": "cfg-fp-xyz",
    }
    with open(model_dir / "probe.json", "w", encoding="utf-8") as f:
        json.dump(probe_data, f)

    # 5. Wrapped comparison report under phase4_monitor_overhead
    monitor_dir = root / "phase4_monitor_overhead" / "mac"
    monitor_dir.mkdir(parents=True, exist_ok=True)
    wrapped_report = {
        "schema_version": 1,
        "kind": "phase4-followup-campaign",
        "monitor": {
            "schema_version": 1,
            "kind": "comparison-report",
            "case_id": "completion-monitor-p1",
            "rows": [
                {
                    "variant_id": "all-off",
                    "metrics": {
                        "wall_seconds": {"median": 0.10},
                    },
                },
                {
                    "variant_id": "collector",
                    "metrics": {
                        "wall_seconds": {"median": 0.12},
                    },
                },
            ],
            "comparisons": [
                {
                    "variant": "collector",
                    "ratio": {"median": 1.20},
                    "difference_ci95": [0.015, 0.025],
                }
            ],
        },
    }
    with open(monitor_dir / "p1.json", "w", encoding="utf-8") as f:
        json.dump(wrapped_report, f)

    # 6. Unparseable JSON file
    unparsed_dir = root / "phase4_foundation_validation"
    unparsed_dir.mkdir(parents=True, exist_ok=True)
    with open(unparsed_dir / "broken.json", "w", encoding="utf-8") as f:
        f.write('{"unclosed_json": [1, 2, 3')

    # 7. A tar.gz archive
    crosshost_dir = root / "phase5_crosshost" / "run1"
    crosshost_dir.mkdir(parents=True, exist_ok=True)
    tar_path = crosshost_dir / "archive.tar.gz"
    with tarfile.open(tar_path, "w:gz") as tar:
        dummy_file = tmp_path / "dummy.txt"
        dummy_file.write_text("dummy archive content\n", encoding="utf-8")
        tar.add(dummy_file, arcname="dummy.txt")

    return root


def test_grouping_math(synthetic_tree: Path) -> None:
    """Assert grouping math: median, p95 linear interpolation, worst, and variation."""
    manifest = evidence_mod.build_evidence_manifest(synthetic_tree)
    groups = manifest["summaries"]["benchmark-run"]
    assert len(groups) == 1

    grp = groups[0]
    cond = grp["condition"]
    assert cond["mode"] == "distributed-async"
    assert cond["storage"] == "ram"
    assert cond["backend"] == "buffered"
    assert cond["ranks"] == 1
    assert cond["nodes"] == 1
    assert cond["evidence_dir"] == "phase5_timing_validation/eta-mac"

    assert grp["run_count"] == 3
    assert grp["completed_count"] == 3
    assert grp["validation_passed_count"] == 3

    # Medians: [1.0, 2.0, 3.0]
    # Median = 2.0
    assert grp["median_seconds"] == pytest.approx(2.0)
    # Worst = max(medians) = 3.0
    assert grp["worst_seconds"] == pytest.approx(3.0)
    # P95 at (n-1)*0.95 = 2 * 0.95 = 1.9
    # low = 1 (val 2.0), high = 2 (val 3.0), frac = 0.9
    # 2.0 + 0.9 * (3.0 - 2.0) = 2.9
    assert grp["p95_seconds"] == pytest.approx(2.9)
    # Variation = (max - min) / median = (3.0 - 1.0) / 2.0 = 1.0
    assert grp["variation"] == pytest.approx(1.0)


def test_unwrap_comparison_report(synthetic_tree: Path) -> None:
    """Assert wrapped comparison-report is unwrapped and properly parsed."""
    manifest = evidence_mod.build_evidence_manifest(synthetic_tree)
    comp_reports = manifest["summaries"]["comparison-report"]
    assert len(comp_reports) == 1

    report = comp_reports[0]
    assert report["case_id"] == "completion-monitor-p1"
    assert report["path"] == "phase4_monitor_overhead/mac/p1.json"

    # Verify rows
    rows = report["rows"]
    assert len(rows) == 2
    assert rows[0]["variant_id"] == "all-off"
    assert rows[0]["median_wall_seconds"] == pytest.approx(0.10)
    assert rows[1]["variant_id"] == "collector"
    assert rows[1]["median_wall_seconds"] == pytest.approx(0.12)

    # Verify comparisons
    comps = report["comparisons"]
    assert len(comps) == 1
    assert comps[0]["variant"] == "collector"
    assert comps[0]["ratio_median"] == pytest.approx(1.20)
    assert comps[0]["difference_ci95"] == [0.015, 0.025]


def test_sha256_correctness_and_unparsed(synthetic_tree: Path) -> None:
    """Assert sha256 of all files matches disk contents and unparsed is recorded."""
    manifest = evidence_mod.build_evidence_manifest(synthetic_tree)
    files = manifest["files"]
    assert len(files) == 9  # 3 runs + 1 model + 1 eta + 1 probe + 1 p1 + 1 broken + 1 tar.gz

    # Check sha256 and size for every single file
    for file_record in files:
        full_path = synthetic_tree / file_record["path"]
        assert full_path.exists()
        assert full_path.is_file()

        actual_sha256 = hashlib.sha256(full_path.read_bytes()).hexdigest()
        assert file_record["sha256"] == actual_sha256
        assert file_record["size"] == full_path.stat().st_size

    # Check unparsed files list
    unparsed = manifest["unparsed"]
    assert unparsed == ["phase4_foundation_validation/broken.json"]

    # Check tar.gz inclusion
    tar_records = [f for f in files if f["path"].endswith(".tar.gz")]
    assert len(tar_records) == 1
    assert tar_records[0]["kind"] == "other"


def test_deterministic_bundle_hash_across_runs(synthetic_tree: Path) -> None:
    """Assert bundle_sha256 is deterministic across multiple independent runs."""
    manifest1 = evidence_mod.build_evidence_manifest(synthetic_tree)
    manifest2 = evidence_mod.build_evidence_manifest(synthetic_tree)

    assert manifest1["bundle_sha256"] == manifest2["bundle_sha256"]
    assert manifest1["files"] == manifest2["files"]
    assert manifest1["unparsed"] == manifest2["unparsed"]
    assert manifest1["summaries"] == manifest2["summaries"]


def test_overwrite_refusal(synthetic_tree: Path, tmp_path: Path) -> None:
    """Assert refusal to overwrite existing outputs unless --overwrite is passed."""
    out_file = tmp_path / "out_manifest.json"
    md_file = tmp_path / "out_report.md"

    # First run without pre-existing files succeeds
    evidence_mod.main(
        [
            "--root",
            str(synthetic_tree),
            "--output",
            str(out_file),
            "--markdown",
            str(md_file),
        ]
    )
    assert out_file.exists()
    assert md_file.exists()

    # Second run without --overwrite must exit with code 1
    with pytest.raises(SystemExit) as exc:
        evidence_mod.main(
            [
                "--root",
                str(synthetic_tree),
                "--output",
                str(out_file),
                "--markdown",
                str(md_file),
            ]
        )
    assert exc.value.code == 1

    # Third run with --overwrite succeeds
    evidence_mod.main(
        [
            "--root",
            str(synthetic_tree),
            "--output",
            str(out_file),
            "--markdown",
            str(md_file),
            "--overwrite",
        ]
    )
    assert out_file.exists()
    assert md_file.exists()


def test_file_limit_refusal(
    synthetic_tree: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Assert refusal (exit 2) if scanned file count exceeds MAX_FILES."""
    out_file = tmp_path / "out_manifest.json"
    md_file = tmp_path / "out_report.md"

    # Monkeypatch the module limit constant to a low threshold
    monkeypatch.setattr(evidence_mod, "MAX_FILES", 3)

    with pytest.raises(SystemExit) as exc:
        evidence_mod.main(
            [
                "--root",
                str(synthetic_tree),
                "--output",
                str(out_file),
                "--markdown",
                str(md_file),
                "--overwrite",
            ]
        )
    assert exc.value.code == 2


def test_other_summaries_and_markdown(synthetic_tree: Path) -> None:
    """Assert extraction of time-model, eta-accuracy, hardware-profile and markdown rendering."""
    manifest = evidence_mod.build_evidence_manifest(synthetic_tree)
    summaries = manifest["summaries"]

    # Time model
    tm = summaries["time-model"][0]
    assert tm["model_id"] == "time-model-test-123"
    assert tm["calibration_run_ids_count"] == 4
    assert tm["excluded_unstable_trial_count"] == 2

    # ETA accuracy
    eta = summaries["runtime-eta-accuracy"][0]
    assert eta["check_count"] == 40
    assert eta["predicted_count"] == 40
    assert eta["median_abs_relative_error"] == pytest.approx(0.05)
    assert eta["p95_abs_relative_error"] == pytest.approx(0.11)
    assert eta["worst_abs_relative_error"] == pytest.approx(0.28)
    assert eta["interval_coverage"] == pytest.approx(1.0)

    # Hardware profile
    prof = summaries["hardware-profile"][0]
    assert prof["profile_id"] == "hw-prof-abc"
    assert prof["configuration_fingerprint"] == "cfg-fp-xyz"

    # Markdown rendering
    md_text = evidence_mod.generate_markdown(manifest)
    assert manifest["bundle_sha256"] in md_text
    assert "Benchmark Execution Groups" in md_text
    assert "Runtime ETA Accuracy" in md_text
    assert "Time Models" in md_text
    assert "Comparison Reports & Monitor Overhead" in md_text
    assert "Hardware Profiles" in md_text
    assert "time-model-test-123"[:12] in md_text
    assert "completion-monitor-p1" in md_text
    assert "hw-prof-abc" in md_text
