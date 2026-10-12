import json
import importlib.util
from pathlib import Path

_MODULE_SPEC = importlib.util.spec_from_file_location(
    "phase1_benchmark", Path(__file__).parents[1] / "examples" / "phase1_benchmark.py")
_MODULE = importlib.util.module_from_spec(_MODULE_SPEC)
_MODULE_SPEC.loader.exec_module(_MODULE)
SCHEMA_VERSION = _MODULE.SCHEMA_VERSION
run_comparison = _MODULE.run_comparison


def test_phase1_comparison_report_schema_config_and_error_budgets(tmp_path):
    output = tmp_path / "report.json"
    report = run_comparison(output, num_qubits=6, block_bits=4,
                            ram_budget=512 * 1024, backend="buffered", alignment=64)
    assert json.loads(output.read_text()) == report
    assert report["schema_version"] == SCHEMA_VERSION
    assert report["configuration"] == {
        "num_qubits": 6,
        "block_bits": 4,
        "ram_budget_bytes": 512 * 1024,
        "backend": "buffered",
        "alignment": 64,
        "precision": "complex128",
        "initial_state_seed": 20261012,
        "dense_seeds": {"3": 7003, "4": 7004},
        "error_tolerance": 1e-8,
    }
    assert len(report["scenarios"]) == 10
    assert {row["mode"] for row in report["scenarios"] if row["comparison"] == "named-modes"} == {
        "eager", "batching", "deferred-no-frame", "per-block-frame",
    }
    assert {row["execution"] for row in report["scenarios"] if row["comparison"] == "named-execution"} == {
        "auto", "fallback",
    }
    assert {(row["dense_width"], row["execution"]) for row in report["scenarios"]
            if row["comparison"].startswith("dense-")} == {
        (3, "auto"), (3, "structured-exact"), (4, "auto"), (4, "structured-exact"),
    }
    named = [row for row in report["scenarios"] if row["circuit_kind"] == "named"]
    assert len({row["initial_state_sha256"] for row in named}) == 1
    assert len({row["circuit_hash"] for row in named}) == 1
    for row in report["scenarios"]:
        assert (tmp_path / "report-traces" / f"{row['scenario_id']}.jsonl").is_file()
        assert row["accuracy"]["within_budget"], (row["scenario_id"], row["accuracy"])
        metrics = row["metrics"]
        assert metrics["managed_peak_within_budget"]
        assert metrics["read_bytes"] >= 0 and metrics["write_bytes"] >= 0
        assert metrics["cache_hits"] >= 0 and metrics["cache_misses"] >= 0
        assert metrics["wall_seconds"] >= metrics["cpu_seconds"] * 0
        assert {"initial_import", "snapshot_commit", "streamed_export", "eager_oracle"} <= set(metrics["includes"])
