import math
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path

import pytest

from svsim.frame_benchmark import FrameCalibration, calibrate_frames

_SPEC = spec_from_file_location("phase2_benchmark", Path(__file__).parents[1] / "examples" / "phase2_benchmark.py")
_BENCHMARK = module_from_spec(_SPEC)
_SPEC.loader.exec_module(_BENCHMARK)
run_phase2_benchmark = _BENCHMARK.run_phase2_benchmark


def test_calibration_reports_positive_finite_rates_for_small_blocks():
    calibration = calibrate_frames(block_bits=2, repetitions=1)
    assert calibration.profile_id.startswith("cpu-")
    assert calibration.version == 1
    assert calibration.block_bits == 2
    for name in ("frame_gate_seconds", "pauli_rotation_seconds", "conjugation_seconds",
                 "synthesis_seconds", "tableau_update_seconds"):
        value = getattr(calibration, name)
        assert math.isfinite(value) and value > 0
    assert calibration.cost("frame_gate", 2) == pytest.approx(2 * calibration.frame_gate_seconds)


@pytest.mark.parametrize("block_bits,repetitions", [(0, 1), (9, 1), (2, 0), (2, 101)])
def test_calibration_bounds_block_size_and_work(block_bits, repetitions):
    with pytest.raises(ValueError):
        calibrate_frames(block_bits=block_bits, repetitions=repetitions)


def test_calibration_rejects_nonfinite_or_nonpositive_measurements():
    with pytest.raises(ValueError):
        FrameCalibration("cpu-test", 1, 2, 0, 1, 1, 1, 1)
    with pytest.raises(ValueError):
        FrameCalibration("cpu-test", 1, 2, 1, float("nan"), 1, 1, 1)


def test_phase2_report_compares_same_state_with_direct_complex_accuracy(tmp_path):
    calibration = calibrate_frames(block_bits=3, repetitions=1)
    trace_directory = tmp_path / "traces"
    report = run_phase2_benchmark(num_qubits=6, calibration=calibration, workdir=tmp_path,
                                  trace_directory=trace_directory)
    assert report["block_bits"] == calibration.block_bits
    assert report["calibration"]["profile_id"] == calibration.profile_id
    assert report["comparison"] == "direct complex amplitude comparison; no phase alignment"
    assert report["seed"] == 20261013
    assert report["state_bytes"] == 16 * (1 << report["num_qubits"])
    assert report["circuit_hash"]
    assert report["initial_state_sha256"]
    assert {case["policy"] for case in report["cases"]} == {
        "materialize", "keep-input-a", "keep-input-b", "auto"
    }
    assert len({case["circuit_hash"] for case in report["cases"]}) == 1
    assert len({case["initial_state_sha256"] for case in report["cases"]}) == 1
    assert len({case["seed"] for case in report["cases"]}) == 1
    assert report["environment"]["numpy_version"]
    assert "process-global" in report["environment"]["process_rss_scope"]
    for case in report["cases"]:
        assert case["bound_profile_id"] == calibration.profile_id
        assert case["max_complex_error"] < 1e-10
        assert case["norm"] == pytest.approx(1.0, abs=1e-11)
        assert case["norm_error"] < 1e-11
        assert case["managed_peak_bytes"] <= report["ram_budget_bytes"]
        assert case["caller_owned_reference_bytes"] == 3 * case["state_bytes"]
        assert case["reference_arrays_outside_managed_budget"]
        assert case["initial_import_seconds"] >= 0
        assert case["compile_seconds"] >= 0
        assert case["eager_reference_seconds"] >= 0
        assert case["export_bytes"] == (1 << report["num_qubits"]) * 16
        assert case["export_readback_seconds"] >= 0
        assert case["import_write_bytes"] > 0
        assert case["snapshot_seconds"] >= 0
        assert case["snapshot_write_bytes"] > 0
        assert case["export_read_bytes"] >= 0
        assert Path(case["trace_path"]).is_file()
    assert len(list(trace_directory.glob("*.jsonl"))) == 4


def test_phase2_report_rejects_calibration_for_different_block_width():
    calibration = calibrate_frames(block_bits=2, repetitions=1)
    with pytest.raises(ValueError, match="block_bits=3"):
        run_phase2_benchmark(calibration=calibration)


def test_sixteen_qubit_case_requires_matching_six_bit_calibration():
    calibration = calibrate_frames(block_bits=3, repetitions=1)
    with pytest.raises(ValueError, match="block_bits=6"):
        run_phase2_benchmark(num_qubits=16, calibration=calibration)
