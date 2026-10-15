"""Validation and bounded failure cases for explicit runtime calibration."""
import pytest

from svsim.runtime_calibration import RuntimeCalibrationSpec, _circuit


def test_calibration_spec_bounds():
    with pytest.raises(ValueError, match="num_qubits"):
        RuntimeCalibrationSpec("/tmp", num_qubits=17)
    with pytest.raises(ValueError, match="repetitions"):
        RuntimeCalibrationSpec("/tmp", repetitions=101)
    with pytest.raises(ValueError, match="warmup"):
        RuntimeCalibrationSpec("/tmp", warmup=0)
    with pytest.raises(ValueError, match="max_io_requests"):
        RuntimeCalibrationSpec("/tmp", max_io_requests=3)
    with pytest.raises(ValueError, match="powers of two"):
        RuntimeCalibrationSpec("/tmp", ranks=(3,))


def test_suite_recipes_are_reproducible_and_only_mpi_suites_touch_owner_bit():
    spec = RuntimeCalibrationSpec("/tmp", num_qubits=6, block_bits=2, ranks=(2,))
    local, _ = _circuit(spec, "kernel-storage", 2)
    remote, _ = _circuit(spec, "storage-mpi", 2)
    assert local == _circuit(spec, "kernel-storage", 2)[0]
    assert all(target[0] < spec.block_bits for _, target, _ in local)
    assert any(target == (5,) for _, target, _ in remote)


def test_calibration_rejects_ram_budget_before_mpi_import(tmp_path):
    from svsim.runtime_calibration import calibrate_runtime

    spec = RuntimeCalibrationSpec(str(tmp_path), num_qubits=4, block_bits=2,
                                  ram_budget_bytes=1, disk_budget_bytes=1024)
    with pytest.raises(MemoryError, match="estimated working set"):
        calibrate_runtime(spec, comm=object())


def test_runtime_flag_uses_common_codec_without_boolean_count(tmp_path):
    import json
    from svsim.runtime_calibration import _observation_dict
    from svsim.profile_document import write_document
    row=_observation_dict({'event_id':'a','warmup':True,'validated':True})
    assert row['is_warmup'] is True and 'warmup' not in row
    path=tmp_path/'report.json'
    write_document(path,{'kind':'runtime-calibration','spec':{'warmup':1},'observations':[row]})
    assert json.loads(path.read_text())['observations'][0]['is_warmup'] is True


def test_held_out_envelope_certifies_only_with_enough_covered_trials():
    from svsim.runtime_calibration import _held_out_envelope
    envelope, reason = _held_out_envelope([1.0, 1.2, 0.9, 1.1, 1.3, 1.0, 1.1, 1.2, 0.95, 1.05], 'fp', 2)
    assert reason == 'CERTIFIED' and envelope['held_out_count'] == 5 and envelope['held_out_coverage'] == 1.0
    assert (envelope['lower_seconds'], envelope['upper_seconds']) == pytest.approx((0.81, 1.43)) and envelope['validation'] == 'held-out'
    assert _held_out_envelope([1.0] * 9, 'fp', 1) == (None, 'INSUFFICIENT_TRIALS:9<10')
    envelope, reason = _held_out_envelope([1.0] * 5 + [2.0] * 5, 'fp', 1)
    assert envelope is None and reason == 'LOW_HELD_OUT_COVERAGE:0.00'


def test_split_observations_moves_rows_to_a_hashed_sidecar(tmp_path):
    import hashlib, json
    from svsim.runtime_calibration import split_observations
    report = {"trials": [{"collector": {"observations": [{"stage": "a"}, {"stage": "b"}], "failed_observations": [{"stage": "c"}]}},
                         {"status": "not-applicable-single-rank"}]}
    split_observations(report, tmp_path / "r.observations.jsonl")
    payload = (tmp_path / "r.observations.jsonl").read_bytes()
    collector = report["trials"][0]["collector"]
    assert "observations" not in collector and collector["observations_count"] == 2 and collector["failed_observations_count"] == 1
    assert collector["observations_line_range"] == [0, 3]
    assert report["observations_file"] == {"name": "r.observations.jsonl", "lines": 3,
                                           "sha256": hashlib.sha256(payload).hexdigest()}
    assert [json.loads(line)["row"]["stage"] for line in payload.splitlines()] == ["a", "b", "c"]
