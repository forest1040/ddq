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
