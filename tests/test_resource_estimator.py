import numpy as np
import pytest

from svsim.cache import BufferLease
from svsim.calibration_conditions import condition_key, kernel_condition
from svsim.profile_document import ProfileDocument
from svsim.resource_estimator import EstimateOptions, estimate
from svsim.workload_spec import WorkloadSpec


def _workload(qubits=4, operations=None, **extra):
    return WorkloadSpec({"kind": "workload", "schema_version": 1, "qubits": qubits,
                         "operations": operations if operations is not None else [{"kind": "H", "targets": [0]}],
                         **extra})


def _profile(calibrations=(), allocation=None, hardware=None):
    return ProfileDocument.create(hardware or {}, {}, allocation or {"ranks": 1, "nodes": 1, "threads": 1},
                                  calibrations)


def _row(condition, seconds=0.25):
    return {"operation": condition["operation"], "condition": condition,
            "condition_key": condition_key(condition), "evidence": "measured", "scope": "same-host",
            "quality": "validated", "units": "calls",
            "summary": {"median_seconds": seconds, "mean_seconds": seconds,
                        "min_seconds": seconds, "max_seconds": seconds, "p95_seconds": seconds},
            "trials": [{"wall_seconds": seconds, "cpu_seconds": seconds, "bytes": 1,
                        "count": 1, "timestamp": "2026-01-01T00:00:00Z"}],
            "timestamp": "2026-01-01T00:00:00Z", "warmup_count": 0,
            "sample_count": 1, "provenance": {"source": "test"}}


def test_huge_repeat_is_bounded_and_never_allocates_simulation_state(monkeypatch):
    spec = _workload(50, [{"kind": "REPEAT", "count": "1000000000000",
                           "body": [{"kind": "H", "targets": [0]}]}])

    def forbidden(*args, **kwargs):
        raise AssertionError("static estimate attempted a state-sized allocation")

    monkeypatch.setattr(BufferLease, "__init__", forbidden)
    monkeypatch.setattr(np, "zeros", forbidden)
    monkeypatch.setattr(np, "empty", forbidden)
    report = estimate(spec, _profile(), EstimateOptions(block_bits=8))
    assert int(report["resources"]["state_payload_bytes"]) == 16 * (1 << 50)
    assert report["work"]["semantic_gate_count"] == "1000000000000"
    assert report["analysis"]["allocated_amplitudes"] is False
    assert report["analysis"]["enumerated_blocks"] is False
    assert report["analysis"]["expanded_repetitions"] is False


def test_capacity_separates_known_failure_success_and_unknown():
    spec = _workload()
    unknown = estimate(spec, _profile(), EstimateOptions(block_bits=2))
    assert unknown["capacity_feasible"] is None
    assert unknown["capacity_unknown"]
    assert unknown["capacity_reasons"] == []

    opts = EstimateOptions(block_bits=2, ram_budget_bytes_per_rank=1,
                           ram_budget_bytes_per_host=1, disk_budget_bytes_per_host=1)
    failed = estimate(spec, _profile(), opts)
    assert failed["capacity_feasible"] is False
    assert {"RANK_RAM_BUDGET_EXCEEDED", "HOST_RAM_BUDGET_EXCEEDED", "HOST_DISK_BUDGET_EXCEEDED"} <= set(failed["capacity_reasons"])

    enough = EstimateOptions(block_bits=2, ram_budget_bytes_per_rank=1 << 30,
                             ram_budget_bytes_per_host=1 << 32, disk_budget_bytes_per_host=1 << 30)
    assert estimate(spec, _profile(), enough)["capacity_feasible"] is True


def test_ownership_requires_rank_count_to_be_power_of_two_and_host_sums_local_ranks():
    with pytest.raises(ValueError, match="power of two"):
        estimate(_workload(5), _profile(), EstimateOptions(block_bits=2, ranks=3))
    report = estimate(_workload(6), _profile(allocation={"ranks": 4, "nodes": 2, "threads": 1}),
                      EstimateOptions(block_bits=2, ranks=4, nodes=2,
                                      ram_budget_bytes_per_rank=1 << 30,
                                      ram_budget_bytes_per_host=1 << 32,
                                      disk_budget_bytes_per_host=1 << 30))
    assert report["resources"]["ranks_per_host"] == 2
    assert int(report["resources"]["host_ram_required_bytes"]) == 2 * int(report["resources"]["rss_reference_bytes_per_rank"]) + 512 * 1024**2


def test_measured_kernel_is_component_evidence_but_never_end_to_end_time():
    condition = kernel_condition("kernel-h", 2, target=0)
    profile = _profile([_row(condition)])
    report = estimate(_workload(), profile, EstimateOptions(block_bits=2))
    assert report["calibration_coverage"]["status"] == "partial"
    assert report["calibration_coverage"]["calibrated_calls"] == "4"
    assert report["time"]["reference_kernel_seconds"] == 1.0
    assert report["time"]["lower_seconds"] is None
    assert report["time"]["expected_seconds"] is None
    assert report["time"]["conditional_upper_seconds"] is None
    assert "compile-and-bind" in report["calibration_coverage"]["missing_components"]


def test_unmatched_kernel_is_not_reported_as_zero_time_and_file_needs_disk_budget():
    spec = _workload(operations=[{"kind": "RX", "targets": [0], "angle": .4}],
                     policies={"storage": "file"})
    report = estimate(spec, _profile(), EstimateOptions(block_bits=2,
                                                         ram_budget_bytes_per_rank=1 << 30,
                                                         ram_budget_bytes_per_host=1 << 32))
    assert report["time"]["reference_kernel_seconds"] is None
    assert report["time"]["expected_seconds"] is None
    assert report["calibration_coverage"]["status"] == "unknown"
    assert report["capacity_feasible"] is None
    assert "disk_budget_bytes_per_host_for_catalog_and_payloads" in report["capacity_unknown"]
    assert int(report["resources"]["append_only_amplitude_peak_bytes_per_rank"]) > 0


def test_append_only_disk_estimate_grows_with_operation_count():
    options = EstimateOptions(block_bits=2, disk_budget_bytes_per_host=1 << 40)
    profile = _profile()
    one = _workload(4, [{"kind": "H", "targets": [0]}], policies={"storage": "file"})
    two = _workload(4, [{"kind": "H", "targets": [0]}, {"kind": "H", "targets": [1]}],
                    policies={"storage": "file"})
    assert int(estimate(two, profile, options)["resources"]["append_only_amplitude_peak_bytes_per_rank"]) > int(estimate(one, profile, options)["resources"]["append_only_amplitude_peak_bytes_per_rank"])


def test_unsupported_qpe_remains_explicit():
    spec = _workload(algorithm={"kind": "ipea-trotter"})
    report = estimate(spec, _profile(), EstimateOptions(block_bits=2))
    assert report["execution_supported"] is False
    assert "ALGORITHM_ENDPOINT_NOT_IMPLEMENTED:ipea-trotter" in report["unsupported_capabilities"]


def test_profile_fixture_requires_raw_sample_summary_consistency():
    condition = kernel_condition("kernel-h", 2)
    row = _row(condition, .25)
    row["summary"]["median_seconds"] = .5
    with pytest.raises(ValueError, match="summary median_seconds inconsistent"):
        _profile([row])


def test_small_real_store_payload_and_managed_peaks_fit_static_reference(tmp_path):
    from svsim.storage import StorageConfig
    from svsim.stored_runtime import StoredSimulator
    from svsim.kernels import eager_reference
    operations = [{'kind':'H','targets':[0]}, {'kind':'RX','targets':[0],'angle':.37}]
    spec = _workload(5, operations, policies={'storage':'file'},initial_state='seeded-random',seed=7)
    options = EstimateOptions(block_bits=3,ram_budget_bytes_per_rank=1<<24,
                              ram_budget_bytes_per_host=1<<30,disk_budget_bytes_per_host=1<<28)
    report = estimate(spec,_profile(),options)
    rng = np.random.default_rng(7)
    initial = rng.normal(size=32)+1j*rng.normal(size=32)
    initial /= np.linalg.norm(initial)
    config = StorageConfig(str(tmp_path/'state'),3,ram_budget_bytes=1<<24,metadata_reserve_bytes=128*1024)
    sim = StoredSimulator(5,config,initial_state=initial,frame_policy='materialize',mode='eager')
    try:
        for entry in spec.gates:
            sim.append_gate(entry.gate)
            sim.materialize()
        np.testing.assert_allclose(sim.state(),eager_reference([g.gate for g in spec.gates],5,None,initial_state=initial),atol=1e-12)
        sim.cache.flush()
        sim.store.flush()
        payload_disk = sum(path.stat().st_size for path in (tmp_path/'state'/'state').glob('segment-*.bin'))
        assert payload_disk <= int(report['resources']['append_only_amplitude_peak_bytes_per_rank'])
        assert sim.budget.peak <= int(report['resources']['managed_peak_bytes_per_rank'])
        assert sim.store.payload_bytes == int(report['resources']['block_payload_bytes'])
        assert sim.store.slot_bytes == int(report['resources']['block_slot_bytes'])
    finally:
        sim.close()


def test_physical_ram_is_only_a_ceiling_and_unsupported_lowering_cannot_have_full_coverage():
    options = EstimateOptions(block_bits=2,ram_budget_bytes_per_rank=1<<30,
                              ram_budget_bytes_per_host=1<<40,disk_budget_bytes_per_host=1<<30)
    report = estimate(_workload(),_profile(hardware={'ram_total_bytes':1<<29}),options)
    assert report['capacity_feasible'] is False
    assert report['resources']['ram_limit_bytes_per_host'] == str(1<<29)
    unknown = estimate(_workload(),_profile(hardware={'ram_total_bytes':1<<29}),EstimateOptions(block_bits=2))
    assert unknown['capacity_feasible'] is None
    spec = _workload(operations=[{'kind':'RZ','targets':[0],'angle':.1}],
                     policies={'execution':'forced-decomposition','basis':'clifford-t'})
    rejected = estimate(spec,_profile(),options)
    assert rejected['execution_supported'] is False
    assert rejected['calibration_coverage']['kernel_call_fraction'] is None
    assert rejected['capacity_feasible'] is None
