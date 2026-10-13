import json

import pytest

from svsim.calibration_conditions import condition_key, kernel_condition, storage_condition
from svsim.profile_document import (
    ProfileDocument,
    read_document,
    wire_decode,
    wire_encode,
    write_document,
)


def _calibration(condition=None):
    condition = condition or kernel_condition("h", 3)
    return {
        "operation": "h",
        "condition": condition,
        "condition_key": condition_key(condition),
        "evidence": "measured",
        "scope": "same-host",
        "quality": "validated",
        "units": "calls",
        "summary": {"median_seconds": 1.0, "mean_seconds": 1.0, "min_seconds": 1.0,
                    "max_seconds": 1.0, "p95_seconds": 1.0},
        "trials": [{"wall_seconds": 1.0, "cpu_seconds": 0.9, "bytes": 128, "count": 1,
                    "timestamp": "2026-01-01T00:00:00Z"}],
        "timestamp": "2026-01-01T00:00:00Z",
        "warmup_count": 1,
        "sample_count": 1,
        "provenance": {"source": "test"},
    }


def test_condition_keys_are_stable_and_remove_result_fields():
    a = {"operation": "read", "block_bytes": 4096, "timestamp": "one",
         "result": {"wall_seconds": 1.0, "cpu_seconds": 0.5}}
    b = {"result": {"wall_seconds": 2.0, "cpu_seconds": 1.0}, "block_bytes": 4096,
         "operation": "read", "timestamp": "two"}
    assert condition_key(a) == condition_key(b)
    assert kernel_condition("h", 4)["block_bytes"] == 256
    assert storage_condition("read", 4096, "buffered", 4096, "dev0")["operation"] == "read"


def test_wire_counts_and_bytes_roundtrip_as_decimal_strings():
    value = {"size_bytes": 1 << 60, "ram_budget_bytes_per_rank": 1 << 61,
             "disk_required_bytes_per_host": 1 << 62, "calibrated_calls": 1 << 54,
             "total_calls": 1 << 55, "sample_count": 3, "ratio": 1.25}
    encoded = wire_encode(value)
    assert encoded["size_bytes"] == str(1 << 60)
    assert encoded["ram_budget_bytes_per_rank"] == str(1 << 61)
    assert encoded["disk_required_bytes_per_host"] == str(1 << 62)
    assert encoded["calibrated_calls"] == str(1 << 54)
    assert encoded["total_calls"] == str(1 << 55)
    assert encoded["sample_count"] == "3"
    assert wire_decode(encoded) == value
    assert wire_encode({"ram_available_bytes": None, "sample_count": None,
                        "qubits": 50, "ranks": 4, "threads": 8, "block_bits": 10}) == {
                            "ram_available_bytes": None, "sample_count": None,
                            "qubits": 50, "ranks": 4, "threads": 8, "block_bits": 10}
    with pytest.raises(ValueError):
        wire_encode({"count": -1})
    with pytest.raises(ValueError):
        wire_encode({"count": True})


def test_profile_roundtrip_integrity_and_deepcopy(tmp_path):
    condition = kernel_condition("h", 3)
    profile = ProfileDocument.create({"ram_total_bytes": 1 << 40}, {"python": "3.x"},
                                     {"scope": "local"}, [_calibration(condition)])
    path = tmp_path / "profile.json"
    profile.save(path)
    loaded = ProfileDocument.load(path)
    assert loaded.profile_id == profile.profile_id
    assert loaded.configuration_fingerprint == profile.configuration_fingerprint
    assert loaded.match(condition)["operation"] == "h"
    exposed = loaded.data
    exposed["hardware"]["ram_total_bytes"] = 1
    assert loaded.data["hardware"]["ram_total_bytes"] == 1 << 40
    forged = loaded.to_dict()
    forged["hardware"]["ram_total_bytes"] = str(12)
    with pytest.raises(ValueError, match="fingerprint|content hash"):
        ProfileDocument.from_dict(forged)


def test_configuration_fingerprint_ignores_observations_and_legacy_identity():
    first = ProfileDocument.create({"ram_total_bytes": 100, "ram_available_bytes": 30,
                                    "devices": [{"free_bytes": 12, "capacity_bytes": 20}]},
                                   {"python": "3.x"}, {"scope": "local"},
                                   discovery={"observed_at": "first"})
    second = ProfileDocument.create({"ram_total_bytes": 100, "ram_available_bytes": 1,
                                     "devices": [{"free_bytes": 2, "capacity_bytes": 20}]},
                                    {"python": "3.x"}, {"scope": "local"},
                                    discovery={"observed_at": "second"})
    assert first.configuration_fingerprint == second.configuration_fingerprint


def test_null_memory_fields_survive_profile_save_and_legacy_ids_do_not_fingerprint(tmp_path):
    from svsim.lifecycle import HardwareProfile

    legacy_a = HardwareProfile("legacy-a", 1, "2026-01-01T00:00:00Z", "host", "Darwin", "x",
                               "3.12", 8, "CPU", 100, None)
    legacy_b = HardwareProfile("legacy-b", 1, "2026-01-02T00:00:00Z", "host", "Darwin", "x",
                               "3.12", 8, "CPU", 100, None)
    profile_a = ProfileDocument.from_legacy(legacy_a)
    profile_b = ProfileDocument.from_legacy(legacy_b)
    assert profile_a.configuration_fingerprint == profile_b.configuration_fingerprint
    path = tmp_path / "legacy-profile.json"
    profile_a.save(path)
    loaded = ProfileDocument.load(path)
    assert loaded.data["hardware"]["ram_available_bytes"] is None


def test_match_returns_latest_exact_calibration():
    condition = kernel_condition("h", 3)
    earlier = _calibration(condition)
    later = _calibration(condition)
    earlier["timestamp"] = "2026-01-01T00:00:00Z"
    later["timestamp"] = "2026-01-02T00:00:00Z"
    profile = ProfileDocument.create({}, {}, {}, [earlier, later])
    assert profile.match(condition)["timestamp"] == later["timestamp"]


def test_calibration_summary_must_match_successful_raw_trials():
    row = _calibration()
    row["trials"][0]["wall_seconds"] = None
    with pytest.raises(ValueError, match="wall_seconds"):
        ProfileDocument.create({}, {}, {}, [row])
    row = _calibration()
    row["summary"]["mean_seconds"] = 99
    with pytest.raises(ValueError, match="inconsistent"):
        ProfileDocument.create({}, {}, {}, [row])


def test_profile_match_rejects_untrusted_and_external_records():
    condition = kernel_condition("h", 2)
    row = _calibration(condition)
    row["provenance"] = {"external_backend": True}
    profile = ProfileDocument.create({}, {}, {}, [row])
    assert profile.match(condition) is None
    row = _calibration(condition)
    row["scope"] = "external-backend"
    with pytest.raises(ValueError):
        ProfileDocument.create({}, {}, {}, [row])


def test_legacy_adapter_reports_missing_data():
    from svsim.lifecycle import HardwareProfile

    legacy = HardwareProfile("old", 1, "now", "host", "Darwin", "x", "3.12", 8, "CPU",
                             100, None, metadata={"machine": "arm64"})
    profile = ProfileDocument.from_legacy(legacy)
    assert profile.profile_id
    assert any("allocation" in entry for entry in profile.loss_report)
    assert profile.calibrations == ()


def test_document_io_rejects_duplicate_keys_limits_and_no_overwrite(tmp_path):
    path = tmp_path / "dup.json"
    path.write_text('{"a":1,"a":2}', encoding="utf-8")
    with pytest.raises(ValueError, match="duplicate"):
        read_document(path)
    path.write_text('{"a":1e999}', encoding="utf-8")
    with pytest.raises(ValueError, match="finite"):
        read_document(path)
    with pytest.raises(ValueError, match="limit"):
        read_document(path, max_bytes=3)
    target = tmp_path / "out.json"
    write_document(target, {"schema_version": 1})
    with pytest.raises(FileExistsError):
        write_document(target, {"schema_version": 1})


def test_profile_rejects_unknown_kind_and_boolean_schema_version():
    profile = ProfileDocument.create({}, {}, {})
    unknown = profile.to_dict()
    unknown["kind"] = "future"
    with pytest.raises(ValueError, match="unsupported"):
        ProfileDocument.from_dict(unknown)
    invalid = profile.to_dict()
    invalid["schema_version"] = True
    with pytest.raises(ValueError, match="unsupported"):
        ProfileDocument.from_dict(invalid)
