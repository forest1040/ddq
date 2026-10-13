import json
import hashlib

from svsim.gradient_calibration import GradientCalibrationSpec, calibrate_gradient_methods
from svsim.gradient_cost_adapter import GradientCostAdapter


def _report(context, measurement="runner", peaks=(2048, 10)):
    return calibrate_gradient_methods(
        GradientCalibrationSpec(context, methods=("adjoint",), warmup_repetitions=0, repetitions=5),
        lambda *_: object(), lambda *_: 2.0, lambda *_: True,
        resource_probe=lambda *_: ({"peak_ram_bytes": peaks[0], "peak_disk_bytes": peaks[1]}
                                   if peaks is not None else {}),
        actual_context_getter=lambda *_: context,
        envelope_certifier=lambda method, rows, key: {
            "lower_seconds": 0, "expected_seconds": 2, "upper_seconds": 3,
            "confidence": .95, "held_out_raw_refs": [row.raw_ref for row in rows],
            "training_raw_refs": ["independent-training-set"], "model_id": "coverage-model-v1",
            "method": "held-out-prediction-interval-v1", "validation": "held-out",
            "evidence": "measured", "scope": "same-host", "units": "seconds/gradient",
            "ram_bytes": 2048, "disk_bytes": 10,
            "measurement": measurement, "held_out_count": len(rows),
            "held_out_success_count": len(rows),
            "held_out_coverage": sum(0 <= (row.wall_seconds if measurement == "whole-trial" else row.runner_seconds) <= 3
                                     for row in rows) / len(rows),
        },
    )


def test_adapter_requires_exact_context_and_certified_resources():
    context = {"source": "s", "binding": "b", "layout": [0, 1]}
    report = _report(context)
    adapter = GradientCostAdapter()
    costs = adapter.adapt(report, context)
    assert costs["adjoint"].upper_seconds == 3
    assert costs["adjoint"].ram_bytes == 2048
    assert adapter.last_reason == "CERTIFIED_HELD_OUT"
    assert adapter.adapt(report, {**context, "layout": [1, 0]}) == {}
    assert adapter.last_reason == "CONTEXT_MISMATCH"


def test_adapter_rejects_forged_or_unqualified_report_rows():
    context = {"source": "s"}
    report = _report(context)
    object.__setattr__(report.observations[0], "wall_seconds", 999)
    adapter = GradientCostAdapter()
    assert adapter.adapt(report, context) == {}
    assert adapter.last_reason == "NO_QUALIFIED_HELD_OUT_ENVELOPE"

    assert adapter.adapt(report.to_dict(), context) == {}
    assert adapter.last_reason == "NO_QUALIFIED_HELD_OUT_ENVELOPE"


def test_serialized_measured_report_roundtrips_and_whole_trial_is_not_adopted():
    context = {"source": "s", "binding": "b", "observable": "o"}
    report = _report(context)
    serialized = json.loads(json.dumps(report.to_dict()))
    adapter = GradientCostAdapter()
    assert "adjoint" in adapter.adapt(serialized, context)

    whole_trial = _report(context, measurement="whole-trial")
    assert adapter.adapt(json.loads(json.dumps(whole_trial.to_dict())), context) == {}
    assert adapter.last_reason == "MEASUREMENT_SCOPE_MISMATCH"
    assert adapter.rejected_methods == {"adjoint": "MEASUREMENT_SCOPE_MISMATCH"}


def test_adapter_rechecks_explicit_resource_bounds_even_after_report_rehash():
    context = {"source": "s", "binding": "b"}
    adapter = GradientCostAdapter()
    report = _report(context)
    assert "adjoint" in adapter.adapt(report, context)

    malformed = report.to_dict()
    malformed["certified_envelopes"]["adjoint"]["ram_bytes"] = 2047
    malformed.pop("report_hash")
    malformed["report_hash"] = hashlib.sha256(
        json.dumps(malformed, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    ).hexdigest()
    assert adapter.adapt(malformed, context) == {}
    assert adapter.last_reason == "NO_QUALIFIED_HELD_OUT_ENVELOPE"

    unknown = _report(context, peaks=None)
    assert unknown.certified_envelopes == {}
    assert adapter.adapt(unknown.to_dict(), context) == {}
