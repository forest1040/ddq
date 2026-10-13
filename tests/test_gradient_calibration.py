from contextlib import contextmanager
import time

import pytest

from svsim.gradient_calibration import (
    GradientCalibrationSpec,
    calibrate_gradient_methods,
    context_fingerprint,
)


def test_calibration_runs_fresh_whole_methods_and_separates_warmup():
    context = {"source": "s", "binding": "b", "observable": "o", "n": 4, "P": 1}
    made = []

    @contextmanager
    def factory(method, repetition, scratch):
        trial = {"method": method, "rep": repetition}
        made.append(trial)
        yield trial

    report = calibrate_gradient_methods(
        GradientCalibrationSpec(context, methods=("adjoint",), warmup_repetitions=1, repetitions=2),
        factory,
        lambda trial, method: (trial["rep"], method),
        lambda trial, method, result: result == (trial["rep"], method),
    )
    assert len({id(item) for item in made}) == 3
    assert [row.warmup for row in report.observations] == [True, False, False]
    assert all(row.success and row.validated for row in report.observations)
    assert report.diagnostic["adjoint"]["successful_repetitions"] == 2
    assert report.certified_envelopes == {}
    assert report.context_hash == context_fingerprint(context)
    assert report.to_dict()["observations"][0]["runner_seconds"] >= 0


def test_calibration_rejects_reused_trials_and_records_oracle_failure():
    context = {"exact": [1, 2]}
    shared = object()
    spec = GradientCalibrationSpec(context, methods=("recompute",), warmup_repetitions=0,
                                   repetitions=2)
    reused = calibrate_gradient_methods(spec, lambda *_: shared, lambda *_: None, lambda *_: True)
    assert reused.observations[0].success
    assert not reused.observations[1].success
    assert "fresh" in reused.observations[1].error
    report = calibrate_gradient_methods(
        GradientCalibrationSpec(context, methods=("recompute",), warmup_repetitions=0, repetitions=1),
        lambda *_: object(), lambda *_: {"gradient": 99}, lambda *_: False,
    )
    assert not report.observations[0].success
    assert "validation failed" in report.observations[0].error
    assert report.diagnostic["recompute"]["successful_repetitions"] == 0


def test_explicit_certifier_must_reference_five_held_out_raw_observations():
    context = {"source": "unique"}
    spec = GradientCalibrationSpec(context, methods=("parameter-shift",), warmup_repetitions=0,
                                   repetitions=5)
    report = calibrate_gradient_methods(
        spec, lambda *_: object(), lambda *_: 2.0, lambda *_: True,
        actual_context_getter=lambda *_: context,
        envelope_certifier=lambda method, rows, key: {
            "lower_seconds": 0.0, "expected_seconds": 2.0, "upper_seconds": 3.0,
            "confidence": 0.95, "held_out_raw_refs": [row.raw_ref for row in rows],
            "method": "predeclared-heldout-validation-v1", "validation": "held-out",
            "model_id": "heldout-coverage-v1", "training_raw_refs": ["external-training-case-01"],
            "evidence": "measured", "scope": "same-host", "units": "seconds/gradient",
            "ram_bytes": 8192, "disk_bytes": 0,
            "measurement": "runner", "held_out_count": len(rows),
            "held_out_success_count": len(rows),
            "held_out_coverage": sum(0 <= row.runner_seconds <= 3 for row in rows) / len(rows),
        },
    )
    assert "parameter-shift" in report.certified_envelopes
    assert report.certified_envelopes["parameter-shift"]["held_out_count"] == 5
    assert report.to_dict()["context_hash"] == context_fingerprint(context)


def test_limits_and_actual_context_mismatch_prevent_certification():
    context = {"source": "s"}
    with pytest.raises(ValueError, match="wall_limit"):
        GradientCalibrationSpec(context, wall_limit_seconds=True)
    with pytest.raises(ValueError, match="bounded"):
        GradientCalibrationSpec(context, repetitions=101)
    with pytest.raises(ValueError, match="size limit"):
        GradientCalibrationSpec({"source": "x" * (256 * 1024)})
    with pytest.raises(TypeError, match="sequence"):
        GradientCalibrationSpec(context, methods="adjoint")
    report = calibrate_gradient_methods(
        GradientCalibrationSpec(context, methods=("adjoint",), warmup_repetitions=0, repetitions=5),
        lambda *_: object(), lambda *_: 1.0, lambda *_: True,
        actual_context_getter=lambda *_: {"source": "different"},
        envelope_certifier=lambda *_: {"lower_seconds": 1, "expected_seconds": 1,
                                      "upper_seconds": 1, "confidence": 1,
                                      "held_out_raw_refs": [], "training_raw_refs": ["prior"],
                                      "method": "v1", "model_id": "m", "validation": "held-out",
                                      "evidence": "measured", "scope": "same-host",
                                      "units": "seconds/gradient", "ram_bytes": 1, "disk_bytes": 0,
                                      "measurement": "runner", "held_out_count": 0,
                                      "held_out_success_count": 0, "held_out_coverage": 0},
    )
    assert report.certified_envelopes == {}
    assert all(not row.success for row in report.observations)


def test_context_is_frozen_and_factory_or_cleanup_failures_are_recorded():
    context = {"source": [1, 2]}
    spec = GradientCalibrationSpec(context, methods=["adjoint"], warmup_repetitions=0, repetitions=1)
    context["source"].append(3)
    assert spec.context["source"] == (1, 2)
    with pytest.raises(TypeError):
        spec.context |= {"extra": True}

    def factory(method, repetition, scratch):
        if repetition == 0:
            raise RuntimeError("factory failure")
        class FailingCleanup:
            def __enter__(self):
                return object()
            def __exit__(self, *args):
                raise RuntimeError("cleanup failure")
        return FailingCleanup()

    report = calibrate_gradient_methods(
        GradientCalibrationSpec({"source": "s"}, methods=("adjoint",), warmup_repetitions=1,
                                repetitions=1), factory, lambda *_: 1, lambda *_: True,
    )
    assert not report.observations[0].success and "factory failure" in report.observations[0].error
    assert not report.observations[1].success and "cleanup failure" in report.observations[1].error


def test_actual_context_mutation_and_resource_overage_stop_trials():
    context = {"source": "s"}
    made = []
    def factory(*_):
        trial = {"context": {"source": "s"}}
        made.append(trial)
        return trial
    def runner(trial, method):
        trial["context"]["source"] = "changed"
        return 1
    changed = calibrate_gradient_methods(
        GradientCalibrationSpec(context, methods=("adjoint",), warmup_repetitions=0, repetitions=1),
        factory, runner, lambda *_: True,
        actual_context_getter=lambda trial, _: trial["context"],
    )
    assert not changed.observations[0].success
    assert "context" in changed.observations[0].error

    calls = []
    limited = calibrate_gradient_methods(
        GradientCalibrationSpec(context, methods=("adjoint", "recompute"), warmup_repetitions=0,
                                repetitions=3, ram_limit_bytes=10),
        lambda *_: object(), lambda *_: calls.append(1), lambda *_: True,
        resource_probe=lambda *_: {"peak_ram_bytes": 11, "peak_disk_bytes": 0},
    )
    assert len(calls) == 1
    assert limited.diagnostic["over_limit"]
    assert not limited.observations[0].success

    negative = calibrate_gradient_methods(
        GradientCalibrationSpec(context, methods=("adjoint",), warmup_repetitions=0, repetitions=1),
        lambda *_: object(), lambda *_: 1, lambda *_: True,
        resource_probe=lambda *_: {"peak_ram_bytes": 1, "peak_disk_bytes": -7},
    )
    assert not negative.observations[0].success
    assert negative.observations[0].peak_disk_bytes == -7


def test_certificate_rejects_false_coverage_injected_context_and_bad_clock():
    spec = GradientCalibrationSpec({"evidence": "manual"}, methods=("adjoint",),
                                   warmup_repetitions=0, repetitions=5)
    certificate = lambda method, rows, key: {
        "lower_seconds": 100, "expected_seconds": 100, "upper_seconds": 100,
        "confidence": .95, "held_out_raw_refs": [row.raw_ref for row in rows],
        "training_raw_refs": ["independent"], "model_id": "model", "method": "interval-v1",
        "validation": "held-out", "evidence": "measured", "scope": "same-host",
        "units": "seconds/gradient", "ram_bytes": 10, "disk_bytes": 0,
        "measurement": "runner", "held_out_count": len(rows),
        "held_out_success_count": len(rows), "held_out_coverage": 1.0,
    }
    injected = calibrate_gradient_methods(
        spec, lambda *_: object(), lambda *_: 1, lambda *_: True,
        actual_context_getter=lambda *_: spec.context, envelope_certifier=certificate,
    )
    assert injected.certified_envelopes == {}
    clean_context = {"source": "s"}
    false_coverage = calibrate_gradient_methods(
        GradientCalibrationSpec(clean_context, methods=("adjoint",), warmup_repetitions=0,
                                repetitions=5),
        lambda *_: object(), lambda *_: 1, lambda *_: True,
        actual_context_getter=lambda *_: clean_context,
        envelope_certifier=lambda method, rows, key: {**certificate(method, rows, key),
                                                       "held_out_raw_refs": [row.raw_ref for row in rows],
                                                       "held_out_coverage": 1.0},
    )
    assert false_coverage.certified_envelopes == {}
    with pytest.raises(ValueError, match="backwards"):
        values = iter((1.0, 0.5))
        calibrate_gradient_methods(
            GradientCalibrationSpec({"source": "s"}, methods=("adjoint",), warmup_repetitions=0,
                                    repetitions=1), lambda *_: object(), lambda *_: 1,
            lambda *_: True, clock=lambda: next(values),
        )


def test_missing_certified_resource_bound_prevents_envelope():
    context = {"source": "s"}
    def cert(method, rows, key):
        times = [row.runner_seconds for row in rows]
        lower, upper = min(times), max(times) + 1
        return {"lower_seconds": lower, "expected_seconds": (lower + upper) / 2,
                "upper_seconds": upper, "confidence": .95,
                "held_out_raw_refs": [row.raw_ref for row in rows],
                "training_raw_refs": ["independent"], "model_id": "model",
                "method": "coverage-v1", "validation": "held-out", "evidence": "measured",
                "scope": "same-host", "units": "seconds/gradient", "measurement": "runner",
                "held_out_count": len(rows), "held_out_success_count": len(rows),
                "held_out_coverage": 1.0}
    report = calibrate_gradient_methods(
        GradientCalibrationSpec(context, methods=("adjoint",), warmup_repetitions=0, repetitions=5),
        lambda *_: object(), lambda *_: 1, lambda *_: True,
        actual_context_getter=lambda *_: context, envelope_certifier=cert,
    )
    assert report.certified_envelopes == {}


def test_whole_trial_wall_includes_context_cleanup_and_wall_limit_is_soft():
    class SlowCleanup:
        def __enter__(self):
            return object()
        def __exit__(self, *args):
            time.sleep(.005)
    report = calibrate_gradient_methods(
        GradientCalibrationSpec({"source": "s"}, methods=("adjoint",), warmup_repetitions=0,
                                repetitions=1),
        lambda *_: SlowCleanup(), lambda *_: 1, lambda *_: True,
    )
    row = report.observations[0]
    assert row.wall_seconds >= .004
    assert row.setup_seconds + row.runner_seconds == pytest.approx(row.wall_seconds)

    class StepClock:
        value = 0.0
        def __call__(self):
            self.value += .01
            return self.value
    limited = calibrate_gradient_methods(
        GradientCalibrationSpec({"source": "s"}, methods=("adjoint", "recompute"),
                                warmup_repetitions=0, repetitions=3, wall_limit_seconds=.045),
        lambda *_: object(), lambda *_: 1, lambda *_: True, clock=StepClock(),
    )
    assert len(limited.observations) == 1
    assert limited.diagnostic["over_limit"]
