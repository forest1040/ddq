import time
from concurrent.futures import CancelledError
import pytest
from svsim.async_io import WorkerIOEngine

from svsim.calibration_conditions import condition_key, kernel_condition
from svsim.profile_document import ProfileDocument
from svsim.runtime_measurement import RuntimeMeasurementCollector
from svsim.runtime_profile_adapter import RuntimeProfileAdapter


def _row(condition, provenance=None):
    return {
        "operation": "h", "condition": condition, "condition_key": condition_key(condition),
        "evidence": "measured", "scope": "same-host", "quality": "validated", "units": "calls",
        "summary": {"median_seconds": 1.0, "mean_seconds": 1.0, "min_seconds": 1.0,
                    "max_seconds": 1.0, "p95_seconds": 1.0},
        "trials": [{"wall_seconds": 1.0, "cpu_seconds": 0.9, "bytes": 0, "count": 1,
                    "timestamp": "2026-01-01T00:00:00Z"}],
        "timestamp": "2026-01-01T00:00:00Z", "warmup_count": 1, "sample_count": 1,
        "provenance": {"source": "test", "execution_context": {}, **(provenance or {})},
    }


def test_completion_validates_after_callback_and_excludes_error_or_warmup():
    collector = RuntimeMeasurementCollector()
    base = {"context": {"build": "b"}, "condition": {"operation": "kernel"},
            "service_seconds": 1.0}
    assert collector.complete("ok", validator=lambda: True, **base)
    assert not collector.complete("bad-validator", validator=lambda: (_ for _ in ()).throw(ValueError()), **base)
    assert not collector.complete("failed", success=False, validated=True, **base)
    assert collector.complete("warm", warmup=True, validated=True, **base)
    assert len(collector.snapshot()["observations"]) == 2  # warmups remain diagnostic
    assert len(collector.snapshot()["failed_observations"]) == 2
    assert len(collector.cost_observations()) == 1
    assert sum(not row["validated"] for row in collector.snapshot()["failed_observations"]) == 2
    assert collector.snapshot()["failures"] == 2


def test_duplicate_events_and_bounds_are_reported():
    collector = RuntimeMeasurementCollector(max_conditions=1, per_condition=1, max_bytes=4096)
    def submit(event, condition):
        return collector.submit(event, context={}, condition={"case": condition}, validated=True)
    assert submit("one", 1)
    assert not submit("one", 1)
    assert submit("two", 1)  # rolling ring replaces old
    assert not submit("three", 2)
    snapshot = collector.snapshot()
    assert [row["event_id"] for row in snapshot["observations"]] == ["two"]
    assert snapshot["duplicate_events"] == 1
    assert snapshot["dropped_conditions"] == 1


def test_profile_adapter_requires_full_identity_scope_and_explicit_envelope():
    condition = kernel_condition("h", 2)
    profile = ProfileDocument.create({}, {}, {}, [_row(condition)])
    adapter = RuntimeProfileAdapter(profile)
    context = {"configuration_fingerprint": profile.configuration_fingerprint}
    assert adapter.match(context, condition)
    assert adapter.match({"configuration_fingerprint": "wrong"}, condition) is None
    assert adapter.match({**context, "build": "unmatched"}, condition) is None
    assert adapter.match(context, condition, scope="cross-host") is None
    # Raw min/max and validated correctness do not create ETA confidence bounds.
    assert adapter.cost_sample(context, condition) is None

    injected = _row(condition, {"injected": True, "source": "runtime-calibration"})
    profile = ProfileDocument.create({}, {}, {}, [injected])
    assert RuntimeProfileAdapter(profile).match(
        {"configuration_fingerprint": profile.configuration_fingerprint}, condition) is None

    base = ProfileDocument.create({}, {}, {})
    provenance = {"source": "test", "certified_envelope": {
        "lower_seconds": 0.8, "expected_seconds": 1.0, "upper_seconds": 1.2,
        "method": "held-out-quantile", "held_out_coverage": 0.95,
        "interval_kind": "conditional-prediction", "validation": "held-out",
        "condition_key": condition_key(condition),
        "configuration_fingerprint": base.configuration_fingerprint,
        "scope": "same-host", "units": "seconds/call", "held_out_count": 20,
        "held_out_success_count": 19}}
    profile = ProfileDocument.create({}, {}, {}, [_row(condition, provenance)])
    sample = RuntimeProfileAdapter(profile).cost_sample(
        {"configuration_fingerprint": profile.configuration_fingerprint}, condition)
    assert sample.lower_seconds == 0.8 and sample.upper_seconds == 1.2

    # A provenance flag alone cannot turn extrema into a usable envelope.
    forged = {"source": "test", "execution_context": {}, "certified_envelope": {"certified": True}}
    rejected = ProfileDocument.create({}, {}, {}, [_row(condition, forged)])
    assert RuntimeProfileAdapter(rejected).cost_sample(
        {"configuration_fingerprint": rejected.configuration_fingerprint}, condition) is None

    weak_provenance = {"execution_context": {}, "certified_envelope": {
        "lower_seconds": 0.8, "expected_seconds": 1.0, "upper_seconds": 1.2,
        "method": "held-out-quantile", "held_out_coverage": 1.0,
        "interval_kind": "conditional-prediction", "validation": "held-out",
        "condition_key": condition_key(condition),
        "configuration_fingerprint": base.configuration_fingerprint,
        "scope": "same-host", "units": "seconds/call", "held_out_count": 4,
        "held_out_success_count": 4}}
    weak = ProfileDocument.create({}, {}, {}, [_row(condition, weak_provenance)])
    assert RuntimeProfileAdapter(weak).cost_sample(
        {"configuration_fingerprint": weak.configuration_fingerprint}, condition) is None


def test_observation_validation_rejects_bad_numbers_and_context_mutation():
    collector = RuntimeMeasurementCollector()
    with pytest.raises(ValueError):
        collector.submit("bad", context={}, condition={}, queue_seconds=float("nan"), validated=True)
    context = {"build": "original"}
    collector.submit("ok", context=context, condition={}, validated=True)
    context["build"] = "changed"
    assert collector.snapshot()["observations"][0]["context"]["build"] == "original"


def test_group_children_count_once_and_failed_parent_disables_child_cost():
    collector = RuntimeMeasurementCollector()
    collector.begin_group("epoch-success")
    collector.begin_group("gate-success")
    collector.submit("mpi-child", context={}, condition={"operation": "mpi"}, validated=True)
    collector.end_group("gate-success")
    collector.submit("gate-event", context={}, condition={"operation": "executor-gate"},
                     group_id="gate-success", validated=True)
    collector.end_group("epoch-success")
    collector.submit("epoch-event", context={}, condition={"operation": "executor-epoch"},
                     group_id="epoch-success", validated=True)
    assert collector.consumed_children("gate-success") == {"mpi-child"}
    assert {row.event_id for row in collector.cost_observations()} == {
        "mpi-child", "gate-event", "epoch-event"}

    collector.begin_group("epoch-failed")
    collector.begin_group("gate-failed")
    collector.submit("validated-child", context={}, condition={"operation": "io"}, validated=True)
    collector.end_group("gate-failed")
    collector.submit("failed-gate", context={}, condition={"operation": "executor-gate"},
                     group_id="gate-failed", success=False, validated=False)
    collector.end_group("epoch-failed")
    collector.submit("failed-epoch", context={}, condition={"operation": "executor-epoch"},
                     group_id="epoch-failed", success=False, validated=False)
    assert "validated-child" not in {row.event_id for row in collector.cost_observations()}

    collector.begin_group("epoch-failed-late")
    collector.begin_group("gate-success-late")
    collector.submit("child-of-successful-gate", context={}, condition={"operation": "mpi"}, validated=True)
    collector.end_group("gate-success-late")
    collector.submit("successful-gate-late", context={}, condition={"operation": "executor-gate"},
                     group_id="gate-success-late", validated=True)
    collector.end_group("epoch-failed-late")
    collector.submit("failed-epoch-late", context={}, condition={"operation": "executor-epoch"},
                     group_id="epoch-failed-late", success=False, validated=False)
    assert "child-of-successful-gate" not in {row.event_id for row in collector.cost_observations()}


def test_io_validation_callback_is_in_whole_wall_and_failures_never_become_cost():
    collector = RuntimeMeasurementCollector()
    engine = WorkerIOEngine(engine="cooperative")

    class GoodRead:
        def read_into(self, offset, view):
            view[:] = b"good"
            return len(view)

    request = engine.submit_read(GoodRead(), 0, bytearray(4))
    def validate(req):
        time.sleep(0.005)
        assert bytes(req._view) == b"good"
        req.mark_validated()
    request.add_done_callback(validate)
    engine.poll()
    assert request.validated
    assert request.wall_seconds >= request.elapsed_seconds
    assert request.wall_seconds >= 0.004
    assert collector.complete(
        "io-good", context={}, condition={"operation": "read"},
        service_seconds=request.elapsed_seconds, protocol_seconds=request.wall_seconds,
        success=request.error is None, validator=lambda: request.validated)

    class ShortRead:
        def read_into(self, offset, view): return 1

    failed = engine.submit_read(ShortRead(), 0, bytearray(4))
    engine.poll()
    assert failed.error is not None and not failed.validated
    collector.complete("io-short", context={}, condition={"operation": "read"},
                       success=failed.error is None, validator=lambda: failed.validated)

    cancelled = engine.submit_read(GoodRead(), 0, bytearray(4))
    assert engine.cancel(cancelled)
    with pytest.raises(CancelledError):
        engine.wait(cancelled)
    collector.complete("io-cancelled", context={}, condition={"operation": "read"},
                       success=cancelled.error is None,
                       validator=lambda: cancelled.validated)
    assert {row.event_id for row in collector.cost_observations()} == {"io-good"}
    assert {row["event_id"] for row in collector.snapshot()["failed_observations"]} == {
        "io-short", "io-cancelled"}
    engine.close()


def test_rolling_ring_replaces_old_sample_at_its_byte_limit():
    from svsim.runtime_measurement import RuntimeMeasurementCollector,CompletionObservation
    row=CompletionObservation('e0',context={},condition={},validated=True)
    import json
    size=len(json.dumps(row.to_dict(),sort_keys=True,separators=(',',':')).encode())
    collector=RuntimeMeasurementCollector(max_conditions=1,per_condition=1,max_bytes=size)
    assert collector.submit('e0',context={},condition={},validated=True)
    assert collector.submit('e1',context={},condition={},validated=True)
    assert [item.event_id for item in collector.cost_observations()]==['e1']


def test_long_identifiers_are_rejected_before_dedup_storage():
    from svsim.runtime_measurement import RuntimeMeasurementCollector
    collector=RuntimeMeasurementCollector(max_bytes=1024)
    with pytest.raises(ValueError,match='identifier'):
        collector.submit('x'*257,context={},condition={},validated=True)
    assert not collector._events


def test_disabled_collection_skips_validator_and_does_not_create_cost_evidence():
    collector = RuntimeMeasurementCollector(enabled=False, max_bytes=1024)
    def unexpected_validator():
        raise AssertionError("a disabled collector must not evaluate a validator")
    collector.begin_group("epoch")
    assert not collector.complete("child", validator=unexpected_validator, context={}, condition={})
    collector.end_group("epoch")
    assert not collector.submit("epoch-event", context={}, condition={}, group_id="epoch", validated=True)
    assert collector.snapshot()["collection_enabled"] is False
    assert collector.snapshot()["observations"] == []
    assert collector.cost_observations() == ()
    assert collector.consumed_children("epoch") == frozenset()
    assert not collector._events and not collector._group_stack
    with pytest.raises(ValueError, match="boolean"):
        RuntimeMeasurementCollector(enabled=1)
