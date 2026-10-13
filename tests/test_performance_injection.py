import math

import pytest

from svsim.performance_injection import InjectionSpec, PerformanceInjection


def spec(**changes):
    fields = dict(seed=11, resource="storage", trigger="time", start=1.0,
                  end=5.0, delay_seconds=0.2, operations=("read",),
                  max_seconds=1.0, max_events=4)
    fields.update(changes)
    return InjectionSpec(**fields)


def test_time_trigger_uses_elapsed_from_common_run_start_and_ready_poll():
    now = [100.0]
    injection = PerformanceInjection(spec(), clock=lambda: now[0])
    injection.start()
    assert injection.request("read").planned_delay == 0
    now[0] = 101.0
    ticket = injection.request("read", event_id="r1")
    assert ticket is not None
    assert ticket.ready_at > now[0]
    assert injection.progress() == ()
    now[0] = ticket.ready_at
    now[0] = ticket.ready_at + 0.25
    assert injection.progress() == (ticket,)
    assert ticket.state == "ready"
    ticket.complete(service_seconds=0.03)
    assert injection.snapshot()["counters"]["completed"] == 1
    with pytest.raises(ValueError, match="duplicate"):
        injection.finish(ticket)
    evidence = injection.snapshot()
    assert evidence["scope"] == "runtime-calibration"
    assert evidence["evidence"] == "injected"
    assert evidence["planned_injection_seconds"] == pytest.approx(ticket.planned_delay)
    assert evidence["actual_injection_seconds"] == pytest.approx(ticket.planned_delay + 0.25)
    assert evidence["actual_service_seconds"] == pytest.approx(0.03)


def test_work_trigger_activates_at_named_barrier_with_same_seeded_law():
    s = spec(trigger="work", start=1, end=3, barrier="semantic-prefix", max_events=8)
    a, b = PerformanceInjection(s), PerformanceInjection(s)
    for driver in (a, b):
        driver.start(now=10)
        assert not driver.barrier("other-prefix")
        assert driver.request("read", now=10).planned_delay == 0
        assert driver.barrier("semantic-prefix")
    assert a.request("read", work=1, now=10).planned_delay == 0
    assert b.request("read", work=1, now=10).planned_delay == 0
    ta = a.request("read", now=10, event_id="same")
    tb = b.request("read", now=10, event_id="same")
    assert ta.planned_delay == tb.planned_delay
    # The window ends at the same work position regardless of source labels.
    assert a.request("read", work=2, now=10).planned_delay > 0
    assert a.request("read", work=2, now=10).planned_delay == 0


def test_bytes_budget_caps_total_planned_delay_and_nonmatching_is_ignored():
    driver = PerformanceInjection(spec(per_bytes=0.001, max_seconds=0.05))
    driver.start(now=0)
    assert driver.request("write", nbytes=100, now=1) is None
    one = driver.request("read", nbytes=100, now=1)
    two = driver.request("read", nbytes=100, now=1)
    three = driver.request("read", nbytes=100, now=1)
    assert one.planned_delay + two.planned_delay + three.planned_delay == pytest.approx(0.05)
    assert driver.request("read", nbytes=100, now=1) is None
    assert driver.snapshot()["counters"]["ignored"] == 1
    assert driver.snapshot()["counters"]["scheduled"] == 3


def test_cancel_does_not_create_completion_or_actual_delay_before_ready():
    driver = PerformanceInjection(spec())
    driver.start(now=0)
    ticket = driver.request("read", now=1)
    assert driver.cancel(ticket)
    assert not driver.cancel(ticket)
    assert ticket.state == "cancelled"
    with pytest.raises(ValueError, match="cancelled"):
        driver.finish(ticket)
    evidence = driver.snapshot()
    assert evidence["counters"]["completed"] == 0
    assert evidence["actual_injection_seconds"] == 0
    assert evidence["actual_service_seconds"] == 0


@pytest.mark.parametrize("changes", [
    {"seed": True}, {"resource": "network"}, {"trigger": "other"},
    {"start": -1}, {"end": math.inf}, {"delay_seconds": math.nan},
    {"max_seconds": -1}, {"max_events": True}, {"operations": ["read"]},
    {"max_events": 257},
    {"resource": "compute", "per_bytes": 0.01},
    {"operations": ("read", "read")}, {"scope": "same-host"},
    {"trigger": "work", "barrier": None},
])
def test_rejects_invalid_specs(changes):
    with pytest.raises((ValueError, TypeError)):
        spec(**changes)


def test_rejects_invalid_inputs_duplicate_ids_and_clock_reversal():
    driver = PerformanceInjection(spec())
    driver.start(now=10)
    with pytest.raises(ValueError):
        driver.request("read", work=True, now=10)
    with pytest.raises(ValueError):
        driver.request("read", nbytes=True, now=10)
    first = driver.request("read", now=11, event_id="duplicate")
    with pytest.raises(ValueError, match="duplicate"):
        driver.request("read", now=11, event_id="duplicate")
    with pytest.raises(ValueError, match="before run start"):
        driver.progress(now=9)
    with pytest.raises(RuntimeError, match="readiness"):
        driver.finish(first)
    with pytest.raises(ValueError, match="backwards"):
        driver.request("read", now=10.5)


def test_rejects_arithmetic_overflow_and_serialized_spec_over_limit():
    with pytest.raises(ValueError, match="4 KiB"):
        spec(operations=tuple(f"{i:02d}" + "x" * 62 for i in range(64)))
    driver = PerformanceInjection(spec(start=0, end=1, delay_seconds=1e308, max_seconds=1e308))
    driver.start(now=1e308)
    with pytest.raises(ValueError, match="overflow"):
        driver.request("read", now=1e308)

    work_driver = PerformanceInjection(spec(trigger="work", start=0, end=1e308,
                                           barrier="prefix", max_events=8))
    work_driver.start(now=0)
    work_driver.barrier("prefix")
    work_driver.request("read", work=1e308, now=0)
    with pytest.raises(ValueError, match="work cursor overflow"):
        work_driver.request("read", work=1e308, now=0)


def test_completion_must_flow_through_owner_to_update_aggregate_counters():
    driver = PerformanceInjection(spec())
    driver.start(now=0)
    ticket = driver.request("read", now=1)
    driver.progress(now=ticket.ready_at)
    ticket.complete(service_seconds=0.1)
    assert driver.snapshot()["counters"]["completed"] == 1


def test_rejects_per_byte_delay_arithmetic_overflow():
    driver = PerformanceInjection(spec(per_bytes=1e308, max_seconds=1e308))
    driver.start(now=0)
    with pytest.raises(ValueError, match="delay overflow"):
        driver.request("read", nbytes=10**308, now=1)
