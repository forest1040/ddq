from dataclasses import dataclass

import pytest

from svsim.runtime_cost import CostSample
from svsim.runtime_eta import CountedTask, InFlightWork, RuntimeETA


@dataclass
class Snapshot:
    snapshot_id: str = "snap"
    complete: bool = True
    stale: bool = False
    unknown_reasons: tuple = ()
    window: tuple = ()
    tail: tuple = ()
    in_flight: tuple = ()
    context: dict | None = None


def task(name, count=1, deps=(), resource="cpu", key=None):
    return CountedTask(name, {"op": name}, {}, count, tuple(deps), resource,
                       cost_key=key or name)


def sample(mean, lower=None, upper=None):
    lower = mean if lower is None else lower
    upper = mean if upper is None else upper
    return CostSample("condition", mean, lower, upper)


def test_truthy_validation_flags_do_not_certify_cost_or_request_completion():
    snapshot=Snapshot(window=(task('x'),))
    report=RuntimeETA().evaluate(snapshot,{'x':CostSample('condition',1,1,1,measured='yes')})
    assert report.expected_seconds is None
    assert 'UNVALIDATED_COST:x' in report.unknown_reasons
    with pytest.raises(ValueError,match='completion flag'):
        RuntimeETA().evaluate(Snapshot(in_flight=(InFlightWork('r','read',completed=1),)))


def test_dag_lower_bound_and_serial_expected_upper_match_oracle():
    # b and c run in parallel after a. The CPU resource serializes b and c.
    snapshot = Snapshot(window=(task("b", deps=("a",)), task("c", deps=("a",)),
                               task("a")))
    report = RuntimeETA().evaluate(snapshot, {
        "a": sample(2, 1, 3), "b": sample(5, 2, 7), "c": sample(4, 3, 6),
    })
    assert report.lower_seconds == 6  # a->c critical path and b+c CPU demand
    assert report.expected_seconds == 11
    assert report.upper_seconds == 16
    assert report.coverage == 1
    assert report.eligible_for_control is False


def test_large_count_is_weighted_without_expansion():
    snapshot = Snapshot(window=(task("repeat", 10**12),))
    report = RuntimeETA().evaluate(snapshot, {"repeat": sample(0.25, 0.2, 0.5)})
    assert report.expected_seconds == 250_000_000_000
    assert report.total_work_count == 10**12


@pytest.mark.parametrize("changes,reason", [
    ({"complete": False}, "INCOMPLETE_SCAN"),
    ({"stale": True}, "STALE_SNAPSHOT"),
    ({"in_flight": (InFlightWork("r1", "read"),)}, "IN_FLIGHT:r1"),
])
def test_partial_snapshots_never_publish_total_eta(changes, reason):
    snapshot = Snapshot(window=(task("a"),))
    for key, value in changes.items():
        setattr(snapshot, key, value)
    report = RuntimeETA().evaluate(snapshot, {"a": sample(2)})
    assert report.expected_seconds is None
    assert reason in report.unknown_reasons
    assert report.partial_expected_seconds == 2


def test_missing_cost_and_task_unknowns_are_reported_with_coverage():
    a = task("a", 3)
    b = CountedTask("b", {}, {}, 2, ("a",), cost_key="b",
                    unknown_reasons=("MISSING_LAYOUT",))
    report = RuntimeETA().evaluate(Snapshot(window=(a, b)), {"a": sample(1)})
    assert report.expected_seconds is None
    assert report.known_work_count == 3
    assert report.total_work_count == 5
    assert report.coverage == 0.6
    assert "MISSING_LAYOUT" in report.unknown_reasons
    assert "MISSING_COST:b" not in report.unknown_reasons


@pytest.mark.parametrize("tasks,message", [
    ((task("a"), task("a")), "Duplicate"),
    ((task("a", deps=("missing",)),), "Missing dependency"),
    ((task("a", deps=("b",)), task("b", deps=("a",))), "Cyclic"),
    ((task("a", True),), "count"),
    ((task("a", -1),), "count"),
])
def test_invalid_graphs_and_counts_are_rejected(tasks, message):
    with pytest.raises(ValueError, match=message):
        RuntimeETA().evaluate(Snapshot(window=tasks), {})


def test_zero_work_complete_snapshot_is_exactly_quiescent():
    report = RuntimeETA().evaluate(Snapshot(), None)
    assert (report.lower_seconds, report.expected_seconds, report.upper_seconds) == (0, 0, 0)
    assert report.reason == "complete"


def test_unmeasured_cost_does_not_price_a_task():
    report = RuntimeETA().evaluate(Snapshot(window=(task("a"),)),
                                   {"a": CostSample("manual", 2, 1, 3, measured=False)})
    assert report.expected_seconds is None
    assert report.known_work_count == 0
    assert report.coverage == 0
    assert "UNVALIDATED_COST:a" in report.unknown_reasons


def test_structural_unknown_cannot_be_counted_as_known_with_a_profile():
    item = CountedTask("a", {"op": "x"}, {}, 4, cost_key="a",
                       unknown_reasons=("UNKNOWN_LAYOUT",))
    report = RuntimeETA().evaluate(Snapshot(window=(item,)), {"a": sample(1)})
    assert report.expected_seconds is None
    assert report.known_work_count == 0
    assert report.total_work_count == 4
    assert report.coverage == 0
    assert report.partial_expected_seconds == 0


def test_completed_requests_are_removed_from_remaining_work():
    snapshot = Snapshot(in_flight=(InFlightWork("done", "write", completed=True),))
    report = RuntimeETA().evaluate(snapshot)
    assert (report.lower_seconds, report.expected_seconds, report.upper_seconds) == (0, 0, 0)
    assert (report.known_work_count, report.total_work_count, report.coverage) == (0, 0, 1)


def test_duplicate_or_excessive_inflight_requests_are_rejected():
    duplicate = Snapshot(in_flight=(InFlightWork("r", "read"), InFlightWork("r", "write")))
    with pytest.raises(ValueError, match="Duplicate"):
        RuntimeETA().evaluate(duplicate)
    too_many = Snapshot(in_flight=tuple(InFlightWork(str(i), "read") for i in range(129)))
    with pytest.raises(ValueError, match="Too many"):
        RuntimeETA().evaluate(too_many)


@pytest.mark.parametrize("changes,message", [
    ({"condition": None}, "condition or context"),
    ({"context": None}, "condition or context"),
    ({"resource": ""}, "resource"),
    ({"units": ""}, "units"),
])
def test_task_cost_metadata_is_validated(changes, message):
    fields = {"condition": {"op": "x"}, "context": {}, "count": 1,
              "resource": "cpu", "units": "groups"}
    fields.update(changes)
    invalid = CountedTask("a", fields["condition"], fields["context"], fields["count"],
                          resource=fields["resource"], units=fields["units"])
    with pytest.raises(ValueError, match=message):
        RuntimeETA().evaluate(Snapshot(window=(invalid,)))


def test_huge_count_coverage_is_finite():
    huge = 10**2000
    report = RuntimeETA().evaluate(Snapshot(window=(task("a", huge),)),
                                   {"a": sample(0)})
    assert report.coverage == 1
    assert report.total_work_count == huge
