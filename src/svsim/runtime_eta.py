"""ETA evaluation for bounded counted task DAGs.

Counted tasks represent many equivalent operations without expanding them into
individual task objects.  Overlap is credited only through the DAG/resource
lower bound; expected and upper values remain serial sums.
"""

from __future__ import annotations

from dataclasses import dataclass
import math
from typing import Mapping

from .runtime_cost import CostSample
from .executor_stages import ExecutorRoute, CONTRACT


@dataclass(frozen=True)
class CountedTask:
    task_id: str
    condition: dict
    context: dict
    count: int
    dependencies: tuple[str, ...] = ()
    resource: str = "compute"
    units: str = "groups"
    kind: str = "work"
    source_ids: tuple[int, ...] = ()
    cost_key: str = ""
    unknown_reasons: tuple[str, ...] = ()
    payload_bytes_per_unit: int | None = None
    kernel_work_per_unit: int | None = None
    stage_route: ExecutorRoute | None = None
    storage_bytes_per_unit: int | None = None


@dataclass(frozen=True)
class WorkCoverage:
    """Coverage of priced work in a named unit; unknown denominators stay null."""
    metric: str
    unit: str
    scope: str
    known_work: int | float | None
    total_work: int | float | None
    coverage: float | None
    unknown_reasons: tuple[str, ...] = ()


@dataclass(frozen=True)
class InFlightWork:
    request_id: str
    kind: str
    completed: bool = False
    unknown_reasons: tuple[str, ...] = ()


@dataclass(frozen=True)
class RuntimeETAReport:
    lower_seconds: float | None
    expected_seconds: float | None
    upper_seconds: float | None
    coverage: float
    reason: str
    snapshot_id: str
    known_work_count: int
    total_work_count: int
    unknown_reasons: tuple[str, ...]
    eligible_for_control: bool = False
    partial_lower_seconds: float | None = None
    partial_expected_seconds: float | None = None
    partial_upper_seconds: float | None = None
    payload_byte_coverage: WorkCoverage | None = None
    kernel_work_coverage: WorkCoverage | None = None
    critical_chain_coverage: WorkCoverage | None = None
    executor_stage_coverage: tuple[dict, ...] = ()
    storage_byte_coverage: WorkCoverage | None = None


class RuntimeETA:
    """Evaluate a duck-typed snapshot using a callable or cost-key mapping."""

    MAX_TASK_CLASSES = 512

    def evaluate(self, snapshot, cost_view=None) -> RuntimeETAReport:
        # An exact epoch-aligned executor shape replaces the semantic tail.
        exact = getattr(snapshot, "future_complete", False) and not getattr(snapshot, "future_reasons", ())
        tasks = (tuple(snapshot.window) + tuple(snapshot.future if exact else snapshot.tail)
                 + tuple(getattr(snapshot, "endpoint_tasks", ())))
        flights = tuple(snapshot.in_flight)
        self._validate_graph(tasks)
        self._validate_flights(flights)

        if len(tasks) > self.MAX_TASK_CLASSES:
            raise ValueError("Too many counted task classes")
        if not isinstance(snapshot.snapshot_id, str):
            raise ValueError("Invalid snapshot id")

        unknown = list(snapshot.unknown_reasons)
        if not (exact or snapshot.complete):
            unknown.append("INCOMPLETE_SCAN")
        if snapshot.stale:
            unknown.append("STALE_SNAPSHOT")

        remaining_flights = tuple(work for work in flights if not work.completed)
        total = sum(task.count for task in tasks) + (0 if getattr(snapshot, "in_flight_covered", False)
                                                      else len(remaining_flights))
        sample_map = {}
        known_count = 0
        for task in tasks:
            unknown.extend(task.unknown_reasons)
            if task.count == 0:
                sample_map[task.task_id] = CostSample("zero", 0, 0, 0)
                continue
            if task.unknown_reasons:
                continue
            route = task.stage_route
            contract_condition = task.condition.get('class', {}) if task.kind == 'residual' else task.condition
            if route is not None and (contract_condition.get('stage_contract') != CONTRACT or
                                      contract_condition.get('stage_route') != route.route):
                unknown.append("UNAUTHENTICATED_INCLUSIVE_STAGE_CONTRACT:" + task.task_id)
                continue
            sample = self._lookup(cost_view, task)
            if sample is None:
                unknown.append("MISSING_COST:" + task.task_id)
                continue
            if not isinstance(sample, CostSample):
                raise TypeError("cost view must return CostSample or None")
            if sample.measured is not True:
                unknown.append("UNVALIDATED_COST:" + task.task_id)
                continue
            sample_map[task.task_id] = sample
            known_count += task.count
        if not getattr(snapshot, "in_flight_covered", False):
            for work in remaining_flights:
                unknown.extend(work.unknown_reasons)
                unknown.append("IN_FLIGHT:" + work.request_id)
        if total == 0:
            # Empty, complete snapshots are an exact quiescent state.
            if not unknown:
                coverage_rows = self._coverage_reports(snapshot, tasks, sample_map, complete=True)
                return RuntimeETAReport(0, 0, 0, 1.0, "complete", snapshot.snapshot_id,
                                        0, 0, (), False, 0, 0, 0, *coverage_rows)

        partial = self._bounds(tasks, sample_map)
        if partial[0] is None:
            unknown.append("ARITHMETIC_OVERFLOW")
        coverage = self._coverage(known_count, total) if total else (1.0 if not unknown else 0.0)
        reasons = tuple(dict.fromkeys(unknown))
        coverage_rows = self._coverage_reports(snapshot, tasks, sample_map,
                                               complete=not reasons and (exact or snapshot.complete))
        if reasons:
            return RuntimeETAReport(None, None, None, coverage,
                                    reasons[0], snapshot.snapshot_id,
                                    known_count, total, reasons, False, *partial, *coverage_rows)
        return RuntimeETAReport(*partial, coverage, "serial-upper-bound-unverified-overlap",
                                snapshot.snapshot_id, known_count, total, (), False, *partial,
                                *coverage_rows)

    def _coverage_reports(self, snapshot, tasks, samples, *, complete):
        flights = tuple(work for work in getattr(snapshot, "in_flight", ()) if not work.completed)
        covered = bool(getattr(snapshot, "in_flight_covered", False))
        snapshot_reasons = list(getattr(snapshot, "unknown_reasons", ()))
        exact_future = getattr(snapshot, "future_complete", False) and not getattr(snapshot, "future_reasons", ())
        if not (exact_future or getattr(snapshot, "complete", False)):
            snapshot_reasons.append("INCOMPLETE_SCAN")
        if getattr(snapshot, "stale", False):
            snapshot_reasons.append("STALE_SNAPSHOT")
        snapshot_reasons.extend(getattr(snapshot, "future_reasons", ()))
        omitted_work = bool(flights and not covered)
        payload = self._weighted_coverage(
            tasks, samples, "payload_bytes_per_unit", metric="payload-bytes", unit="bytes",
            scope="MPI amplitude send payload (each direction once); excludes storage and control traffic", snapshot_reasons=snapshot_reasons,
            omitted_work=omitted_work)
        kernel = self._weighted_coverage(
            tasks, samples, "kernel_work_per_unit", metric="kernel-work", unit="explicit kernel work units",
            scope="rank-local main kernel amplitude visits; excludes Frame and packing", snapshot_reasons=snapshot_reasons,
            omitted_work=omitted_work)
        chain = self._critical_chain_coverage(tasks, samples, snapshot_reasons,
                                              complete=complete, omitted_work=omitted_work)
        stages = tuple({"task_id": task.task_id, "route": task.stage_route.route,
                        "calls": task.count, "stage_classes": len(task.stage_route.stages),
                        "included_in_priced_parent": task.task_id in samples,
                        "independently_calibrated_stages": 0,
                        "conditional_stage_reasons": tuple(dict.fromkeys(
                            reason for node in task.stage_route.stages for reason in node.unknown_reasons))}
                       for task in tasks if task.stage_route is not None)
        storage = self._weighted_coverage(
            tasks, samples, "storage_bytes_per_unit", metric="amplitude-storage-bytes", unit="bytes",
            scope="physical amplitude storage reads plus writes; excludes catalog and journals",
            snapshot_reasons=snapshot_reasons, omitted_work=omitted_work)
        return payload, kernel, chain, stages, storage

    @classmethod
    def _weighted_coverage(cls, tasks, samples, field, *, metric, unit, scope,
                           snapshot_reasons=(), omitted_work=False):
        known_work = 0
        total_work = 0
        reasons = list(snapshot_reasons)
        for task in tasks:
            if task.count == 0:
                continue
            weight = getattr(task, field)
            if weight is None:
                reasons.append(f"UNESTIMATED_{metric.upper().replace('-', '_')}:{task.task_id}")
                continue
            weighted = weight * task.count
            total_work += weighted
            if task.task_id in samples:
                known_work += weighted
        if omitted_work:
            reasons.append("UNMEASURED_INFLIGHT_WORK")
        reasons = tuple(dict.fromkeys(reasons))
        if reasons:
            return WorkCoverage(metric, unit, scope, known_work, None, None, reasons)
        ratio = cls._coverage(known_work, total_work) if total_work else 1.0
        return WorkCoverage(metric, unit, scope, known_work, total_work, ratio, ())

    @classmethod
    def _critical_chain_coverage(cls, tasks, samples, snapshot_reasons, *, complete, omitted_work):
        known_lower_bound = cls._bounds(tasks, samples)[0]
        reasons = list(snapshot_reasons)
        reasons.extend("UNKNOWN_DURATION_CANDIDATE:" + task.task_id for task in tasks
                        if task.count and task.task_id not in samples)
        if omitted_work:
            reasons.append("UNKNOWN_DURATION_CANDIDATE:in-flight")
        if not complete:
            reasons.append("CRITICAL_CHAIN_INCOMPLETE_SNAPSHOT")
        if known_lower_bound is None:
            reasons.append("CRITICAL_CHAIN_ARITHMETIC_OVERFLOW")
        reasons = tuple(dict.fromkeys(reasons))
        if reasons:
            return WorkCoverage("critical-chain", "seconds",
                                "dependency/resource lower-bound DAG", known_lower_bound,
                                None, None, reasons)
        return WorkCoverage("critical-chain", "seconds", "dependency/resource lower-bound DAG",
                            known_lower_bound, known_lower_bound, 1.0, ())

    @staticmethod
    def _lookup(cost_view, task):
        if task.count == 0:
            return None
        if cost_view is None:
            return None
        if callable(cost_view):
            return cost_view(task)
        if isinstance(cost_view, Mapping):
            return cost_view.get(task.cost_key)
        raise TypeError("cost_view must be callable, a mapping, or None")

    @classmethod
    def _validate_graph(cls, tasks):
        if len(tasks) > cls.MAX_TASK_CLASSES:
            raise ValueError("Too many counted task classes")
        ids = set()
        for task in tasks:
            if not isinstance(task.task_id, str) or not task.task_id:
                raise ValueError("Invalid counted task id")
            if task.task_id in ids:
                raise ValueError("Duplicate counted task id")
            ids.add(task.task_id)
            if isinstance(task.count, bool) or not isinstance(task.count, int) or task.count < 0:
                raise ValueError("Invalid counted task count")
            if not isinstance(task.dependencies, tuple):
                raise ValueError("Invalid dependencies")
            if not isinstance(task.condition, dict) or not isinstance(task.context, dict):
                raise ValueError("Invalid task condition or context")
            if not isinstance(task.resource, str) or not task.resource:
                raise ValueError("Invalid task resource")
            if not isinstance(task.units, str) or not task.units:
                raise ValueError("Invalid task units")
            if task.stage_route is not None:
                route = task.stage_route
                if not isinstance(route, ExecutorRoute) or route.contract != CONTRACT:
                    raise ValueError("Invalid executor stage route")
                if len(route.stages) > 16:
                    raise ValueError("Too many executor stages")
                if (task.payload_bytes_per_unit != route.payload_bytes or
                        task.kernel_work_per_unit != route.kernel_work):
                    raise ValueError("Executor route coverage weights disagree")
                seen = set()
                for node in route.stages:
                    if (node.stage in seen or any(dep not in seen for dep in node.dependencies)
                            or type(node.count_lower) is not int or type(node.count_upper) is not int
                            or not 0 <= node.count_lower <= node.count_upper):
                        raise ValueError("Invalid executor stage DAG")
                    seen.add(node.stage)
            for name in ("payload_bytes_per_unit", "kernel_work_per_unit", "storage_bytes_per_unit"):
                value = getattr(task, name)
                if value is not None and (type(value) is not int or value < 0):
                    raise ValueError("Invalid task coverage weight: " + name)
        for task in tasks:
            if any(dep not in ids for dep in task.dependencies):
                raise ValueError("Missing dependency")
        pending = {task.task_id: task for task in tasks}
        done = set()
        while pending:
            ready = [key for key, task in pending.items()
                     if all(dep in done for dep in task.dependencies)]
            if not ready:
                raise ValueError("Cyclic DAG")
            done.update(ready)
            for key in ready:
                del pending[key]

    @staticmethod
    def _validate_flights(flights):
        if len(flights) > 128:
            raise ValueError("Too many in-flight requests")
        seen = set()
        for work in flights:
            if not isinstance(work.request_id, str) or not work.request_id:
                raise ValueError("Invalid in-flight request id")
            if work.request_id in seen:
                raise ValueError("Duplicate in-flight request id")
            seen.add(work.request_id)
            if type(work.completed) is not bool:
                raise ValueError("Invalid in-flight completion flag")

    @staticmethod
    def _coverage(known, total):
        # Scale large arbitrary-precision integers before converting to float.
        shift = max(0, total.bit_length() - 1000)
        numerator = known >> shift
        denominator = total >> shift
        return numerator / denominator if denominator else 0.0

    @staticmethod
    def _bounds(tasks, samples):
        ends = {}
        resource = {}
        expected = upper = 0.0
        try:
            pending = {task.task_id: task for task in tasks}
            while pending:
                ready = [task for task in pending.values()
                         if all(dep in ends or dep not in pending for dep in task.dependencies)]
                # A task behind an unpriced predecessor still has no calibrated start;
                # its own duration remains useful in the diagnostic partial sum.
                if not ready:
                    ready = list(pending.values())
                for task in ready:
                    pending.pop(task.task_id)
                    sample = samples.get(task.task_id)
                    if sample is None:
                        ends.pop(task.task_id, None)
                        continue
                    mult = task.count
                    lo = sample.lower_seconds * mult
                    mean = sample.mean_seconds * mult
                    hi = sample.upper_seconds * mult
                    if not all(math.isfinite(v) for v in (lo, mean, hi)):
                        raise OverflowError
                    start = max((ends[d] for d in task.dependencies if d in ends), default=0.0)
                    ends[task.task_id] = start + lo
                    resource[task.resource] = resource.get(task.resource, 0.0) + lo
                    expected += mean
                    upper += hi
                    if not all(math.isfinite(v) for v in (ends[task.task_id], expected, upper,
                                                           resource[task.resource])):
                        raise OverflowError
            lower = max(max(ends.values(), default=0.0), max(resource.values(), default=0.0))
            return (lower, expected, upper)
        except OverflowError:
            return (None, None, None)
