"""Condition-matched finite DAG ETA with conservative overlap accounting."""
from dataclasses import dataclass
import math


@dataclass(frozen=True)
class CostSample:
    condition: str
    mean_seconds: float
    lower_seconds: float
    upper_seconds: float
    measured: bool = True

    def __post_init__(self):
        if not self.condition or not all(math.isfinite(x) for x in
            (self.lower_seconds, self.mean_seconds, self.upper_seconds)):
            raise ValueError('Invalid cost measurement')
        if not 0 <= self.lower_seconds <= self.mean_seconds <= self.upper_seconds:
            raise ValueError('Invalid cost confidence interval')


@dataclass(frozen=True)
class CostTask:
    task_id: str
    condition: str
    dependencies: tuple[str, ...] = ()
    resource: str = 'compute'


@dataclass(frozen=True)
class ETAEstimate:
    lower_seconds: float | None
    expected_seconds: float | None
    upper_seconds: float | None
    coverage: float
    reason: str


class RuntimeCostModel:
    def __init__(self, *, max_conditions=256):
        if max_conditions < 1:
            raise ValueError('Invalid cost profile limit')
        self.max_conditions = max_conditions
        self.samples = {}
        self.version = 1

    def record(self, sample):
        if not sample.measured:
            raise ValueError('ETA profile requires measured successful work')
        self.samples[sample.condition] = sample
        if len(self.samples) > self.max_conditions:
            self.samples.pop(next(iter(self.samples)))
        self.version += 1

    def estimate(self, tasks, *, calibrated_overlap=False):
        tasks = tuple(tasks)
        ids = {task.task_id for task in tasks}
        if len(ids) != len(tasks):
            raise ValueError('Duplicate cost task id')
        if not tasks:
            return ETAEstimate(0, 0, 0, 1, 'complete')
        matched = sum(task.condition in self.samples for task in tasks)
        if matched != len(tasks):
            return ETAEstimate(None, None, None, matched/len(tasks), 'INSUFFICIENT_BASELINE')
        pending = {task.task_id: task for task in tasks}
        ends = {}
        resource_demand = {}
        expected = upper = 0
        while pending:
            ready = [task for task in pending.values() if all(d in ends for d in task.dependencies)]
            if not ready:
                raise ValueError('Cyclic DAG or missing dependency')
            for task in ready:
                sample = self.samples[task.condition]
                ends[task.task_id] = max((ends[d] for d in task.dependencies), default=0)+sample.lower_seconds
                resource_demand[task.resource] = resource_demand.get(task.resource, 0)+sample.lower_seconds
                expected += sample.mean_seconds
                upper += sample.upper_seconds
                del pending[task.task_id]
        lower = max(max(ends.values()), max(resource_demand.values()))
        if calibrated_overlap:
            # A mixed-condition sample must already include the observed overlap;
            # this API never creates a speedup from a boolean alone.
            reason = 'mixed-condition-costs-no-unmeasured-speedup'
        else:
            reason = 'serial-upper-bound-unverified-overlap'
        return ETAEstimate(lower, expected, upper, 1, reason)
