"""Fair, bounded parameter suffix execution on one collective rank group."""
from collections import deque
from dataclasses import dataclass

from .batch_resources import close_shared, share_budget


@dataclass(frozen=True)
class BatchResult:
    binding_id: str
    expectation: float
    gradient: dict | None
    managed_peak_bytes: int


class ParameterBatchExecutor:
    def __init__(self, template, prefix, *, rank_budget=None, policy=None):
        self.template, self.prefix = template, prefix
        self.policy = policy or template.policy
        self.rank_budget = rank_budget
        if rank_budget is not None and rank_budget.ram_budget_bytes_per_rank != template.budget.limit:
            raise ValueError('Batch uses the template shared rank budget')
        self.closed = False
        self.trace = []

    def run(self, bindings, *, observable, gradient=None):
        if self.closed:
            raise ValueError('Closed batch executor')
        pending = iter(enumerate(bindings))
        active = deque()
        results = {}
        exhausted = False
        template = self.template
        template.drain()
        try:
            while active or not exhausted:
                while len(active) < self.policy.max_active_jobs and not exhausted:
                    try:
                        index, binding = next(pending)
                    except StopIteration:
                        exhausted = True
                        break
                    child = template.fork(self.prefix, binding=binding)
                    try:
                        share_budget(template, child)
                    except BaseException:
                        child.close()
                        raise
                    child._gradient_origin = template._gradient_origin or (
                        self.prefix['snapshot_id'] if isinstance(self.prefix, dict) else self.prefix)
                    active.append((index, child))
                if not active:
                    break
                index, child = active[0]
                last = len(child.sequencer.operations)-1
                if child.frontier < last:
                    end = min(last, child.frontier+self.policy.lookahead_gates)
                    child.materialize(end, binding=child.binding)
                    self.trace.append({'job': index, 'frontier': child.frontier})
                    if len(self.trace) > 64:
                        del self.trace[0]
                if child.frontier == last:
                    try:
                        value = child.expectation(observable)
                        derivative = child.gradient(observable, method=gradient) if gradient else None
                        results[index] = BatchResult(child.binding.binding_id, value, derivative, template.budget.peak)
                    finally:
                        close_shared(child)
                    active.popleft()
                else:
                    active.rotate(-1)
            return tuple(results[index] for index in sorted(results))
        finally:
            # All failures are collective in the child; never abandon a peer's
            # posted operation or its immutable-prefix lease.
            for _, child in active:
                close_shared(child)

    def close(self):
        self.closed = True

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()
