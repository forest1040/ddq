"""Rolling measured costs from the runtime completion collector, served through the cost registry.

Only collector rows that are successful, validated, measured and outside
warmup are used (``cost_observations``), and a class answers only after
``MIN_SAMPLES`` of them. Executor conditions are matched by gate class: the
per-occurrence ``op_id``/``source_op_id`` are removed, everything else
(angles, layout, policy, engine) must be equal. Values are seconds per unit;
lower/upper are the observed extrema, not a confidence interval.
"""
from __future__ import annotations

from statistics import median

from .journal import canonical
from .runtime_cost import CostSample

MIN_SAMPLES = 5
EXECUTOR_STAGES = ('executor-gate', 'executor-group')


def _class(condition):
    condition = dict(condition)
    if isinstance(condition.get('operations'), list):
        condition['operations'] = [{k: v for k, v in op.items() if k not in {'op_id', 'source_op_id'}}
                                   for op in condition['operations']]
    return canonical(condition)


def _per_unit(rows):
    values = []
    for row in rows:
        seconds = row.protocol_seconds if row.protocol_seconds is not None else row.service_seconds
        if seconds is None or not row.quantity: continue
        values.append(seconds / row.quantity)
    return values


def observed_task_cost(collector, condition):
    """CostSample for an executor task condition from at least MIN_SAMPLES same-class groups, or None."""
    if collector is None or not collector.enabled or not isinstance(condition, dict): return None
    if condition.get('operation') not in EXECUTOR_STAGES: return None
    key = _class(condition)
    values = _per_unit([row for row in collector.cost_observations()
                        if row.stage in EXECUTOR_STAGES and _class(row.condition) == key])
    if len(values) < MIN_SAMPLES: return None
    return CostSample('observed:' + condition['operation'], median(values), min(values), max(values), measured=True)


def observed_cache_cost(collector, scope):
    """Clean re-fetch and dirty write-back seconds from measured storage completions, or None."""
    from .cache_cost import CacheCost
    if collector is None or not collector.enabled: return None
    rows = collector.cost_observations()
    reads = _per_unit([r for r in rows if r.stage == 'storage-read'])
    if len(reads) < MIN_SAMPLES: return None
    writes = _per_unit([r for r in rows if r.stage == 'storage-write'])
    return CacheCost(whole_fetch=median(reads), dirty_writeback=median(writes) if len(writes) >= MIN_SAMPLES else None,
                     evidence='measured', validated=True, units='seconds', source='runtime-collector', scope=scope)
