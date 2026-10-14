"""Rolling measured costs from the runtime completion collector, served through the cost registry.

Only collector rows that are successful, validated, measured and outside
warmup are used (``cost_observations``), and a class answers only after
``MIN_SAMPLES`` of them. Executor conditions are matched by gate class: the
per-occurrence ``op_id``/``source_op_id`` are removed, everything else
(angles, layout, policy, engine) must be equal. Values are seconds per unit;
lower/upper are the training envelope widened by the held-out extrema, not a
confidence interval.

Freshness: only rows whose context ``policy_epoch``/``plan_version`` equal the
current runtime's count, and at most the newest ``WINDOW`` per class. Quality:
the older half of the values trains an envelope [min, max]; the newer half is
held out and ``coverage`` is the fraction inside it. A class is certified only
with ``CERTIFY_SAMPLES`` values and ``coverage >= MIN_COVERAGE``; production
serves certified results only, the diagnostic source serves the rest labelled.
"""
from __future__ import annotations

from statistics import median

from .journal import canonical
from .runtime_cost import CostSample

MIN_SAMPLES = 5
CERTIFY_SAMPLES = 8
WINDOW = 32
MIN_COVERAGE = .8
EXECUTOR_STAGES = ('executor-gate', 'executor-group')
CERTIFIED, INSUFFICIENT, LOW_COVERAGE, STALE = 'CERTIFIED', 'INSUFFICIENT_SAMPLES', 'LOW_HELD_OUT_COVERAGE', 'STALE_POLICY_EPOCH'


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


def _fresh(row, policy_epoch, plan_version):
    context = row.context or {}
    return context.get('policy_epoch') == policy_epoch and context.get('plan_version') == plan_version


def _assess(rows, policy_epoch, plan_version):
    """(reason, values, lower, upper, coverage) of the fresh newest-WINDOW values of ``rows`` (oldest first)."""
    fresh = [r for r in rows if _fresh(r, policy_epoch, plan_version)]
    values = _per_unit(fresh[-WINDOW:])
    if not values: return (STALE if rows else INSUFFICIENT), values, None, None, None
    half = len(values) // 2
    train, held = values[:half] or values, values[half:]
    lower, upper = min(train), max(train)
    coverage = sum(lower <= v <= upper for v in held) / len(held)
    reason = INSUFFICIENT if len(values) < CERTIFY_SAMPLES else LOW_COVERAGE if coverage < MIN_COVERAGE else CERTIFIED
    return reason, values, min(lower, *held), max(upper, *held), coverage


def _note(decisions, key, reason):
    if decisions is not None: decisions[key] = reason


def observed_task_cost(collector, condition, policy_epoch=None, plan_version=None, *, diagnostic=False, decisions=None):
    """Certified CostSample for an executor task class (or, with ``diagnostic``, the uncertified one, labelled), or None.

    ``decisions`` (if given) receives ``{class_key: reason}`` for the last decision.
    """
    if collector is None or not collector.enabled or not isinstance(condition, dict): return None
    if condition.get('operation') not in EXECUTOR_STAGES: return None
    key = _class(condition)
    reason, values, lower, upper, coverage = _assess(
        [row for row in collector.cost_observations() if row.stage in EXECUTOR_STAGES and _class(row.condition) == key],
        policy_epoch, plan_version)
    _note(decisions, key, reason)
    if not values or len(values) < MIN_SAMPLES: return None
    if reason == CERTIFIED:
        if diagnostic: return None
        return CostSample('observed:' + condition['operation'], median(values), lower, upper, measured=True)
    if not diagnostic: return None
    return CostSample(f"diagnostic:{condition['operation']}:{reason}:coverage={coverage:.2f}", median(values), lower, upper,
                      measured=False)


def observed_cache_cost(collector, scope, policy_epoch=None, plan_version=None, *, diagnostic=False, decisions=None):
    """Clean re-fetch and dirty write-back seconds from fresh storage completions; certified only unless ``diagnostic``."""
    from .cache_cost import CacheCost
    if collector is None or not collector.enabled: return None
    rows = collector.cost_observations()
    read, rvalues = _assess([r for r in rows if r.stage == 'storage-read'], policy_epoch, plan_version)[:2]
    write, wvalues = _assess([r for r in rows if r.stage == 'storage-write'], policy_epoch, plan_version)[:2]
    _note(decisions, 'storage-read', read)
    _note(decisions, 'storage-write', write)
    if len(rvalues) < MIN_SAMPLES or (read == CERTIFIED) == diagnostic: return None
    writeback = median(wvalues) if (write == CERTIFIED or diagnostic) and len(wvalues) >= MIN_SAMPLES else None
    return CacheCost(whole_fetch=median(rvalues), dirty_writeback=writeback,
                     evidence='derived' if diagnostic else 'measured', validated=not diagnostic, units='seconds',
                     source='runtime-collector-diagnostic' if diagnostic else 'runtime-collector', scope=scope)
