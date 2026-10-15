"""Rolling measured costs from the runtime completion collector, served through the cost registry.

Only collector rows that are successful, validated, measured and outside
warmup are used (``cost_observations``), and a class answers only after
``MIN_SAMPLES`` of them. Executor conditions (and the once-per-epoch publication and boundary stages) are matched by gate class: the
per-occurrence ``op_id``/``source_op_id`` are removed, everything else
(angles, layout, policy, engine) must be equal; an exact class with too few samples falls back to its
angle-pooled class (angles removed, Clifford/diagonal/permutation flags kept), labelled ``observed-pooled-angle``
and certified by the same held-out rule. Values are seconds per unit;
lower/upper are the training envelope widened by the held-out extrema, not a
confidence interval.

Freshness: only rows whose context ``policy_epoch``/``plan_version`` equal the
current runtime's count, and at most the newest ``WINDOW`` per class. Quality:
rows follow actual collector submission order, even when different angle classes
share a pooled class (identifiers and condition-ring iteration are not clocks).
The older half of the values trains an envelope [min, max] widened by ``TOLERANCE``
(relative; the ETA error target, since a min/max of a few i.i.d. values alone bounds only a fraction of
repeats); the newer half is held out and ``coverage`` is the fraction inside it. A class is certified only
with ``CERTIFY_SAMPLES`` values and ``coverage >= MIN_COVERAGE``; production
serves certified results only, the diagnostic source serves the rest labelled.
Burn-in: the oldest ``BURN_IN`` fresh values of an executor class (cold caches, warmup beyond the collector's flag; a
policy change resets the class too) are dropped before windowing and never count toward ``CERTIFY_SAMPLES``. After
burn-in, training and held-out medians differing by more than ``DRIFT_LIMIT`` (relative) report ``DRIFTING``.
Storage completions are assessed without burn-in. Reasons: CERTIFIED, INSUFFICIENT_SAMPLES, LOW_HELD_OUT_COVERAGE,
STALE_POLICY_EPOCH, DRIFTING.
"""
from __future__ import annotations

from statistics import median

from .journal import canonical
from .runtime_cost import CostSample

MIN_SAMPLES = 5
CERTIFY_SAMPLES = 8
WINDOW = 32
MIN_COVERAGE = .8
BURN_IN = 3
DRIFT_LIMIT = .5
TOLERANCE = .1
EXECUTOR_STAGES = ('executor-gate', 'executor-group')
EPOCH_STAGES = ('epoch-publication', 'epoch-boundary')  # once per epoch; priced like executor classes
# ETA endpoint stages (expectation, gradient, durable checkpoint): repeated across iterations of one run.
ENDPOINT_STAGES = ('expectation', 'gradient', 'checkpoint-commit')
OVERRUN = 'RESIDUAL_OVERRUN'
CERTIFIED, INSUFFICIENT, LOW_COVERAGE, STALE, DRIFTING = ('CERTIFIED', 'INSUFFICIENT_SAMPLES', 'LOW_HELD_OUT_COVERAGE', 'STALE_POLICY_EPOCH',
                                                     'DRIFTING')


def _class(condition):
    condition = dict(condition)
    if isinstance(condition.get('operations'), list):
        condition['operations'] = [{k: v for k, v in op.items() if k not in {'op_id', 'source_op_id'}}
                                   for op in condition['operations']]
    return canonical(condition)


def _pooled(condition):
    """Class key with gate angles removed (Clifford/diagonal/permutation flags kept), or None without angles."""
    ops = condition.get('operations')
    if not isinstance(ops, list) or all(op.get('angle') is None for op in ops): return None
    return canonical({**condition, 'operations': [{k: v for k, v in op.items() if k not in {'op_id', 'source_op_id', 'angle'}}
                                                  for op in ops]})


def _stages(condition):
    operation = condition.get('operation')
    return (EXECUTOR_STAGES if operation in EXECUTOR_STAGES else ENDPOINT_STAGES if operation in ENDPOINT_STAGES
            else EPOCH_STAGES)


def _by_class(collector, stages):
    """Fresh-revision index {class key: rows (oldest first)} of the collector's cost observations in ``stages``."""
    cache = getattr(collector, '_class_index', None)
    revision = getattr(collector, 'revision', None)
    if cache is None or revision is None or cache[0] != revision:
        index = {}
        rows = collector.cost_rows() if hasattr(collector, 'cost_rows') else collector.cost_observations()
        for row in rows:
            if row.stage in EXECUTOR_STAGES or row.stage in EPOCH_STAGES or row.stage in ENDPOINT_STAGES:
                index.setdefault((row.stage in EXECUTOR_STAGES, _class(row.condition)), []).append(row)
                pooled = _pooled(row.condition)
                if pooled is not None: index.setdefault((row.stage in EXECUTOR_STAGES, 'pooled', pooled), []).append(row)
        cache = (revision, index)
        if revision is not None: collector._class_index = cache
    return cache[1]


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


def _assess(rows, policy_epoch, plan_version, burn_in=0):
    """(reason, values, lower, upper, coverage) of the fresh newest-WINDOW values of ``rows`` (oldest first)."""
    fresh = [r for r in rows if _fresh(r, policy_epoch, plan_version)]
    values = _per_unit(fresh[burn_in:][-WINDOW:])
    if not values: return (STALE if rows and not fresh else INSUFFICIENT), values, None, None, None
    half = len(values) // 2
    train, held = values[:half] or values, values[half:]
    lower, upper = min(train), max(train)
    coverage = sum(lower * (1 - TOLERANCE) <= v <= upper * (1 + TOLERANCE) for v in held) / len(held)
    base = median(train)
    drifting = abs(median(held) - base) > DRIFT_LIMIT * abs(base)
    reason = (INSUFFICIENT if len(values) < CERTIFY_SAMPLES else DRIFTING if drifting
              else LOW_COVERAGE if coverage < MIN_COVERAGE else CERTIFIED)
    return reason, values, min(lower, *held), max(upper, *held), coverage


def _note(decisions, key, reason):
    if decisions is not None: decisions[key] = reason


def _select(collector, condition, stages, policy_epoch, plan_version, decisions):
    """(label, reason, values, lower, upper, coverage) of the exact class, else of its angle-pooled class.

    Pooling is tried only when the exact class lacks samples; it must pass the
    same held-out certification, so it is accepted only where the measured
    durations do not depend on the angle values.
    """
    index, executor = _by_class(collector, stages), stages is EXECUTOR_STAGES
    key = _class(condition)
    result = _assess(index.get((executor, key), []), policy_epoch, plan_version, BURN_IN)
    _note(decisions, key, result[0])
    pooled = _pooled(condition)
    if result[0] != INSUFFICIENT or pooled is None: return ('observed',) + result
    other = _assess(index.get((executor, 'pooled', pooled), []), policy_epoch, plan_version, BURN_IN)
    _note(decisions, 'pooled-angle:' + pooled, other[0])
    return ('observed-pooled-angle',) + other if other[0] == CERTIFIED else ('observed',) + result


def observed_task_cost(collector, condition, policy_epoch=None, plan_version=None, *, diagnostic=False, decisions=None):
    """Certified CostSample for an executor task class (or, with ``diagnostic``, the uncertified one, labelled), or None.

    ``decisions`` (if given) receives ``{class_key: reason}`` for the last decision.
    """
    if collector is None or not collector.enabled or not isinstance(condition, dict): return None
    if condition.get('operation') == 'call-residual':
        return _residual_cost(collector, condition, policy_epoch, plan_version, diagnostic, decisions)
    stages = _stages(condition)
    if condition.get('operation') not in stages: return None
    label, reason, values, lower, upper, coverage = _select(collector, condition, stages, policy_epoch, plan_version,
                                                            decisions)
    if not values or len(values) < MIN_SAMPLES: return None
    if reason == CERTIFIED:
        if diagnostic: return None
        return CostSample(label + ':' + condition['operation'], median(values), lower, upper, measured=True)
    if not diagnostic: return None
    return CostSample(f"diagnostic:{condition['operation']}:{reason}:coverage={coverage:.2f}", median(values), lower, upper,
                      measured=False)


def _residual_cost(collector, condition, policy_epoch, plan_version, diagnostic, decisions):
    """Remaining time of a running call of a certified class, given its elapsed seconds.

    The residuals ``d - elapsed`` of the class's fresh durations that exceed the
    elapsed time; a call already past most of them (fewer than ``MIN_SAMPLES``
    longer ones) has no calibrated remainder and stays unknown (OVERRUN).
    """
    inner, elapsed = condition['class'], condition['elapsed_seconds']
    stages = _stages(inner)
    if inner.get('operation') not in stages or diagnostic: return None
    label, reason, values = _select(collector, inner, stages, policy_epoch, plan_version, None)[:3]
    if reason != CERTIFIED: return None
    longer = [v - elapsed for v in values if v > elapsed]
    if len(longer) < MIN_SAMPLES:
        _note(decisions, _class(inner) + ':residual', OVERRUN)
        return None
    return CostSample(label + ':residual:' + inner['operation'], median(longer), min(longer) * (1 - TOLERANCE),
                      max(longer) * (1 + TOLERANCE), measured=True)


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
