"""End-to-end phase time model fitted to measured workload trials, model v2.

Each phase is a nonnegative linear model over bounded work features and the
same features scaled by the trial's measured device sync latency. Fits use
only completed, oracle-validated runs of one configuration and condition class.
Predictions outside the calibrated feature range are labelled extrapolated;
features never calibrated make the total unknown instead of zero.
"""
from __future__ import annotations

from copy import deepcopy
import math

import numpy as np

from .journal import content_hash
from .ownership import OwnershipMap
from .profile_document import read_document, wire_decode, wire_encode, write_document
from .workload_spec import bounded_tree

MODEL_VERSION = 'end-to-end-sync-linear-nnls-v2'
PHASES = ('setup', 'compile', 'execute', 'flush', 'cleanup')
CONDITION_KEYS = ('block_bits', 'ranks', 'nodes', 'threads', 'storage', 'backend', 'mode', 'executor_policy',
                  'frame', 'swap', 'execution', 'initial_state')
MAX_RUNS = 256
STABLE_SYNC_RATIO = 2.0
RELATIVE_FLOOR_SECONDS = 1e-3


def gate_class(gate, block_bits, rank_boundary=None):
    """Kernel route class: local Block pass, joint group across intra-rank upper bits,
    or remote exchange when a target is a rank bit (qubit >= ``rank_boundary``)."""
    upper = [q for q in gate.targets if q >= block_bits]
    if gate.kind == 'PAULI_ROT':
        upper = [q for q, p in zip(gate.targets, gate.paulis) if q >= block_bits and p in {'X', 'Y'}]
    if gate.diagonal: upper = []
    route = 'local' if not upper else 'remote' if rank_boundary is not None and max(upper) >= rank_boundary else 'joint'
    return f'{gate.kind}:{route}'


def workload_features(workload, block_bits, ranks):
    """Return exact per-rank work counts, or None plus reasons when lowering is incomplete."""
    ownership = OwnershipMap(workload.qubits, block_bits, ranks)
    lowered, unsupported = workload.lowered()
    if unsupported: return None, unsupported
    local = ownership.blocks_per_rank
    boundary = workload.qubits - ownership.rank_bits
    gates = sum(entry.count for entry in lowered)
    features = {'constant': 1, 'blocks': local, 'gates': gates, 'passes': gates * local}
    for entry in lowered:
        key = 'passes:' + gate_class(entry.gate, block_bits, boundary)
        features[key] = features.get(key, 0) + entry.count * local
    return features, []


def phase_features(phase, available):
    base = {'setup': ['constant', 'blocks'], 'compile': ['constant', 'gates'],
            'execute': ['constant', 'gates'], 'flush': ['constant', 'blocks'],
            'cleanup': ['constant', 'blocks', 'passes']}[phase]
    if phase == 'execute': base += sorted(k for k in available if k.startswith('passes:'))
    return base


def nnls(a, b, iterations=None):
    """Lawson-Hanson nonnegative least squares for small dense systems."""
    a, b = np.asarray(a, float), np.asarray(b, float)
    rows, cols = a.shape
    scale = np.maximum(np.abs(a).max(axis=0), 1e-300)
    a = a / scale
    x, passive = np.zeros(cols), np.zeros(cols, bool)
    for _ in range(iterations or 3 * cols + 10):
        w = a.T @ (b - a @ x)
        if passive.all() or w[~passive].max(initial=-1) <= 1e-12 * max(1.0, np.abs(b).max()): break
        passive[np.argmax(np.where(passive, -np.inf, w))] = True
        while True:
            z = np.zeros(cols)
            z[passive] = np.linalg.lstsq(a[:, passive], b, rcond=None)[0]
            if (z[passive] > 0).all():
                x = z
                break
            mask = passive & (z <= 0)
            alpha = np.min(x[mask] / np.maximum(x[mask] - z[mask], 1e-300))
            x = x + alpha * (z - x)
            passive &= x > 1e-15
            x[~passive] = 0
    return x / scale


def run_condition(run):
    case = run['case']
    return {key: case[key] for key in CONDITION_KEYS}


def _validated(run):
    if run.get('kind') != 'benchmark-run' or run.get('run_type') != 'workload-execution':
        raise ValueError('time model input must be a workload-execution benchmark run')
    if run['status'] != 'completed' or run['validation']['passed'] is not True:
        raise ValueError(f'run {run["run_id"]} is not a completed validated run')
    return run


def _trial_rows(run):
    trials = run['trials']
    if not trials: raise ValueError(f'run {run["run_id"]} has no measured trials')
    if any('sync_probe_median_seconds' not in t for t in trials):
        raise ValueError(f'run {run["run_id"]} lacks the pre-trial sync probe; re-measure with this version')
    return trials


def _stable(trial):
    """Keep trials whose device sync state did not switch between the pre and post probes."""
    before, after = trial['sync_probe_median_seconds'], trial.get('sync_probe_after_seconds')
    if after is None: return True  # runs recorded before the post probe existed
    return max(before, after) <= STABLE_SYNC_RATIO * min(before, after)


def fit(runs):
    """Fit phase models; every run must share configuration fingerprint and condition class.

    Each trial is one row. Durable commits make phase time depend on the
    device sync latency seen by that trial, so every work feature also enters
    multiplied by the trial's measured sync-probe latency.
    """
    runs = [_validated(wire_decode(deepcopy(run))) for run in runs]
    if not runs or len(runs) > MAX_RUNS: raise ValueError(f'time model needs 1..{MAX_RUNS} runs')
    fingerprint, condition = runs[0]['configuration_fingerprint'], run_condition(runs[0])
    for run in runs:
        if run['configuration_fingerprint'] != fingerprint: raise ValueError('runs mix hardware/build configurations')
        if run_condition(run) != condition: raise ValueError('runs mix execution condition classes')
    if len({run['run_id'] for run in runs}) != len(runs): raise ValueError('duplicate calibration run')
    features = [{row['feature']: row['count'] for row in run['work']['features']} for run in runs]
    candidates = [(f, t) for f, run in zip(features, runs) for t in _trial_rows(run)]
    rows = [(f, t) for f, t in candidates if _stable(t)]
    if not rows: raise ValueError('no calibration trial kept one device sync state')
    sync = sorted(t['sync_probe_median_seconds'] for _, t in rows)
    names = sorted(set().union(*features))
    phases, ranges = {}, []
    for phase in PHASES:
        columns = phase_features(phase, names)
        if len(runs) < len(columns) + 1:
            raise ValueError(f'phase {phase} needs at least {len(columns)+1} calibration runs, got {len(runs)}')
        matrix = np.asarray([[f.get(c, 0) for c in columns] + [f.get(c, 0) * t['sync_probe_median_seconds'] for c in columns]
                             for f, t in rows], float)
        observed = np.asarray([t['wall_seconds'][phase] for _, t in rows])
        solution = nnls(matrix, observed)
        absolute = np.abs(matrix @ solution - observed)
        # Relative residuals of near-zero phases are noise; they are bounded by the absolute term instead.
        relevant = observed >= RELATIVE_FLOOR_SECONDS
        relative = absolute[relevant] / observed[relevant]
        phases[phase] = {'features': columns, 'base': [float(v) for v in solution[:len(columns)]],
                         'per_sync_second': [float(v) for v in solution[len(columns):]],
                         'max_abs_residual_seconds': float(absolute.max()),
                         'max_abs_relative_residual': float(relative.max()) if relative.size else 0.0}
    for name in names:
        values = [f.get(name, 0) for f in features]
        ranges.append({'feature': name, 'min_count': min(values), 'max_count': max(values)})
    model = {'schema_version': 1, 'kind': 'time-model', 'model_version': MODEL_VERSION,
             'configuration_fingerprint': fingerprint, 'condition': condition,
             'profile_ids': sorted({run['profile_id'] for run in runs}),
             'calibration_run_ids': sorted(run['run_id'] for run in runs),
             'calibration_workload_ids': sorted({run['workload_id'] for run in runs}),
             'phases': phases, 'feature_ranges': ranges,
             'sync_latency': {'min_seconds': sync[0], 'median_seconds': float(np.median(sync)), 'max_seconds': sync[-1],
                              'trial_count': len(sync), 'probe': '4 KiB pwrite+fsync median before each trial',
                              'excluded_unstable_trial_count': len(candidates) - len(rows),
                              'stability_rule': f'before/after probe ratio <= {STABLE_SYNC_RATIO}'},
             'evidence': 'derived', 'scope': 'same-host',
             'interval_kind': 'calibrated sync-latency range plus fit residuals; not a confidence interval',
             'residual_rule': {'relative_floor_seconds': RELATIVE_FLOOR_SECONDS,
                               'lower': 'fit at minimum calibrated sync latency',
                               'expected': 'fit at median calibrated sync latency',
                               'conditional_upper': 'fit at maximum calibrated sync latency*(1+max relative residual)+max absolute residual'},
             'assumptions': ['phases are serial on the main thread; no overlap credit',
                             'per-pass cost is linear in the device sync latency within one gate route class and Block size',
                             'future device sync latency stays within the calibrated range',
                             'no scaling across rank, thread, node, storage backend or Block size']}
    return TimeModelDocument(_with_id(model))


def _with_id(model):
    encoded = wire_encode(model)
    encoded.pop('model_id', None)
    encoded['model_id'] = content_hash(encoded)
    return encoded


class TimeModelDocument:
    def __init__(self, data):
        bounded_tree(data)
        allowed = {'schema_version', 'kind', 'model_version', 'model_id', 'configuration_fingerprint', 'condition',
                   'profile_ids', 'calibration_run_ids', 'calibration_workload_ids', 'phases', 'feature_ranges',
                   'sync_latency', 'evidence', 'scope', 'interval_kind', 'residual_rule', 'assumptions'}
        if not isinstance(data, dict) or set(data) != allowed: raise ValueError('time model has missing or unknown fields')
        if data['schema_version'] != 1 or data['kind'] != 'time-model' or data['model_version'] != MODEL_VERSION:
            raise ValueError('unsupported time model kind/version')
        normalized = wire_encode(data)
        if normalized['model_id'] != content_hash({k: v for k, v in normalized.items() if k != 'model_id'}):
            raise ValueError('model_id content hash mismatch')
        decoded = wire_decode(normalized)
        if set(decoded['phases']) != set(PHASES): raise ValueError('time model phases are incomplete')
        for phase, row in decoded['phases'].items():
            for kind in ('base', 'per_sync_second'):
                values = row[kind]
                if len(values) != len(row['features']) or any(
                        type(v) not in (int, float) or not math.isfinite(v) or v < 0 for v in values):
                    raise ValueError(f'invalid {phase} {kind} coefficients')
        self._data = decoded

    @classmethod
    def load(cls, path): return cls(read_document(path, max_bytes=4 * 1024**2))

    @property
    def data(self): return deepcopy(self._data)

    @property
    def model_id(self): return self._data['model_id']

    def to_dict(self): return wire_encode(self._data)

    def save(self, path, overwrite=False): write_document(path, self.to_dict(), overwrite=overwrite)

    def predict(self, features, condition, configuration_fingerprint, sync_seconds=None):
        """Phase and total lower/expected/conditional-upper seconds, or unknown with reasons.

        ``sync_seconds`` evaluates expected at a freshly probed device sync
        latency instead of the calibration median; lower/upper keep the
        calibrated range widened to include it.
        """
        return self._evaluate(features, condition, configuration_fingerprint, sync_seconds,
                              phases=PHASES, dropped=(), ignored=())

    def predict_remaining(self, features, condition, configuration_fingerprint, sync_seconds=None):
        """Execute-phase time of remaining gate work for a gates-only runtime ETA.

        Setup, compile, flush and cleanup are outside the gates-only endpoint,
        and the run intercept was already paid, so only the execute row is used
        without its constant term. The initial state only affects setup.
        """
        result = self._evaluate(features, condition, configuration_fingerprint, sync_seconds,
                                phases=('execute',), dropped=('constant',), ignored=('initial_state',))
        return {**result, 'endpoint': 'gates-only-execute'}

    def _evaluate(self, features, condition, configuration_fingerprint, sync_seconds, *, phases, dropped, ignored):
        model = self._data
        if sync_seconds is not None and (type(sync_seconds) not in (int, float) or not math.isfinite(sync_seconds)
                                         or sync_seconds <= 0):
            raise ValueError('sync_seconds must be positive and finite')
        mismatch = [key for key in CONDITION_KEYS if key not in ignored and model['condition'].get(key) != condition.get(key)]
        if configuration_fingerprint != model['configuration_fingerprint']: mismatch.append('configuration_fingerprint')
        if mismatch: return _unknown('TIME_MODEL_CONDITION_MISMATCH', mismatch=mismatch)
        if features is None: return _unknown('LOWERING_INCOMPLETE')
        ranges = {row['feature']: row for row in model['feature_ranges']}
        missing = sorted(k for k, v in features.items() if v and k not in ranges)
        if missing: return _unknown('UNCALIBRATED_WORK_FEATURES', missing=missing)
        used = {f for phase in phases for f in model['phases'][phase]['features']} - set(dropped)
        outside = sorted(k for k, row in ranges.items()
                         if k in used and not row['min_count'] <= features.get(k, 0) <= row['max_count'])
        selected, phases, totals = phases, {}, {'lower': 0.0, 'expected': 0.0, 'upper': 0.0}
        calibrated = model['sync_latency']
        latency = {'min_seconds': calibrated['min_seconds'], 'max_seconds': calibrated['max_seconds'],
                   'median_seconds': calibrated['median_seconds'] if sync_seconds is None else sync_seconds}
        sync_outside = sync_seconds is not None and not calibrated['min_seconds'] <= sync_seconds <= calibrated['max_seconds']
        if sync_seconds is not None:
            latency['min_seconds'] = min(latency['min_seconds'], sync_seconds)
            latency['max_seconds'] = max(latency['max_seconds'], sync_seconds)
        scenarios = {'fast_sync_seconds': 0.0, 'slow_sync_seconds': 0.0}
        for phase in selected:
            row = model['phases'][phase]
            base = sum(c * features.get(f, 0) for c, f in zip(row['base'], row['features']) if f not in dropped)
            per_sync = sum(c * features.get(f, 0) for c, f in zip(row['per_sync_second'], row['features']) if f not in dropped)
            lower = base + per_sync * latency['min_seconds']
            expected = base + per_sync * latency['median_seconds']
            upper = ((base + per_sync * latency['max_seconds']) * (1 + row['max_abs_relative_residual'])
                     + row['max_abs_residual_seconds'])
            if not all(math.isfinite(v) for v in (lower, expected, upper)): return _unknown('TIME_OVERFLOW')
            phases[phase] = {'lower_seconds': lower, 'expected_seconds': expected, 'conditional_upper_seconds': upper}
            scenarios['fast_sync_seconds'] += lower
            scenarios['slow_sync_seconds'] += base + per_sync * latency['max_seconds']
            for kind, value in (('lower', lower), ('expected', expected), ('upper', upper)): totals[kind] += value
        return {'lower_seconds': totals['lower'], 'expected_seconds': totals['expected'],
                'conditional_upper_seconds': totals['upper'], 'phases': phases,
                'evidence': 'extrapolated' if outside or sync_outside else 'derived', 'out_of_range_features': outside,
                'sync_scenarios': scenarios,
                'sync_source': 'calibration-median' if sync_seconds is None else 'supplied-probe',
                'warnings': (['DEVICE_SYNC_LATENCY_NONSTATIONARY: calibrated sync latency spans more than 2x; '
                              'expected assumes one state for the whole run'] if calibrated['max_seconds'] > 2 * calibrated['min_seconds'] else [])
                            + (['SYNC_LATENCY_OUTSIDE_CALIBRATED_RANGE'] if sync_outside else []),
                'missing_features': [], 'model_id': model['model_id'], 'model_version': MODEL_VERSION,
                'sync_latency_assumption': latency,
                'reason': 'outside calibrated feature range' if outside else 'within calibrated feature range'}


def _unknown(reason, *, mismatch=(), missing=()):
    return {'lower_seconds': None, 'expected_seconds': None, 'conditional_upper_seconds': None, 'phases': None,
            'evidence': 'unknown', 'out_of_range_features': [], 'missing_features': list(missing), 'warnings': [],
            'condition_mismatch': list(mismatch), 'model_id': None, 'model_version': MODEL_VERSION, 'reason': reason}
