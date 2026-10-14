"""End-to-end phase time model fitted to measured workload runs, model v1.

Each phase is a nonnegative linear model over bounded work features. Fits use
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

MODEL_VERSION = 'end-to-end-linear-nnls-v1'
PHASES = ('setup', 'compile', 'execute', 'flush', 'cleanup')
CONDITION_KEYS = ('block_bits', 'ranks', 'nodes', 'threads', 'storage', 'backend', 'mode', 'frame',
                  'swap', 'execution', 'initial_state')
MAX_RUNS = 256
RELATIVE_FLOOR_SECONDS = 1e-3


def gate_class(gate, block_bits):
    """Kernel route class: local block pass or joint group across upper Block bits."""
    upper = any(q >= block_bits for q in gate.targets)
    if gate.kind == 'PAULI_ROT':
        upper = any(q >= block_bits and p in {'X', 'Y'} for q, p in zip(gate.targets, gate.paulis))
    if gate.diagonal: upper = False
    return f'{gate.kind}:{"joint" if upper else "local"}'


def workload_features(workload, block_bits, ranks):
    """Return exact per-rank work counts, or None plus reasons when lowering is incomplete."""
    ownership = OwnershipMap(workload.qubits, block_bits, ranks)
    lowered, unsupported = workload.lowered()
    if unsupported: return None, unsupported
    local = ownership.blocks_per_rank
    gates = sum(entry.count for entry in lowered)
    features = {'constant': 1, 'blocks': local, 'gates': gates, 'passes': gates * local}
    for entry in lowered:
        key = 'passes:' + gate_class(entry.gate, block_bits)
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


def fit(runs):
    """Fit phase models; every run must share configuration fingerprint and condition class."""
    runs = [_validated(wire_decode(deepcopy(run))) for run in runs]
    if not runs or len(runs) > MAX_RUNS: raise ValueError(f'time model needs 1..{MAX_RUNS} runs')
    fingerprint, condition = runs[0]['configuration_fingerprint'], run_condition(runs[0])
    for run in runs:
        if run['configuration_fingerprint'] != fingerprint: raise ValueError('runs mix hardware/build configurations')
        if run_condition(run) != condition: raise ValueError('runs mix execution condition classes')
    if len({run['run_id'] for run in runs}) != len(runs): raise ValueError('duplicate calibration run')
    features = [{row['feature']: row['count'] for row in run['work']['features']} for run in runs]
    names = sorted(set().union(*features))
    phases, ranges = {}, []
    for phase in PHASES:
        columns = phase_features(phase, names)
        if len(runs) < len(columns) + 1:
            raise ValueError(f'phase {phase} needs at least {len(columns)+1} calibration runs, got {len(runs)}')
        matrix = [[f.get(c, 0) for c in columns] for f in features]
        fitted = {}
        for kind, field in (('expected', 'median_seconds'), ('lower', 'min_seconds'), ('upper', 'max_seconds')):
            fitted[kind] = [float(v) for v in nnls(matrix, [run['summary'][phase][field] for run in runs])]
        predicted = np.asarray(matrix, float) @ np.asarray(fitted['expected'])
        observed = np.asarray([run['summary'][phase]['median_seconds'] for run in runs])
        absolute = np.abs(predicted - observed)
        # Relative residuals of near-zero phases are noise; they are bounded by the absolute term instead.
        relevant = observed >= RELATIVE_FLOOR_SECONDS
        relative = absolute[relevant] / observed[relevant]
        phases[phase] = {'features': columns, **fitted, 'max_abs_residual_seconds': float(absolute.max()),
                         'max_abs_relative_residual': float(relative.max()) if relative.size else 0.0}
    for name in names:
        values = [f.get(name, 0) for f in features]
        ranges.append({'feature': name, 'min_count': min(values), 'max_count': max(values)})
    model = {'schema_version': 1, 'kind': 'time-model', 'model_version': MODEL_VERSION,
             'configuration_fingerprint': fingerprint, 'condition': condition,
             'profile_ids': sorted({run['profile_id'] for run in runs}),
             'calibration_run_ids': sorted(run['run_id'] for run in runs),
             'calibration_workload_ids': sorted({run['workload_id'] for run in runs}),
             'phases': phases, 'feature_ranges': ranges, 'evidence': 'derived', 'scope': 'same-host',
             'interval_kind': 'nonnegative fits to per-run sample median/min/max; not a confidence interval',
             'residual_rule': {'relative_floor_seconds': RELATIVE_FLOOR_SECONDS,
                               'conditional_upper': 'max(upper fit, expected*(1+max relative residual)+max absolute residual)'},
             'assumptions': ['phases are serial on the main thread; no overlap credit',
                             'per-pass cost is constant within one gate route class and Block size',
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
                   'evidence', 'scope', 'interval_kind', 'residual_rule', 'assumptions'}
        if not isinstance(data, dict) or set(data) != allowed: raise ValueError('time model has missing or unknown fields')
        if data['schema_version'] != 1 or data['kind'] != 'time-model' or data['model_version'] != MODEL_VERSION:
            raise ValueError('unsupported time model kind/version')
        normalized = wire_encode(data)
        if normalized['model_id'] != content_hash({k: v for k, v in normalized.items() if k != 'model_id'}):
            raise ValueError('model_id content hash mismatch')
        decoded = wire_decode(normalized)
        if set(decoded['phases']) != set(PHASES): raise ValueError('time model phases are incomplete')
        for phase, row in decoded['phases'].items():
            for kind in ('expected', 'lower', 'upper'):
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

    def predict(self, features, condition, configuration_fingerprint):
        """Phase and total lower/expected/conditional-upper seconds, or unknown with reasons."""
        model = self._data
        mismatch = [key for key in CONDITION_KEYS if model['condition'].get(key) != condition.get(key)]
        if configuration_fingerprint != model['configuration_fingerprint']: mismatch.append('configuration_fingerprint')
        if mismatch: return _unknown('TIME_MODEL_CONDITION_MISMATCH', mismatch=mismatch)
        if features is None: return _unknown('LOWERING_INCOMPLETE')
        ranges = {row['feature']: row for row in model['feature_ranges']}
        missing = sorted(k for k, v in features.items() if v and k not in ranges)
        if missing: return _unknown('UNCALIBRATED_WORK_FEATURES', missing=missing)
        outside = sorted(k for k, row in ranges.items() if not row['min_count'] <= features.get(k, 0) <= row['max_count'])
        phases, totals = {}, {'lower': 0.0, 'expected': 0.0, 'upper': 0.0}
        for phase in PHASES:
            row = model['phases'][phase]
            values = {kind: sum(c * features.get(f, 0) for c, f in zip(row[kind], row['features']))
                      for kind in ('lower', 'expected', 'upper')}
            expected = values['expected']
            lower = min(values['lower'], expected)
            upper = max(values['upper'], expected * (1 + row['max_abs_relative_residual']) + row['max_abs_residual_seconds'])
            if not all(math.isfinite(v) for v in (lower, expected, upper)): return _unknown('TIME_OVERFLOW')
            phases[phase] = {'lower_seconds': lower, 'expected_seconds': expected, 'conditional_upper_seconds': upper}
            for kind, value in (('lower', lower), ('expected', expected), ('upper', upper)): totals[kind] += value
        return {'lower_seconds': totals['lower'], 'expected_seconds': totals['expected'],
                'conditional_upper_seconds': totals['upper'], 'phases': phases,
                'evidence': 'extrapolated' if outside else 'derived', 'out_of_range_features': outside,
                'missing_features': [], 'model_id': model['model_id'], 'model_version': MODEL_VERSION,
                'reason': 'outside calibrated feature range' if outside else 'within calibrated feature range'}


def _unknown(reason, *, mismatch=(), missing=()):
    return {'lower_seconds': None, 'expected_seconds': None, 'conditional_upper_seconds': None, 'phases': None,
            'evidence': 'unknown', 'out_of_range_features': [], 'missing_features': list(missing),
            'condition_mismatch': list(mismatch), 'model_id': None, 'model_version': MODEL_VERSION, 'reason': reason}
