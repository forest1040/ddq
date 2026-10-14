"""Gates-only runtime ETA from a fitted Phase 5 end-to-end time model.

The remaining-work snapshot counts semantic gates exactly: window tasks hold
the physical executor operations and the tail holds counted semantic classes.
Each gate becomes the same per-rank features the model was fitted on. The view
assumes the current layout for the tail (S0 keeps it fixed) and does not price
in-flight requests, unclassified overflow or private epochs; those leave the
totals unknown while the partial value is still reported.
"""
from __future__ import annotations

from .journal import decode_gate
from .time_model import gate_class

VIEW_NAME = 'time-model'
BLOCKING_REASONS = {'SUMMARY_CLASS_LIMIT', 'CAPTURE_METADATA_LIMIT', 'ETA_MONITOR_BUDGET',
                    'EVALUATION_INJECTION_ACTIVE', 'POST_SWAP_LAYOUT_UNKNOWN', 'FUTURE_S1_LAYOUT_UNKNOWN',
                    'UNBOUND_PARAMETERS', 'ENDPOINT_ALREADY_PASSED'}
# Shape uncertainty of the tail is exactly what the per-gate model absorbs.
MODELLED_REASONS = {'FUTURE_EXECUTOR_SHAPE_UNKNOWN', 'EXECUTOR_WINDOW_TRUNCATED'}


def remaining_features(snapshot, block_bits, blocks_per_rank, rank_boundary=None):
    """Per-rank work features of the remaining gates, plus reasons they are incomplete."""
    features = {'blocks': blocks_per_rank, 'gates': 0, 'passes': 0}
    reasons = set(snapshot.unknown_reasons) & BLOCKING_REASONS

    def add(data, count):
        key = 'passes:' + gate_class(decode_gate(data), block_bits, rank_boundary)
        features['gates'] += count
        features['passes'] += count * blocks_per_rank
        features[key] = features.get(key, 0) + count * blocks_per_rank

    for task in snapshot.window:
        reasons.update(set(task.unknown_reasons) - MODELLED_REASONS)
        if task.kind == 'publication': continue
        for data in task.condition['operations']: add(data, 1)
    for task in snapshot.tail:
        reasons.update(set(task.unknown_reasons) - MODELLED_REASONS)
        if task.condition.get('operation') == 'semantic-suffix': add(task.condition['class'], task.count)
    if not snapshot.complete: reasons.add('REMAINING_SCAN_INCOMPLETE')
    if snapshot.stale: reasons.add('STALE_SNAPSHOT')
    if snapshot.private: reasons.add('PRIVATE_EPOCH_RESIDUAL_UNCALIBRATED')
    if snapshot.in_flight: reasons.add('IN_FLIGHT_RESIDUAL_UNCALIBRATED')
    return features, sorted(reasons)


def runtime_condition(sim, *, nodes, threads):
    """The time-model condition class of a running async simulator."""
    from dataclasses import asdict
    from .journal import content_hash
    return {'block_bits': sim.block_bits, 'ranks': sim.size, 'nodes': nodes, 'threads': threads,
            'storage': sim.config.amplitude_store, 'backend': sim.config.backend, 'mode': 'distributed-async',
            'executor_policy': content_hash(asdict(sim.policy)), 'frame': sim.config.frame_policy,
            'swap': sim.policy.swap_policy, 'execution': 'auto-native', 'initial_state': None}


def evaluate(snapshot, model, *, condition, configuration_fingerprint, block_bits, blocks_per_rank, rank_boundary=None,
             sync_seconds=None):
    """ETA view shaped like the other runtime views, with model evidence attached."""
    features, reasons = remaining_features(snapshot, block_bits, blocks_per_rank, rank_boundary)
    predicted = model.predict_remaining(features, condition, configuration_fingerprint, sync_seconds)
    known = predicted['expected_seconds'] is not None
    usable = known and not reasons
    return {'lower_seconds': predicted['lower_seconds'] if usable else None,
            'expected_seconds': predicted['expected_seconds'] if usable else None,
            'upper_seconds': predicted['conditional_upper_seconds'] if usable else None,
            'partial_expected_seconds': predicted['expected_seconds'],
            'unknown_reasons': tuple(reasons) + (() if known else (predicted['reason'],)),
            'reason': 'complete' if usable else 'UNKNOWN_REMAINING_WORK' if known else predicted['reason'],
            'evidence': predicted['evidence'], 'out_of_range_features': predicted['out_of_range_features'],
            'warnings': predicted['warnings'], 'model_id': predicted['model_id'],
            'endpoint': 'gates-only-execute', 'remaining_gate_count': features['gates'],
            'assumptions': ['tail gates keep the current layout (S0)', 'per-gate executor cost as calibrated',
                            'monitoring and publication are inside the calibrated execute phase'],
            'interval_kind': 'calibrated sync-latency range plus fit residuals; not a confidence interval'}


def eta_accuracy(runs):
    """Prediction-vs-actual summary of ETA checks recorded by ``svsim benchmark --eta-model``."""
    import math
    from .profile_document import wire_decode
    rows = []
    for run in runs:
        run = wire_decode(run)
        if run['status'] != 'completed' or run['validation']['passed'] is not True: continue
        for trial in run['trials']:
            for check in trial.get('eta_checks', []):
                predicted, observed = check['predicted_expected_seconds'], check['observed_seconds']
                median = check.get('median_sync_predicted_expected_seconds')
                rows.append({'case': run['case']['label'], 'frontier': check['frontier'],
                             'sync_probe_seconds': check.get('sync_probe_seconds'),
                             'median_sync_relative_error': None if median is None or observed <= 0 else (median - observed) / observed,
                             'remaining_gate_count': check['remaining_gate_count'], 'evidence': check['evidence'],
                             'predicted_expected_seconds': predicted, 'observed_seconds': observed,
                             'relative_error': None if predicted is None or observed <= 0 else (predicted - observed) / observed,
                             'within_interval': None if check['predicted_upper_seconds'] is None else
                                 check['predicted_lower_seconds'] <= observed <= check['predicted_upper_seconds'],
                             'unknown_reasons': check['unknown_reasons']})
    median_errors = sorted(abs(r['median_sync_relative_error']) for r in rows if r['median_sync_relative_error'] is not None)
    errors = sorted(abs(r['relative_error']) for r in rows if r['relative_error'] is not None)
    covered = [r['within_interval'] for r in rows if r['within_interval'] is not None]
    def quantile(q):
        if not errors: return None
        index = (len(errors) - 1) * q
        low, high = math.floor(index), math.ceil(index)
        return errors[low] + (errors[high] - errors[low]) * (index - low)
    return {'schema_version': 1, 'kind': 'runtime-eta-accuracy', 'check_count': len(rows),
            'predicted_count': len(errors), 'median_abs_relative_error': quantile(.5),
            'p95_abs_relative_error': quantile(.95), 'worst_abs_relative_error': errors[-1] if errors else None,
            'interval_coverage': sum(covered) / len(covered) if covered else None,
            'median_sync_median_abs_relative_error': median_errors[(len(median_errors) - 1) // 2] if median_errors else None,
            'median_sync_worst_abs_relative_error': median_errors[-1] if median_errors else None,
            'primary_prediction': 'sync latency probed immediately before each check (untimed)',
            'target': {'statistic': 'median |relative error|', 'threshold': 0.30,
                       'meets_target': None if not errors else quantile(.5) <= 0.30},
            'checks': rows,
            'notes': ['observed is the rank-MAX actual gates-only execution time from the check frontier',
                      'the 30% target is an evaluation goal, not a guarantee']}
