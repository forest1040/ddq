import importlib.util
from dataclasses import replace

import pytest

from svsim.journal import content_hash
from svsim.profile_document import ProfileDocument, wire_encode
from svsim.runtime_time_model_eta import VIEW_NAME, eta_accuracy, remaining_features
from svsim.time_model import PHASES, fit, workload_features
from svsim.workload_spec import WorkloadSpec

mpi = pytest.mark.skipif(importlib.util.find_spec('mpi4py') is None, reason='mpi4py extra not installed')
PER_PASS = {'H:local': 4e-3, 'H:joint': 6e-3, 'RX:local': 5e-3}


def _condition(policy_hash):
    return {'block_bits': 3, 'ranks': 1, 'nodes': 1, 'threads': 2, 'storage': 'ram', 'backend': 'buffered',
            'mode': 'distributed-async', 'executor_policy': policy_hash, 'frame': 'materialize', 'swap': 'S0',
            'execution': 'auto-native', 'initial_state': 'zero'}


def _seconds(features, phase):
    if phase == 'execute':
        return .01 + 1e-3 * features['gates'] + sum(c * features.get('passes:' + k, 0) for k, c in PER_PASS.items())
    return .02 + 1e-4 * features['blocks']


def _model(fingerprint, policy_hash):
    runs = []
    for n in (4, 5, 6):
        for name, body in (('empty', []), ('h', [{'kind': 'H', 'targets': [0]}]),
                           ('rx', [{'kind': 'RX', 'targets': [1], 'angle': .37}]),
                           ('joint', [{'kind': 'H', 'targets': [n - 1]}])):
            for count in ((0,) if not body else (1, 4)):
                ops = [{'kind': 'REPEAT', 'count': count, 'body': body}] if body else []
                w = WorkloadSpec({'kind': 'workload', 'schema_version': 1, 'qubits': n, 'operations': ops})
                f, _ = workload_features(w, 3, 1)
                trials = [{'wall_seconds': {p: _seconds(f, p) for p in PHASES}, 'sync_probe_median_seconds': 1e-3}]
                trials[0]['wall_seconds']['total'] = sum(trials[0]['wall_seconds'].values())
                doc = {'kind': 'benchmark-run', 'run_type': 'workload-execution', 'workload_id': w.workload_id,
                       'profile_id': 'p', 'configuration_fingerprint': fingerprint,
                       'case': {**_condition(policy_hash), 'label': f'{name}{n}{count}'},
                       'work': {'features': [{'feature': k, 'count': v} for k, v in sorted(f.items())]},
                       'trials': trials, 'status': 'completed', 'validation': {'passed': True}}
                encoded = wire_encode(doc)
                encoded['run_id'] = content_hash(encoded)
                runs.append(encoded)
    return fit(runs)


def test_predict_remaining_uses_execute_without_intercept_and_ignores_initial_state():
    model = _model('fp', 'policy')
    features = {'blocks': 4, 'gates': 3, 'passes': 12, 'passes:H:local': 8, 'passes:RX:local': 4}
    condition = {**_condition('policy'), 'initial_state': None}
    predicted = model.predict_remaining(features, condition, 'fp')
    expected = 1e-3 * 3 + 4e-3 * 8 + 5e-3 * 4
    assert predicted['expected_seconds'] == pytest.approx(expected, rel=1e-6)
    assert set(predicted['phases']) == {'execute'} and predicted['endpoint'] == 'gates-only-execute'
    assert model.predict(features, condition, 'fp')['condition_mismatch'] == ['initial_state']
    assert model.predict_remaining(features, {**condition, 'swap': 'S2'}, 'fp')['expected_seconds'] is None


class _Task:
    def __init__(self, condition, count=1, kind='executor', unknown=()):
        self.condition, self.count, self.kind, self.unknown_reasons = condition, count, kind, unknown


class _Snapshot:
    def __init__(self, window, tail, **flags):
        self.window, self.tail = window, tail
        self.unknown_reasons = flags.get('unknown', ())
        self.complete, self.stale = flags.get('complete', True), False
        self.private, self.in_flight = False, flags.get('in_flight', ())


def _gate(kind, target, angle=None):
    from svsim.ir import GateNormalizer
    from svsim.journal import gate_data
    return gate_data(GateNormalizer(6).gate(kind, (target,), **({'angle': angle} if angle else {})))


def test_remaining_features_count_window_operations_and_tail_classes_exactly():
    window = [_Task({'operations': [_gate('H', 0), _gate('RX', 1, .37)]}),
              _Task({'operation': 'epoch-publication'}, kind='publication')]
    tail = [_Task({'operation': 'semantic-suffix', 'class': _gate('H', 5)}, count=7,
                  unknown=('FUTURE_EXECUTOR_SHAPE_UNKNOWN',))]
    features, reasons = remaining_features(_Snapshot(window, tail), 3, 8)
    assert features == {'blocks': 8, 'gates': 9, 'passes': 72, 'passes:H:local': 8,
                        'passes:RX:local': 8, 'passes:H:joint': 56}
    assert reasons == []
    _, reasons = remaining_features(_Snapshot(window, tail, complete=False, in_flight=('io',),
                                              unknown=('SUMMARY_CLASS_LIMIT',)), 3, 8)
    assert reasons == ['IN_FLIGHT_RESIDUAL_UNCALIBRATED', 'REMAINING_SCAN_INCOMPLETE', 'SUMMARY_CLASS_LIMIT']


def test_eta_accuracy_summarizes_checks_and_skips_invalid_runs():
    def run(checks, passed=True):
        return {'status': 'completed', 'validation': {'passed': passed}, 'case': {'label': 'x'},
                'trials': [{'eta_checks': checks}]}
    check = lambda p, o, lo=None, hi=None: {'frontier': -1, 'remaining_gate_count': 4, 'evidence': 'derived',
                                            'predicted_expected_seconds': p, 'observed_seconds': o,
                                            'predicted_lower_seconds': lo, 'predicted_upper_seconds': hi,
                                            'unknown_reasons': [], 'median_sync_predicted_expected_seconds': p}
    summary = eta_accuracy([run([check(1.1, 1.0, .9, 1.2), check(.6, 1.0, .4, .5), check(None, 1.0)]),
                            run([check(9.0, 1.0)], passed=False)])
    assert summary['check_count'] == 3 and summary['predicted_count'] == 2
    assert summary['median_abs_relative_error'] == pytest.approx(.25)
    assert summary['worst_abs_relative_error'] == pytest.approx(.4)
    assert summary['interval_coverage'] == .5 and summary['target']['meets_target'] is True


@mpi
def test_async_runtime_reports_time_model_view_for_remaining_gates(tmp_path):
    from dataclasses import asdict
    from mpi4py import MPI
    from svsim.async_scheduler import AsyncDistributedSimulator, AsyncPolicy
    from svsim.distributed import DistributedConfig
    policy = AsyncPolicy()
    model = _model('fp', content_hash(asdict(policy)))
    config = DistributedConfig(str(tmp_path / 'data'), 3, ram_budget_bytes_per_rank=1 << 26, amplitude_store='ram',
                               frame_policy='materialize', swap_policy='S0')
    sim = AsyncDistributedSimulator(5, config, comm=MPI.COMM_SELF, policy=policy)
    try:
        for _ in range(3): sim.gate('H', (0,)); sim.gate('H', (4,))
        sim.runtime.configure_eta(max_bytes=64 * 1024, scan_items=4096, scan_seconds=1.0)
        sim.runtime.attach_time_model(model, configuration_fingerprint='fp')
        result = sim.runtime.update_eta(endpoint_frontier=5, collective=True)
        view = result['views'][VIEW_NAME]
        assert view['remaining_gate_count'] == 6 and view['unknown_reasons'] == ()
        expected = 6e-3 + 3 * 4 * (4e-3 + 6e-3)
        assert view['expected_seconds'] == pytest.approx(expected, rel=1e-6)
        assert result['rank_max_expected_seconds'][VIEW_NAME] == pytest.approx(expected, rel=1e-6)
        assert result['cost_view_sources'][VIEW_NAME] == 'time-model:' + model.model_id
        sim.materialize(frontier=2)
        later = sim.runtime.update_eta(endpoint_frontier=5, collective=True)['views'][VIEW_NAME]
        assert later['remaining_gate_count'] == 3
        sim.runtime.attach_time_model(model, configuration_fingerprint='other-host')
        wrong = sim.runtime.update_eta(endpoint_frontier=5, collective=True)['views'][VIEW_NAME]
        assert wrong['expected_seconds'] is None and 'TIME_MODEL_CONDITION_MISMATCH' in wrong['unknown_reasons']
        sim.materialize()
    finally:
        sim.close()
