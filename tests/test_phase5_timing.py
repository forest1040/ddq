import importlib.util
import json
from pathlib import Path

import numpy as np
import pytest

from svsim.benchmark_run import CapacityError, RunCase, RunOptions, calibration_suite, run_cases
from svsim.journal import content_hash
from svsim.profile_document import ProfileDocument, wire_encode
from svsim.resource_estimator import EstimateOptions, estimate
from svsim.time_model import PHASES, TimeModelDocument, fit, nnls, workload_features
from svsim.workload_spec import WorkloadSpec, gate_arguments

mpi = pytest.mark.skipif(importlib.util.find_spec('mpi4py') is None, reason='mpi4py extra not installed')
TRUE = {'setup': {'constant': .05, 'blocks': 1e-4}, 'compile': {'constant': 1e-3, 'gates': 2e-3},
        'execute': {'gates': 5e-3, 'passes:H:local': 4e-4, 'passes:H:joint': 9e-4, 'passes:RX:local': 5e-4},
        'flush': {'constant': 1e-4, 'blocks': 2e-3}, 'cleanup': {'constant': 2e-3, 'passes': 1e-6}}
CONDITION = {'block_bits': 3, 'ranks': 1, 'nodes': 1, 'threads': 1, 'storage': 'ram', 'backend': 'buffered',
             'mode': 'distributed-sync', 'executor_policy': None, 'frame': 'materialize', 'swap': 'S0', 'execution': 'auto-native',
             'initial_state': 'zero'}


def _workload(n, operations, **extra):
    return WorkloadSpec({'kind': 'workload', 'schema_version': 1, 'qubits': n, 'operations': operations, **extra})


def _profile():
    return ProfileDocument.create({}, {}, {'ranks': 1, 'nodes': 1, 'threads': 1})


SYNC_COST = {'execute': {'passes': 2.0}, 'flush': {'blocks': 3.0}}  # seconds per count per sync-second


def _phase_seconds(features, phase, sync):
    return (sum(c * features.get(k, 0) for k, c in TRUE[phase].items()) +
            sync * sum(c * features.get(k, 0) for k, c in SYNC_COST.get(phase, {}).items()))


def _run(workload, profile, *, status='completed', passed=True, condition=None, syncs=(1e-3, 5e-3)):
    features, _ = workload_features(workload, 3, 1)
    trials = [{'wall_seconds': {p: _phase_seconds(features, p, s) for p in PHASES}, 'sync_probe_median_seconds': s}
              for s in syncs]
    for t in trials: t['wall_seconds']['total'] = sum(t['wall_seconds'][p] for p in PHASES)
    summary = {p: {'median_seconds': float(np.median([t['wall_seconds'][p] for t in trials]))} for p in (*PHASES, 'total')}
    data = {'schema_version': 1, 'kind': 'benchmark-run', 'run_type': 'workload-execution',
            'workload_id': workload.workload_id, 'plan_id': None, 'profile_id': profile.profile_id,
            'configuration_fingerprint': profile.configuration_fingerprint,
            'case': {**(condition or CONDITION), 'label': 'x', 'variant': 'default', 'qubits': workload.qubits},
            'work': {'features': [{'feature': k, 'count': v} for k, v in sorted(features.items())]},
            'trials': trials, 'summary': summary, 'status': status,
            'validation': {'oracle': 'eager-reference', 'max_abs_error': 0.0, 'passed': passed}}
    encoded = wire_encode(data)
    encoded['run_id'] = content_hash(encoded)
    return encoded


def _suite(profile):
    docs = calibration_suite(3, (4, 5, 6), (1, 3, 6), classes=('empty', 'h-local', 'rx-local', 'h-joint'))
    return {name: WorkloadSpec(doc) for name, doc in docs.items()}, [_run(WorkloadSpec(d), profile) for d in docs.values()]


def test_nnls_recovers_nonnegative_solution_and_clamps_negative_terms():
    rng = np.random.default_rng(3)
    a = rng.random((20, 4))
    assert np.allclose(nnls(a, a @ [.5, 0, 2, 1e-3]), [.5, 0, 2, 1e-3], atol=1e-9)
    b = a @ [1, -1, 0, 0]
    assert (nnls(a, b) >= 0).all()


def test_fitted_model_predicts_held_out_work_and_labels_extrapolation():
    profile = _profile()
    _, runs = _suite(profile)
    model = fit(runs)
    held = _workload(5, [{'kind': 'REPEAT', 'count': 2, 'body': [{'kind': 'H', 'targets': [0]},
                                                                  {'kind': 'RX', 'targets': [1], 'angle': .37},
                                                                  {'kind': 'H', 'targets': [4]}]}])
    features, _ = workload_features(held, 3, 1)
    truth = lambda sync: sum(_phase_seconds(features, phase, sync) for phase in PHASES)
    predicted = model.predict(features, CONDITION, profile.configuration_fingerprint)
    assert predicted['evidence'] == 'derived'
    assert predicted['sync_latency_assumption'] == {'min_seconds': 1e-3, 'median_seconds': 3e-3, 'max_seconds': 5e-3}
    assert predicted['expected_seconds'] == pytest.approx(truth(3e-3), rel=1e-6)
    assert predicted['lower_seconds'] == pytest.approx(truth(1e-3), rel=1e-6)
    assert predicted['conditional_upper_seconds'] >= truth(5e-3) * (1 - 1e-9)
    assert predicted['sync_scenarios']['slow_sync_seconds'] == pytest.approx(truth(5e-3), rel=1e-6)
    assert predicted['warnings'][0].startswith('DEVICE_SYNC_LATENCY_NONSTATIONARY')
    probed = model.predict(features, CONDITION, profile.configuration_fingerprint, sync_seconds=4e-3)
    assert probed['expected_seconds'] == pytest.approx(truth(4e-3), rel=1e-6) and probed['evidence'] == 'derived'
    outside = model.predict(features, CONDITION, profile.configuration_fingerprint, sync_seconds=2e-2)
    assert outside['evidence'] == 'extrapolated' and 'SYNC_LATENCY_OUTSIDE_CALIBRATED_RANGE' in outside['warnings']
    assert outside['conditional_upper_seconds'] >= truth(2e-2) * (1 - 1e-9)
    with pytest.raises(ValueError):
        model.predict(features, CONDITION, profile.configuration_fingerprint, sync_seconds=0)
    assert set(predicted['phases']) == set(PHASES)
    big = _workload(9, [{'kind': 'REPEAT', 'count': 40, 'body': [{'kind': 'H', 'targets': [0]}]}])
    far = model.predict(workload_features(big, 3, 1)[0], CONDITION, profile.configuration_fingerprint)
    assert far['evidence'] == 'extrapolated' and {'blocks', 'gates'} <= set(far['out_of_range_features'])


def test_uncalibrated_class_or_condition_is_unknown_not_zero():
    profile = _profile()
    _, runs = _suite(profile)
    model = fit(runs)
    pauli = _workload(5, [{'kind': 'PAULI_ROT', 'targets': [0, 1, 2], 'paulis': ['Y', 'X', 'Z'], 'angle': .37}])
    missing = model.predict(workload_features(pauli, 3, 1)[0], CONDITION, profile.configuration_fingerprint)
    assert missing['expected_seconds'] is None and missing['missing_features'] == ['passes:PAULI_ROT:local']
    simple = workload_features(_workload(5, [{'kind': 'H', 'targets': [0]}]), 3, 1)[0]
    assert model.predict(simple, {**CONDITION, 'storage': 'file'}, profile.configuration_fingerprint)['condition_mismatch'] == ['storage']
    assert model.predict(simple, CONDITION, 'other')['expected_seconds'] is None


def test_fit_rejects_mixed_invalid_or_insufficient_runs_and_document_is_tamper_evident(tmp_path):
    profile = _profile()
    workloads, runs = _suite(profile)
    some = next(iter(workloads.values()))
    with pytest.raises(ValueError, match='condition classes'):
        fit(runs + [_run(some, profile, condition={**CONDITION, 'backend': 'direct'})])
    with pytest.raises(ValueError, match='hardware/build'):
        fit(runs + [_run(some, _profile_with_host())])
    with pytest.raises(ValueError, match='not a completed validated'):
        fit(runs + [_run(some, profile, passed=False)])
    with pytest.raises(ValueError, match='calibration runs'):
        fit(runs[:3])
    legacy = json.loads(json.dumps(runs[0]))
    del legacy['trials'][0]['sync_probe_median_seconds']
    with pytest.raises(ValueError, match='sync probe'):
        fit(runs[1:] + [legacy])
    switched = json.loads(json.dumps(runs[0]))
    for trial in switched['trials']: trial['sync_probe_after_seconds'] = trial['sync_probe_median_seconds'] * 5
    switched['trials'][0]['wall_seconds']['execute'] *= 10  # a mid-trial slowdown must not bias the fit
    switched.pop('run_id'); switched['run_id'] = content_hash(switched)
    robust = fit(runs[1:] + [switched])
    assert robust.data['sync_latency']['excluded_unstable_trial_count'] == len(switched['trials'])
    model = fit(runs)
    path = tmp_path / 'model.json'
    model.save(path)
    assert TimeModelDocument.load(path).model_id == model.model_id
    data = json.loads(path.read_text())
    data['phases']['execute']['base'][0] = 1.0
    with pytest.raises(ValueError, match='hash mismatch'):
        TimeModelDocument(data)


def _profile_with_host():
    return ProfileDocument.create({'host_id': 'other'}, {}, {'ranks': 1, 'nodes': 1, 'threads': 1})


def test_estimate_uses_matching_time_model_for_complete_coverage_only():
    profile = _profile()
    _, runs = _suite(profile)
    model = fit(runs)
    spec = _workload(5, [{'kind': 'H', 'targets': [0]}, {'kind': 'H', 'targets': [4]}])
    options = EstimateOptions(block_bits=3, ram_budget_bytes_per_rank=1 << 30, ram_budget_bytes_per_host=1 << 32,
                              disk_budget_bytes_per_host=1 << 30)
    plan = estimate(spec, profile, options, time_model=model)
    assert plan['time']['evidence'] == 'derived' and plan['time']['expected_seconds'] > 0
    assert plan['calibration_coverage']['status'] == 'complete'
    assert plan['calibration_coverage']['missing_components'] == []
    assert estimate(spec, profile, EstimateOptions(block_bits=3, backend='direct'), time_model=model)['time']['expected_seconds'] is None
    qpe = estimate(_workload(5, [{'kind': 'H', 'targets': [0]}], algorithm={'kind': 'ipea-trotter'}), profile, options, time_model=model)
    assert qpe['time']['expected_seconds'] is None and qpe['calibration_coverage']['status'] != 'complete'
    huge = _workload(40, [{'kind': 'REPEAT', 'count': '1000000000000', 'body': [{'kind': 'H', 'targets': [0]}]}])
    dry = estimate(huge, profile, EstimateOptions(block_bits=3), time_model=model)
    assert dry['time']['evidence'] == 'extrapolated' and dry['execution_supported'] is False
    assert dry['analysis']['allocated_amplitudes'] is False


def test_expanded_operations_keep_program_order_and_bound_execution():
    spec = _workload(3, [{'kind': 'H', 'targets': [0]},
                         {'kind': 'REPEAT', 'count': 2, 'body': [{'kind': 'RX', 'targets': [1], 'angle': .1},
                                                                 {'kind': 'H', 'targets': [2]}]}], iterations=2)
    kinds = [(n['kind'], n['targets'][0]) for n in spec.expanded_operations()]
    assert kinds == [('H', 0), ('RX', 1), ('H', 2), ('RX', 1), ('H', 2)] * 2
    assert gate_arguments({'kind': 'RX', 'targets': [1], 'angle': .1})[2]['angle'] == .1
    with pytest.raises(ValueError, match='limit'):
        list(spec.expanded_operations(limit=9))


def test_calibration_suite_varies_one_route_class_at_a_time():
    docs = calibration_suite(3, (4, 5), (1, 2))
    assert len(docs) == 2 + 4 * 2 * 2
    for name, doc in docs.items():
        features, _ = workload_features(WorkloadSpec(doc), 3, 1)
        assert len([k for k in features if k.startswith('passes:')]) == (0 if 'empty' in name else 1)
    with pytest.raises(ValueError):
        calibration_suite(3, (3,), (1,))


@mpi
def test_real_interleaved_runs_validate_and_clean_scratch(tmp_path):
    from mpi4py import MPI
    profile = _profile()
    a = _workload(5, [{'kind': 'H', 'targets': [0]}, {'kind': 'RX', 'targets': [4], 'angle': .37}])
    b = _workload(5, [{'kind': 'H', 'targets': [0]}], policies={'storage': 'file'},
                  initial_state='seeded-random', seed=7)
    options = RunOptions(block_bits=3, repetitions=2, warmup=1, ram_budget_bytes_per_rank=1 << 26,
                         disk_budget_bytes=1 << 26, walltime_seconds=60)
    docs = run_cases([RunCase(a, 'a', 'ram'), RunCase(b, 'b', 'file')], profile, tmp_path, options, MPI.COMM_SELF)
    assert [d['status'] for d in docs] == ['completed', 'completed']
    assert all(d['validation']['passed'] is True for d in docs)
    assert docs[1]['case']['storage'] == 'file' and docs[1]['sample_count'] == '2'
    for doc in docs:
        assert set(doc['summary']) == {*PHASES, 'total'}
        for trial in doc['trials']:
            assert trial['wall_seconds']['total'] == pytest.approx(sum(trial['wall_seconds'][p] for p in PHASES))
    assert int(docs[1]['trials'][0]['disk_payload_bytes']) > 0
    assert all(t['sync_probe_median_seconds'] > 0 for d in docs for t in d['trials'])
    orders = sorted(t['order_index'] for d in docs for t in d['trials'])
    assert orders == [2, 3, 4, 5]  # warmup round excluded, both cases interleaved per round
    assert list(tmp_path.iterdir()) == []
    assert content_hash({k: v for k, v in docs[0].items() if k != 'run_id'}) == docs[0]['run_id']
    model_runs = [_run(w, profile) for w in _suite(profile)[0].values()]
    assert fit(model_runs)  # synthetic and real documents share one schema


@mpi
def test_runs_reject_unsupported_policies_and_budgets_before_creating_data(tmp_path):
    from mpi4py import MPI
    profile = _profile()
    spec = _workload(5, [{'kind': 'H', 'targets': [0]}])
    with pytest.raises(CapacityError, match='RAM budget'):
        run_cases([RunCase(spec, 'a')], profile, tmp_path, RunOptions(block_bits=3, ram_budget_bytes_per_rank=1024), MPI.COMM_SELF)
    keep = _workload(5, [{'kind': 'H', 'targets': [0]}], policies={'frame': 'keep'})
    with pytest.raises(CapacityError, match='frame=keep'):
        run_cases([RunCase(keep, 'k')], profile, tmp_path, RunOptions(block_bits=3), MPI.COMM_SELF)
    assert list(Path(tmp_path).iterdir()) == []


@mpi
def test_cli_benchmark_fit_estimate_and_report_flow(tmp_path, monkeypatch):
    from svsim.cli import svsim_main
    import svsim.cli as cli
    profile = _profile()
    monkeypatch.setattr(cli, 'probe_profile', lambda *a, **k: profile)  # same build/host for the test
    profile.save(tmp_path / 'profile.json')
    assert svsim_main(['calibration-suite', '--block-bits', '3', '--qubits', '4,5', '--counts', '1,2',
                       '--output-dir', str(tmp_path / 'suite')]) == 0
    common = ['--profile', str(tmp_path / 'profile.json'), '--scratch', str(tmp_path / 'scratch'),
              '--ram-budget-per-rank', str(1 << 26), '--disk-budget', str(1 << 26), '--wall-seconds', '120',
              '--block-bits', '3', '--repetitions', '1', '--warmup', '0']
    suite = sorted((tmp_path / 'suite').glob('*.json'))
    args = [x for f in suite for x in ('--workload', str(f))]
    assert svsim_main(['benchmark', *args, '--output-dir', str(tmp_path / 'cal'), *common]) == 0
    runs = [x for f in sorted((tmp_path / 'cal').glob('*.json')) for x in ('--run', str(f))]
    assert svsim_main(['calibrate-time', *runs, '--output', str(tmp_path / 'model.json')]) == 0
    held = tmp_path / 'held.json'
    held.write_text(json.dumps({'kind': 'workload', 'schema_version': 1, 'qubits': 5,
                                'operations': [{'kind': 'H', 'targets': [0]}, {'kind': 'H', 'targets': [4]}]}))
    plan = tmp_path / 'held.plan.json'
    assert svsim_main(['estimate', '--workload', str(held), '--profile', str(tmp_path / 'profile.json'),
                       '--block-bits', '3', '--ram-budget-per-rank', str(1 << 26), '--ram-budget-per-host', str(1 << 32),
                       '--disk-budget-per-host', str(1 << 26), '--time-model', str(tmp_path / 'model.json'),
                       '--output', str(plan)]) == 0
    assert svsim_main(['benchmark', '--workload', str(held), '--plan', str(plan), '--label', 'held',
                       '--output-dir', str(tmp_path / 'held'), *common]) == 0
    model = json.loads((tmp_path / 'model.json').read_text())
    (tmp_path / 'campaign.json').write_text(json.dumps({
        'schema_version': 1, 'kind': 'comparison-campaign', 'calibration_workload_ids': model['calibration_workload_ids'],
        'cases': [{'case_id': 'held', 'plan': 'held.plan.json', 'run': 'held/held.run.json', 'variant': 'default', 'group': 'g'}]}))
    out = ['report', '--campaign', str(tmp_path / 'campaign.json'), '--output', str(tmp_path / 'report.json'),
           '--markdown', str(tmp_path / 'report.md')]
    assert svsim_main(out) == 0
    report = json.loads((tmp_path / 'report.json').read_text())
    case = report['cases'][0]
    assert case['status'] == 'compared' and case['held_out'] is True and 'compile' in case['phases']
    assert 'held' in (tmp_path / 'report.md').read_text()
    assert svsim_main(out) == 2  # no implicit overwrite


def test_rank_bit_targets_form_a_remote_route_class_and_suite_covers_it():
    from svsim.ir import GateNormalizer
    from svsim.time_model import gate_class
    gate = lambda q: GateNormalizer(8).gate('H', (q,))
    assert [gate_class(gate(q), 4, 7) for q in (0, 6, 7)] == ['H:local', 'H:joint', 'H:remote']
    assert gate_class(gate(7), 4) == 'H:joint'  # one rank: every upper bit is intra-rank
    two = calibration_suite(4, (6, 7), (1,), ranks=2)
    remote = WorkloadSpec(two['cal-h-remote-n7-g1'])
    assert workload_features(remote, 4, 2)[0]['passes:H:remote'] == 4
    joint = WorkloadSpec(two['cal-h-joint-n7-g1'])
    assert workload_features(joint, 4, 2)[0]['passes:H:joint'] == 4
    assert 'cal-h-remote-n6-g1' in calibration_suite(4, (6,), (1,), ranks=2)
    assert not any('remote' in k for k in calibration_suite(4, (6,), (1,)))
    with pytest.raises(ValueError, match='power of two'):
        calibration_suite(4, (7,), (1,), ranks=3)


@pytest.mark.skipif(importlib.util.find_spec('mpi4py') is None or __import__('shutil').which('mpiexec') is None,
                    reason='MPI launcher not available')
@pytest.mark.parametrize('executor', ['sync', 'async'])
def test_two_rank_runs_share_one_dataset_and_validate(tmp_path, executor):
    import os, shutil, subprocess, sys
    threads = '2' if executor == 'async' else '1'
    run = lambda *args: subprocess.run([sys.executable, '-c', 'import sys;from svsim.cli import svbench_main,svsim_main;'
                                        f'sys.exit({args[0]}(sys.argv[1:]))', *args[1:]],
                                       check=True, timeout=300, env={**os.environ, 'OMP_NUM_THREADS': '1'})
    run('svbench_main', 'probe', '--storage-path', str(tmp_path / 'scratch'), '--threads', threads, '--ranks', '2',
        '--output', str(tmp_path / 'probe.json'))
    workload = tmp_path / 'w.json'
    workload.write_text(json.dumps({'kind': 'workload', 'schema_version': 1, 'qubits': 6, 'policies': {'storage': 'file'},
                                    'operations': [{'kind': 'H', 'targets': [0]}, {'kind': 'H', 'targets': [5]}]}))
    subprocess.run([shutil.which('mpiexec'), '-n', '2', sys.executable, '-c',
                    'import sys;from svsim.cli import svsim_main;sys.exit(svsim_main(sys.argv[1:]))',
                    'benchmark', '--workload', str(workload), '--executor', executor, '--threads', threads,
                    '--profile', str(tmp_path / 'probe.json'), '--scratch', str(tmp_path / 'scratch'),
                    '--output-dir', str(tmp_path / 'out'), '--ram-budget-per-rank', str(1 << 26),
                    '--disk-budget', str(1 << 28), '--wall-seconds', '120', '--block-bits', '3',
                    '--repetitions', '1', '--warmup', '0'],
                   check=True, timeout=300, env={**os.environ, 'OMP_NUM_THREADS': '1'})
    doc = json.loads((tmp_path / 'out' / 'w.run.json').read_text())
    assert doc['status'] == 'completed' and doc['validation']['passed'] is True
    assert doc['case']['ranks'] == 2 and doc['case']['mode'] == f'distributed-{executor}'
    features = {r['feature']: int(r['count']) for r in doc['work']['features']}
    assert features['blocks'] == 4 and features['passes:H:remote'] == 4
    assert int(doc['trials'][0]['disk_payload_bytes']) > 0
    assert list((tmp_path / 'scratch').iterdir()) == []
