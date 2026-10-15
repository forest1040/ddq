"""One registry answers every runtime cost consumer; non-measured evidence never reaches production."""
from dataclasses import replace

import pytest

from svsim.runtime_cost_registry import RuntimeCostRegistry

MPI = pytest.importorskip('mpi4py.MPI')

from svsim.async_scheduler import AsyncDistributedSimulator, AsyncPolicy
from svsim.distributed import DistributedConfig
from svsim.lifecycle import BenchmarkSample
from svsim.runtime_cost import CostSample
from svsim import runtime_observed_costs as observed_costs


def test_purpose_decides_which_provenance_may_answer():
    registry = RuntimeCostRegistry()
    for provenance in ('measured', 'derived', 'injected', 'manual', 'legacy'):
        registry.register('k:' + provenance, 's', provenance, lambda c, d, p=provenance: p)
    allowed = {'production': {'measured'}, 'evaluation': {'measured', 'injected'},
               'health': {'measured', 'legacy'},
               'diagnostic': {'measured', 'derived', 'injected', 'manual', 'legacy'}}
    for purpose, accepted in allowed.items():
        for provenance in ('measured', 'derived', 'injected', 'manual', 'legacy'):
            result = registry.resolve('k:' + provenance, purpose=purpose)
            assert result.known == (provenance in accepted), (purpose, provenance)
            if not result.known:
                assert result.reason == provenance.upper() + '_NOT_PRODUCTION'
    assert registry.resolve('absent').reason == 'UNKNOWN_COST' and registry.resolve('absent').value is None


def test_newest_acceptable_source_wins_and_rejections_are_explained():
    registry = RuntimeCostRegistry()
    registry.register('cost', 'old-measured', 'measured', lambda c, d: 1.0, units='seconds/call')
    registry.register('cost', 'new-manual', 'manual', lambda c, d: 99.0, units='seconds/call')
    result = registry.resolve('cost')
    assert result.value == 1.0 and result.source_id == 'old-measured'
    assert result.rejected == (('new-manual', 'MANUAL_NOT_PRODUCTION'),)
    assert registry.resolve('cost', select='first').source_id == 'old-measured'
    assert ('old-measured', 'UNITS_MISMATCH') in registry.resolve('cost', units='seconds/gradient').rejected
    registry.register('cost', 'old-measured', 'measured', lambda c, d: None)
    missed = registry.resolve('cost')
    assert missed.value is None and missed.reason == 'NO_MATCHING_COST'  # newest rejection explains the miss
    assert ('new-manual', 'MANUAL_NOT_PRODUCTION') in missed.rejected
    with pytest.raises(ValueError):
        registry.register('cost', 's', 'guessed', lambda c, d: 1)


@pytest.fixture
def sim(tmp_path):
    policy = AsyncPolicy(mode='auto', io_engine='cooperative', io_workers=1, prefetch_blocks=0, cooldown_seconds=0)
    config = DistributedConfig(str(tmp_path / 'data'), 2, ram_budget_bytes_per_rank=8 * 1024**2,
                               storage_scope='node-local', frame_policy='materialize')
    with AsyncDistributedSimulator(4, config, comm=MPI.COMM_SELF, policy=policy) as simulator:
        for _ in range(4): simulator.gate('RX', (0,), angle=.3)
        simulator.materialize(binding=simulator.bind())
        yield simulator


def test_every_consumer_resolves_through_the_runtime_registry(sim):
    runtime = sim.runtime
    calls = []
    original = runtime.costs.resolve
    runtime.costs.resolve = lambda kind, *a, **k: calls.append((kind, k.get('purpose'))) or original(kind, *a, **k)
    # ETA diagnostic view, guard baseline, cache eviction, gradient auto and replan evidence.
    runtime.set_eta_cost_view('observed', lambda task: CostSample('diagnostic', .1, .1, .1))
    runtime.configure_eta(max_bytes=64 * 1024, scan_items=4096, scan_seconds=1.0)
    for _ in range(4): sim.gate('RX', (1,), angle=.2)
    runtime.update_eta(endpoint_frontier=len(sim.sequencer.operations) - 1)
    condition = runtime.io_condition('read')
    profile = replace(sim.local.profile, benchmarks=(BenchmarkSample(condition, bandwidth_bytes_per_second=100,
                                                                    latency_ms=10),))
    runtime.configure_guard(profile, window_size=2, consecutive_windows=2)
    runtime.observe(BenchmarkSample(condition, bandwidth_bytes_per_second=100, latency_ms=10))
    assert runtime.condition_provenance == 'legacy'
    runtime.attach_gradient_calibration({'kind': 'gradient-calibration', 'schema_version': 1})
    assert runtime.costs.resolve('gradient-method', {'injected': True, 'build': 'b'}).reason == 'NO_MATCHING_COST'
    assert runtime.gradient_cost_reason == 'INJECTED_EVALUATION'
    runtime.observe_evaluation({'health': 'DEGRADED', 'estimate': {'scope': 'injected', 'service_lower_per_gate': .1,
        'service_upper_per_gate': .1, 'extra_lower_per_call': .1, 'extra_upper_per_call': .1}})
    kinds = {kind for kind, _ in calls}
    assert {'eta-view:observed', 'guard-baseline', 'gradient-method'} <= kinds
    assert ('guard-baseline', 'health') in calls
    described = {(row['kind'], row['provenance']) for row in runtime.costs.describe()}
    assert {('eta-view:observed', 'manual'), ('guard-baseline', 'legacy'), ('gradient-method', 'measured'),
            ('replan-per-gate', 'injected')} <= described


def test_non_measured_evidence_never_answers_production_decisions(sim):
    runtime = sim.runtime
    runtime.observe_evaluation({'health': 'DEGRADED', 'estimate': {'scope': 'injected', 'service_lower_per_gate': .1,
        'service_upper_per_gate': .1, 'extra_lower_per_call': .1, 'extra_upper_per_call': .1}})
    assert runtime.costs.resolve('replan-per-gate', purpose='production').reason == 'INJECTED_NOT_PRODUCTION'
    assert runtime.costs.resolve('replan-per-gate', purpose='evaluation').known
    condition = runtime.io_condition('read')
    profile = replace(sim.local.profile, benchmarks=(BenchmarkSample(condition, bandwidth_bytes_per_second=1,
                                                                    latency_ms=1),))
    runtime.configure_guard(profile)
    assert runtime.costs.resolve('guard-baseline', None, condition).reason == 'LEGACY_NOT_PRODUCTION'
    runtime.set_eta_cost_view('adapted', lambda task: CostSample('manual', 1, 1, 1))
    assert runtime.costs.resolve('eta-view:adapted', purpose='production').reason == 'MANUAL_NOT_PRODUCTION'


def test_cache_eviction_costs_need_a_measured_registry_source(sim):
    from svsim.cache_cost import CacheCost, FutureUseSummary
    runtime = sim.runtime
    if not hasattr(sim.cache, 'set_cost_hints'): pytest.skip('RAM cache has no cost hints')
    claimed = lambda version, entry, hint: CacheCost(raw_read=1.0, rematerialize=1.0, dirty_writeback=1.0,
                                                     validated=True, evidence='measured', source='caller',
                                                     scope='same-host')
    runtime.set_cache_cost_hints(FutureUseSummary((), 's', ()), cost_resolver=claimed)
    assert runtime.costs.resolve('cache-entry', ('v', None, None)).reason == 'MANUAL_NOT_PRODUCTION'
    runtime.set_cache_cost_hints(FutureUseSummary((), 's', ()), cost_resolver=claimed, provenance='measured')
    assert runtime.costs.resolve('cache-entry', ('v', None, None)).known  # declared measured source answers


def test_unknown_stays_unknown_without_sources(sim):
    runtime = sim.runtime
    for kind in ('task-seconds', 'eta-remaining', 'gradient-method', 'replan-per-gate'):
        result = runtime.costs.resolve(kind)
        assert result.value is None and result.reason == 'UNKNOWN_COST'
    # The run's own collector is registered but has too few measured samples to answer.
    assert runtime.costs.resolve('cache-entry', ('v', None, None)).reason == 'NO_MATCHING_COST'
    eta = runtime.update_eta(endpoint_frontier=sim.frontier)
    assert eta['cost_view_sources'] == {'baseline': 'unavailable', 'observed': 'rolling-observed', 'adapted': 'unavailable'}


def _real_same_class_run(path):
    policy = AsyncPolicy(io_engine='cooperative', io_workers=1, prefetch_blocks=0, lookahead_gates=1)
    config = DistributedConfig(str(path), 2, ram_budget_bytes_per_rank=8 * 1024**2,
                               storage_scope='node-local', frame_policy='materialize')
    with AsyncDistributedSimulator(4, config, comm=MPI.COMM_SELF, policy=policy) as sim:
        runtime = sim.runtime
        # Stage records share the RAM summary budget; 512 KiB keeps all 36 executor groups and still fits.
        runtime.configure_measurements(max_bytes=512 * 1024)
        runtime.configure_eta(max_bytes=64 * 1024, scan_items=4096, scan_seconds=1.0)
        for _ in range(40): sim.gate('RX', (0,), angle=.3)
        sim.materialize(frontier=35, binding=sim.bind())  # real same-class executor calls only; no synthetic rows
        probe = next(t for t in runtime.remaining_extractor.capture(sim, endpoint_frontier=39).window if t.kind != 'publication')
        rows = [r for r in runtime.measurements.cost_observations()
                if r.stage in observed_costs.EXECUTOR_STAGES and observed_costs._class(r.condition) == observed_costs._class(probe.condition)]
        assessed = observed_costs._assess(rows, runtime.policy_epoch, runtime.plan_version, observed_costs.BURN_IN)
        eta = runtime.update_eta(endpoint_frontier=39)
        resolved = runtime.costs.resolve('observed-task-seconds', None, probe.condition)
        if not resolved.known:  # an uncertified class still answers the diagnostic purpose, labelled with its reason
            diagnostic = observed_costs.observed_task_cost(runtime.measurements, probe.condition, runtime.policy_epoch,
                                                           runtime.plan_version, diagnostic=True)
            assert diagnostic is not None and assessed[0] in diagnostic.condition and not diagnostic.measured
        injected = [r for r in runtime.measurements.cost_observations() if r.evidence != 'measured']
        return eta, resolved, injected, (len(rows), assessed[0], assessed[4])


def test_rolling_observations_answer_production_after_enough_same_class_groups(tmp_path):
    # Linux devices can switch state mid-run (per-call time ~60 ms -> ~245 ms); such a run is correctly refused as
    # DRIFTING or LOW_HELD_OUT_COVERAGE, so only those refusals allow another real run (2 of 15 runs on ent).
    outcomes = []
    for attempt in range(3):
        eta, resolved, injected, outcome = _real_same_class_run(tmp_path / f'data{attempt}')
        outcomes.append(outcome)
        assert injected == []  # only measured, validated, non-warmup rows feed production costs
        if resolved.known: break
        assert outcome[1] in {observed_costs.DRIFTING, observed_costs.LOW_COVERAGE}, outcomes
    assert resolved.known, outcomes
    assert resolved.provenance == 'measured' and resolved.value.lower_seconds <= resolved.value.mean_seconds
    observed = eta['views']['observed']
    assert eta['cost_view_sources']['observed'] == 'rolling-observed'
    assert observed['partial_expected_seconds'] > 0 and observed['coverage'] > 0


def _assess_values(values):
    rows = [type('R', (), dict(context={'policy_epoch': 1, 'plan_version': 1}, protocol_seconds=v, service_seconds=None,
                               quantity=1))() for v in values]
    return observed_costs._assess(rows, 1, 1, observed_costs.BURN_IN)


def test_burn_in_drops_monotone_warmup_prefix_then_certifies():
    values = [.020, .015, .012] + [.010, .0101, .0099, .0102, .0100, .0098, .0101, .0100, .0099, .0100]
    reason, kept, *_ = _assess_values(values)
    assert reason == observed_costs.CERTIFIED and len(kept) == len(values) - observed_costs.BURN_IN
    assert max(kept) < .0103  # warmup values are excluded from the envelope


def test_steady_step_change_is_drifting_not_certified():
    reason, _, _, _, coverage = _assess_values([.01] * 3 + [.01] * 8 + [.03] * 8)
    assert reason == observed_costs.DRIFTING and coverage < observed_costs.MIN_COVERAGE


def test_burn_in_rows_do_not_count_toward_certify_samples():
    assert _assess_values([.01] * (observed_costs.CERTIFY_SAMPLES - 1 + observed_costs.BURN_IN))[0] == observed_costs.INSUFFICIENT
    assert _assess_values([.01] * (observed_costs.CERTIFY_SAMPLES + observed_costs.BURN_IN))[0] == observed_costs.CERTIFIED


def test_cache_eviction_uses_measured_storage_completions_from_the_collector(tmp_path):
    policy = AsyncPolicy(io_engine='cooperative', io_workers=1, prefetch_blocks=0)
    config = DistributedConfig(str(tmp_path / 'io'), 2, ram_budget_bytes_per_rank=8 * 1024**2, storage_scope='node-local')
    with AsyncDistributedSimulator(4, config, comm=MPI.COMM_SELF, policy=policy) as fresh:
        _check_cache_costs(fresh.runtime)


def _ctx(runtime):
    return {'policy_epoch': runtime.policy_epoch, 'plan_version': runtime.plan_version}


def _io(runtime, rows, context=None):
    for i, (operation, seconds) in enumerate(rows):
        runtime.measurements.complete(f'io-{operation}-{i}-{seconds}', context=context or _ctx(runtime),
                                      condition={'operation': operation}, stage='storage-' + operation,
                                      protocol_seconds=seconds, validated=True,
                                      evidence='injected' if seconds == 9.0 else 'measured')


def _check_cache_costs(runtime):
    runtime.configure_measurements(max_bytes=64 * 1024)
    _io(runtime, [('read', .01)] * 8 + [('write', .02)] * 8 + [('read', 9.0)] * 3)
    cost = runtime.costs.resolve('cache-entry', ('v', None, None))
    assert cost.known and cost.source_id == 'runtime-collector-io'
    assert cost.value.whole_fetch == pytest.approx(.01) and cost.value.dirty_writeback == pytest.approx(.02)
    assert cost.value.eviction_score(True) == pytest.approx(.03)  # injected 9 s reads never counted


WARMUP = [.02, .015, .012]  # first calls of a class run slower; burn-in drops BURN_IN of them


def _task_rows(runtime, seconds, context=None, start=0, condition=None):
    condition = condition or {'operation': 'executor-group', 'kind': 'x'}
    for i, value in enumerate(seconds):
        runtime.measurements.complete(f'g-{start + i}', context=context or _ctx(runtime), condition=condition,
                                      group_id=f'g-{start + i}', stage=condition['operation'], units='groups', quantity=1,
                                      protocol_seconds=value, success=True, validated=True)
    return condition


def _resolve(runtime, condition, purpose='production'):
    return runtime.costs.resolve('observed-task-seconds', None, condition, purpose=purpose)


def _runtime(tmp_path, **kw):
    policy = AsyncPolicy(io_engine='cooperative', io_workers=1, prefetch_blocks=0)
    config = DistributedConfig(str(tmp_path / 'd'), 2, ram_budget_bytes_per_rank=8 * 1024**2, storage_scope='node-local')
    return AsyncDistributedSimulator(4, config, comm=MPI.COMM_SELF, policy=policy)


def test_certified_rolling_cost_answers_production_and_refuses_diagnostic(tmp_path):
    with _runtime(tmp_path) as sim:
        runtime = sim.runtime
        runtime.configure_measurements(max_bytes=512 * 1024)
        condition = _task_rows(runtime, WARMUP + [.01, .011, .0105, .0102, .0108, .0104, .0109, .0103])
        resolved = _resolve(runtime, condition)
        assert resolved.known and resolved.provenance == 'measured' and resolved.value.measured
        assert resolved.value.lower_seconds <= resolved.value.mean_seconds <= resolved.value.upper_seconds
        assert 'CERTIFIED' in runtime.cost_decisions.values()
        assert not _resolve(runtime, condition, 'diagnostic').value.condition.startswith('diagnostic')


def test_stale_policy_epoch_refuses_rows_recorded_before_the_change(tmp_path):
    with _runtime(tmp_path) as sim:
        runtime = sim.runtime
        runtime.configure_measurements(max_bytes=512 * 1024)
        condition = _task_rows(runtime, WARMUP + [.01] * 10)
        assert _resolve(runtime, condition).known
        runtime.apply_policy({'prefetch_blocks': 1})
        assert not _resolve(runtime, condition).known
        assert runtime.cost_decisions[next(iter(runtime.cost_decisions))] == 'STALE_POLICY_EPOCH'
        _task_rows(runtime, [.01] * 3, start=100)
        assert not _resolve(runtime, condition).known
        assert 'INSUFFICIENT_SAMPLES' in runtime.cost_decisions.values()
        _task_rows(runtime, [.01] * 8, start=200)  # 3 burn-in + 8 certify
        assert _resolve(runtime, condition).known


def test_low_held_out_coverage_refuses_production_but_diagnostic_is_labelled(tmp_path):
    with _runtime(tmp_path) as sim:
        runtime = sim.runtime
        runtime.configure_measurements(max_bytes=512 * 1024)
        condition = _task_rows(runtime, WARMUP + [.01, .011, .0105, .0102, .0104, .5, .0103, .6, .0105, .7, .0102, .0104])
        assert not _resolve(runtime, condition).known
        assert 'LOW_HELD_OUT_COVERAGE' in runtime.cost_decisions.values()
        diagnostic = _resolve(runtime, condition, 'diagnostic')
        assert diagnostic.known and diagnostic.provenance == 'derived' and not diagnostic.value.measured
        assert 'LOW_HELD_OUT_COVERAGE' in diagnostic.value.condition
        assert diagnostic.value.upper_seconds >= .7


def test_cache_cost_requires_fresh_certified_reads_and_writes(tmp_path):
    with _runtime(tmp_path) as sim:
        runtime = sim.runtime
        runtime.configure_measurements(max_bytes=64 * 1024)
        _io(runtime, [('read', .01)] * 8 + [('write', .02)] * 6)
        cost = runtime.costs.resolve('cache-entry', ('v', None, None))
        assert cost.known and cost.value.dirty_writeback is None
        runtime.apply_policy({'prefetch_blocks': 1})
        assert not runtime.costs.resolve('cache-entry', ('v', None, None)).known
        assert runtime.cost_decisions['storage-read'] == 'STALE_POLICY_EPOCH'
