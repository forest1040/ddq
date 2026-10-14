"""One registry answers every runtime cost consumer; non-measured evidence never reaches production."""
from dataclasses import replace

import pytest

from svsim.runtime_cost_registry import RuntimeCostRegistry

MPI = pytest.importorskip('mpi4py.MPI')

from svsim.async_scheduler import AsyncDistributedSimulator, AsyncPolicy
from svsim.distributed import DistributedConfig
from svsim.lifecycle import BenchmarkSample
from svsim.runtime_cost import CostSample


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
    for kind in ('task-seconds', 'eta-remaining', 'gradient-method', 'cache-entry', 'replan-per-gate'):
        result = runtime.costs.resolve(kind)
        assert result.value is None and result.reason == 'UNKNOWN_COST'
    eta = runtime.update_eta(endpoint_frontier=sim.frontier)
    assert eta['cost_view_sources'] == {'baseline': 'unavailable', 'observed': 'unavailable', 'adapted': 'unavailable'}
