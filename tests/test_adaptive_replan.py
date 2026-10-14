"""Runtime-generated replan costs: injected sustained compute slowdown -> S2 switch -> VERIFY."""
from dataclasses import asdict

import numpy as np
import pytest

MPI = pytest.importorskip('mpi4py.MPI')

from svsim.adaptive_evaluation import AdaptiveEvaluation
from svsim.async_scheduler import AsyncDistributedSimulator, AsyncPolicy
from svsim.distributed import DistributedConfig
from svsim.kernels import eager_reference
from svsim.performance_injection import InjectionSpec
from svsim.replan_cost_producer import remaining_calls


def _config(path):
    return DistributedConfig(str(path), 2, ram_budget_bytes_per_rank=8 * 1024**2, metadata_reserve_bytes=128 * 1024,
                             tile_bytes=4096, frame_policy='materialize', storage_scope='node-local')


# The injected per-call delay must dominate host service jitter (Linux durable commits vary by
# tens of ms per gate); otherwise the conservative bound correctly refuses as uncertain gain.
def _run(tmp_path, *, mode='auto', gates=32, start=4, end=None, delay=.2, prime_checkpoint=True, baseline=4):
    policy = AsyncPolicy(mode=mode, io_engine='cooperative', io_workers=1, prefetch_blocks=0, lookahead_gates=4,
                         cooldown_seconds=0)
    session = AdaptiveEvaluation(baseline_groups=baseline)
    with AsyncDistributedSimulator(4, _config(tmp_path / 'data'), comm=MPI.COMM_SELF, policy=policy) as sim:
        for _ in range(gates): sim.gate('RX', (0,), angle=.025)
        binding = sim.bind()
        if prime_checkpoint: sim.commit_snapshot()  # measured durable-cut cost for FRONTIER candidates
        spec = InjectionSpec(1, 'compute', 'work', start, end if end is not None else gates + 1, delay,
                             ('executor-group',), 60.0, 256, barrier='semantic-prefix')

        def observe(event):
            verifying = sim.runtime.verifying
            if verifying is not None and session.verify is None and session.verification is None:
                session.begin_verify(service_upper_per_gate=verifying['service_upper_per_gate'],
                                     extra_upper_per_call=verifying['extra_upper_per_call'])
            row = session.observe(event, profile=sim.local.profile, circuit_hash=sim.circuit_hash,
                                  num_qubits=4, block_bits=2, ranks=1)
            sim.runtime.observe_evaluation(row)

        sim.runtime.configure_performance_injection(spec, observer=observe).barrier('semantic-prefix')
        sim.materialize(binding=binding)
        expected = eager_reference(sim.sequencer.operations, 4, sim.assets, binding)
        np.testing.assert_allclose(sim.state(allow_gather=True), expected, atol=1e-10)
        return sim.runtime, session


def test_remaining_calls_count_s2_fusion_exactly(tmp_path):
    policy = AsyncPolicy(io_engine='cooperative', io_workers=1, lookahead_gates=4)
    with AsyncDistributedSimulator(4, _config(tmp_path / 'd'), comm=MPI.COMM_SELF, policy=policy) as sim:
        for _ in range(10): sim.gate('RX', (0,), angle=.1)
        sim.gate('H', (3,))
        from dataclasses import replace
        assert remaining_calls(sim, sim.policy, 0, 10) == 11
        s2 = replace(sim.policy, swap_policy='S2')
        assert remaining_calls(sim, s2, 0, 10) == 4  # [4 RX], [4 RX], [2 RX + H]: fused RX runs, H separate
        assert remaining_calls(sim, s2, 4, 7) == 1


def test_sustained_injected_slowdown_switches_to_fused_policy_and_verifies(tmp_path):
    runtime, session = _run(tmp_path)
    applies = [d for d in runtime.replan_decisions if d['kind'] == 'apply']
    assert len(applies) == 1 and runtime.transitions[-1]['updates'] == {'swap_policy': 'S2'}
    evaluation = next(d for d in runtime.replan_decisions if d['kind'] == 'evaluate')
    chosen = next(c for c in evaluation['candidates'] if c['proposal_id'] == evaluation['selected'])
    assert chosen['candidate_calls'] < chosen['current_calls'] and chosen['gain_lower_seconds'] > 0
    refused = [c for c in evaluation['candidates'] if c['classification'] == 'LIVE_TUNE']
    assert refused and not any(c['eligible'] for c in refused)  # no call reduction -> no priced gain
    assert runtime.plan_version == 2 and session.verification == {'verified': True, 'calls': 2}
    assert runtime.health == 'STABLE'
    assert [d for d in runtime.replan_decisions if d['kind'] == 'verify'][0]['result'] == 'STABLE'


@pytest.mark.parametrize('case', ['observe', 'recommend', 'short-remaining', 'no-checkpoint-cost', 'transient',
                                  'missing-baseline'])
def test_negative_cases_do_not_switch(tmp_path, case):
    kwargs = {'observe': {'mode': 'observe'}, 'recommend': {'mode': 'recommend'},
              'short-remaining': {'gates': 12, 'start': 8},
              'no-checkpoint-cost': {'prime_checkpoint': False},
              'transient': {'end': 6}, 'missing-baseline': {'baseline': 0, 'start': 0}}[case]
    runtime, session = _run(tmp_path, **kwargs)
    assert runtime.transitions == [] and runtime.plan_version == 1
    evaluations = [d for d in runtime.replan_decisions if d['kind'] == 'evaluate']
    if case == 'recommend':
        assert evaluations and evaluations[0]['selected'] is not None  # recommended, never applied
    if case == 'no-checkpoint-cost':
        reasons = {c['reason'] for d in evaluations for c in d['candidates'] if c['classification'] == 'FRONTIER_REPLAN'}
        assert reasons <= {'UNKNOWN_COST_COMPONENT'}
    if case == 'short-remaining':
        assert all(d['selected'] is None for d in evaluations)
