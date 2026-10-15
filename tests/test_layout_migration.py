"""P4-004 layout migration: executor-class profile, candidates, pricing and the MPI positive."""
import json
import shutil
import subprocess
import sys
from types import SimpleNamespace

import pytest

from svsim.executor_profile import ExecutorProfile, document, merge, validate
from svsim.layout import QubitLayout
from svsim.replan_candidates import RuntimeCapabilityRegistry, layout_options
from svsim.ir import GateOp


def _row(key, lower=.1, expected=.2, upper=.3, context=None):
    return {'class_key': key, 'context': context or {'n': 4, 'block_bits': 2, 'backend': 'buffered', 'engine': 'e',
                                                     'ranks': 2, 'threads': 1},
            'lower_seconds': lower, 'expected_seconds': expected, 'upper_seconds': upper, 'held_out_count': 5,
            'held_out_coverage': 1.0, 'samples': 10, 'validation': 'held-out', 'evidence': 'measured',
            'units': 'seconds/call'}


def test_profile_documents_are_content_addressed_and_only_hold_measured_envelopes():
    condition = {'operation': 'physical-bit-swap', 'bits': [0, 3]}
    doc = document([_row(json.dumps(condition, sort_keys=True, separators=(',', ':')))], 'fp')
    profile = ExecutorProfile(doc)
    context = {**_row('x')['context'], 'binding_id': 'any', 'policy_epoch': 7, 'plan_version': 3}
    sample = profile.cost_sample(context, condition)  # Binding/epoch/version do not change physical cost
    assert sample.measured and (sample.lower_seconds, sample.upper_seconds) == (.1, .3)
    assert profile.cost_sample({**context, 'ranks': 4}, condition) is None
    assert profile.cost_sample(context, {**condition, 'bits': [1, 3]}) is None
    with pytest.raises(ValueError):
        validate({**doc, 'rows': doc['rows'] + [_row('y')]})  # content no longer matches its id
    with pytest.raises(ValueError):
        ExecutorProfile(document([{**_row('z'), 'evidence': 'injected'}], 'fp'))
    with pytest.raises(ValueError):
        merge([doc, document([_row('w')], 'other-host')])
    assert len(merge([doc, document([_row('w')], 'fp')])['rows']) == 2


def test_layout_migration_capability_accepts_only_one_rank_bit_local_bit_pair():
    registry = RuntimeCapabilityRegistry(64, 64, 1 << 20, False, num_qubits=4, rank_bits=1)
    policy = SimpleNamespace()
    assert registry.check(policy, {'layout_swap': (3, 0)}) is None
    assert registry.check(policy, {'layout_swap': [0, 3]}) is None
    assert registry.check(policy, {'layout_swap': (1, 0)}) == 'INVALID_LAYOUT_MIGRATION'  # no rank bit
    assert registry.check(policy, {'layout_swap': (3, 4)}) == 'INVALID_LAYOUT_MIGRATION'
    assert registry.check(policy, {'layout_swap': (3, 0), 'prefetch_blocks': 0}) == 'INVALID_LAYOUT_MIGRATION'
    single = RuntimeCapabilityRegistry(64, 64, 1 << 20, False, num_qubits=4, rank_bits=0)
    assert single.check(policy, {'layout_swap': (3, 0)}) == 'INVALID_LAYOUT_MIGRATION'
    private = RuntimeCapabilityRegistry(64, 64, 1 << 20, True, num_qubits=4, rank_bits=1)
    assert private.check(policy, {'layout_swap': (3, 0)}) == 'PRIVATE_CUT'


def _sim(gates, *, rank_bits=1, layout=(0, 1, 2, 3)):
    operations = [GateOp(kind, targets, op_id=i) for i, (kind, targets) in enumerate(gates)]
    return SimpleNamespace(ownership=SimpleNamespace(rank_bits=rank_bits), frontier=-1, num_qubits=4,
                           sequencer=SimpleNamespace(operations=operations), layout=QubitLayout(layout))


def test_layout_options_localize_the_most_used_rank_bit_qubit():
    sim = _sim([('H', (3,))] * 5 + [('H', (1,)), ('H', (2,))])
    assert layout_options(sim, 6) == [(3, 0)]  # q0 is unused: lowest least-used local bit
    # Already local work, a single rank, or a remainder beyond the scan bound propose nothing.
    assert layout_options(_sim([('H', (0,))] * 4), 3) == []
    assert layout_options(_sim([('H', (3,))] * 4, rank_bits=0), 3) == []
    assert layout_options(sim, 10**6) == []
    # The rank bit holds whichever logical qubit the current layout put there.
    moved = _sim([('H', (0,))] * 4, layout=(3, 1, 2, 0))
    assert layout_options(moved, 3) == [(3, 0)]  # physical 0 now holds the unused q3


launcher = shutil.which('mpiexec')


@pytest.mark.skipif(launcher is None, reason='MPI launcher not available')
def test_mpi_slowdown_migrates_the_rank_bit_with_a_profile_and_refuses_without(tmp_path):
    import os
    env = {**os.environ, 'PYTHONPATH': 'src', 'OMP_NUM_THREADS': '1'}
    profile = tmp_path / 'profile'
    subprocess.run([launcher, '-n', '2', sys.executable, 'examples/p4_004_layout_migration.py', '--output', str(profile),
                    '--gates', '24', '--swaps', '12'], check=True, timeout=600, capture_output=True, env=env)
    rows = {}
    for name, extra in (('with', ['--executor-profile', str(profile)]), ('without', [])):
        output = tmp_path / f'{name}.json'
        subprocess.run([launcher, '-n', '2', sys.executable, 'examples/phase4_remaining_campaign.py', '--action',
                        'adaptive', '--resource', 'mpi', '--injection-delay', '.05', '--repetitions', '1', '--warmup',
                        '0', '--output', str(output), '--scratch', str(tmp_path / f'scratch-{name}'), *extra],
                       check=True, timeout=600, capture_output=True, env=env)
        rows[name] = {r['variant_id']: r for r in json.loads(output.read_text())['adaptive']['rows']}
    auto = rows['with']['auto']['metrics']
    # The campaign itself asserts the final state against the eager oracle.
    assert auto['switch_count']['median'] == 1 and auto['verified']['median'] == 1
    assert auto['predicted_gain_lower_seconds']['median'] > 0
    assert rows['without']['auto']['metrics']['switch_count']['median'] == 0
    assert rows['with']['fixed']['metrics']['switch_count']['median'] == 0


def test_layout_pricing_uses_only_profiled_classes_and_attributes_mpi_excess(tmp_path):
    from mpi4py import MPI
    from svsim import AsyncDistributedSimulator, AsyncPolicy, DistributedConfig
    from svsim.replan_cost_producer import layout_costs_for
    from svsim.runtime_cost import CostSample
    config = DistributedConfig(str(tmp_path / 'd'), 2, ram_budget_bytes_per_rank=8 * 1024**2,
                               metadata_reserve_bytes=128 * 1024)
    policy = AsyncPolicy(io_engine='cooperative', io_workers=1, prefetch_blocks=0, lookahead_gates=4)
    with AsyncDistributedSimulator(4, config, comm=MPI.COMM_SELF, policy=policy) as sim:
        for _ in range(8): sim.gate('RX', (3,), angle=.025)
        sim.binding=sim.bind()
        from svsim.journal import content_hash, gate_data
        data=gate_data(sim._physical(sim.sequencer.operations[0]));data.pop('op_id',None);data.pop('source_op_id',None)
        work_class=content_hash({'operations':[data],'layout':sim.layout.logical_to_physical,
            'n':sim.num_qubits,'b':sim.block_bits,'P':sim.size,'binding':sim.binding.binding_id})
        quality={'certified':True,'scope':'injected','production_profile':False,'work_class':work_class}
        estimate = {'baseline_quality':quality,'service_lower_per_gate': .5, 'extra_lower_per_call': .2, 'extra_upper_per_call': .3,
                    'resource': 'mpi'}
        seen = []
        def resolve(kind, context, condition):
            seen.append(condition['operation'])
            return CostSample('profile', .1, .05, .15)
        kwargs = dict(end=7, planning_seconds=.01, drain_seconds=.0, checkpoint_seconds=.02)
        costs, counts = layout_costs_for(sim, (3, 0), estimate, resolve=resolve, **kwargs)
        assert costs.qualified is False and seen.count('physical-bit-swap') == 1
        assert counts['candidate_calls'] == 8 and counts['crossing_candidate_calls'] == 0  # single rank: no rank bit
        assert counts['charged_excess_calls'] == 0  # MPI excess only on calls that still cross ranks
        assert costs.execution_upper_seconds == pytest.approx(8 * .15)
        assert costs.continue_lower_seconds == pytest.approx(8 * .5 + 8 * .2)
        assert costs.migration_seconds == .15
        # Unattributed evidence charges every call; a missing class or swap leaves the candidate unknown.
        costs, counts = layout_costs_for(sim, (3, 0), {**estimate, 'resource': None}, resolve=resolve, **kwargs)
        assert counts['charged_excess_calls'] == 8
        assert costs.execution_upper_seconds == pytest.approx(8 * .15 + 8 * .3)
        missing = lambda kind, context, condition: None if condition['operation'] != 'physical-bit-swap' else \
            CostSample('profile', .1, .05, .15)
        costs, counts = layout_costs_for(sim, (3, 0), estimate, resolve=missing, **kwargs)
        assert costs.execution_upper_seconds is None and counts['layout_reason'] == 'UNPROFILED_CANDIDATE_CLASS'
        costs, counts = layout_costs_for(sim, (3, 0), None, resolve=resolve, **kwargs)
        assert costs.execution_upper_seconds is None
        # Even a complete profile cannot rescue missing or unstable baseline evidence.
        for quality in ({}, {**quality,'certified':False}, {**quality,'work_class':'other-class'}):
            costs, counts=layout_costs_for(sim,(3,0),{**estimate,'baseline_quality':quality},resolve=resolve,
                                         qualified=True,**kwargs)
            assert costs.continue_lower_seconds is None and costs.execution_upper_seconds is None
            assert counts['baseline_reason'] in {'UNCERTIFIED_BASELINE','BASELINE_EXECUTOR_CLASS_MISMATCH'}
        # A later source with a different angle cannot inherit the measured prefix service bound.
        sim.gate('RX',(3,),angle=.5)
        costs,counts=layout_costs_for(sim,(3,0),estimate,resolve=resolve,**{**kwargs,'end':8})
        assert costs.continue_lower_seconds is None
        assert counts['baseline_reason']=='BASELINE_EXECUTOR_CLASS_MISMATCH'


def test_layout_migration_apply_is_collective_and_failure_keeps_the_old_layout(tmp_path):
    import numpy as np
    from mpi4py import MPI
    from svsim import AsyncDistributedSimulator, AsyncPolicy, DistributedConfig
    from svsim.kernels import eager_reference
    config = DistributedConfig(str(tmp_path / 'd'), 2, ram_budget_bytes_per_rank=8 * 1024**2,
                               metadata_reserve_bytes=128 * 1024, frame_policy='materialize')
    policy = AsyncPolicy(io_engine='cooperative', io_workers=1, prefetch_blocks=0, lookahead_gates=4)
    with AsyncDistributedSimulator(4, config, comm=MPI.COMM_SELF, policy=policy) as sim:
        for index in range(12): sim.gate('RX' if index % 2 else 'H', (3,), angle=.1 * index if index % 2 else None)
        binding = sim.bind()
        sim.materialize(frontier=3, binding=binding)
        runtime, layout, version = sim.runtime, sim.layout, sim.runtime.plan_version
        original = sim._physical_swap
        sim._physical_swap = lambda a, b: (_ for _ in ()).throw(OSError('injected swap failure'))
        with pytest.raises(Exception):
            runtime.apply_layout_migration((3, 0))
        sim._physical_swap = original
        assert sim.layout == layout and runtime.plan_version == version and sim.frontier == 3
        transition = runtime.apply_layout_migration((3, 0))
        assert runtime.plan_version == version + 1 and sim.layout.logical_to_physical[3] == 0
        assert transition['updates'] == {'layout_swap': [3, 0]} and sim.history[-1]['kind'] == 'layout'
        assert runtime.latest_eta is None or runtime.latest_eta.get('stale', True)
        sim.materialize(binding=binding)
        expected = eager_reference(sim.sequencer.operations, 4, sim.assets, binding)
        np.testing.assert_allclose(sim.state(allow_gather=True), expected, atol=1e-10)
