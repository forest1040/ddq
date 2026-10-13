from dataclasses import replace

import pytest

MPI = pytest.importorskip('mpi4py.MPI')
from svsim import AsyncDistributedSimulator, AsyncPolicy, DistributedConfig, ReplanEvidence
from svsim.lifecycle import BenchmarkSample
from svsim.runtime_cost import CostSample, CostTask, RuntimeCostModel


def test_eta_requires_matched_measured_conditions_and_dag():
    model = RuntimeCostModel()
    tasks = (CostTask('read', 'ssd', resource='ssd'),
             CostTask('mpi', 'nic', ('read',), 'mpi'),
             CostTask('compute', 'cpu', ('mpi',), 'cpu'))
    assert model.estimate(tasks).expected_seconds is None
    for condition, duration in (('ssd', 2), ('nic', 3), ('cpu', 5)):
        model.record(CostSample(condition, duration, duration*.9, duration*1.1))
    eta = model.estimate(tasks)
    assert eta.lower_seconds == pytest.approx(9)
    assert eta.expected_seconds == 10
    assert eta.upper_seconds == 11
    with pytest.raises(ValueError, match='Cyclic'):
        model.estimate((CostTask('a', 'cpu', ('a',)),))
    with pytest.raises(ValueError, match='measured'):
        model.record(CostSample('fake', 1, 1, 1, measured=False))


def test_sustained_guard_live_tune_and_candidate_validation(tmp_path):
    policy = AsyncPolicy(mode='auto', prefetch_blocks=2, cooldown_seconds=0)
    with AsyncDistributedSimulator(4, DistributedConfig(str(tmp_path), 2),
                                   comm=MPI.COMM_SELF, policy=policy) as sim:
        condition = sim.runtime.io_condition('read')
        profile = replace(sim.local.profile, benchmarks=(BenchmarkSample(condition,
                              bandwidth_bytes_per_second=100, latency_ms=10),))
        sim.runtime.configure_guard(profile, window_size=2, consecutive_windows=2)
        for _ in range(3):
            sim.runtime.observe(BenchmarkSample(condition, bandwidth_bytes_per_second=10, latency_ms=100))
        assert sim.policy.prefetch_blocks == 2
        sim.runtime.observe(BenchmarkSample(condition, bandwidth_bytes_per_second=10, latency_ms=100))
        assert sim.runtime.health == 'DEGRADED'
        sim.runtime.frontier()
        assert sim.policy.prefetch_blocks == 0
        assert sim.runtime.health == 'VERIFYING'
        before = len(list(tmp_path.rglob('prepare-*.json')))
        rejected = sim.runtime.request_replan({'swap_policy': 'S2'})
        assert not rejected.eligible
        assert len(list(tmp_path.rglob('prepare-*.json'))) == before
        with pytest.raises(RuntimeError, match='insufficient'):
            sim.runtime.apply_candidate(rejected.candidate_id)
        evidence = ReplanEvidence(100, 50, condition_hash=sim.runtime.condition_hash, measured=True)
        accepted = sim.runtime.request_replan({'swap_policy': 'S2'}, evidence=evidence)
        sim.runtime.apply_candidate(accepted.candidate_id)
        assert sim.policy.swap_policy == 'S2'
        assert sim.runtime.plan_version == 2
        assert len(list(tmp_path.rglob('prepare-*.json'))) > before
        with pytest.raises(RuntimeError, match='Stale'):
            sim.runtime.apply_candidate(accepted.candidate_id)


def test_observe_mode_and_failed_policy_install_preserve_state(tmp_path):
    with AsyncDistributedSimulator(4, DistributedConfig(str(tmp_path), 2), comm=MPI.COMM_SELF) as sim:
        old = sim.policy
        with pytest.raises(RuntimeError, match='restart'):
            sim.runtime.apply_policy({'io_engine': 'cooperative'})
        assert sim.policy == old
        # Operational mode changes have no gate or amplitude side effects.
        root = sim.root
        sim.runtime.set_mode('recommend')
        assert sim.root == root
