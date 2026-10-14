"""FRONTIER_REPLAN faults at each stage: before COMMIT keep the old policy/cut, after it the new decision."""
import os
import shutil
import subprocess
import sys
import textwrap

import numpy as np
import pytest

MPI = pytest.importorskip('mpi4py.MPI')

import svsim.distributed_checkpoint as checkpoint
from svsim.async_scheduler import AsyncDistributedSimulator, AsyncPolicy
from svsim.distributed import DistributedConfig
from svsim.disk_budget import DiskAdmissionError
from svsim.kernels import eager_reference


def _config(path):
    return DistributedConfig(str(path), 2, ram_budget_bytes_per_rank=8 * 1024**2, metadata_reserve_bytes=128 * 1024,
                             tile_bytes=4096, frame_policy='materialize', storage_scope='node-local')


def _policy():
    return AsyncPolicy(mode='auto', io_engine='cooperative', io_workers=1, prefetch_blocks=0, lookahead_gates=4,
                       cooldown_seconds=0)


def _prepared(path):
    sim = AsyncDistributedSimulator(4, _config(path), comm=MPI.COMM_SELF, policy=_policy())
    for q in (0, 1, 0, 1, 0, 1, 0, 1): sim.gate('RX', (q,), angle=.3)
    sim.gate('H', (3,))
    binding = sim.bind()
    sim.materialize(frontier=3, binding=binding)
    sim.commit_snapshot()
    return sim, binding


def _fault(stage, sim, patch):
    if stage == 'drain':
        patch.setattr(sim, 'drain', lambda: (_ for _ in ()).throw(OSError('injected drain fault')))
    elif stage == 'install':
        original = sim._install_policy
        def install(policy):
            if policy.swap_policy == 'S2': raise OSError('injected install fault')
            return original(policy)
        patch.setattr(sim, '_install_policy', install)
    elif stage == 'reserve':
        budget = sim.disk_budget
        original = budget.reserve
        def reserve(amount, **kwargs):
            if kwargs.get('checkpoint_link') is not None or kwargs.get('operation') == 'checkpoint':
                raise DiskAdmissionError('injected checkpoint reservation fault')
            return original(amount, **kwargs)
        patch.setattr(budget, 'reserve', reserve)
        original_atomic = checkpoint.atomic_json
        def guarded(path, data, **kwargs):  # every checkpoint file is admitted before it is written
            if kwargs.get('checkpoint_context'): raise DiskAdmissionError('injected checkpoint reservation fault')
            return original_atomic(path, data, **kwargs)
        patch.setattr(checkpoint, 'atomic_json', guarded)
    else:
        original_atomic = checkpoint.atomic_json
        marker = {'prepare': 'prepare-', 'decision': '/global/manifests/', 'ack': 'decision-'}[stage]
        def failing(path, data, **kwargs):
            if marker in str(path) and (stage != 'ack' or '/global/' not in str(path)):
                raise OSError(f'injected {stage} fault')
            return original_atomic(path, data, **kwargs)
        patch.setattr(checkpoint, 'atomic_json', failing)


@pytest.mark.parametrize('stage', ['drain', 'install', 'reserve', 'prepare', 'decision'])
def test_faults_before_the_durable_decision_keep_the_old_policy_and_cut(tmp_path, monkeypatch, stage):
    sim, binding = _prepared(tmp_path / 'data')
    try:
        root, frontier = sim.root, sim.frontier
        with monkeypatch.context() as patch:
            _fault(stage, sim, patch)
            with pytest.raises(Exception, match='injected'):
                sim.runtime.apply_policy({'swap_policy': 'S2'}, classification='FRONTIER_REPLAN')
        assert sim.policy.swap_policy == 'S0'
        assert (sim.runtime.plan_version, sim.runtime.policy_epoch) == (1, 1) and not sim.runtime.transitions
        assert (sim.root, sim.frontier) == (root, frontier)
        sim.materialize(binding=binding)  # the run continues under the old policy
        expected = eager_reference(sim.sequencer.operations, 4, sim.assets, binding)
        np.testing.assert_allclose(sim.state(allow_gather=True), expected, atol=1e-10)
    finally:
        sim.close()
    with AsyncDistributedSimulator.resume(_config(tmp_path / 'data'), comm=MPI.COMM_SELF) as resumed:
        assert resumed.policy.swap_policy == 'S0' and resumed.runtime.plan_version == 1 and resumed.frontier == 3


def test_ack_fault_after_the_durable_decision_stops_and_resumes_on_the_new_decision(tmp_path, monkeypatch):
    sim, binding = _prepared(tmp_path / 'data')
    with monkeypatch.context() as patch:
        _fault('ack', sim, patch)
        with pytest.raises(Exception, match='injected ack fault'):
            sim.runtime.apply_policy({'swap_policy': 'S2'}, classification='FRONTIER_REPLAN')
    assert sim._checkpoint_decision_durable and sim.checkpoint_requires_recovery
    assert sim.policy.swap_policy == 'S2' and sim.runtime.plan_version == 2  # no rollback past COMMIT
    with pytest.raises(Exception):
        sim.materialize(binding=binding)  # the runtime stops until recovery
    sim.close()
    with AsyncDistributedSimulator.resume(_config(tmp_path / 'data'), comm=MPI.COMM_SELF) as resumed:
        assert resumed.policy.swap_policy == 'S2' and resumed.runtime.plan_version == 2 and resumed.frontier == 3
        resumed.materialize(binding=resumed.bind())
        expected = eager_reference(resumed.sequencer.operations, 4, resumed.assets, resumed.bind())
        np.testing.assert_allclose(resumed.state(allow_gather=True), expected, atol=1e-10)


def test_successful_switch_keeps_state_expectation_and_gradient(tmp_path):
    import math
    from svsim.observables import ObservableIR, PauliTerm
    from svsim.parameters import ParameterSymbol
    theta = ParameterSymbol('theta', parameter_id='replan-fault-theta')
    with AsyncDistributedSimulator(4, _config(tmp_path / 'data'), comm=MPI.COMM_SELF, policy=_policy()) as sim:
        sim.gate('H', (0,))
        for _ in range(3): sim.gate('RX', (1,), angle=.2)
        sim.gate('RY', (3,), angle=theta)
        binding = sim.bind({theta.parameter_id: .37})
        sim.materialize(frontier=1, binding=binding)
        sim.commit_snapshot()
        sim.runtime.apply_policy({'swap_policy': 'S2'}, classification='FRONTIER_REPLAN')
        assert sim.policy.swap_policy == 'S2' and sim.runtime.plan_version == 2
        sim.materialize(binding=binding)
        expected = eager_reference(sim.sequencer.operations, 4, sim.assets, binding)
        np.testing.assert_allclose(sim.state(allow_gather=True), expected, atol=1e-10)
        observable = ObservableIR((PauliTerm(0, 1 << 3),))
        np.testing.assert_allclose(sim.expectation(observable), math.cos(.37), atol=1e-10)
        gradient = sim.gradient(observable, parameters=[theta], method='adjoint')
        np.testing.assert_allclose(gradient[theta.parameter_id], -math.sin(.37), atol=2e-9)


WORKER = textwrap.dedent('''
    import sys
    from mpi4py import MPI
    import numpy as np
    from svsim.async_scheduler import AsyncDistributedSimulator, AsyncPolicy
    from svsim.distributed import DistributedConfig
    from svsim.kernels import eager_reference
    comm = MPI.COMM_WORLD
    config = DistributedConfig(sys.argv[1], 2, ram_budget_bytes_per_rank=8 * 1024**2, metadata_reserve_bytes=128 * 1024,
                               tile_bytes=4096, frame_policy='materialize', storage_scope='node-local')
    policy = AsyncPolicy(mode='auto', io_engine='cooperative', io_workers=1, prefetch_blocks=0, lookahead_gates=4,
                         cooldown_seconds=0)
    with AsyncDistributedSimulator(5, config, comm=comm, policy=policy) as sim:
        for q in (0, 1, 4, 0, 1, 4): sim.gate('RX', (q,), angle=.3)
        binding = sim.bind(); sim.materialize(frontier=2, binding=binding); sim.commit_snapshot()
        original = sim._install_policy
        def install(p):
            if comm.Get_rank() == 1 and p.swap_policy == 'S2': raise OSError('rank-1 install fault')
            return original(p)
        sim._install_policy = install
        try:
            sim.runtime.apply_policy({'swap_policy': 'S2'}, classification='FRONTIER_REPLAN')
            raise SystemExit('fault was not reported')
        except SystemExit: raise
        except Exception: pass
        assert sim.policy.swap_policy == 'S0' and sim.runtime.plan_version == 1, (comm.Get_rank(), sim.policy)
        sim._install_policy = original
        sim.materialize(binding=binding)
        state = sim.state(allow_gather=True)
        if comm.Get_rank() == 0:
            expected = eager_reference(sim.sequencer.operations, 5, sim.assets, binding)
            assert np.allclose(state, expected, atol=1e-10)
            print('ok')
''')


@pytest.mark.skipif(shutil.which('mpiexec') is None, reason='MPI launcher not available')
def test_one_rank_install_fault_rolls_back_every_rank(tmp_path):
    result = subprocess.run([shutil.which('mpiexec'), '-n', '2', sys.executable, '-c', WORKER, str(tmp_path / 'data')],
                            capture_output=True, text=True, timeout=300,
                            env={**os.environ, 'PYTHONPATH': 'src', 'OMP_NUM_THREADS': '1'})
    assert result.returncode == 0, result.stderr[-2000:]
    assert 'ok' in result.stdout


KILLER = textwrap.dedent('''
    import os, sys
    from mpi4py import MPI
    import svsim.distributed_checkpoint as checkpoint
    from svsim.async_scheduler import AsyncDistributedSimulator, AsyncPolicy
    from svsim.distributed import DistributedConfig
    root, stage = sys.argv[1], sys.argv[2]
    config = DistributedConfig(root, 2, ram_budget_bytes_per_rank=8 * 1024**2, metadata_reserve_bytes=128 * 1024,
                               tile_bytes=4096, frame_policy='materialize', storage_scope='node-local')
    policy = AsyncPolicy(mode='auto', io_engine='cooperative', io_workers=1, prefetch_blocks=0, lookahead_gates=4,
                         cooldown_seconds=0)
    sim = AsyncDistributedSimulator(4, config, comm=MPI.COMM_SELF, policy=policy)
    for q in (0, 1, 0, 1, 0, 1): sim.gate('RX', (q,), angle=.3)
    binding = sim.bind(); sim.materialize(frontier=2, binding=binding); sim.commit_snapshot()
    original = checkpoint.atomic_json
    def crash(path, data, **kwargs):
        if stage == 'before-decision' and '/global/manifests/' in str(path): os._exit(0)
        result = original(path, data, **kwargs)
        if stage == 'after-decision' and '/global/manifests/' in str(path): os._exit(0)
        return result
    checkpoint.atomic_json = crash
    sim.runtime.apply_policy({'swap_policy': 'S2'}, classification='FRONTIER_REPLAN')
''')


@pytest.mark.parametrize('stage,policy,plan', [('before-decision', 'S0', 1), ('after-decision', 'S2', 2)])
def test_process_killed_around_the_decision_resumes_on_the_authoritative_side(tmp_path, stage, policy, plan):
    subprocess.run([sys.executable, '-c', KILLER, str(tmp_path / 'data'), stage], check=True, timeout=120,
                   env={**os.environ, 'PYTHONPATH': 'src', 'OMP_NUM_THREADS': '1'})
    with AsyncDistributedSimulator.resume(_config(tmp_path / 'data'), comm=MPI.COMM_SELF) as resumed:
        assert resumed.policy.swap_policy == policy and resumed.runtime.plan_version == plan
        assert resumed.frontier == 2
        resumed.materialize(binding=resumed.bind())
        expected = eager_reference(resumed.sequencer.operations, 4, resumed.assets, resumed.bind())
        np.testing.assert_allclose(resumed.state(allow_gather=True), expected, atol=1e-10)
