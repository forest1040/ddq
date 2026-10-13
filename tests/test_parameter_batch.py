import numpy as np
import pytest

MPI = pytest.importorskip('mpi4py.MPI')
from svsim import (AsyncDistributedSimulator, AsyncPolicy, DistributedConfig,
                   ObservableIR, PauliTerm, ParameterSymbol, ParameterBatchExecutor)


def test_batch_interleaves_suffixes_and_shares_memory_ledger(tmp_path):
    theta = ParameterSymbol('batch-theta', parameter_id='batch-theta')
    policy = AsyncPolicy(max_active_jobs=2, lookahead_gates=1)
    config = DistributedConfig(str(tmp_path), 2, ram_budget_bytes_per_rank=1024**2)
    with AsyncDistributedSimulator(4, config, policy=policy, comm=MPI.COMM_SELF) as sim:
        sim.gate('H', (3,))
        sim.gate('RY', (0,), angle=theta)
        sim.gate('RZ', (1,), angle=.2)
        bindings = [sim.bind({theta: x}) for x in (.1, .4, -.8)]
        prefix = sim.commit_prefix([theta], binding=bindings[0])
        before = sim.budget.used
        with ParameterBatchExecutor(sim, prefix, policy=policy) as batch:
            results = batch.run(bindings, observable=ObservableIR((PauliTerm(0, 1),)), gradient='adjoint')
            order = [event['job'] for event in batch.trace]
        assert order[:2] == [0, 1]
        assert sim.budget.used <= before + 64*1024
        for result, x in zip(results, (.1, .4, -.8)):
            assert result.expectation == pytest.approx(np.cos(x), abs=1e-10)
            assert result.gradient[theta.parameter_id] == pytest.approx(-np.sin(x), abs=1e-9)
            assert result.managed_peak_bytes <= config.ram_budget_bytes_per_rank
        assert not list(tmp_path.rglob('global/leases/*.json'))


def test_batch_parameter_shift_uses_template_origin(tmp_path):
    theta = ParameterSymbol('batch-shift', parameter_id='batch-shift')
    policy = AsyncPolicy(max_active_jobs=1, lookahead_gates=1)
    config = DistributedConfig(str(tmp_path), 2, ram_budget_bytes_per_rank=2*1024**2)
    observable = ObservableIR((PauliTerm(0, 1),))
    with AsyncDistributedSimulator(4, config, policy=policy, comm=MPI.COMM_SELF) as sim:
        sim.gate('H', (3,))
        sim.gate('RY', (0,), angle=theta)
        bindings = [sim.bind({theta: value}) for value in (.2, .7)]
        prefix = sim.commit_prefix([theta], binding=bindings[0])
        with ParameterBatchExecutor(sim, prefix, policy=policy) as batch:
            results = batch.run(bindings, observable=observable, gradient='parameter-shift')
        for result, value in zip(results, (.2, .7)):
            assert result.expectation == pytest.approx(np.cos(value), abs=1e-10)
            assert result.gradient[theta.parameter_id] == pytest.approx(-np.sin(value), abs=1e-9)
