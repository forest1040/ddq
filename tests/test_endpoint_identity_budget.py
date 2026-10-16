"""Endpoint identity capture stays bounded even when execution may be large."""
import json
from types import SimpleNamespace

import numpy as np
import pytest

MPI=pytest.importorskip('mpi4py.MPI')
from svsim import ObservableIR,PauliTerm,ParameterSymbol
from svsim.ir import GateOp
from svsim.parameters import expression
from svsim.remaining_work import endpoint_tasks,materialize_prepare_condition,gradient_condition,ENDPOINT_IDENTITY_BUDGET
from svsim.runtime_eta import RuntimeETA
from svsim.runtime_cost import CostSample
from test_endpoint_prepare import simulator


class HugeSources:
    def __len__(self):return 10**12
    def __iter__(self):raise AssertionError('Unbounded source enumeration')


class Model:
    num_qubits=4;block_bits=2;size=1;frontier=-1
    config=SimpleNamespace(backend='buffered',amplitude_store='file')
    io_engine=SimpleNamespace(identity='cooperative')
    @property
    def circuit_hash(self):raise AssertionError('Unbounded circuit hash')


@pytest.mark.parametrize('parameters',[None,['p']])
def test_huge_gradient_sources_are_rejected_before_iteration_or_hash_and_never_priced(parameters):
    sim=Model();sim.sequencer=SimpleNamespace(operations=HugeSources())
    spec={'gradient':{'observable':ObservableIR((PauliTerm(0,1),)),'parameters':parameters,'method':'adjoint'}}
    sim.runtime=SimpleNamespace(_eta_endpoint_spec=spec)
    condition=materialize_prepare_condition(sim,pending_sources=0)
    tasks,_=endpoint_tasks(sim,spec,{})
    assert len(json.dumps(condition))<1024 and len(tasks)==2
    assert all(t.unknown_reasons==(ENDPOINT_IDENTITY_BUDGET+':gradient',) for t in tasks)
    # A resolver may even contain learned samples of the sentinel; unknown
    # identity remains stronger than those numbers.
    snapshot=SimpleNamespace(snapshot_id='bounded',complete=True,stale=False,unknown_reasons=(),
        window=(),tail=(),endpoint_tasks=tasks,in_flight=(),context={})
    result=RuntimeETA().evaluate(snapshot,{t.cost_key:CostSample('learned-sentinel',1,1,1) for t in tasks})
    assert result.expected_seconds is None and result.known_work_count==0
    assert ENDPOINT_IDENTITY_BUDGET+':gradient' in result.unknown_reasons


@pytest.mark.parametrize('spec',[
    {'expectation':[PauliTerm(0,1)]*257},
    {'expectation':ObservableIR((PauliTerm(0,1<<1024),))},
    {'gradient':{'observable':ObservableIR((PauliTerm(0,1),)),'parameters':['p']*257}},
    {'gradient':{'observable':ObservableIR((PauliTerm(0,1),)),'parameters':['p'*129]}}])
def test_oversized_explicit_spec_stays_bounded_unknown_without_breaking_materialize(tmp_path,spec):
    with simulator(tmp_path/'s') as sim:
        sim.gate('H',(0,));sim.runtime.set_eta_endpoint(**spec)
        condition=materialize_prepare_condition(sim,pending_sources=0)
        assert condition['eta_unknown_reasons'] and len(json.dumps(condition))<1024
        sim.materialize()  # Monitoring refusal must not reject valid execution.
        assert sim.frontier==0
        tasks,_=endpoint_tasks(sim,spec,{})
        assert any(t.unknown_reasons for t in tasks)
        assert sim.runtime.update_eta()['views']['observed']['expected_seconds'] is None
        # Read state directly without asking to execute the oversized endpoint.
        np.testing.assert_allclose(sim.state(allow_gather=True)[:2],np.array([1,1])/np.sqrt(2),atol=1e-12)


def test_small_none_and_explicit_gradient_conditions_keep_exact_identity(tmp_path):
    with simulator(tmp_path/'s') as sim:
        theta=ParameterSymbol('theta','bounded-theta')
        sim.gate('RY',(0,),angle=theta);sim.materialize(binding=sim.bind({theta:.3}))
        observable=ObservableIR((PauliTerm(0,1),))
        for params in (None,['bounded-theta']):
            spec={'gradient':{'observable':observable,'parameters':params,'method':'adjoint'}}
            sim.runtime.set_eta_endpoint(**spec)
            tasks,_=endpoint_tasks(sim,spec,{})
            assert not any(t.unknown_reasons for t in tasks)
            assert tasks[1].condition==gradient_condition(sim,observable,['bounded-theta'],'adjoint')
            assert tasks[0].condition==materialize_prepare_condition(sim,pending_sources=0)


def test_expression_tree_budget_is_checked_without_recursive_dependencies_or_hash():
    theta=ParameterSymbol('theta','p');angle=expression(theta)
    for _ in range(40):angle=angle+1
    sim=Model();sim.sequencer=SimpleNamespace(operations=[GateOp('RY',(0,),angle=angle)])
    spec={'gradient':{'observable':ObservableIR((PauliTerm(0,1),))}}
    sim.runtime=SimpleNamespace(_eta_endpoint_spec=spec)
    assert materialize_prepare_condition(sim,pending_sources=0)['eta_unknown_reasons']


def test_128_gate_identity_remains_exact_with_none_parameters():
    class SmallModel(Model):circuit_hash='c'*64
    sim=SmallModel();theta=ParameterSymbol('theta','p')
    sim.sequencer=SimpleNamespace(operations=[GateOp('RY',(0,),angle=expression(theta)) for _ in range(128)])
    observable=ObservableIR((PauliTerm(0,1),))
    spec={'gradient':{'observable':observable,'method':'adjoint'}}
    sim.runtime=SimpleNamespace(_eta_endpoint_spec=spec)
    tasks,_=endpoint_tasks(sim,spec,{})
    assert not any(t.unknown_reasons for t in tasks)
    assert tasks[1].condition==gradient_condition(sim,observable,['p'],'adjoint')
