"""Real prelude samples are distinct from executed gates and endpoint bodies."""
import json
from time import perf_counter

import numpy as np
import pytest

MPI=pytest.importorskip('mpi4py.MPI')
from svsim import AsyncDistributedSimulator,AsyncPolicy,DistributedConfig,ObservableIR,PauliTerm,ParameterSymbol
from svsim.remaining_work import cost_context,endpoint_tasks,materialize_prepare_condition,gradient_condition
from svsim import runtime_observed_costs as roc


def simulator(path):
    return AsyncDistributedSimulator(4,DistributedConfig(str(path),2,ram_budget_bytes_per_rank=16*1024**2,
        metadata_reserve_bytes=2*1024**2,frame_policy='materialize'),comm=MPI.COMM_SELF,
        policy=AsyncPolicy(io_engine='cooperative',prefetch_blocks=0,lookahead_gates=4))


def test_prepare_records_real_update_eta_before_gates_and_each_endpoint_has_one_call(tmp_path,monkeypatch):
    with simulator(tmp_path/'s') as sim:
        sim.runtime.configure_measurements(max_bytes=256*1024)
        sim.gate('RX',(0,),angle=.37)
        observable=ObservableIR((PauliTerm(0,1),));sim.runtime.set_eta_endpoint(expectation=observable)
        before_condition=materialize_prepare_condition(sim,pending_sources=1)
        elapsed=[];update=sim.runtime.update_eta
        def timed_update(*args,**kwargs):
            tick=perf_counter()
            try:return update(*args,**kwargs)
            finally:elapsed.append(perf_counter()-tick)
        monkeypatch.setattr(sim.runtime,'update_eta',timed_update)
        timeline=[];record=sim._record_stage
        def recorded(stage,condition,**kwargs):
            timeline.append((stage,sim.frontier));return record(stage,condition,**kwargs)
        monkeypatch.setattr(sim,'_record_stage',recorded)
        sim.materialize()
        prepare=[r for r in sim.runtime.measurements.cost_rows() if r.stage=='materialize-prepare'][-1]
        assert prepare.condition==before_condition
        assert prepare.evidence=='measured' and prepare.success and prepare.validated
        assert prepare.protocol_seconds>=elapsed[0]>0
        assert timeline[0]==('materialize-prepare',-1) and sim.frontier==0
        executors_before=[r.event_id for r in sim.runtime.measurements.cost_rows() if r.stage in roc.EXECUTOR_STAGES]
        assert executors_before
        for _ in range(12):
            np.testing.assert_allclose(sim.expectation(observable),np.cos(.37),atol=1e-12)
        executors_after=[r.event_id for r in sim.runtime.measurements.cost_rows() if r.stage in roc.EXECUTOR_STAGES]
        assert executors_after==executors_before  # empty materialize did not execute/pay gates again
        tasks,label=endpoint_tasks(sim,sim.runtime._eta_endpoint_spec,cost_context(sim))
        assert label=='gates+expectation'
        assert [(t.task_id,t.count) for t in tasks]==[('endpoint:expectation:prepare',1),('endpoint:expectation',1)]
        rows=[r for r in sim.runtime.measurements.cost_rows() if r.stage=='materialize-prepare']
        current=tasks[0].condition
        assert sum(r.condition==current for r in rows)==12
        assert current['pending_sources']==0 and before_condition['pending_sources']==1
        assert roc._class(current)!=roc._class(before_condition)


def test_prepare_source_and_endpoint_spec_changes_are_unmeasured_bounded_classes(tmp_path):
    with simulator(tmp_path/'s') as sim:
        sim.runtime.configure_measurements(max_bytes=256*1024)
        sim.gate('RX',(0,),angle=.37);sim.materialize()
        observable=ObservableIR((PauliTerm(0,1),));sim.runtime.set_eta_endpoint(expectation=observable)
        for _ in range(12):sim.expectation(observable)
        original=materialize_prepare_condition(sim,pending_sources=0)
        sim.gate('RX',(1,),angle=.2)
        grown=materialize_prepare_condition(sim,pending_sources=0)
        assert grown['source_entries']==original['source_entries']+1
        assert roc._class(grown)!=roc._class(original)
        assert roc.observed_task_cost(sim.runtime.measurements,grown,sim.runtime.policy_epoch,sim.runtime.plan_version) is None
        assert roc.observed_task_rejection(sim.runtime.measurements,grown,sim.runtime.policy_epoch,sim.runtime.plan_version)==('INSUFFICIENT_SAMPLES',)
        eta=sim.runtime.update_eta()
        assert 'COST_UNCERTIFIED:INSUFFICIENT_SAMPLES:endpoint:expectation:prepare' in eta['views']['observed']['unknown_reasons']
        sim.runtime.set_eta_endpoint(expectation=ObservableIR((PauliTerm(0,2),)))
        changed=materialize_prepare_condition(sim,pending_sources=0)
        assert changed['endpoint_spec']!=grown['endpoint_spec']
        assert len(json.dumps(changed))<1024  # only scalar counts/flags and a fixed hash, no embedded source/spec
        assert roc.observed_task_cost(sim.runtime.measurements,changed,sim.runtime.policy_epoch,sim.runtime.plan_version) is None


def test_combined_endpoint_counts_two_prepares_and_gradient_has_new_measurement_identity(tmp_path):
    with simulator(tmp_path/'s') as sim:
        sim.runtime.configure_measurements(max_bytes=256*1024)
        theta=ParameterSymbol('theta','prepare-test-theta');sim.gate('RY',(0,),angle=theta)
        binding=sim.bind({theta:.37});sim.materialize(binding=binding)
        observable=ObservableIR((PauliTerm(0,1),));parameters=[theta.parameter_id]
        sim.runtime.set_eta_endpoint(expectation=observable,gradient={'observable':observable,'parameters':parameters,'method':'adjoint'},checkpoint=True)
        tasks,_=endpoint_tasks(sim,sim.runtime._eta_endpoint_spec,cost_context(sim))
        assert [t.condition['operation'] for t in tasks]==['materialize-prepare','expectation','materialize-prepare','gradient','checkpoint-commit']
        assert all(t.count==1 for t in tasks)
        actual=sim.gradient(observable,parameters=parameters,binding=binding,method='adjoint')
        np.testing.assert_allclose(actual[theta.parameter_id],-np.sin(.37),atol=1e-10)
        row=[r for r in sim.runtime.measurements.cost_rows() if r.stage=='gradient'][-1]
        assert row.condition==gradient_condition(sim,observable,parameters,'adjoint')
        assert row.condition['measurement_scope']=='schedule-and-execute-v2'
        legacy={key:value for key,value in row.condition.items() if key!='measurement_scope'}
        assert roc._class(legacy)!=roc._class(row.condition)
        assert sum(r.stage=='materialize-prepare' and r.condition==tasks[0].condition for r in sim.runtime.measurements.cost_rows())==1
