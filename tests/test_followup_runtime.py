import math
import pytest

MPI=pytest.importorskip('mpi4py.MPI')
from svsim import AsyncDistributedSimulator,AsyncPolicy,DistributedConfig,ObservableIR,PauliTerm,ParameterSymbol
from svsim.gradient_calibration import GradientCalibrationSpec,calibrate_gradient_methods
from svsim.gradient_runtime_context import gradient_context
from svsim.performance_injection import InjectionSpec


def test_compute_injection_is_opt_in_bounded_and_not_production_cost(tmp_path):
    config=DistributedConfig(str(tmp_path/'injected'),2,ram_budget_bytes_per_rank=4*1024**2,frame_policy='materialize')
    with AsyncDistributedSimulator(4,config,comm=MPI.COMM_SELF,
        policy=AsyncPolicy(io_engine='cooperative',prefetch_blocks=0,lookahead_gates=2)) as sim:
        before=sim.budget.used
        events=[]
        driver=sim.runtime.configure_performance_injection(InjectionSpec(0,'compute','work',0,4,.001,
            ('executor-group',),.005,8,barrier='prefix'),observer=events.append)
        assert sim.budget.used>before
        driver.barrier('prefix')
        for _ in range(4):sim.gate('RY',(0,),angle=.1)
        sim.materialize()
        assert len(events)==4 and all(e['success'] for e in events)
        assert driver.snapshot()['counters']['completed']==4
        assert driver.snapshot()['planned_injection_seconds']<=.005
        assert driver.snapshot()['actual_injection_seconds']>0
        assert all(row.evidence!='injected' for row in sim.runtime.measurements.cost_observations())
        assert sim.runtime.latest_eta['views']['observed']['expected_seconds'] is None
        assert 'EVALUATION_INJECTION_ACTIVE' in sim.runtime.latest_eta['views']['observed']['unknown_reasons']
        assert sim.expectation(ObservableIR((PauliTerm(0,1),)))==pytest.approx(math.cos(.4),abs=1e-10)
    assert sim.runtime._injection_budget==0


def test_raw_gradient_attachment_never_runs_pilot_and_preserves_binding(tmp_path):
    theta=ParameterSymbol('theta','followup-runtime-theta')
    config=DistributedConfig(str(tmp_path/'gradient'),2,ram_budget_bytes_per_rank=4*1024**2)
    with AsyncDistributedSimulator(4,config,comm=MPI.COMM_SELF,
        policy=AsyncPolicy(io_engine='cooperative',prefetch_blocks=0)) as sim:
        sim.gate('RY',(0,),angle=theta)
        sim.materialize(binding=sim.bind({theta:.37}))
        observable=ObservableIR((PauliTerm(0,1),))
        context=gradient_context(sim,observable,[theta])
        report=calibrate_gradient_methods(GradientCalibrationSpec(context,methods=('adjoint',),
            warmup_repetitions=0,repetitions=1),lambda *_:object(),lambda *_:0,lambda *_:True)
        root,binding,history=sim.root,sim.binding.binding_id,tuple(sim.history)
        sim.runtime.attach_gradient_calibration(report)
        assert (sim.root,sim.binding.binding_id,tuple(sim.history))==(root,binding,history)
        result=sim.gradient(observable,parameters=[theta],method='auto')
        assert result[theta.parameter_id]==pytest.approx(-math.sin(.37),abs=1e-9)
        assert sim.runtime.latest_gradient['method']=='adjoint'
        assert sim.runtime.latest_gradient['cost_origin']=='INSUFFICIENT_BUILD_BASELINE'
        assert sim.binding.binding_id==binding
        generator_result=sim.gradient((term for term in observable.terms),parameters=[theta],method='adjoint')
        assert generator_result[theta.parameter_id]==pytest.approx(result[theta.parameter_id],abs=1e-9)
    assert sim.runtime._gradient_profile_budget==0
