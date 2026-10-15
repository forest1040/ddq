from dataclasses import replace
from types import SimpleNamespace

import pytest
MPI = pytest.importorskip('mpi4py.MPI')

from svsim import AsyncDistributedSimulator, AsyncPolicy, DistributedConfig
from svsim.executor_stages import executor_route, route_kind, CONTRACT
from svsim.remaining_work import RemainingWorkExtractor, CaptureLimits
from svsim.runtime_eta import RuntimeETA
from svsim.runtime_cost import CostSample
from svsim.ir import GateOp
from svsim.ownership import OwnershipMap


def make_sim(path, *, policy='S0', frame='materialize', lookahead=4):
    config = DistributedConfig(str(path),2,frame_policy=frame,
        ram_budget_bytes_per_rank=32*1024**2,metadata_reserve_bytes=1024**2)
    return AsyncDistributedSimulator(4,config,comm=MPI.COMM_SELF,
        policy=AsyncPolicy(io_engine='cooperative',swap_policy=policy,
                           lookahead_gates=lookahead,prefetch_blocks=0))


def executor(snapshot):
    return next(t for t in snapshot.window if t.stage_route)


@pytest.mark.parametrize('target,controls,expected_route,expected_calls,work',[
    (0,(), 'operation-shipping',4,16),
    (0,(3,), 'operation-shipping',2,8),
    (2,(), 'joint-async',2,16),
])
def test_stage_oracle_matches_actual_kernel_dispatch(tmp_path,monkeypatch,target,controls,
                                                     expected_route,expected_calls,work):
    import svsim.distributed as distributed
    import svsim.async_scheduler as scheduler
    with make_sim(tmp_path / str(target)) as sim:
        sim.runtime.configure_measurements(max_bytes=1024**2)
        if controls:sim.gate('MCX',(target,),controls=controls,control_values=(1,))
        else:sim.gate('H',(target,))
        snap=RemainingWorkExtractor().capture(sim); task=executor(snap)
        assert task.stage_route.route==expected_route
        calls=[]
        module,name=distributed,'apply_local'
        # Joint apply_group is imported inside _execute, so patch its source.
        if expected_route=='joint-async':
            import svsim.streaming_kernels as kernels
            module,name=kernels,'apply_group'
        original=getattr(module,name)
        def count(*a,**kw):
            calls.append(1);return original(*a,**kw)
        monkeypatch.setattr(module,name,count)
        sim.materialize()
        assert len(calls)==expected_calls
        kernel=next(n for n in task.stage_route.stages if n.stage=='kernel')
        assert kernel.count_lower==kernel.count_upper==len(calls)
        assert task.kernel_work_per_unit==work
        rows=[r for r in sim.runtime.measurements.cost_observations()
              if r.stage in {'executor-gate','executor-group'}]
        assert rows and rows[0].condition==task.condition
        assert all(n.resource in {'main','storage','nic'} for n in task.stage_route.stages)
        for i,node in enumerate(task.stage_route.stages):
            assert node.dependencies==(() if i==0 else (task.stage_route.stages[i-1].stage,))


@pytest.mark.parametrize('kind,targets,route',[
    ('H',(0,),'virtual-local-rule'),('SWAP',(2,3),'virtual-permutation')])
def test_virtual_routes_have_no_data_fetch_or_kernel(tmp_path,monkeypatch,kind,targets,route):
    with make_sim(tmp_path / kind,frame='keep') as sim:
        sim.gate(kind,targets)
        task=executor(RemainingWorkExtractor().capture(sim))
        assert task.stage_route.route==route
        assert [n.stage for n in task.stage_route.stages]==['metadata','commit']
        monkeypatch.setattr(sim,'fetch',lambda *a,**k:pytest.fail('virtual route fetched data'))
        sim.materialize()
        assert task.kernel_work_per_unit==task.payload_bytes_per_unit==0


def test_fused_oracle_counts_gate_calls_and_one_state_pass(tmp_path,monkeypatch):
    import svsim.async_scheduler as scheduler
    with make_sim(tmp_path/'fused',policy='S2') as sim:
        sim.gate('H',(0,));sim.gate('H',(1,))
        task=executor(RemainingWorkExtractor().capture(sim))
        assert task.source_ids==(0,1) and task.stage_route.route=='fused-local'
        calls=[];fetches=[]
        apply=scheduler.apply_local;fetch=sim.fetch
        def apply_count(*a,**kw):calls.append(1);return apply(*a,**kw)
        def fetch_count(*a,**kw):fetches.append(1);return fetch(*a,**kw)
        monkeypatch.setattr(scheduler,'apply_local',apply_count)
        monkeypatch.setattr(sim,'fetch',fetch_count)
        sim.materialize()
        assert len(calls)==8 and len(fetches)==4
        assert task.kernel_work_per_unit==32


def test_inclusive_parent_prices_all_stages_once_and_missing_parent_is_unknown(tmp_path):
    with make_sim(tmp_path/'inclusive') as sim:
        sim.gate('H',(2,));snap=RemainingWorkExtractor().capture(sim)
        costs=lambda t:CostSample(t.task_id,2,1,3)
        report=RuntimeETA().evaluate(snap,costs)
        assert report.expected_seconds==2*len(snap.window+snap.future)
        assert report.executor_stage_coverage[0]['included_in_priced_parent']
        assert report.executor_stage_coverage[0]['independently_calibrated_stages']==0
        assert not report.eligible_for_control
        assert RuntimeETA().evaluate(snap,{}).expected_seconds is None
        t=executor(snap)
        forged=replace(t,condition={k:v for k,v in t.condition.items() if k!='stage_contract'})
        bad=replace(snap,window=tuple(forged if x is t else x for x in snap.window))
        assert any(r.startswith('UNAUTHENTICATED_INCLUSIVE_STAGE_CONTRACT')
                   for r in RuntimeETA().evaluate(bad,costs).unknown_reasons)


def test_future_and_window_routes_share_actual_condition_contract(tmp_path):
    with make_sim(tmp_path/'future',policy='S2',lookahead=2) as sim:
        for _ in range(8):sim.gate('H',(0,));sim.gate('H',(2,))
        snap=RemainingWorkExtractor(CaptureLimits(scan_seconds=.5)).capture(sim)
        assert snap.future_complete
        for task in snap.window+snap.future:
            if task.kind not in {'executor','executor-count'}:continue
            assert task.stage_route and task.condition['stage_contract']==CONTRACT
            assert task.condition['stage_route']==task.stage_route.route
        assert snap.metadata_bytes<=65536


def test_huge_block_count_uses_algebra_and_remote_payload_stays_unknown():
    sim=SimpleNamespace(num_qubits=50,block_bits=2,size=2,rank=0,
        ownership=OwnershipMap(50,2,2),policy=SimpleNamespace(swap_policy='S0'),
        config=SimpleNamespace(frame_policy='materialize'),store=SimpleNamespace(payload_bytes=64))
    route=executor_route(sim,(GateOp('H',(49,)),))
    assert route.route=='joint-async' and len(route.stages)<=9
    assert route.payload_bytes is None and route.payload_bytes_lower==0
    assert route.payload_bytes_upper>0
    assert route.kernel_work==(1<<50)
    assert any(n.stage=='mpi-input' and n.unknown_reasons for n in route.stages)


def test_s1_and_rotation_compaction_stay_conditional(tmp_path):
    with make_sim(tmp_path/'rotation',frame='keep') as sim:
        sim.gate('RX',(0,),angle=.123)
        task=executor(RemainingWorkExtractor().capture(sim))
        assert task.stage_route.route=='virtual-rotation-rule'
        assert task.kernel_work_per_unit is None
        assert 'LOG_BOUND_COMPACTION_CONDITIONAL' in task.stage_route.unknown_reasons
    sim=SimpleNamespace(num_qubits=8,block_bits=2,size=2,rank=0,
        ownership=OwnershipMap(8,2,2),policy=SimpleNamespace(swap_policy='S1'),
        config=SimpleNamespace(frame_policy='materialize'),store=SimpleNamespace(payload_bytes=64))
    route=executor_route(sim,(GateOp('H',(7,)),))
    assert route.stages[0].stage=='layout-relocation'
    assert route.kernel_work is None and route.payload_bytes is None


def test_storage_coverage_is_separate_from_zero_mpi_payload(tmp_path):
    with make_sim(tmp_path/'storage') as sim:
        sim.gate('H',(0,));snap=RemainingWorkExtractor().capture(sim)
        report=RuntimeETA().evaluate(snap,lambda t:CostSample(t.task_id,2,1,3))
        assert report.payload_byte_coverage.coverage==1
        assert report.payload_byte_coverage.total_work==0
        assert report.storage_byte_coverage.coverage is None
        assert report.storage_byte_coverage.total_work is None
        assert any('UNESTIMATED_AMPLITUDE_STORAGE_BYTES' in r
                   for r in report.storage_byte_coverage.unknown_reasons)


@pytest.mark.parametrize('exchange,expected_work',[('packed',8),('full',16)])
def test_mcx_dispatch_oracle_distinguishes_active_packing_from_full_kernel(tmp_path,monkeypatch,exchange,expected_work):
    import svsim.distributed as distributed
    config=DistributedConfig(str(tmp_path/exchange),2,frame_policy='materialize',mcx_exchange=exchange,
        ram_budget_bytes_per_rank=32*1024**2,metadata_reserve_bytes=1024**2)
    with AsyncDistributedSimulator(4,config,comm=MPI.COMM_SELF,
            policy=AsyncPolicy(io_engine='cooperative',prefetch_blocks=0)) as sim:
        sim.gate('MCX',(2,),controls=(0,),control_values=(1,))
        task=executor(RemainingWorkExtractor().capture(sim))
        assert task.stage_route.route=='mcx-'+exchange
        assert task.kernel_work_per_unit==expected_work
        calls=[]
        name='partial_mcx_pair' if exchange=='packed' else 'apply_group'
        original=getattr(distributed,name)
        def count(*a,**kw):calls.append(1);return original(*a,**kw)
        monkeypatch.setattr(distributed,name,count)
        sim.materialize()
        assert len(calls)==2
        assert next(n.count_lower for n in task.stage_route.stages if n.stage=='kernel')==2


def test_exact_pi_rule_does_not_claim_log_compaction(tmp_path):
    from svsim.parameters import ExactPi
    with make_sim(tmp_path/'exact',frame='keep') as sim:
        sim.gate('RX',(0,),angle=ExactPi(1,2))
        task=executor(RemainingWorkExtractor().capture(sim))
        assert task.stage_route.route=='virtual-local-rule'
        assert task.kernel_work_per_unit==0
        sim.materialize()


def test_full_32_call_window_preserves_a_useful_capture_under_default_budget(tmp_path):
    with make_sim(tmp_path/'full-window',lookahead=32) as sim:
        for _ in range(64):sim.gate('H',(2,))
        snap=RemainingWorkExtractor(CaptureLimits(scan_seconds=.5)).capture(sim)
        assert len([t for t in snap.window if t.stage_route])==32
        assert snap.future_complete and not snap.unknown_reasons
        assert snap.metadata_bytes<=65536
        assert RuntimeETA().evaluate(snap,lambda t:CostSample(t.task_id,2,1,3)).expected_seconds is not None


def test_packed_remote_route_counts_tiles_and_actual_send_lower_bound():
    sim=SimpleNamespace(num_qubits=8,block_bits=4,size=2,rank=0,binding=None,
        ownership=OwnershipMap(8,4,2),policy=SimpleNamespace(swap_policy='S0'),
        network_profile=None,config=SimpleNamespace(frame_policy='materialize',mcx_exchange='packed',tile_bytes=16),
        store=SimpleNamespace(payload_bytes=256))
    route=executor_route(sim,(GateOp('MCX',(7,),controls=(0,),control_values=(1,)),))
    assert route.route=='mcx-packed'
    nodes={n.stage:n for n in route.stages}
    assert nodes['packing'].count_lower==nodes['packing'].count_upper==64
    assert nodes['mpi-exchange'].count_lower==64
    assert nodes['unpacking'].count_lower==64
    assert nodes['packing'].dependencies==('mpi-input',)
    assert nodes['mpi-exchange'].dependencies==('packing',)
    assert route.payload_bytes is None and route.payload_bytes_lower==1024
    assert route.kernel_work==0


def test_s1_operation_shipping_zero_transfer_does_not_hide_possible_relocation(tmp_path):
    with make_sim(tmp_path/'s1-shipping') as sim:
        physical=(GateOp('H',(0,)),)
        sim.policy=replace(sim.policy,swap_policy='S1')
        # A rank-local synthetic view preserves the real local gate classifier.
        proxy=SimpleNamespace(num_qubits=sim.num_qubits,block_bits=sim.block_bits,size=2,rank=0,
            ownership=OwnershipMap(4,2,2),policy=sim.policy,config=sim.config,store=sim.store,local=sim.local)
        route=executor_route(proxy,physical)
        assert route.route=='operation-shipping' and route.payload_bytes is None
        assert route.payload_bytes_upper>0
