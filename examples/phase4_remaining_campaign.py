"""Finite Mac-local paired comparisons, injected refusal and gradient pilots."""
import argparse
from contextlib import contextmanager
from dataclasses import asdict
from hashlib import sha256
import json
import math
import os
from pathlib import Path
import platform
import shutil
from time import perf_counter
from uuid import uuid4

import numpy as np
from mpi4py import MPI

from svsim import AsyncDistributedSimulator,AsyncPolicy,DistributedConfig,ObservableIR,PauliTerm,ParameterSymbol
from svsim.adaptive_evaluation import AdaptiveEvaluation
from svsim.gradient_calibration import GradientCalibrationSpec,calibrate_gradient_methods
from svsim.gradient_cost_adapter import GradientCostAdapter
from svsim.gradient_runtime_context import gradient_context
from svsim.journal import content_hash,gate_data
from svsim.ir import GateNormalizer,GlobalSequencer
from svsim.kernels import eager_reference
from svsim.performance_injection import InjectionSpec
from svsim.phase4_evaluation import CaseManifest,Variant,Trial,Campaign,paired_order


def tree_bytes(folder):
    return sum(p.stat().st_size for p in Path(folder).rglob('*') if p.is_file())


def cleanup(comm,directory):
    comm.Barrier()
    error=None
    try:
        if directory.exists():shutil.rmtree(directory)
    except Exception as exc:error=str(exc)
    errors=comm.allgather(error)
    if any(errors):raise RuntimeError('Trial cleanup failed: '+str(errors))


def config(path,args):
    return DistributedConfig(str(path),2,ram_budget_bytes_per_rank=args.ram_bytes,
        metadata_reserve_bytes=128*1024,tile_bytes=4096,backend=args.backend,
        frame_policy='materialize',disk_quota_bytes=args.disk_bytes,
        storage_scope='node-local')


def run_comparison(comm,folder,args,*,adaptive=False,scenario='sustained'):
    rank=comm.Get_rank();size=comm.Get_size()
    recipe=([('RX',(0,),.025)]*12 if adaptive else
        [('H',(3,),None)]+[item for _ in range(4) for item in [('RY',(0,),.07),('RZ',(1,),.1)]])
    variants=(Variant('fixed',{'mode':'observe'}),Variant('auto',{'mode':'auto'})) if adaptive else (
        Variant('S0',{'swap_policy':'S0'}),Variant('S2',{'swap_policy':'S2'}))
    starts={'sustained':(4,12),'transient':(4,5),'recovery':(4,8),'missing-baseline':(0,12)}
    start,end=starts[scenario]
    spec=InjectionSpec(1,'compute','work',start,end,.15,('executor-group',),1.5,16,barrier='semantic-prefix') if adaptive else None
    shared_recipe={'recipe':recipe,'injection':asdict(spec) if spec else None}
    normalizer=GateNormalizer(4);sequencer=GlobalSequencer()
    expected_source=content_hash([gate_data(sequencer.append(normalizer.gate(kind,target,
        **({'angle':angle} if angle is not None else {})))) for kind,target,angle in recipe])
    case=CaseManifest(f'{"adaptive-"+scenario if adaptive else "layout"}-p{size}',
        source={'hash':expected_source,'recipe':shared_recipe},initial_state={'kind':'initial-one'},
        binding={'kind':'empty'},exact_witness={'kind':'none'},observable={'Z':0},
        endpoint='construct+forward+state-validation+checkpoint+close+cleanup',n=4,b=2,ranks=size,
        resources={'ram_per_rank_bytes':args.ram_bytes,'disk_per_rank_bytes':args.disk_bytes,
            'threads':1,'scope':'injected-same-host' if adaptive else 'same-host'},
        backend=args.backend,build=args.build,seed=1,tolerances={'atol':1e-10,'rtol':1e-10},
        warmups=args.warmup,trials=args.repetitions,budget_seconds=args.wall_seconds)
    trials=[];warmup_rows=[];started=perf_counter()
    order=list(paired_order(variants,args.warmup,1)) if args.warmup else []
    order=[(v,i,True) for v,i in order]+[(v,i,False) for v,i in paired_order(variants,args.repetitions,1)]
    source_actual=expected_source
    for serial,(variant_id,index,warmup) in enumerate(order):
        expired=comm.bcast(perf_counter()-started>=args.wall_seconds if rank==0 else None,root=0)
        if expired:
            if not warmup:trials.append(Trial(variant_id,index,'unmeasured',reason='CAMPAIGN_WALL_LIMIT'))
            continue
        variant=next(v for v in variants if v.variant_id==variant_id)
        directory=folder/f'owner-{rank}'/f'trial-{serial}'
        policy=AsyncPolicy(io_engine='cooperative',io_workers=1,prefetch_blocks=0,lookahead_gates=4,
            cooldown_seconds=0,**dict(variant.axes))
        begin=perf_counter();background_load=os.getloadavg()
        session=AdaptiveEvaluation(baseline_groups=0 if scenario=='missing-baseline' else 4)
        failure=None;metrics={};adaptive_report=None;source=source_actual;resolved=[]
        try:
            with AsyncDistributedSimulator(4,config(directory,args),comm=comm,policy=policy) as sim:
                for kind,target,angle in recipe:sim.gate(kind,target,**({'angle':angle} if angle is not None else {}))
                binding=sim.bind()
                source=content_hash([gate_data(g) for g in sim.sequencer.operations])
                if source_actual is None:source_actual=source
                sim.transport.agree({'source':source,'expected':source_actual})
                if source!=source_actual:raise AssertionError('Trial source differs from preregistered source')
                expected=eager_reference(sim.sequencer.operations,4,sim.assets,binding)
                if adaptive:
                    def observe(event):
                        event={**event,'actual_service_seconds':sim.transport.max_value(event['actual_service_seconds']),
                            'injected_delay_seconds':sim.transport.max_value(event['injected_delay_seconds'])}
                        row=None
                        if rank==0:row=session.observe(event,profile=sim.local.profile,circuit_hash=sim.circuit_hash,
                            num_qubits=4,block_bits=2,ranks=size)
                        row=comm.bcast(row,root=0)
                        sim.runtime.health=row['health']
                    driver=sim.runtime.configure_performance_injection(spec,observer=observe)
                    driver.barrier('semantic-prefix')
                execute_start=perf_counter();sim.materialize(binding=binding)
                execute_seconds=sim.transport.max_value(perf_counter()-execute_start)
                observed=sim.state(allow_gather=True)
                error=None
                if rank==0:
                    try:np.testing.assert_allclose(observed,expected,atol=1e-10,rtol=1e-10)
                    except Exception as exc:error=str(exc)
                sim.transport.vote(error)
                adaptive_report=session.report(transitions=sim.runtime.transitions,
                    candidates=tuple(sim.runtime.generated_candidates.values())) if adaptive and rank==0 else None
                if adaptive:
                    assert not sim.runtime.transitions
                    assert not any(row.evidence=='injected' for row in sim.runtime.measurements.cost_observations())
                metrics={'execute_seconds':execute_seconds,'managed_peak_bytes_max':int(sim.transport.max_value(sim.budget.peak)),
                    'send_bytes_sum':comm.allreduce(sim.transport.send_bytes,op=MPI.SUM),
                    'read_bytes_sum':comm.allreduce(sim.trace.read_bytes,op=MPI.SUM),
                    'write_bytes_sum':comm.allreduce(sim.trace.write_bytes,op=MPI.SUM),
                    'scratch_bytes_max':int(sim.transport.max_value(tree_bytes(directory)))}
                sim.commit_snapshot()
                metrics['scratch_bytes_max']=int(sim.transport.max_value(tree_bytes(directory)))
                resolved=sorted({h.backend_identity for h in sim.store.files.values()})
        except Exception as exc:
            failure=f'{type(exc).__name__}: {exc}'[:512]
        try:cleanup(comm,directory)
        except Exception as exc:failure=failure or f'cleanup: {exc}'[:512]
        failures=comm.allgather(failure)
        failure='; '.join(f'rank {i}: {e}' for i,e in enumerate(failures) if e)[:1024]
        rank_wall=tuple(comm.allgather(perf_counter()-begin))
        raw={'case_manifest_hash':case.manifest_hash,'variant_id':variant_id,'trial_index':index,
            'scope':'injected-same-host' if adaptive else 'same-host','source_actual_hash':source,
            'validator':'eager-state-rtol-atol-1e-10','gradient':None,'adaptive':adaptive_report,
            'resolved_backends':resolved,
            'background_load':{'before':background_load,'after':os.getloadavg()}}
        if rank==0:
            if warmup:warmup_rows.append({'variant':variant_id,'index':index,'wall_seconds':max(rank_wall),
                'status':'failed' if failure else 'completed','reason':failure})
            elif failure:trials.append(Trial(variant_id,index,'failed',wall_seconds=max(rank_wall),
                rank_wall_seconds=rank_wall,metrics=metrics,validator_passed=False,raw=raw,raw_sha256=content_hash(raw),reason=failure))
            else:trials.append(Trial.completed(variant_id,index,max(rank_wall),raw,
                rank_wall_seconds=rank_wall,metrics=metrics))
    if rank!=0:return None
    report=Campaign(case,variants,tuple(trials)).compare().to_dict()
    report.update({'case':case.identity(),'warmup_rows':warmup_rows,'scope':'injected-same-host' if adaptive else 'same-host',
        'injection':asdict(spec) if spec else None,'pilot_positive_gain_verified':False})
    return report


def run_gradient(comm,folder,args):
    rank=comm.Get_rank();theta=ParameterSymbol('theta','phase4-calibration-theta')
    observable=ObservableIR((PauliTerm(0,1),));seq=0
    @contextmanager
    def factory(method,repetition,scratch):
        nonlocal seq
        seq+=1;directory=folder/f'owner-{rank}'/f'gradient-{seq}'
        with AsyncDistributedSimulator(4,config(directory,args),comm=comm,
            policy=AsyncPolicy(io_engine='cooperative',io_workers=1,prefetch_blocks=0,swap_policy='S2',lookahead_gates=4)) as sim:
            sim.gate('H',(3,));sim.gate('RY',(0,),angle=theta*.5);sim.gate('RY',(0,),angle=theta*.5)
            sim.materialize(binding=sim.bind({theta:.37}))
            yield sim
        cleanup(comm,directory)
    with factory('context',0,None) as probe:context=gradient_context(probe,observable,[theta])
    spec=GradientCalibrationSpec(context,warmup_repetitions=args.warmup,repetitions=args.repetitions,
        wall_limit_seconds=args.wall_seconds,ram_limit_bytes=args.ram_bytes,disk_limit_bytes=args.disk_bytes)
    def validator(sim,method,result):
        error=None
        try:
            np.testing.assert_allclose(result[theta.parameter_id],-math.sin(.37),atol=1e-9,rtol=1e-9)
            np.testing.assert_allclose(sim.expectation(observable),math.cos(.37),atol=1e-10,rtol=1e-10)
        except Exception as exc:error=str(exc)
        sim.transport.vote(error)
        return True
    report=calibrate_gradient_methods(spec,factory,
        lambda sim,method:sim.gradient(observable,parameters=[theta],method=method),validator,
        actual_context_getter=lambda sim,method:gradient_context(sim,observable,[theta]),
        resource_probe=lambda sim,method:{'peak_ram_bytes':int(sim.transport.max_value(sim.budget.peak)),
            'peak_disk_bytes':int(sim.transport.max_value(tree_bytes(sim.directory)))},
        clock=lambda:comm.bcast(perf_counter() if rank==0 else None,root=0))
    # Normal auto uses explicit report attachment only; raw extrema stay diagnostic.
    auto={'status':'unmeasured','reason':'CALIBRATION_LIMIT_OR_RUNTIME_ATTACH_SIZE'}
    if not report.diagnostic['over_limit'] and len(json.dumps(report.to_dict(),separators=(',',':')).encode())<=65536:
        with factory('auto',0,None) as sim:
            sim.runtime.attach_gradient_calibration(report)
            validator(sim,'auto',sim.gradient(observable,parameters=[theta],method='auto'))
            auto=dict(sim.runtime.latest_gradient)
    if rank==0:
        data={'kind':'gradient-calibration','scope':'same-host','calibration':report.to_dict(),
            'ranks':comm.Get_size(),'auto_diagnostic':auto,
            'production_cost_count':len(GradientCostAdapter().adapt(report,context))}
        return data


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output',required=True);parser.add_argument('--scratch',required=True)
    parser.add_argument('--action',choices=('comparison','adaptive','gradient','all'),default='all')
    parser.add_argument('--scenario',choices=('sustained','transient','recovery','missing-baseline'),default='sustained')
    parser.add_argument('--repetitions',type=int,default=7);parser.add_argument('--warmup',type=int,default=2)
    parser.add_argument('--backend',choices=('buffered','direct'),default='buffered')
    parser.add_argument('--wall-seconds',type=float,default=300)
    parser.add_argument('--ram-bytes',type=int,default=8*1024**2)
    parser.add_argument('--disk-bytes',type=int,default=64*1024**2)
    args=parser.parse_args()
    if not 1<=args.repetitions<=20 or not 0<=args.warmup<=10 or not math.isfinite(args.wall_seconds) or args.wall_seconds<=0:
        parser.error('Finite repetitions 1..20, warmup 0..10, positive wall seconds required')
    if args.ram_bytes<4*1024**2 or args.disk_bytes<16*1024**2:parser.error('RAM>=4MiB/rank and SSD>=16MiB required')
    comm=MPI.COMM_WORLD;rank=comm.Get_rank()
    if comm.Get_size() not in {1,2,4} or len(set(comm.allgather(platform.node())))!=1:
        raise ValueError('Only same-host P=1/2/4 campaigns are supported')
    root=Path(__file__).resolve().parents[1]
    args.build={'source':content_hash({str(p.relative_to(root)):sha256(p.read_bytes()).hexdigest()
        for p in sorted((root/'src').rglob('*')) if p.suffix in {'.py','.c','.h'}}),'worker':sha256(Path(__file__).read_bytes()).hexdigest(),
        'lock':sha256((root/'uv.lock').read_bytes()).hexdigest()}
    args.build.update({'python':__import__('sys').version,'platform':platform.platform(),
        'machine':platform.machine(),'mpi':MPI.Get_library_version()})
    args.build['thread_environment']={k:os.environ.get(k) for k in ('OMP_NUM_THREADS','OPENBLAS_NUM_THREADS','VECLIB_MAXIMUM_THREADS')}
    folder=Path(comm.bcast(str(Path(args.scratch)/f'campaign-{uuid4().hex}') if rank==0 else None,root=0))
    reports={}
    campaign_started=perf_counter();total_wall=args.wall_seconds
    for action in ('comparison','adaptive','gradient'):
        if args.action not in {action,'all'}:continue
        remaining=comm.bcast(total_wall-(perf_counter()-campaign_started) if rank==0 else None,root=0)
        if remaining<=0:
            reports[action]={'status':'unmeasured','reason':'CAMPAIGN_WALL_LIMIT'}
            continue
        args.wall_seconds=remaining
        if action=='gradient':reports[action]=run_gradient(comm,folder/action,args)
        else:reports[action]=run_comparison(comm,folder/action,args,
            adaptive=action=='adaptive',scenario=args.scenario)
    if rank==0:
        reports.update({'schema_version':1,'kind':'phase4-followup-campaign','scope':'same-host',
            'ranks':comm.Get_size(),'backend':args.backend,'build':args.build,'cluster_used':False,
            'unmeasured_groups':['cross-host','storage-mpi-positive-adaptation','full-control-migration',
                'large-state-paired','tile-paired','cache-fairness-paired','lifecycle-fault-matrix']})
        data=json.dumps(reports,ensure_ascii=False,indent=2,allow_nan=False)+'\n'
        if len(data.encode())>4*1024**2:raise ValueError('Campaign report exceeds 4MiB')
        output=Path(args.output);output.parent.mkdir(parents=True,exist_ok=True);output.write_text(data)
        print(json.dumps({'ranks':comm.Get_size(),'output':str(output),'sections':list(reports)}),flush=True)


if __name__=='__main__':main()
