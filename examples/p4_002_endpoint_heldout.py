"""Real endpoint ETA prequential holdouts; no synthetic cost rows or thresholds.

Each independent simulator calibrates a fixed source/payload shape, then queries
immediately before each heldout endpoint. Earlier heldout completions naturally
enter the rolling production collector; a future completion never prices itself.
"""
import argparse
from collections import Counter
from hashlib import sha256
import json
import math
from pathlib import Path
import platform
import statistics
import subprocess
from time import perf_counter


def metrics(rows,target='actual_wall_seconds'):
    eligible=[r for r in rows if r.get('prediction',{}).get('expected_seconds') is not None and r.get(target) is not None and not r.get('error')]
    def describe(values):
        values=sorted(values)
        if not values:return {'median':None,'p95':None,'worst':None}
        index=.95*(len(values)-1);lo=int(index);hi=min(lo+1,len(values)-1)
        return {'median':statistics.median(values),'p95':values[lo]+(values[hi]-values[lo])*(index-lo),'worst':max(values)}
    absolute=[abs(r['prediction']['expected_seconds']-r[target]) for r in eligible]
    relative=[a/r[target] for a,r in zip(absolute,eligible) if r[target]>0]
    intervals=[r for r in eligible if r['prediction'].get('lower_seconds') is not None and r['prediction'].get('upper_seconds') is not None]
    return {'checks':len(rows),'answered':len(eligible),'answer_fraction':len(eligible)/len(rows) if rows else None,
        'errors':sum(bool(r.get('error')) for r in rows),'unknown':sum(r.get('prediction',{}).get('expected_seconds') is None for r in rows),
        'unknown_reasons':dict(Counter(reason for r in rows for reason in r.get('prediction',{}).get('unknown_reasons',[]))),
        'absolute_error_seconds':describe(absolute),'absolute_relative_error':describe(relative),
        'interval_checks':len(intervals),'interval_coverage':sum(r['prediction']['lower_seconds']<=r[target]<=r['prediction']['upper_seconds'] for r in intervals)/len(intervals) if intervals else None}


def trial(directory,endpoint,size,trial_id,calibration,holdouts,case):
    import numpy as np
    from mpi4py import MPI
    from svsim import AsyncDistributedSimulator,AsyncPolicy,DistributedConfig,ObservableIR,PauliTerm,ParameterSymbol
    from svsim.kernels import eager_reference
    from svsim.remaining_work import checkpoint_condition,expectation_condition,gradient_condition
    from svsim import runtime_observed_costs as roc
    cfg=DistributedConfig(str(directory),2,ram_budget_bytes_per_rank=64*1024**2,
        metadata_reserve_bytes=8*1024**2,amplitude_store='file',frame_policy='materialize',storage_scope='node-local')
    policy=AsyncPolicy(io_engine='cooperative',swap_policy='S2',lookahead_gates=4,prefetch_blocks=0)
    with AsyncDistributedSimulator(4,cfg,comm=MPI.COMM_SELF,policy=policy) as sim:
        sim.runtime.configure_measurements(max_bytes=4*1024**2)
        theta=ParameterSymbol('theta','endpoint-heldout-theta');angle=.37
        sim.gate('RY',(0,),angle=theta)
        for _ in range(size-1):sim.gate('RX',(1,),angle=.13)
        binding=sim.bind({theta:angle});sim.materialize(binding=binding);sim.drain();sim.cache.flush()
        observable=ObservableIR((PauliTerm(0,1),));parameters=[theta.parameter_id]
        stage={'checkpoint':'checkpoint-commit','expectation':'expectation','gradient':'gradient'}[endpoint]
        def condition():
            if endpoint=='checkpoint':return checkpoint_condition(sim)
            if endpoint=='expectation':return expectation_condition(sim,observable)
            return gradient_condition(sim,observable,parameters,'adjoint')
        if endpoint=='checkpoint':sim.runtime.set_eta_endpoint(checkpoint=True)
        elif endpoint=='expectation':sim.runtime.set_eta_endpoint(expectation=observable)
        else:sim.runtime.set_eta_endpoint(gradient={'observable':observable,'parameters':parameters,'method':'adjoint'})
        def complete():
            if endpoint=='checkpoint':return sim.commit_snapshot()
            if endpoint=='expectation':return sim.expectation(observable,binding=binding)
            return sim.gradient(observable,parameters=parameters,binding=binding,method='adjoint')
        def measure(phase,index,query):
            entries_before=len(sim.cache.entries)
            prep=perf_counter();sim.drain();sim.cache.flush();sim.drain();sim.cache.clear()
            preparation_seconds=perf_counter()-prep
            shape=condition();before_revision=sim.runtime.measurements.revision
            matching=[r for r in sim.runtime.measurements.cost_rows() if r.stage==stage and roc._class(r.condition)==roc._class(shape)]
            reason,values,_,_,coverage=roc._assess(matching,sim.runtime.policy_epoch,sim.runtime.plan_version,roc.BURN_IN)
            row={'phase':phase,'index':index,'condition':shape,'collector_revision_before_query':before_revision,
                'quiescent_preparation_seconds':preparation_seconds,'cache_entries_before_preparation':entries_before,
                'cache_entries_at_query':len(sim.cache.entries),
                'matching_prior_completions':len(matching),'certification':{'reason':reason,'post_burn_in_samples':len(values),'heldout_coverage':coverage}}
            if query:
                tick=perf_counter();eta=sim.runtime.update_eta(endpoint_frontier=sim.frontier)
                row['query_seconds']=perf_counter()-tick;row['eta']=eta
                row['prediction']=eta['views']['observed']
                row['collector_revision_after_query']=sim.runtime.measurements.revision
                assert row['collector_revision_after_query']==before_revision,'Query itself added completion costs'
            tick=perf_counter()
            try:
                result=complete();row['actual_wall_seconds']=perf_counter()-tick
                if endpoint=='expectation':
                    row['numerical_result']=float(result);np.testing.assert_allclose(result,math.cos(angle),atol=1e-11)
                elif endpoint=='gradient':
                    row['numerical_result']={key:float(value) for key,value in result.items()}
                    np.testing.assert_allclose(result[theta.parameter_id],-math.sin(angle),atol=1e-10)
                else:row['snapshot_id']=result['snapshot_id']
                latest=[r for r in sim.runtime.measurements.cost_rows() if r.stage==stage][-1]
                row['completion']=latest.to_dict()
                assert latest.evidence=='measured' and latest.success and latest.validated
                assert roc._class(latest.condition)==roc._class(shape),'Commit recorded a different pre-call shape'
                row['actual_stage_seconds']=latest.protocol_seconds if latest.protocol_seconds is not None else latest.service_seconds
                if query and row['prediction']['expected_seconds'] is not None:
                    predicted=row['prediction']['expected_seconds'];actual=row['actual_wall_seconds']
                    row['signed_error_seconds']=predicted-actual
                    row['absolute_relative_error']=abs(predicted-actual)/actual
                    row['interval_covered']=row['prediction']['lower_seconds']<=actual<=row['prediction']['upper_seconds']
            except Exception as exc:row['error']=repr(exc)
            return row
        for i in range(calibration):case['calibration'].append(measure('calibration',i,False))
        for i in range(holdouts):case['holdout'].append(measure('same-shape-heldout',i,True))
        # Append and execute a fresh source/history shape, then price only the
        # endpoint (no outstanding gates). A checkpoint class must not borrow
        # same-size calibration. Record the actual next completion regardless.
        if endpoint=='checkpoint':
            for _ in range(4):sim.gate('RX',(1,),angle=.13)
            sim.materialize(binding=sim.bind({theta:angle}));sim.drain();sim.cache.flush()
            changed=measure('unseen-growth-shape',0,True);case['growth'].append(changed)
            case['growth_refused']=changed['prediction']['expected_seconds'] is None and any(
                reason=='COST_UNCERTIFIED:INSUFFICIENT_SAMPLES:endpoint:checkpoint' for reason in changed['prediction']['unknown_reasons'])
            if not case['growth_refused']:case['error']='Unseen checkpoint payload shape borrowed a prior cost'
        expected=eager_reference(sim.sequencer.operations,4,sim.assets,sim.bind({theta:angle}))
        np.testing.assert_allclose(sim.state(allow_gather=True),expected,atol=1e-11)
        case['state_oracle_max_abs_error']=float(np.max(np.abs(sim.state(allow_gather=True)-expected)))
        if endpoint=='checkpoint':
            latest=case['growth'][-1].get('snapshot_id')
            if latest:
                with AsyncDistributedSimulator.resume(cfg,latest,comm=MPI.COMM_SELF) as resumed:
                    np.testing.assert_allclose(resumed.state(allow_gather=True),expected,atol=1e-11)
                case['resume_oracle']=True
        snapshot=sim.runtime.measurements.snapshot()
        observations=snapshot.pop('observations')
        snapshot['endpoint_observations']=[row for row in observations if row['stage']==stage]
        snapshot['other_stage_observations_count']=sum(row['stage']!=stage for row in observations)
        case['collector_snapshot']=snapshot
    return case


def main(argv=None):
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output',type=Path,required=True);parser.add_argument('--scratch',type=Path,required=True)
    parser.add_argument('--sizes',default='4,32,128');parser.add_argument('--trials',type=int,default=3)
    parser.add_argument('--calibration',type=int,default=16);parser.add_argument('--holdouts',type=int,default=5)
    parser.add_argument('--endpoints',default='checkpoint,expectation,gradient')
    args=parser.parse_args(argv)
    sizes=[int(value) for value in args.sizes.split(',')];endpoints=args.endpoints.split(',')
    if not all(1<=size<=512 for size in sizes) or not 1<=args.trials<=10 or not 11<=args.calibration<=40 or not 1<=args.holdouts<=20:
        parser.error('Bounded sizes1..512/trials1..10/calibration11..40/holdouts1..20 required')
    if any(item not in ('checkpoint','expectation','gradient') for item in endpoints):parser.error('Unknown endpoint')
    if args.output.exists() or args.scratch.exists():parser.error('Output and scratch must be fresh')
    args.output.parent.mkdir(parents=True,exist_ok=True);args.scratch.mkdir(parents=True)
    root=Path(__file__).resolve().parents[1]
    source=[(str(p.relative_to(root)),sha256(p.read_bytes()).hexdigest()) for p in sorted((root/'src').rglob('*')) if p.is_file() and p.suffix in ('.py','.c','.h')]
    report={'schema_version':1,'platform':platform.platform(),'python':platform.python_version(),
        'revision':subprocess.check_output(['git','rev-parse','HEAD'],cwd=root,text=True).strip(),
        'runtime_source_sha256':sha256(json.dumps(source).encode()).hexdigest(),
        'runner_sha256':sha256(Path(__file__).read_bytes()).hexdigest(),
        'parameters':{**vars(args),'output':str(args.output),'scratch':str(args.scratch)},
        'protocol':'prequential: prediction precedes its own real completion; prior holdout completions enter rolling calibration',
        'comparison':'quiescent endpoint public API wall time; pre-query drain/cache flush/clear duration and measured stage time retained separately',
        'threshold_changes':False,'manual_cost_injection':False,'background':'Other agents may execute unit tests; no ent workload or controlled idle-host claim.',
        'cases':[],'status':'running'}
    started=perf_counter()
    def save():
        held=[r for case in report['cases'] for r in case.get('holdout',[])];growth=[r for case in report['cases'] for r in case.get('growth',[])]
        report['summary']={'same_shape':metrics(held),'same_shape_measured_stage':metrics(held,'actual_stage_seconds'),'growth':metrics(growth),
            'by_endpoint':{endpoint:metrics([r for case in report['cases'] if case['endpoint']==endpoint for r in case.get('holdout',[])]) for endpoint in endpoints}}
        args.output.write_text(json.dumps(report,indent=2,allow_nan=False)+'\n')
    for endpoint in endpoints:
        for size in sizes:
            for trial_id in range(args.trials):
                case={'endpoint':endpoint,'gates':size,'trial':trial_id,'calibration':[],'holdout':[],'growth':[]}
                try:trial(args.scratch/f'{endpoint}-{size}-{trial_id}',endpoint,size,trial_id,args.calibration,args.holdouts,case)
                except Exception as exc:case['error']=repr(exc)
                report['cases'].append(case);save()
                print(json.dumps({'endpoint':endpoint,'gates':size,'trial':trial_id,'error':case.get('error'),'summary':metrics(case.get('holdout',[]))}),flush=True)
    report['seconds']=perf_counter()-started
    report['status']='failed' if any(case.get('error') or any(row.get('error') for phase in ('calibration','holdout','growth') for row in case.get(phase,[])) for case in report['cases']) else 'completed'
    save();return 0 if report['status']=='completed' else 1


if __name__=='__main__':raise SystemExit(main())
