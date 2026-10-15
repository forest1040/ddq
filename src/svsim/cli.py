"""Explicit local calibration and read-only static estimation entry points."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import sys

from .benchmark_campaign import BenchmarkOptions, run_calibration
from .estimate_report import ExecutionPlanDocument
from .profile_document import ProfileDocument, probe_profile, write_document
from .resource_estimator import EstimateOptions, estimate
from .workload_spec import WorkloadSpec, integer


def _positive(text):
    try: return integer(text,'argument',1)
    except ValueError as exc: raise argparse.ArgumentTypeError(str(exc)) from exc


def _nonnegative(text):
    try: return integer(text,'argument',0)
    except ValueError as exc: raise argparse.ArgumentTypeError(str(exc)) from exc


def _output(path, document, overwrite):
    if path: write_document(path,document,overwrite=overwrite)
    else: print(json.dumps(document,ensure_ascii=False,sort_keys=True,allow_nan=False))


def svbench_main(argv=None):
    parser = argparse.ArgumentParser(prog='svbench',description='Budgeted same-host measurement; never submits jobs')
    commands = parser.add_subparsers(dest='command',required=True)
    probe = commands.add_parser('probe',help='read-only discovery; no benchmark or MPI initialization')
    probe.add_argument('--output',required=True)
    probe.add_argument('--storage-path',action='append',default=[])
    probe.add_argument('--threads',type=_positive,default=1)
    probe.add_argument('--ranks',type=_positive,default=1,help='allocation label: MPI ranks of the runs')
    probe.add_argument('--nodes',type=_positive,default=1,help='allocation label: hosts of the runs')
    probe.add_argument('--overwrite',action='store_true')
    bench = commands.add_parser('all',help='small RAM/kernel/storage/mixed campaign in explicit scratch')
    bench.add_argument('--profile')
    bench.add_argument('--output',required=True)
    bench.add_argument('--scratch',required=True)
    bench.add_argument('--ram-budget',type=_positive,required=True,help='bytes')
    bench.add_argument('--disk-budget',type=_positive,required=True,help='bytes')
    bench.add_argument('--wall-seconds',type=float,required=True)
    bench.add_argument('--block-bits',type=_positive,default=12)
    bench.add_argument('--repetitions',type=_positive,default=5)
    bench.add_argument('--warmup',type=_nonnegative,default=1)
    bench.add_argument('--backend',choices=('buffered','direct'),default='buffered')
    bench.add_argument('--suites',default='ram,kernel,storage,mixed')
    bench.add_argument('--seed',type=_nonnegative,default=0)
    bench.add_argument('--threads',type=_positive,default=1,help='allocation label; does not set or pin library threads')
    bench.add_argument('--overwrite',action='store_true')
    runtime = commands.add_parser('runtime',help='finite actual Phase 4 executor calibration; MPI is initialized only here')
    runtime.add_argument('--output',required=True)
    runtime.add_argument('--scratch',required=True)
    runtime.add_argument('--ram-budget',type=_positive,required=True)
    runtime.add_argument('--disk-budget',type=_positive,required=True)
    runtime.add_argument('--wall-seconds',type=float,required=True)
    runtime.add_argument('--num-qubits',type=_positive,default=8)
    runtime.add_argument('--block-bits',type=_positive,default=4)
    runtime.add_argument('--ranks',type=_positive,default=1)
    runtime.add_argument('--repetitions',type=_positive,default=5)
    runtime.add_argument('--warmup',type=_positive,default=1)
    runtime.add_argument('--backend',choices=('buffered','direct'),default='buffered')
    runtime.add_argument('--engine',choices=('worker','cooperative'),default='worker')
    runtime.add_argument('--max-io-requests',type=_positive,choices=(1,2),default=1)
    runtime.add_argument('--suites',default='kernel-storage,memory-mpi,storage-mpi')
    runtime.add_argument('--seed',type=_nonnegative,default=1)
    runtime.add_argument('--overwrite',action='store_true')
    args = parser.parse_args(argv)
    try:
        if args.command == 'runtime':
            return _runtime_command(args)
        if args.command == 'probe':
            profile = probe_profile({'scope':'local','ranks':args.ranks,'nodes':args.nodes,'threads':args.threads},args.storage_path)
            profile.save(args.output,overwrite=args.overwrite)
            return 0
        # Reject accidental output replacement before executing expensive probes.
        output = Path(args.output)
        run_output = output.with_name(output.name + '.run.json')
        if not args.overwrite and (output.exists() or run_output.exists()): raise FileExistsError('output document already exists')
        Path(args.scratch).expanduser().mkdir(parents=True,exist_ok=True)
        baseline = probe_profile({'scope':'local','ranks':1,'nodes':1,'threads':args.threads},(args.scratch,))
        if args.profile:
            previous = ProfileDocument.load(args.profile)
            if previous.configuration_fingerprint != baseline.configuration_fingerprint:
                raise ValueError('profile configuration differs from current host/allocation; run probe again')
            baseline = previous
        options = BenchmarkOptions(block_bits=args.block_bits,repetitions=args.repetitions,warmup=args.warmup,
                                   ram_budget_bytes=args.ram_budget,disk_budget_bytes=args.disk_budget,
                                   walltime_seconds=args.wall_seconds,backend=args.backend,
                                   suites=tuple(args.suites.split(',')),seed=args.seed)
        calibrated = run_calibration(baseline,Path(args.scratch),options)
        calibrated.save(output,overwrite=args.overwrite)
        raw_hash = hashlib.sha256(output.read_bytes()).hexdigest()
        omitted = calibrated.data['discovery'].get('omitted_calibrations',[])
        # Raw trial records reside in this content-addressed profile; the run references their exact file.
        write_document(run_output,{'schema_version':1,'kind':'benchmark-run',
                                  'baseline_profile_id':baseline.profile_id,'profile_id':calibrated.profile_id,
                                  'configuration_fingerprint':calibrated.configuration_fingerprint,
                                  'raw_trials':{'path':str(output.resolve()),'sha256':raw_hash,'container':'profile.calibrations'},
                                  'status':'partial' if omitted else 'completed','omitted_calibrations':omitted,
                                  'provenance':calibrated.data['provenance']},overwrite=args.overwrite)
        print(f'profile: {output}\nraw trial manifest: {run_output}')
        return 4 if omitted else 0
    except (ValueError,TypeError,OSError,ArithmeticError,RecursionError) as exc:
        print(f'svbench: {exc}',file=sys.stderr)
        return 4 if args.command in {'all','runtime'} else 2


def _runtime_command(args):
    from .runtime_calibration import RuntimeCalibrationSpec, calibrate_runtime, MAX_REPORT_BYTES, split_observations
    spec=RuntimeCalibrationSpec(args.scratch,num_qubits=args.num_qubits,block_bits=args.block_bits,
        ranks=(args.ranks,),seed=args.seed,ram_budget_bytes=args.ram_budget,
        disk_budget_bytes=args.disk_budget,walltime_seconds=args.wall_seconds,
        repetitions=args.repetitions,warmup=args.warmup,backend=args.backend,engine=args.engine,
        max_io_requests=args.max_io_requests,suites=tuple(args.suites.split(',')))
    from mpi4py import MPI
    comm=MPI.COMM_WORLD
    error=None
    if comm.Get_rank()==0 and Path(args.output).exists() and not args.overwrite:
        error='output document already exists'
    error=comm.bcast(error,root=0)
    if error: raise FileExistsError(error)
    report=calibrate_runtime(spec,comm=comm)
    code=0
    if comm.Get_rank()==0:
        try:
            sidecar=Path(args.output).with_suffix('.observations.jsonl')
            if sidecar.exists() and not args.overwrite:raise OSError(f'{sidecar} already exists')
            split_observations(report,sidecar)
            write_document(args.output,report,overwrite=args.overwrite,max_bytes=MAX_REPORT_BYTES)
            print(f'runtime calibration: {args.output} ({report["status"]})')
            code=0 if report['status']=='complete' else 4
        except (ValueError,TypeError,OSError) as exc:
            print(f'svbench: {exc}',file=sys.stderr)
            code=4
    return comm.bcast(code,root=0)


def svsim_main(argv=None):
    parser = argparse.ArgumentParser(prog='svsim',description='Bounded workload registration and static estimates; no jobs or amplitude allocation')
    commands = parser.add_subparsers(dest='command',required=True)
    register = commands.add_parser('register',help='validate and save a versioned workload document')
    register.add_argument('--input',required=True)
    register.add_argument('--output',required=True)
    register.add_argument('--overwrite',action='store_true')
    est = commands.add_parser('estimate',help='write a planning report without executing the workload')
    est.add_argument('--workload',required=True)
    est.add_argument('--profile',required=True)
    est.add_argument('--output')
    est.add_argument('--overwrite',action='store_true')
    est.add_argument('--planning-only',action='store_true',help='allow partial/unsupported estimates; inspect report flags')
    est.add_argument('--block-bits',type=_positive,default=12)
    est.add_argument('--ranks',type=_positive,default=1)
    est.add_argument('--nodes',type=_positive,default=1)
    est.add_argument('--threads',type=_positive,default=1)
    est.add_argument('--ram-budget-per-rank',type=_positive)
    est.add_argument('--ram-budget-per-host',type=_positive)
    est.add_argument('--disk-budget-per-host',type=_positive)
    est.add_argument('--alignment',type=_positive,default=4096)
    est.add_argument('--segment-bytes',type=_positive,default=1<<30)
    est.add_argument('--backend',choices=('buffered','direct'),default='buffered')
    est.add_argument('--time-model',help='fitted end-to-end model from svsim calibrate-time')
    est.add_argument('--sync-latency-seconds',type=float,help='freshly probed device fsync latency for expected time')
    bench = commands.add_parser('benchmark',help='budgeted real runs of small workloads in explicit scratch; MPI is initialized only here')
    bench.add_argument('--workload',action='append',required=True,help='repeat for interleaved A/B cases')
    bench.add_argument('--label',action='append',default=[],help='one per workload; defaults to the file stem')
    bench.add_argument('--variant',action='append',default=[],help='one per workload; defaults to "default"')
    bench.add_argument('--plan',action='append',default=[],help='optional execution plan per workload, same order')
    bench.add_argument('--profile',required=True)
    bench.add_argument('--scratch',required=True)
    bench.add_argument('--output-dir',required=True)
    bench.add_argument('--ram-budget-per-rank',type=_positive,required=True,help='bytes')
    bench.add_argument('--disk-budget',type=_positive,required=True,help='bytes for all ranks of one trial')
    bench.add_argument('--wall-seconds',type=float,required=True)
    bench.add_argument('--block-bits',type=_positive,default=4)
    bench.add_argument('--repetitions',type=_positive,default=5)
    bench.add_argument('--warmup',type=_nonnegative,default=1)
    bench.add_argument('--backend',choices=('buffered','direct'),default='buffered')
    bench.add_argument('--threads',type=_positive,default=1,help='allocation label; does not set or pin library threads')
    bench.add_argument('--seed',type=_nonnegative,default=0)
    bench.add_argument('--oracle-max-qubits',type=_nonnegative,default=14)
    bench.add_argument('--executor',choices=('sync','async'),default='sync',help='async uses the Phase 4 runtime; --threads must equal its io_workers')
    bench.add_argument('--eta-model',help='record runtime time-model ETA checks against actual remaining time (async only)')
    bench.add_argument('--overwrite',action='store_true')
    suite = commands.add_parser('calibration-suite',help='write single-class calibration workloads for svsim benchmark')
    suite.add_argument('--block-bits',type=_positive,default=4)
    suite.add_argument('--qubits',default='5,6,7,8')
    suite.add_argument('--counts',default='1,4,16')
    suite.add_argument('--storage',choices=('ram','file'),default='ram')
    suite.add_argument('--ranks',type=_positive,default=1,help='MPI ranks of the runs; adds a remote (rank-bit) class')
    suite.add_argument('--output-dir',required=True)
    suite.add_argument('--overwrite',action='store_true')
    fit = commands.add_parser('calibrate-time',help='fit the phase time model from validated benchmark runs')
    fit.add_argument('--run',action='append',required=True)
    fit.add_argument('--output',required=True)
    fit.add_argument('--overwrite',action='store_true')
    eta = commands.add_parser('eta-report',help='summarize runtime ETA checks recorded by benchmark --eta-model')
    eta.add_argument('--run',action='append',required=True)
    eta.add_argument('--output',required=True)
    eta.add_argument('--overwrite',action='store_true')
    rep = commands.add_parser('report',help='compare execution plans with observed benchmark runs')
    rep.add_argument('--campaign',required=True)
    rep.add_argument('--output',required=True)
    rep.add_argument('--markdown')
    rep.add_argument('--overwrite',action='store_true')
    args = parser.parse_args(argv)
    if args.command == 'benchmark': return _benchmark_command(args)
    try:
        if args.command == 'calibration-suite':
            from .benchmark_run import calibration_suite
            numbers = lambda text: tuple(_positive(v) for v in text.split(','))
            documents = calibration_suite(args.block_bits,numbers(args.qubits),numbers(args.counts),args.storage,ranks=args.ranks)
            for name, document in documents.items():
                _output(str(Path(args.output_dir)/f'{name}.json'),WorkloadSpec(document).to_dict(),args.overwrite)
            print(f'{len(documents)} workloads: {args.output_dir}')
            return 0
        if args.command == 'calibrate-time':
            from .profile_document import read_document
            from .time_model import fit as fit_time
            model = fit_time([read_document(path,max_bytes=4*1024**2) for path in args.run])
            model.save(args.output,overwrite=args.overwrite)
            return 0
        if args.command == 'eta-report':
            from .profile_document import read_document
            from .runtime_time_model_eta import eta_accuracy
            summary = eta_accuracy([read_document(path,max_bytes=4*1024**2) for path in args.run])
            _output(args.output,summary,args.overwrite)
            return 0
        if args.command == 'report':
            from .comparison_report import build_report, render_markdown, save_report
            if not args.overwrite and any(p and Path(p).exists() for p in (args.output,args.markdown)):
                raise FileExistsError('output document already exists')
            report = build_report(args.campaign)
            save_report(report,args.output,overwrite=args.overwrite)
            if args.markdown: Path(args.markdown).write_text(render_markdown(report),encoding='utf-8')
            return 0
        if args.command == 'register':
            workload = WorkloadSpec.load(args.input)
            _output(args.output,workload.to_dict(),args.overwrite)
            return 0
        workload = WorkloadSpec.load(args.workload)
        profile = ProfileDocument.load(args.profile)
        options = EstimateOptions(block_bits=args.block_bits,ranks=args.ranks,nodes=args.nodes,threads=args.threads,
                                  ram_budget_bytes_per_rank=args.ram_budget_per_rank,
                                  ram_budget_bytes_per_host=args.ram_budget_per_host,
                                  disk_budget_bytes_per_host=args.disk_budget_per_host,alignment=args.alignment,
                                  segment_size_bytes=args.segment_bytes,backend=args.backend)
        time_model = None
        if args.time_model:
            from .time_model import TimeModelDocument
            time_model = TimeModelDocument.load(args.time_model)
        if args.sync_latency_seconds is not None and time_model is None: raise ValueError('--sync-latency-seconds requires --time-model')
        report = estimate(workload,profile,options,time_model=time_model,sync_seconds=args.sync_latency_seconds)
        report = ExecutionPlanDocument(report).to_dict()
        _output(args.output,report,args.overwrite)
        if args.planning_only: return 0
        if report['capacity_feasible'] is not True or not report['execution_supported']: return 3
        if report['calibration_coverage']['status'] != 'complete': return 5
        return 0
    except (ValueError,TypeError,OSError,ArithmeticError,RecursionError,KeyError) as exc:
        print(f'svsim: {exc}',file=sys.stderr)
        return 2


def _benchmark_command(args):
    from .benchmark_run import CapacityError, RunCase, RunOptions, run_cases
    from mpi4py import MPI
    comm = MPI.COMM_WORLD
    error, code, cases, profile, options, eta_model, hosts = None, 2, [], None, None, None, None
    import socket
    from .profile_document import read_document
    hosts = comm.allgather(socket.gethostname())
    nodes = len(set(hosts))
    # Inputs live on the coordinator's node-local disk: rank 0 reads and checks them, every rank
    # receives the same documents (other hosts never need a shared tree or a copy).
    payload = None
    if comm.Get_rank() == 0:
        try:
            count = len(args.workload)
            for name, values in (('label',args.label),('variant',args.variant),('plan',args.plan)):
                if values and len(values) != count: raise ValueError(f'--{name} must be given once per --workload')
            profile_data = read_document(args.profile)
            current = probe_profile({'scope':'local','ranks':comm.Get_size(),'nodes':nodes,'threads':args.threads},(args.scratch,))
            if current.configuration_fingerprint != ProfileDocument.from_dict(profile_data).configuration_fingerprint:
                raise ValueError('profile configuration differs from current host/build/allocation; run svbench probe with the same scratch')
            payload = {'profile': profile_data,
                       'workloads': [read_document(path) for path in args.workload],
                       'plans': [read_document(path, max_bytes=4*1024**2) for path in args.plan] if args.plan else None,
                       'eta_model': read_document(args.eta_model, max_bytes=4*1024**2) if args.eta_model else None}
        except (ValueError,TypeError,OSError,KeyError,RecursionError) as exc: payload = {'error': str(exc)}
    payload = comm.bcast(payload, root=0)
    try:
        if 'error' in payload: raise ValueError(payload['error'])
        profile = ProfileDocument.from_dict(payload['profile'])
        for i, (path, data) in enumerate(zip(args.workload, payload['workloads'])):
            plan = ExecutionPlanDocument(payload['plans'][i]).data if payload['plans'] else None
            cases.append(RunCase(WorkloadSpec(data),args.label[i] if args.label else Path(path).stem,
                                 args.variant[i] if args.variant else 'default',plan))
        options = RunOptions(block_bits=args.block_bits,repetitions=args.repetitions,warmup=args.warmup,
                             ram_budget_bytes_per_rank=args.ram_budget_per_rank,disk_budget_bytes=args.disk_budget,
                             walltime_seconds=args.wall_seconds,backend=args.backend,seed=args.seed,
                             threads=args.threads,oracle_max_qubits=args.oracle_max_qubits,executor=args.executor,
                             nodes=nodes)
        eta_model = None
        if payload['eta_model'] is not None:
            from .time_model import TimeModelDocument
            eta_model = TimeModelDocument(payload['eta_model'])
        outputs = [Path(args.output_dir)/f'{case.label}.run.json' for case in cases]
        if not args.overwrite and any(p.exists() for p in outputs): raise FileExistsError('output document already exists')
    except (ValueError,TypeError,OSError,KeyError,RecursionError) as exc: error = str(exc)
    errors = [e for e in comm.allgather(error) if e]
    if errors:
        if comm.Get_rank() == 0: print(f'svsim: {errors[0]}',file=sys.stderr)
        return 2
    try:
        documents = run_cases(cases,profile,args.scratch,options,comm,eta_model=eta_model,hosts=hosts)
    except CapacityError as exc:
        if comm.Get_rank() == 0: print(f'svsim: {exc}',file=sys.stderr)
        return 3
    except (ValueError,OSError,ArithmeticError,MemoryError,RuntimeError) as exc:
        if comm.Get_rank() == 0: print(f'svsim: {exc}',file=sys.stderr)
        return 4
    code = 0
    if comm.Get_rank() == 0:
        try:
            for path, document in zip(outputs,documents):
                write_document(path,document,overwrite=args.overwrite)
                print(f'{path}: {document["status"]} validation={document["validation"]["passed"]}')
            if any(d['status'] != 'completed' or d['validation']['passed'] is False for d in documents): code = 4
        except (ValueError,TypeError,OSError) as exc:
            print(f'svsim: {exc}',file=sys.stderr)
            code = 4
    return comm.bcast(code,root=0)
