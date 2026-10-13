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
    args = parser.parse_args(argv)
    try:
        if args.command == 'probe':
            profile = probe_profile({'scope':'local','ranks':1,'nodes':1,'threads':args.threads},args.storage_path)
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
        return 4 if args.command == 'all' else 2


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
    args = parser.parse_args(argv)
    try:
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
                                  segment_size_bytes=args.segment_bytes)
        report = estimate(workload,profile,options)
        report = ExecutionPlanDocument(report).to_dict()
        _output(args.output,report,args.overwrite)
        if args.planning_only: return 0
        if report['capacity_feasible'] is not True or not report['execution_supported']: return 3
        if report['calibration_coverage']['status'] != 'complete': return 5
        return 0
    except (ValueError,TypeError,OSError,ArithmeticError,RecursionError,KeyError) as exc:
        print(f'svsim: {exc}',file=sys.stderr)
        return 2
