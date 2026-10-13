"""Finite Mac-local runtime matrix; runs MPI sequentially and never submits jobs."""
import argparse
import itertools
import json
from pathlib import Path
import shutil
import subprocess
import sys
import time

from svsim.profile_document import write_document


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--scratch',required=True)
    parser.add_argument('--output-dir',required=True)
    parser.add_argument('--ram-budget',type=int,required=True)
    parser.add_argument('--disk-budget',type=int,required=True)
    parser.add_argument('--wall-seconds',type=float,required=True,help='per campaign')
    parser.add_argument('--repetitions',type=int,default=5)
    parser.add_argument('--num-qubits',type=int,default=6)
    args=parser.parse_args()
    launcher=shutil.which('mpiexec')
    if not launcher:parser.error('local mpiexec is required')
    output=Path(args.output_dir)
    output.mkdir(parents=True,exist_ok=True)
    cases=[]
    for ranks,engine,backend,depth in itertools.product((1,2,4),('worker','cooperative'),('buffered','direct'),(1,2)):
        name=f'p{ranks}-{engine}-{backend}-q{depth}'
        path=output/f'{name}.json'
        if path.exists():parser.error(f'output already exists: {path}')
        command=[launcher,'-n',str(ranks),sys.executable,'-c',
            'from svsim.cli import svbench_main; raise SystemExit(svbench_main())',
            'runtime','--scratch',args.scratch,'--output',str(path),'--num-qubits',str(args.num_qubits),
            '--block-bits','2','--ranks',str(ranks),'--repetitions',str(args.repetitions),
            '--warmup','1','--ram-budget',str(args.ram_budget),'--disk-budget',str(args.disk_budget),
            '--wall-seconds',str(args.wall_seconds),'--engine',engine,'--backend',backend,
            '--max-io-requests',str(depth)]
        start=time.monotonic()
        completed=subprocess.run(command,timeout=args.wall_seconds+60,check=False)
        cases.append({'case':name,'command':command,'return_code':completed.returncode,
                      'elapsed_seconds':time.monotonic()-start,'path':str(path)})
        print(name,completed.returncode,flush=True)
        if completed.returncode:break
    write_document(output/'matrix-run.json',{'kind':'runtime-matrix','schema_version':1,
        'status':'complete' if len(cases)==24 and all(c['return_code']==0 for c in cases) else 'failed',
        'cases':cases})
    return 0 if len(cases)==24 and all(c['return_code']==0 for c in cases) else 4


if __name__=='__main__':raise SystemExit(main())
