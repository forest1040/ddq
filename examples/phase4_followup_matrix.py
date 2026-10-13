"""Sequential finite same-host jobs for the Phase 4 followup evidence."""
import argparse
from hashlib import sha256
import json
import math
from pathlib import Path
import shutil
import subprocess
import sys
from time import perf_counter

from svsim import JobSpec,LocalDevelopmentRunner
from svsim.development_runner import JobState


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output-dir',required=True);parser.add_argument('--scratch',required=True)
    parser.add_argument('--repetitions',type=int,default=7);parser.add_argument('--warmup',type=int,default=2)
    parser.add_argument('--backends',nargs='+',choices=('buffered','direct'),default=['buffered','direct'])
    parser.add_argument('--ranks',nargs='+',type=int,choices=(1,2,4),default=[1,2,4])
    parser.add_argument('--wall-seconds',type=float,default=300)
    parser.add_argument('--action',choices=('all','monitor'),default='all')
    args=parser.parse_args()
    if not 1<=args.repetitions<=20 or not 0<=args.warmup<=10:parser.error('Finite trial limits required')
    if not math.isfinite(args.wall_seconds) or not 0<args.wall_seconds<=3600:parser.error('wall seconds must be finite and <=3600')
    if len(set(args.backends))!=len(args.backends):parser.error('backend cases must be unique')
    if len(set(args.ranks))!=len(args.ranks):parser.error('rank cases must be unique')
    root=Path(__file__).resolve().parents[1];output=Path(args.output_dir).resolve();output.mkdir(parents=True,exist_ok=True)
    launcher=shutil.which('mpiexec')
    if launcher is None:raise RuntimeError('Local MPI launcher required')
    revision=subprocess.check_output(['git','rev-parse','HEAD'],cwd=root,text=True).strip()
    lock_hash=sha256((root/'uv.lock').read_bytes()).hexdigest()
    rows=[];started=perf_counter()
    with LocalDevelopmentRunner(output/'jobs',max_ram_bytes=512*1024**2,max_disk_bytes=512*1024**2,max_concurrent_jobs=1) as runner:
        for backend in args.backends:
            for ranks in args.ranks:
                name=f'{backend}-p{ranks}';destination=output/f'{name}.json'
                spec=JobSpec(revision,lock_hash,(sys.executable,str(root/'examples/phase4_remaining_campaign.py'),
                    '--action',args.action,'--scratch',str(Path(args.scratch).resolve()/name),'--output',str(destination),
                    '--repetitions',str(args.repetitions),'--warmup',str(args.warmup),
                    '--backend',backend,'--wall-seconds',str(args.wall_seconds)),
                    ranks=ranks,rank_ram_bytes=8*1024**2,host_ram_bytes=512*1024**2,
                    rank_disk_bytes=64*1024**2,threads=1,walltime_seconds=args.wall_seconds,
                    scratch_root=str(Path(args.scratch).resolve()/name),cwd=str(root),test_case=name,
                    launcher_argv=(launcher,'-n',str(ranks)),env={'PYTHONPATH':str(root/'src'),'OMP_NUM_THREADS':'1',
                        'OPENBLAS_NUM_THREADS':'1','VECLIB_MAXIMUM_THREADS':'1'})
                handle=runner.submit(spec)
                while handle.record.state not in runner._DONE:
                    try:runner.wait(handle.job_id,timeout=30)
                    except TimeoutError:print(json.dumps({'case':name,'status':'running'}),flush=True)
                record=handle.record
                summary={'case':name,'state':record.state.value,'error':record.error,'raw':destination.name}
                if record.state==JobState.COMPLETED:
                    data=json.loads(destination.read_text())
                    gradient=data.get('gradient',{}).get('calibration',{}).get('observations',[])
                    sections=('monitor',) if args.action=='monitor' else ('comparison','adaptive')
                    summary['validated']=all(len(data.get(key,{}).get('rows',[]))==2 and all(row['status']=='completed'
                        for row in data[key]['rows']) and all(row['status']=='completed' for row in data[key]['warmup_rows'])
                        for key in sections) and (args.action=='monitor' or
                        len(gradient)==3*(args.repetitions+args.warmup) and all(row['success'] and row['validated'] for row in gradient))
                    summary['sha256']=sha256(destination.read_bytes()).hexdigest()
                else:summary['validated']=False
                rows.append(summary)
                print(json.dumps(summary),flush=True)
                (output/'matrix.json').write_text(json.dumps({'kind':'phase4-followup-matrix','revision':revision,
                    'scope':'same-host','cluster_used':False,'action':args.action,'repetitions':args.repetitions,'warmup':args.warmup,
                    'rows':rows,'elapsed_seconds':perf_counter()-started},indent=2)+'\n')
    if not all(row['validated'] for row in rows):raise SystemExit(1)


if __name__=='__main__':main()
