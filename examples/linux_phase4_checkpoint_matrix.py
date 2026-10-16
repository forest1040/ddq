"""Container-only exact checkpoint kill/ENOSPC acceptance, no slow exclusion."""
import argparse
from hashlib import sha256
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import sys
import time
import xml.etree.ElementTree as ET


NODES=('tests/test_checkpoint_stage_kills_mpi.py::test_full_kill_matrix',
       'tests/test_checkpoint_stage_kills_mpi.py::test_external_writer_fills_the_disk_after_admission_in_a_checkpoint_stage')
REASON='only rank 0 writes the global decision'
POINTS=('before','reserved','partial','pre-rename','post-rename')


def expected():
    all_names={f'test_full_kill_matrix[{stage}-{point}-{victim}]'
        for stage in ('prepare','decision','decision-copy') for point in POINTS for victim in (0,1)}
    all_names.update(f'test_external_writer_fills_the_disk_after_admission_in_a_checkpoint_stage[{stage}-{victim}]'
        for stage in ('prepare','decision','decision-copy') for victim in (0,1))
    skipped={f'test_full_kill_matrix[decision-{point}-1]' for point in POINTS}
    skipped.add('test_external_writer_fills_the_disk_after_admission_in_a_checkpoint_stage[decision-1]')
    return all_names,skipped


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--revision',required=True)
    parser.add_argument('--backend',choices=('buffered','direct'),required=True)
    parser.add_argument('--deadline-seconds',type=int,default=1200)
    args=parser.parse_args()
    if not sys.platform.startswith('linux') or os.geteuid()==0:parser.error('Linux nonroot required')
    if socket.gethostname()!='ddq-phase4-checkpoint':parser.error('Unexpected hostname')
    if not 1<=args.deadline_seconds<=1200:parser.error('Deadline must be 1..1200')
    if os.environ.get('SVSIM_TEST_BACKEND')!=args.backend:parser.error('Backend environment mismatch')
    full=Path('/full')
    if os.environ.get('SVSIM_FULL_FS')!=str(full) or any(full.iterdir()):parser.error('Empty dedicated /full required')
    if not any(row.split()[4]=='/full' and ' - tmpfs ' in row for row in Path('/proc/self/mountinfo').read_text().splitlines()):
        parser.error('Dedicated tmpfs required')
    info=os.statvfs(full)
    if info.f_blocks*info.f_frsize!=96*1024**2:parser.error('96 MiB /full required')
    if sorted(p.name for p in Path('/sys/class/net').iterdir())!=['lo']:parser.error('Network-none required')
    root=Path(__file__).resolve().parents[1];output=Path('/results')
    summary=output/(args.backend+'.json');junit=output/(args.backend+'.xml');log=output/(args.backend+'.log')
    if any(p.exists() for p in (summary,junit,log)):parser.error('Fresh per-backend results required')
    files=[]
    for directory in ('src','tests','examples'):
        for path in sorted((root/directory).rglob('*')):
            if '__pycache__' in path.parts or path.suffix in ('.pyc','.pyo'):continue
            if path.is_symlink():parser.error('Source symlink refused')
            if path.is_file():files.append((str(path.relative_to(root)),sha256(path.read_bytes()).hexdigest()))
    command=[sys.executable,'-m','pytest','-q','-ra','-p','no:cacheprovider',
             '--basetemp=/scratch/pytest','--junitxml='+str(junit),*NODES]
    report={'revision':args.revision,'backend':args.backend,'image_id':os.environ.get('DDQ_IMAGE_ID'),
        'source_files':files,'source_sha256':sha256(json.dumps(files,separators=(',',':')).encode()).hexdigest(),
        'command':command,'uid':os.geteuid(),'hostname':socket.gethostname(),'state':'running',
        'expected':{'cases':36,'passed':30,'skipped':6,'skip_reason':REASON}}
    def save():summary.write_text(json.dumps(report,indent=2)+'\n')
    save();started=time.monotonic()
    try:
        with log.open('w') as stream:
            process=subprocess.Popen(command,cwd=root,stdout=stream,stderr=subprocess.STDOUT,start_new_session=True)
            try:report['exit_code']=process.wait(timeout=args.deadline_seconds)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid,signal.SIGKILL);process.wait();raise
        cases=list(ET.parse(junit).getroot().iter('testcase'))
        all_names,skip_names=expected()
        names=[case.get('name') for case in cases]
        skips={case.get('name'):case.find('skipped').get('message','') for case in cases if case.find('skipped') is not None}
        failures=[case.get('name') for case in cases if case.find('failure') is not None or case.find('error') is not None]
        report.update(cases=len(cases),skips=skips,failures=failures)
        if (report['exit_code']!=0 or len(names)!=36 or set(names)!=all_names or failures or
                set(skips)!=skip_names or any(reason!=REASON for reason in skips.values())):
            raise RuntimeError('Checkpoint matrix count/case/skip/failure contract violated')
        report['state']='passed'
    except Exception as exc:report.update(state='failed',error=repr(exc))
    finally:report['seconds']=time.monotonic()-started;save()
    print(json.dumps({'backend':args.backend,'state':report['state'],'summary':str(summary)}),flush=True)
    return 0 if report['state']=='passed' else 1


if __name__=='__main__':raise SystemExit(main())
