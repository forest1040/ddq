"""Local launch/control-flow regression: no PBS, network or MPI job is run."""
import os
from pathlib import Path
import subprocess
import sys
import tarfile

import pytest


FAKE = r'''#!PYTHON
import os,pathlib,subprocess,sys
name=pathlib.Path(sys.argv[0]).name
job=pathlib.Path(os.environ['DDQ_JOB'])
mode=os.environ.get('HARNESS_FAILURE','')
if name=='pbsdsh':
    calls=job.parent/'calls'
    count=int(calls.read_text())+1 if calls.exists() else 1
    calls.parent.mkdir(parents=True,exist_ok=True);calls.write_text(str(count))
    if count==1:
        job.mkdir(parents=True,exist_ok=True)
        (job/'scratch').mkdir();(job/'tmp').mkdir()
        (job/'source-check.txt').write_text('source OK\n')
        if mode=='bootstrap':sys.exit(12)
    elif 'rm -rf' in sys.argv[-1]:
        import shutil
        shutil.rmtree(job/'scratch');shutil.rmtree(job/'tmp')
        if mode=='cleanup':sys.exit(13)
    elif mode=='capacity':sys.exit(11)
elif name=='python':
    code=sys.argv[-1]
    assert 'from mpi4py import MPI' not in code, 'singleton MPI import'
    (job/'diagnostic-code.txt').write_text(code)
    if mode=='diagnostics':sys.exit(14)
    sys.exit(subprocess.call([sys.executable,*sys.argv[1:]]))
elif name=='timeout':
    sys.exit(subprocess.call(sys.argv[4:]))
elif name=='mpiexec':
    with (job/'workers.txt').open('a') as out:out.write(' '.join(sys.argv[1:])+'\n')
    print('fake real-worker launch')
    if mode=='worker' and 'mismatch' in sys.argv:sys.exit(9)
elif name=='ssh':
    if mode=='archive':sys.exit(15)
    sys.exit(subprocess.call(['/bin/bash','-c',sys.argv[-1]]))
'''


@pytest.mark.parametrize('failure',['','bootstrap','diagnostics','capacity','worker','cleanup','archive','node-count'])
def test_job_returns_logs_and_preserves_failure_without_singleton_mpi(tmp_path,failure):
    script=Path(__file__).with_name('coordinator.pbs').read_text()
    code=tmp_path/'code';code.mkdir()
    tools=tmp_path/'tools';tools.mkdir()
    for name in ('pbsdsh','python','timeout','mpiexec','ssh'):
        path=tools/name;path.write_text(FAKE.replace('PYTHON',sys.executable,1));path.chmod(0o700)
    script=script.replace('export DDQ_ENV=/var/tmp/ddq-20009/phase4-dfdd4a403586',f'export DDQ_ENV={tmp_path}/env')
    script=script.replace('export DDQ_CODE=/var/tmp/ddq-20009/p4-acceptance-20261016',f'export DDQ_CODE={code}')
    script=script.replace('/opt/pbs/bin/pbsdsh',str(tools/'pbsdsh'))
    script=script.replace('export PATH="$DDQ_ENV/.venv/bin:',f'export PATH="{tools}:$DDQ_ENV/.venv/bin:')
    local=tmp_path/'job.sh';local.write_text(script)
    nodes=tmp_path/'nodes';nodes.write_text('host-a\nhost-a\nhost-b\nhost-b\n')
    if failure=='node-count':nodes.write_text('host-a\nhost-a\n')
    result=subprocess.run(['/bin/bash',str(local)],env={**os.environ,'PBS_JOBID':'local-test',
        'PATH':str(tools)+os.pathsep+os.environ['PATH'],
        'PBS_NODEFILE':str(nodes),'HARNESS_FAILURE':failure},capture_output=True,text=True,timeout=20)
    job=code/'jobs/local-test'
    assert (result.returncode==0)==(not failure),result.stdout+result.stderr
    assert not (job/'scratch').exists() and not (job/'tmp').exists()
    assert (job/'status.txt').read_text().strip()==('0' if not failure else str(result.returncode))
    archive=code/'results-local-test.tar.gz'
    if failure=='archive':
        assert not archive.exists() and (job/'exit-summary.txt').exists() and (job/'archive-failure.txt').exists()
        return
    with tarfile.open(archive) as saved:
        names=saved.getnames()
        assert './status.txt' in names and './exit-summary.txt' in names and './cleanup.log' in names
        assert not any('/scratch/' in name or '/tmp/' in name for name in names)
    if failure in ('','worker','cleanup'):
        workers=(job/'workers.txt').read_text().splitlines()
        assert len(workers)==8
        assert all('--require-cross-host' in line and '--map-by ppr:2:node -n 4' in line for line in workers)
        assert len((job/'cases.txt').read_text().splitlines())==8
