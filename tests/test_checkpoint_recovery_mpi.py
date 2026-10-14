import importlib.util
import os
from pathlib import Path
import shutil
import subprocess
import sys

import pytest


@pytest.mark.skipif(not shutil.which('mpiexec') or importlib.util.find_spec('mpi4py') is None,reason='Requires local MPI')
@pytest.mark.parametrize(('ranks','same_root'),[(1,False),(2,False),(4,False),(2,True)])
def test_collective_checkpoint_recovery(tmp_path,ranks,same_root):
    root=Path(__file__).resolve().parents[1]
    env=dict(os.environ,PYTHONPATH=str(root/'src'),MPI4PY_RC_THREAD_LEVEL='single',
             OMPI_ALLOW_RUN_AS_ROOT='1',OMPI_ALLOW_RUN_AS_ROOT_CONFIRM='1',
             OMP_NUM_THREADS='1',OPENBLAS_NUM_THREADS='1',VECLIB_MAXIMUM_THREADS='1')
    command=[shutil.which('mpiexec'),'-n',str(ranks),sys.executable,str(Path(__file__).with_name('mpi_checkpoint_recovery_worker.py')),str(tmp_path)]
    if same_root:command.append('--same-root')
    completed=subprocess.run(command,cwd=root,env=env,capture_output=True,text=True,timeout=120)
    assert completed.returncode==0,completed.stdout+'\n'+completed.stderr
    assert f'checkpoint reservation recovery PASS {ranks}' in completed.stdout
