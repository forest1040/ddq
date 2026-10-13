"""Real Mac MPI integration; never connects to the lab cluster or submits PBS."""
import importlib.util
import os
import shutil
import subprocess
import sys
from pathlib import Path
import pytest

available=bool(shutil.which('mpiexec')) and importlib.util.find_spec('mpi4py') is not None
pytestmark=pytest.mark.skipif(not available,reason='Local MPI runtime unavailable')
backends=['buffered','direct'] if sys.platform=='darwin' else ['buffered']


def run_worker(tmp_path,ranks,policy,backend,*extra):
    env=dict(os.environ,PYTHONPATH='src')
    env.setdefault('OMPI_ALLOW_RUN_AS_ROOT','1');env.setdefault('OMPI_ALLOW_RUN_AS_ROOT_CONFIRM','1')
    subprocess.run([shutil.which('mpiexec'),'-n',str(ranks),sys.executable,
                    str(Path(__file__).with_name('mpi_phase4_worker.py')),str(tmp_path),
                    '--backend',backend,'--policy',policy,*extra],
                   check=True,env=env,timeout=120)


@pytest.mark.parametrize('ranks',[1,2,4])
@pytest.mark.parametrize('policy',['S0','S2'])
@pytest.mark.parametrize('backend',backends)
def test_async_exact_state_resume_and_rank_local_manifests(tmp_path,ranks,policy,backend):
    extended=['--extended'] if policy=='S2' and backend=='buffered' else []
    run_worker(tmp_path,ranks,policy,backend,*extended)


@pytest.mark.parametrize('ranks',[1,2,4])
def test_async_file_state_larger_than_managed_budget(tmp_path,ranks):
    run_worker(tmp_path,ranks,'S2','buffered','--bounded')
