"""Local processes only; no SSH, PBS or cluster access."""
import importlib.util
import os
from pathlib import Path
import shutil
import subprocess
import sys

import pytest


@pytest.mark.skipif(not shutil.which('mpiexec') or importlib.util.find_spec('mpi4py') is None,
                    reason='Local MPI unavailable')
@pytest.mark.parametrize('ranks',[1,2,4])
def test_remaining_work_collective_order_and_actual_state(tmp_path,ranks):
    env=dict(os.environ,PYTHONPATH='src',OMPI_ALLOW_RUN_AS_ROOT='1',OMPI_ALLOW_RUN_AS_ROOT_CONFIRM='1')
    subprocess.run([shutil.which('mpiexec'),'-n',str(ranks),sys.executable,
        str(Path(__file__).with_name('mpi_remaining_work_worker.py')),str(tmp_path)],
        env=env,check=True,timeout=120)


@pytest.mark.skipif(not shutil.which('mpiexec') or importlib.util.find_spec('mpi4py') is None,
                    reason='Local MPI unavailable')
def test_s1_layout_decisions_are_replayed_exactly(tmp_path):
    env=dict(os.environ,PYTHONPATH='src',OMPI_ALLOW_RUN_AS_ROOT='1',OMPI_ALLOW_RUN_AS_ROOT_CONFIRM='1')
    subprocess.run([shutil.which('mpiexec'),'-n','2',sys.executable,
        str(Path(__file__).with_name('mpi_s1_eta_worker.py')),str(tmp_path)],env=env,check=True,timeout=120)
