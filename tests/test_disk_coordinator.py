import importlib.util
import os
from pathlib import Path
import shutil
import subprocess
import sys
import pytest

from svsim import DistributedConfig


def test_coordinator_path_requires_explicit_absolute_node_local_namespace(tmp_path):
    with pytest.raises(ValueError,match='absolute node-local'):
        DistributedConfig(str(tmp_path),2,disk_admission_ledger=str(tmp_path/'ledger'))
    with pytest.raises(ValueError,match='absolute node-local'):
        DistributedConfig(str(tmp_path),2,storage_scope='node-local',disk_admission_ledger='relative')


available=bool(shutil.which('mpiexec')) and importlib.util.find_spec('mpi4py') is not None

@pytest.mark.skipif(not available,reason='Local MPI unavailable')
@pytest.mark.parametrize('ranks',[1,2,4])
@pytest.mark.parametrize('backend',['buffered','direct'] if sys.platform=='darwin' else ['buffered'])
def test_shared_volume_dataset_fork_and_gradient(tmp_path,ranks,backend):
    _run(tmp_path,ranks,'--backend',backend)


@pytest.mark.skipif(not available,reason='Local MPI unavailable')
def test_same_volume_rank_path_mismatch_fails_collectively_before_ledger_open(tmp_path):
    _run(tmp_path,2,'--mismatched-rank-ledger')


def _run(scratch,ranks,*extra):
    env=dict(os.environ,PYTHONPATH='src',OMP_NUM_THREADS='1',OPENBLAS_NUM_THREADS='1',
        VECLIB_MAXIMUM_THREADS='1',OMPI_ALLOW_RUN_AS_ROOT='1',OMPI_ALLOW_RUN_AS_ROOT_CONFIRM='1')
    result=subprocess.run([shutil.which('mpiexec'),'-n',str(ranks),sys.executable,
        str(Path(__file__).with_name('mpi_disk_coordinator_worker.py')),str(scratch),*extra],
        capture_output=True,text=True,env=env,timeout=120)
    assert result.returncode==0,result.stdout+'\n'+result.stderr
    assert 'shared-volume coordinator validation passed' in result.stdout
