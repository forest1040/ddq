"""Small real-MPI integration runs for the Phase 3 distributed runtime."""
import importlib.util
import os
from pathlib import Path
import shutil
import subprocess
import sys

import pytest


ROOT = Path(__file__).resolve().parents[1]
# Docker/ext4 durability tests can exceed the short local smoke deadline.
# The development runner selects a finite bound without changing the workload.
MPI_TEST_TIMEOUT = int(os.environ.get('DDQ_MPI_TEST_TIMEOUT', '120'))
if not 1 <= MPI_TEST_TIMEOUT <= 1800:
    raise ValueError('DDQ_MPI_TEST_TIMEOUT must be in 1..1800 seconds')


@pytest.mark.parametrize("ranks", (1, 2, 4))
@pytest.mark.skipif(not (shutil.which("mpiexec") or shutil.which("mpirun")) or
                    importlib.util.find_spec("mpi4py") is None,
                    reason="MPI runtime and mpi4py are not installed")
def test_distributed_state_observables_and_checkpoint(tmp_path, ranks):
    launcher = shutil.which("mpiexec") or shutil.which("mpirun")
    directory = tmp_path / f"phase3-{ranks}"
    env = dict(os.environ)
    env["PYTHONPATH"] = os.pathsep.join((str(ROOT / "src"), str(ROOT / "tests"), env.get("PYTHONPATH", "")))
    env.setdefault("OMPI_ALLOW_RUN_AS_ROOT", "1")
    env.setdefault("OMPI_ALLOW_RUN_AS_ROOT_CONFIRM", "1")
    env.setdefault("UV_CACHE_DIR", "/private/tmp/ddq-uv-cache")
    result = subprocess.run(
        [launcher, "-n", str(ranks), sys.executable, "-m", "mpi4py",
         str(ROOT / "tests" / "mpi_phase3_worker.py"), "--directory", str(directory)],
        cwd=ROOT, env=env, capture_output=True, text=True, timeout=MPI_TEST_TIMEOUT)
    assert result.returncode == 0, (
        f"{ranks}-rank worker failed ({result.returncode})\n"
        f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}")
