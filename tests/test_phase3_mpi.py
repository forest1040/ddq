"""Small real-MPI integration runs for the Phase 3 distributed runtime."""
import importlib.util
import os
from pathlib import Path
import shutil
import subprocess
import sys

import pytest


ROOT = Path(__file__).resolve().parents[1]


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
        cwd=ROOT, env=env, capture_output=True, text=True, timeout=120)
    assert result.returncode == 0, (
        f"{ranks}-rank worker failed ({result.returncode})\n"
        f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}")
