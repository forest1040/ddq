"""Public distributed calibration, S1 and observe-only MPI health policies."""
import importlib.util
import os
from pathlib import Path
import shutil
import subprocess
import sys

import numpy as np
import pytest

MPI = pytest.importorskip("mpi4py.MPI")

from svsim.distributed import DistributedConfig, DistributedSimulator


ROOT = Path(__file__).resolve().parents[1]


def test_comm_self_calibration_and_profile_setter(tmp_path):
    config = DistributedConfig(str(tmp_path / "comm-self"), 1,
                               ram_budget_bytes_per_rank=4 * 1024**2,
                               metadata_reserve_bytes=256 * 1024,
                               amplitude_store="ram")
    with DistributedSimulator(3, config, comm=MPI.COMM_SELF) as sim:
        assert sim.calibrate_network(repetitions=1) is None
        assert sim.network_profile is None
        assert sim.transport.observer is not None
        guard = sim.set_network_profile(None)
        assert guard is sim.mpi_guard
        assert sim.transport.observer is not None


@pytest.mark.skipif(not (shutil.which("mpiexec") or shutil.which("mpirun")) or
                    importlib.util.find_spec("mpi4py") is None,
                    reason="MPI runtime and mpi4py are not installed")
def test_two_rank_s1_and_slow_rank_observation(tmp_path):
    launcher = shutil.which("mpiexec") or shutil.which("mpirun")
    env = dict(os.environ)
    env["PYTHONPATH"] = os.pathsep.join((str(ROOT / "src"), str(ROOT / "tests"),
                                         env.get("PYTHONPATH", "")))
    env.setdefault("OMPI_ALLOW_RUN_AS_ROOT", "1")
    env.setdefault("OMPI_ALLOW_RUN_AS_ROOT_CONFIRM", "1")
    env.setdefault("UV_CACHE_DIR", "/private/tmp/ddq-uv-cache")
    result = subprocess.run(
        [launcher, "-n", "2", sys.executable, "-m", "mpi4py",
         str(ROOT / "tests" / "mpi_distributed_policies_worker.py"),
         "--directory", str(tmp_path / "distributed-policies")],
        cwd=ROOT, env=env, capture_output=True, text=True, timeout=120)
    assert result.returncode == 0, (
        f"two-rank policy worker failed ({result.returncode})\n"
        f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}")
