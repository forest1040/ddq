"""Opt-in MPI integration coverage for the real Linux io_uring engine.

Unlike the deterministic backend tests, these tests launch independent MPI
ranks and exercise the simulator's real storage and checkpoint paths. They are
disabled unless DDQ_NATIVE_URING=1 is explicitly set.
"""
import importlib.util
import os
from pathlib import Path
import shutil
import subprocess
import sys

import pytest


NATIVE_OPT_IN = os.environ.get('DDQ_NATIVE_URING') == '1'
pytestmark = pytest.mark.skipif(
    not NATIVE_OPT_IN,
    reason='native io_uring MPI tests require explicit DDQ_NATIVE_URING=1',
)

RANK_COUNTS = (1, 2, 4)
STORAGE_BACKENDS = ('buffered', 'direct')
WORKER = Path(__file__).with_name('mpi_phase4_worker.py')


def _require_opted_in_environment():
    # Once opted in, environment/capability problems are failures rather than
    # skips so a requested native campaign cannot silently become a no-op.
    if not sys.platform.startswith('linux'):
        pytest.fail('DDQ_NATIVE_URING=1 requires Linux')
    launcher = shutil.which('mpiexec')
    if launcher is None:
        pytest.fail('DDQ_NATIVE_URING=1 requires mpiexec on PATH')
    if importlib.util.find_spec('mpi4py') is None:
        pytest.fail('DDQ_NATIVE_URING=1 requires mpi4py')
    return launcher


@pytest.mark.parametrize('ranks', RANK_COUNTS)
@pytest.mark.parametrize('backend', STORAGE_BACKENDS)
def test_native_io_uring_phase4_mpi(tmp_path, ranks, backend):
    launcher = _require_opted_in_environment()
    env = dict(os.environ)
    env['PYTHONPATH'] = os.pathsep.join(
        filter(None, ('src', env.get('PYTHONPATH', ''))))
    env['MPI4PY_RC_THREAD_LEVEL'] = 'single'
    env.setdefault('OMPI_ALLOW_RUN_AS_ROOT', '1')
    env.setdefault('OMPI_ALLOW_RUN_AS_ROOT_CONFIRM', '1')

    command = [
        launcher, '-n', str(ranks), sys.executable, str(WORKER), str(tmp_path),
        '--backend', backend,
        '--policy', 'S2',
        '--io-engine', 'io_uring',
        '--extended',
    ]
    try:
        completed = subprocess.run(
            command, check=False, env=env, capture_output=True, text=True,
            timeout=180,
        )
    except subprocess.TimeoutExpired as exc:
        pytest.fail(f'native io_uring MPI run timed out after 180s: {command!r}\n{exc}')
    assert completed.returncode == 0, (
        f'native io_uring MPI run failed (exit {completed.returncode})\n'
        f'command: {command!r}\nstdout:\n{completed.stdout}\n'
        f'stderr:\n{completed.stderr}')
    assert f'phase4 local MPI PASS {ranks} S2 {backend}' in completed.stdout, (
        f'MPI worker did not report successful completion\nstdout:\n{completed.stdout}\n'
        f'stderr:\n{completed.stderr}')
