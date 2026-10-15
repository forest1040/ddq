"""Two real MPI ranks killed (or disk-starved) in each checkpoint stage; recovery in a second job (P4-009)."""
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

import pytest

pytest.importorskip('mpi4py')
pytestmark = pytest.mark.skipif(not shutil.which('mpiexec'), reason='Requires local MPI')

ROOT = Path(__file__).resolve().parents[1]
WORKER = str(Path(__file__).with_name('mpi_checkpoint_stage_worker.py'))
ENV = dict(os.environ, PYTHONPATH=str(ROOT / 'src'), MPI4PY_RC_THREAD_LEVEL='single', OMPI_ALLOW_RUN_AS_ROOT='1',
           OMPI_ALLOW_RUN_AS_ROOT_CONFIRM='1', OMP_NUM_THREADS='1', OPENBLAS_NUM_THREADS='1', VECLIB_MAXIMUM_THREADS='1')
FULL = os.environ.get('SVSIM_FULL_FS')
POINTS = ['before', 'reserved', 'partial', 'pre-rename', 'post-rename']


def outcome(stage, point):
    """Which cut the recovered dataset must expose: the new snapshot is authoritative once the global decision is renamed."""
    if stage == 'decision': return 'new' if point == 'post-rename' else 'old'
    return 'new' if stage == 'decision-copy' else 'old'


def mpirun(*args, ranks=2, timeout=300):
    return subprocess.run([shutil.which('mpiexec'), '-n', str(ranks), sys.executable, WORKER, *map(str, args)], cwd=ROOT,
                          env=ENV, capture_output=True, text=True, timeout=timeout)


def case(base, stage, point, victim, fault, filler=None):
    extra = [filler] if filler else []
    killed = mpirun('run', base, stage, point, victim, fault, *extra)
    assert killed.returncode != 0, 'the checkpointing job must die'
    marker = Path(base) / f'fault-{victim}.txt'
    assert marker.exists() and marker.read_text() == point, (killed.stdout, killed.stderr)
    if filler:
        errors = ' | '.join(line for line in killed.stdout.splitlines() if line.startswith('commit-error'))
        print('ENOSPC-ERRORS', stage, victim, errors)
        assert any(w in errors.lower() for w in ('full', 'no space', 'incomplete', 'short write')), errors
        Path(filler).unlink()  # the operator frees the space before recovery
    want = outcome(stage, point)
    done = mpirun('recover', base, want)
    assert done.returncode == 0 and f'stage recovery PASS {want}' in done.stdout, done.stdout + '\n' + done.stderr


@pytest.mark.parametrize('stage,point,victim', [
    ('prepare', 'partial', 1), ('decision', 'pre-rename', 0), ('decision', 'post-rename', 0), ('decision-copy', 'partial', 1)])
def test_rank_killed_in_checkpoint_stage_recovers(tmp_path, stage, point, victim):
    case(tmp_path, stage, point, victim, 'kill')


@pytest.mark.slow
@pytest.mark.parametrize('victim', [0, 1])
@pytest.mark.parametrize('point', POINTS)
@pytest.mark.parametrize('stage', ['prepare', 'decision', 'decision-copy'])
def test_full_kill_matrix(tmp_path, stage, point, victim):
    if stage == 'decision' and victim == 1: pytest.skip('only rank 0 writes the global decision')
    case(tmp_path, stage, point, victim, 'kill')


@pytest.mark.skipif(not FULL, reason='SVSIM_FULL_FS must name a size-limited mount')
@pytest.mark.parametrize('victim', [0, 1])
@pytest.mark.parametrize('stage', ['prepare', 'decision', 'decision-copy'])
def test_external_writer_fills_the_disk_after_admission_in_a_checkpoint_stage(stage, victim):
    if stage == 'decision' and victim == 1: pytest.skip('only rank 0 writes the global decision')
    base = Path(tempfile.mkdtemp(dir=FULL, prefix='ckpt-'))
    filler = str(Path(FULL) / ('filler-' + base.name))
    try:
        case(base, stage, 'reserved', victim, 'fill', filler)
    finally:
        Path(filler).unlink(missing_ok=True)
        shutil.rmtree(base, ignore_errors=True)
