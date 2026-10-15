"""The volume coordinator through DistributedConfig: config, sharing, fork, resume, multi-rank, offline republish."""
import importlib.util
import os
import shutil
import subprocess
import sys
import textwrap
from dataclasses import replace
from pathlib import Path

import numpy as np
import pytest

from svsim import DistributedConfig
from svsim.disk_budget import DiskAdmissionError, DiskBudget
from svsim.storage import FileBlockStore, StorageConfig
from svsim.uncommitted_recovery import recover_uncommitted_reservations
from svsim.volume_coordinator import VolumeCoordinator

ROOT = Path(__file__).resolve().parents[1]
MIB = 1024 ** 2
QUOTA = 96 * MIB
BACKEND = os.environ.get('SVSIM_TEST_BACKEND', 'buffered')
mpi_available = bool(shutil.which('mpiexec')) and importlib.util.find_spec('mpi4py') is not None
needs_mpi = pytest.mark.skipif(not mpi_available, reason='Local MPI unavailable')


def _config(tmp_path, name, coordinator=None, **extra):
    coordinator = tmp_path / 'vol' / 'coordinator.sqlite3' if coordinator is None else coordinator
    return DistributedConfig(str(tmp_path / name), 2, ram_budget_bytes_per_rank=2 * MIB, tile_bytes=4096,
                             frame_policy='materialize', metadata_reserve_bytes=256 * 1024, backend=BACKEND,
                             storage_scope='node-local', disk_quota_bytes=QUOTA,
                             volume_coordinator=str(coordinator), **extra)


# ---------------------------------------------------------------------------- configuration
def test_coordinator_setting_is_validated_like_the_other_disk_fields(tmp_path):
    path = str(tmp_path / 'c.sqlite3')
    assert DistributedConfig(str(tmp_path), 2, storage_scope='node-local', volume_coordinator=path).volume_coordinator == path
    assert DistributedConfig(str(tmp_path), 2).volume_coordinator is None  # opt-in
    for bad in ('relative.sqlite3', '', 'x' * 5000, str(tmp_path / ('d' * 4097))):
        with pytest.raises(ValueError, match='absolute node-local'):
            DistributedConfig(str(tmp_path), 2, storage_scope='node-local', volume_coordinator=bad)
    with pytest.raises(ValueError, match='absolute node-local'):
        DistributedConfig(str(tmp_path), 2, volume_coordinator=path)  # shared scope
    with pytest.raises(ValueError, match='absolute node-local'):
        DistributedConfig(str(tmp_path), 2, storage_scope='node-local', volume_coordinator=7)
    with pytest.raises(ValueError, match='must not be the disk admission ledger'):
        DistributedConfig(str(tmp_path), 2, storage_scope='node-local', volume_coordinator=path, disk_admission_ledger=path)


# ---------------------------------------------------------------------------- normal API, in process
MPI = pytest.importorskip('mpi4py.MPI')


def _sim(config, **kwargs):
    from svsim import DistributedSimulator
    return DistributedSimulator(4, config, comm=MPI.COMM_SELF, **kwargs)


def test_two_independent_simulators_share_the_coordinator_budget(tmp_path):
    with _sim(_config(tmp_path, 'a')) as a, _sim(_config(tmp_path, 'b')) as b:
        assert a.disk_budget._ledger_path != b.disk_budget._ledger_path
        coord = a.disk_budget._coord
        assert str(coord.path) == str((tmp_path / 'vol' / 'coordinator.sqlite3').resolve())
        assert {str(a.disk_budget._ledger_path), str(b.disk_budget._ledger_path)} <= set(coord.snapshot()['ledgers'])
        token = a.disk_budget.reserve(30 * MIB)
        rows = b.disk_budget.db.execute('SELECT count(*) FROM reservations').fetchone()[0]
        with pytest.raises(DiskAdmissionError, match='Volume coordinator'):
            b.disk_budget.reserve(60 * MIB)  # within b's own quota, jointly over the volume's
        assert b.disk_budget.db.execute('SELECT count(*) FROM reservations').fetchone()[0] == rows
        assert coord.claim(b.disk_budget._ledger_path)['pending'] == b.disk_budget.reserved_bytes  # no phantom claim
        other = b.disk_budget.reserve(5 * MIB)
        a.disk_budget.release(token)
        b.disk_budget.release(other)
        assert coord.snapshot()['pending_bytes'] == a.disk_budget.reserved_bytes + b.disk_budget.reserved_bytes
        assert not coord.snapshot()['strict_capacity_guarantee']
        assert not a.disk_budget.strict_capacity_guarantee


def test_uncoordinated_simulator_on_a_coordinated_ledger_is_refused(tmp_path):
    with _sim(_config(tmp_path, 'a')) as a:
        ledger = str(a.disk_budget._ledger_path)
        plain = replace(_config(tmp_path, 'plain'), volume_coordinator=None, disk_admission_ledger=ledger)
        with pytest.raises(Exception, match='bound to a volume coordinator'):
            _sim(plain)
        assert not (tmp_path / 'plain' / 'rank-0000').exists()


def test_resume_with_missing_or_different_coordinator_is_refused_before_mutation(tmp_path):
    from svsim import DistributedSimulator
    config = _config(tmp_path, 'a')
    with _sim(config) as sim:
        sim.gate('H', (0,)); sim.gate('RY', (3,), angle=.2); sim.materialize(binding=sim.bind())
        snapshot = sim.commit_snapshot(); expected = sim.state(allow_gather=True).copy()
        ledger = Path(sim.disk_budget._ledger_path)
    coordinator = Path(config.volume_coordinator)
    before = (ledger.read_bytes(), coordinator.read_bytes())
    with pytest.raises(Exception, match='bound to volume coordinator .*missing'):
        DistributedSimulator.resume(replace(config, volume_coordinator=None), snapshot, comm=MPI.COMM_SELF)
    other = tmp_path / 'other' / 'coordinator.sqlite3'
    with pytest.raises(Exception, match='a different one'):
        DistributedSimulator.resume(replace(config, volume_coordinator=str(other)), snapshot, comm=MPI.COMM_SELF)
    assert not other.exists()
    assert (ledger.read_bytes(), coordinator.read_bytes()) == before  # nothing was written
    with DistributedSimulator.resume(config, snapshot, comm=MPI.COMM_SELF) as restored:
        np.testing.assert_allclose(restored.state(allow_gather=True), expected, atol=1e-12)
        assert restored.disk_budget._coord is not None


def test_dataset_created_without_a_coordinator_refuses_a_new_binding(tmp_path):
    from svsim import DistributedSimulator
    plain = replace(_config(tmp_path, 'a'), volume_coordinator=None)
    with _sim(plain) as sim:
        sim.gate('H', (0,)); sim.materialize(binding=sim.bind())
        snapshot = sim.commit_snapshot()
    with pytest.raises(Exception, match='created without a volume coordinator'):
        DistributedSimulator.resume(_config(tmp_path, 'a'), snapshot, comm=MPI.COMM_SELF)
    assert not (tmp_path / 'vol' / 'coordinator.sqlite3').exists()


def test_unsupported_filesystem_refuses_the_simulator_before_any_ledger(tmp_path, monkeypatch):
    monkeypatch.setitem(VolumeCoordinator.__init__.__kwdefaults__, 'filesystem_probe', lambda path: 'nfs4')
    with pytest.raises(Exception, match='Unsupported filesystem'):
        _sim(_config(tmp_path, 'a'))
    assert not list((tmp_path / 'a').rglob('*.sqlite3'))
    assert not (tmp_path / 'vol' / 'coordinator.sqlite3').exists()


def test_fork_and_parameter_batch_children_inherit_the_binding(tmp_path):
    from svsim import AsyncDistributedSimulator, AsyncPolicy, ParameterSymbol, ObservableIR, PauliTerm
    policy = AsyncPolicy(io_engine='cooperative', io_workers=1, prefetch_blocks=0, swap_policy='S2')
    theta = ParameterSymbol('vc-theta', 'vc-theta')
    with AsyncDistributedSimulator(4, _config(tmp_path, 'a'), comm=MPI.COMM_SELF, policy=policy) as a:
        a.gate('H', (3,)); a.gate('RY', (0,), angle=theta)
        binding = a.bind({theta: .37})
        prefix = a.commit_prefix([theta], binding=binding)
        with a.fork(prefix, binding=binding) as child:
            assert child.config.volume_coordinator == a.config.volume_coordinator
            assert child.disk_budget._coord is not None and child.disk_budget._coord.path == a.disk_budget._coord.path
            assert child.disk_budget._ledger_path == a.disk_budget._ledger_path
            child.materialize(binding=binding)
        a.materialize(binding=binding)
        observable = ObservableIR((PauliTerm(0, 1),))
        shifted = a.gradient(observable, parameters=[theta], method='parameter-shift')
        assert abs(shifted[theta.parameter_id] + np.sin(.37)) < 1e-9
        # Children of the gradient / batch paths were coordinated: the coordinator saw their roots.
        roots = a.disk_budget._coord.snapshot()['roots']
        assert any(Path(r) == Path(a.config.directory).resolve() or Path(a.config.directory).resolve().is_relative_to(r) for r in roots)


# ---------------------------------------------------------------------------- multi-rank (real mpiexec)
def _mpiexec(scratch, ranks, mode, *extra):
    env = dict(os.environ, PYTHONPATH='src', OMP_NUM_THREADS='1', OPENBLAS_NUM_THREADS='1', VECLIB_MAXIMUM_THREADS='1',
               OMPI_ALLOW_RUN_AS_ROOT='1', OMPI_ALLOW_RUN_AS_ROOT_CONFIRM='1')
    result = subprocess.run([shutil.which('mpiexec'), '-n', str(ranks), sys.executable,
                             str(Path(__file__).with_name('mpi_volume_coordinator_runtime_worker.py')), str(scratch), mode,
                             '--backend', BACKEND, *extra], capture_output=True, text=True, env=env, timeout=240, cwd=ROOT)
    assert result.returncode == 0, result.stdout + '\n' + result.stderr
    assert f'volume coordinator runtime {mode} passed' in result.stdout


@needs_mpi
@pytest.mark.parametrize('ranks', [1, 2])
def test_ranks_use_the_host_coordinator_for_datasets_fork_and_gradient(tmp_path, ranks):
    _mpiexec(tmp_path, ranks, 'run')


@needs_mpi
def test_same_volume_ranks_must_agree_on_the_coordinator(tmp_path):
    _mpiexec(tmp_path, 2, 'mismatch')


@needs_mpi
def test_filesystem_refusal_on_one_rank_is_collective(tmp_path):
    _mpiexec(tmp_path, 2, 'unsupported')


@needs_mpi
@pytest.mark.parametrize('ranks', [1, 2])
def test_checkpoint_recovery_republishes_the_settled_claim(tmp_path, ranks):
    _mpiexec(tmp_path, ranks, 'recover')


# ---------------------------------------------------------------------------- offline republish (unlinked tokens)
def _kill_writer(tmp_path):
    script = textwrap.dedent('''
        import os, sys
        from svsim.disk_budget import DiskBudget
        from svsim.storage import FileBlockStore, StorageConfig
        from svsim.volume_coordinator import VolumeCoordinator
        root, coordinator, backend = sys.argv[1], sys.argv[2], sys.argv[3]
        os.makedirs(os.path.join(root, 'store'), exist_ok=True)
        coord = VolumeCoordinator(coordinator, quota_bytes=%d)
        budget = DiskBudget(os.path.join(root, 'ledger', 'ledger.sqlite3'), [os.path.join(root, 'store')],
                            quota_bytes=%d, shared_policy=True, volume_coordinator=coord)
        store = FileBlockStore(StorageConfig(os.path.join(root, 'store'), 2, backend=backend), 4, disk_budget=budget)
        version = store.new_version()
        token = budget.reserve(store.slot_bytes, operation='amplitude-extent', writing=True)
        store.reserve_extent(version, token)
        os._exit(0)
    ''' % (QUOTA, QUOTA))
    coordinator = tmp_path / 'vol' / 'coordinator.sqlite3'
    child = subprocess.run([sys.executable, '-c', script, str(tmp_path), str(coordinator), BACKEND], cwd=ROOT,
                           env={**os.environ, 'OMP_NUM_THREADS': '1'}, capture_output=True, text=True, timeout=120)
    assert child.returncode == 0, child.stderr
    return tmp_path / 'ledger' / 'ledger.sqlite3', coordinator


def test_offline_recovery_republishes_the_true_pending_idempotently(tmp_path):
    ledger, coordinator = _kill_writer(tmp_path)
    coord = VolumeCoordinator.open_bound(coordinator)
    try:
        high = coord.claim(ledger)['pending']
        assert high > 0
        dry = recover_uncommitted_reservations(ledger)
        assert dry['status'] == 'dry-run' and coord.claim(ledger)['pending'] == high  # a dry run publishes nothing
        assert dry['volume_claim']['status'] == 'dry-run' and dry['volume_claim']['previous_claim_bytes'] == high
        applied = recover_uncommitted_reservations(ledger, apply=True, confirm_plan=dry['plan_sha256'])
        receipt = applied['volume_claim']
        assert applied['status'] == 'applied' and receipt['status'] == 'republished'
        assert receipt['previous_claim_bytes'] == high and receipt['published_pending_bytes'] == 0 and receipt['changed']
        assert coord.claim(ledger)['pending'] == 0 and coord.snapshot()['pending_bytes'] == 0
        again = recover_uncommitted_reservations(ledger)
        repeat = recover_uncommitted_reservations(ledger, apply=True, confirm_plan=again['plan_sha256'])
        assert repeat['volume_claim']['changed'] is False and coord.claim(ledger)['pending'] == 0
        # The volume is usable by another ledger again, and the bound ledger reopens with the coordinator.
        other = tmp_path / 'other'
        other.mkdir()
        free = DiskBudget(other / 'ledger.sqlite3', [other], volume_coordinator=coord)
        free.reserve(1024)
        free.close()
        again_open = DiskBudget(ledger, [tmp_path / 'store'], quota_bytes=QUOTA, shared_policy=True, volume_coordinator=coord)
        assert again_open.reserved_bytes == 0 and coord.claim(ledger)['pending'] == 0
        again_open.close()
    finally:
        coord.close()


def test_offline_recovery_needs_the_bound_coordinator(tmp_path):
    ledger, coordinator = _kill_writer(tmp_path)
    moved = coordinator.with_name('moved.sqlite3')
    coordinator.rename(moved)
    dry = None
    with pytest.raises(DiskAdmissionError, match='coordinator'):
        dry = recover_uncommitted_reservations(ledger)
    assert dry is None
    moved.rename(coordinator)
    assert recover_uncommitted_reservations(ledger)['volume_claim']['status'] == 'dry-run'


def test_writer_maintenance_scope_adopts_the_bound_coordinator(tmp_path):
    from svsim.uncommitted_recovery import offline_scope
    ledger, coordinator = _kill_writer(tmp_path)
    with offline_scope(ledger, writer=True) as scope:
        assert scope['budget']._coord is scope['coordinator'] and scope['budget'].reserved_bytes > 0
        assert scope['coordinator'].claim(ledger)['pending'] == scope['budget'].reserved_bytes
    with offline_scope(ledger) as scope:  # recovery-only scope keeps the coordinator for the republish
        assert scope['coordinator'] is not None and scope['budget']._coord is None
