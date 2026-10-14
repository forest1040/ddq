"""Gate-level catalog batches: fewer durable commits, same snapshot recovery contract."""
import os
import subprocess
import sys
import textwrap

import numpy as np
import pytest

from svsim.catalog_admission import CatalogAdmissionError, CatalogConnection
from svsim.disk_budget import DiskBudget

MPI = pytest.importorskip('mpi4py.MPI')

from svsim.distributed import DistributedConfig, DistributedSimulator
from svsim.kernels import eager_reference


@pytest.fixture
def catalog(tmp_path):
    budget = DiskBudget(tmp_path / 'ledger.sqlite', [tmp_path], quota_bytes=32 * 1024**2)
    db = CatalogConnection(tmp_path / 'catalog.sqlite', budget_getter=lambda: budget)
    db.execute('CREATE TABLE t(v)')
    try: yield db
    finally: db.close(); budget.close()


def test_batch_absorbs_inner_commits_and_runs_hooks_in_order(catalog):
    db, events = catalog, []
    with db.batch():
        db.execute('INSERT INTO t VALUES(1)'); db.commit()
        db.execute('BEGIN IMMEDIATE')  # a nested writer's BEGIN joins the batch
        with db: db.execute('INSERT INTO t VALUES(2)')
        db.on_commit(lambda: events.append('commit'))
        db.on_rollback(lambda: events.append('rollback'))
        db.after_batch(lambda: events.append('after'))
        assert db.in_transaction
    assert events == ['commit', 'after']
    assert db.admission_status()['absorbed_commit_count'] >= 1 and db.admission_status()['batch_count'] == 1
    assert db.execute('SELECT v FROM t ORDER BY v').fetchall() == [(1,), (2,)]


def test_failed_batch_rolls_back_everything_and_restores_process_state(catalog):
    db, events = catalog, []
    with pytest.raises(RuntimeError):
        with db.batch():
            db.execute('INSERT INTO t VALUES(3)')
            db.on_rollback(lambda: events.append('undo'))
            db.after_batch(lambda: db.execute('INSERT INTO t VALUES(99)'))
            raise RuntimeError('gate failed')
    assert events == ['undo']
    assert db.execute('SELECT v FROM t').fetchall() == [(99,)]  # after-hooks still apply in their own transaction
    assert not db.in_transaction


def test_rolled_back_statement_breaks_the_batch_and_full_envelope_splits(catalog):
    db = catalog
    with pytest.raises(CatalogAdmissionError, match='rolled back'):
        with db.batch():
            db.execute('INSERT INTO t VALUES(1)')
            db.rollback()
            db.execute('INSERT INTO t VALUES(2)')
    assert db.execute('SELECT count(*) FROM t').fetchone() == (0,)
    with db.batch():
        for i in range(40):
            db.execute('INSERT INTO t VALUES(?)', (b'x' * 4000,))
    assert db.admission_status()['batch_split_count'] >= 1
    assert db.execute('SELECT count(*) FROM t').fetchone() == (40,)


def _config(path, store):
    return DistributedConfig(str(path), 2, ram_budget_bytes_per_rank=4 * 1024**2, metadata_reserve_bytes=256 * 1024,
                             amplitude_store=store, frame_policy='materialize')


@pytest.mark.parametrize('store', ['file', 'ram'])
def test_gate_failure_rolls_back_catalog_and_cache_so_the_run_can_continue(tmp_path, monkeypatch, store):
    with DistributedSimulator(4, _config(tmp_path / 's', store), comm=MPI.COMM_SELF) as sim:
        sim.gate('H', (0,)); sim.gate('RX', (3,), angle=.37)
        binding = sim.bind()
        original = DistributedSimulator._commit_stage
        calls = []

        def fail_once(self, new, frontier):
            calls.append(frontier)
            if len(calls) == 2: raise OSError('injected stage failure')
            return original(self, new, frontier)

        monkeypatch.setattr(DistributedSimulator, '_commit_stage', fail_once)
        with pytest.raises(OSError, match='injected'):
            sim.materialize(binding=binding)
        monkeypatch.setattr(DistributedSimulator, '_commit_stage', original)
        sim.materialize(binding=binding)
        expected = eager_reference(list(sim.sequencer.operations), 4, sim.assets, binding)
        np.testing.assert_allclose(sim.state(allow_gather=True), expected, atol=1e-12)
        status = sim.store.db.admission_status()
        assert status['batch_count'] >= 2 and status['absorbed_commit_count'] > status['batch_count']
        if store == 'file':
            sim.commit_snapshot()  # pending extent addresses were applied after their batches


CHILD = textwrap.dedent('''
    import json, os, sys
    import numpy as np
    from mpi4py import MPI
    from svsim.distributed import DistributedConfig, DistributedSimulator
    root, store = sys.argv[1], sys.argv[2]
    config = DistributedConfig(root, 2, ram_budget_bytes_per_rank=4 * 1024**2, metadata_reserve_bytes=256 * 1024,
                               amplitude_store=store, frame_policy='materialize')
    sim = DistributedSimulator(4, config, comm=MPI.COMM_SELF)
    sim.gate('H', (0,)); sim.materialize(binding=sim.bind())
    snapshot = sim.commit_snapshot()
    np.save(os.path.join(root, '..', 'expected.npy'), sim.state(allow_gather=True))
    for q in (1, 2, 3): sim.gate('RX', (q,), angle=.37)
    original = DistributedSimulator._commit_stage
    calls = []
    def crash(self, new, frontier):
        calls.append(frontier)
        if len(calls) == int(sys.argv[3]): os._exit(0)  # owner vanishes inside an open gate batch
        return original(self, new, frontier)
    DistributedSimulator._commit_stage = crash
    sim.materialize(binding=sim.bind())
''')


@pytest.mark.parametrize('crash_at', [1, 3])
def test_process_killed_inside_a_gate_batch_resumes_from_the_last_snapshot(tmp_path, crash_at):
    root = tmp_path / 'state'
    subprocess.run([sys.executable, '-c', CHILD, str(root), 'file', str(crash_at)], check=True, timeout=120,
                   env={**os.environ, 'OMP_NUM_THREADS': '1'})
    expected = np.load(tmp_path / 'expected.npy')
    config = _config(root, 'file')
    with DistributedSimulator.resume(config, comm=MPI.COMM_SELF) as sim:
        np.testing.assert_allclose(sim.state(allow_gather=True), expected, atol=1e-12)
        assert sim.frontier == 0
        sim.commit_snapshot()  # the catalog left by the killed batch accepts a new durable cut


@pytest.mark.parametrize('outcome', ['commit', 'rollback'])
def test_ram_resident_marker_follows_its_batch(tmp_path, outcome):
    from svsim.block_store import MemoryBlockStore
    from svsim.cache import MemoryBudget
    from svsim.storage import StorageConfig
    config = StorageConfig(str(tmp_path / 'ram'), 2)
    store = MemoryBlockStore(config, 4, MemoryBudget(1 << 20))
    version = store.new_version()
    try:
        with store.db.batch():
            store.write_version(version, bytes(store.slot_bytes))
            assert version in store.resident
            if outcome == 'rollback': raise RuntimeError('gate failed')
    except RuntimeError:
        pass
    assert (version in store.resident) == (outcome == 'commit')
    store.close()
    if outcome == 'commit':  # a committed volatile payload keeps resume fail-closed
        with pytest.raises(OSError, match='cannot be resumed'):
            MemoryBlockStore(config, 4, MemoryBudget(1 << 20), create=False)
    else:
        MemoryBlockStore(config, 4, MemoryBudget(1 << 20), create=False).close()
