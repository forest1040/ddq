"""Real-process kill matrix: named writer stages, hot journal, partial tail, token reconciliation."""
import json
import os
import signal
import sqlite3
import subprocess
import sys
import textwrap
import time
from pathlib import Path

import pytest

from svsim.catalog_admission import CatalogConnection
from svsim.disk_budget import DiskAdmissionError, DiskBudget
from svsim.journal import Journal

ROOT = Path(__file__).resolve().parents[1]
ENV = {**os.environ, 'OMP_NUM_THREADS': '1'}


def _run(script, *args, timeout=120):
    return subprocess.run([sys.executable, '-c', textwrap.dedent(script), *map(str, args)], cwd=ROOT, env=ENV,
                          capture_output=True, text=True, timeout=timeout)


def _reconcile_all(ledger, root):
    """Explicit recovery as a caller would do it: settle each live token exactly once, never twice."""
    budget = DiskBudget(ledger, [root])
    try:
        live = [r for r in budget.db.execute("SELECT id,materialized FROM reservations WHERE state IN ('RESERVED','WRITING')").fetchall()]
        for key, materialized in live:
            budget.reconcile(key, materialized_bytes=materialized, evidence='test-recovery;owner-exited')
            with pytest.raises(DiskAdmissionError, match='already terminal'):
                budget.reconcile(key, materialized_bytes=materialized, evidence='duplicate')
        assert budget.reserved_bytes == 0
        return len(live)
    finally: budget.close()


CATALOG_CHILD = '''
    import os, sys
    from svsim.catalog_admission import CatalogConnection
    from svsim.disk_budget import DiskBudget
    root, stage, nth = sys.argv[1], sys.argv[2], int(sys.argv[3])
    budget = DiskBudget(os.path.join(root, 'ledger.sqlite'), [root])
    db = CatalogConnection(os.path.join(root, 'catalog.sqlite'), budget_getter=lambda: budget, require_native=True)
    db.execute('CREATE TABLE items(value BLOB)')
    with db: db.execute('INSERT INTO items VALUES(?)', (b'old',))
    count = [0]
    def arm(cls, name, when):
        original = getattr(cls, name)
        def wrapper(self, *a, **k):
            if when == 'before':
                count[0] += 1
                if count[0] == nth: os._exit(0)
            result = original(self, *a, **k)
            if when == 'after':
                count[0] += 1
                if count[0] == nth: os._exit(0)
            return result
        setattr(cls, name, wrapper)
    if stage == 'before-reserve': arm(CatalogConnection, '_reserve', 'before')
    elif stage == 'after-reserve': arm(CatalogConnection, '_reserve', 'after')
    elif stage == 'after-sql': arm(CatalogConnection, 'execute', 'after')
    elif stage == 'before-commit': arm(CatalogConnection, 'commit', 'before')
    elif stage == 'after-commit': arm(CatalogConnection, 'commit', 'after')
    with db.batch():
        for i in range(3): db.execute('INSERT INTO items VALUES(?)', (bytes([i + 1]) * 5000,))
    print('completed', flush=True)
'''

NEW_ROWS = [(bytes([i + 1]) * 5000,) for i in range(3)]


@pytest.mark.parametrize('stage,nth,outcome', [
    ('before-reserve', 1, 'old'), ('after-reserve', 1, 'old'),   # no BEGIN yet
    ('after-sql', 1, 'old'), ('after-sql', 3, 'old'),            # dirty pages / hot journal, no commit
    ('before-commit', 1, 'old'),                                 # killed at COMMIT's door
    ('after-commit', 1, 'new'),                                  # durable, hooks and release skipped
])
def test_catalog_batch_kill_stage_leaves_old_or_new_cut_and_recoverable_tokens(tmp_path, stage, nth, outcome):
    child = _run(CATALOG_CHILD, tmp_path, stage, nth)
    assert child.returncode == 0 and 'completed' not in child.stdout, child.stderr
    # Reopening replays any hot rollback journal; the cut is exactly the old or the new one.
    budget = DiskBudget(tmp_path / 'ledger.sqlite', [tmp_path])
    try:
        db = CatalogConnection(tmp_path / 'catalog.sqlite', budget_getter=lambda: budget, require_native=True)
        rows = db.execute('SELECT value FROM items').fetchall()
        assert rows == ([(b'old',)] if outcome == 'old' else [(b'old',)] + NEW_ROWS)
        assert db.execute('PRAGMA integrity_check').fetchone() == ('ok',)
        # A journal killed before its sync has no valid header: SQLite leaves that non-hot file in
        # place, and the next durable transaction must consume it.
        with db: db.execute('INSERT INTO items VALUES(?)', (b'after-recovery',))  # the catalog accepts new durable work
        assert not Path(str(db.path) + '-journal').exists()
        db.close()
    finally: budget.close()
    # The killed owner's catalog token is still charged until explicit, single reconciliation.
    assert _reconcile_all(tmp_path / 'ledger.sqlite', tmp_path) >= 1 or stage == 'before-reserve'


JOURNAL_CHILD = '''
    import os, sys
    from svsim.disk_budget import DiskBudget
    from svsim.journal import Journal
    root, stage = sys.argv[1], sys.argv[2]
    budget = DiskBudget(os.path.join(root, 'ledger.sqlite'), [root])
    journal = Journal(os.path.join(root, 'journal'), disk_budget_getter=lambda: budget)
    cursor = journal.append('gates', {'value': 'committed'})
    print(cursor, flush=True)
    if stage == 'after-admit':  # token covers the record, nothing written
        journal._append.admit(500)
    elif stage == 'partial-record':  # half a record reaches the file
        journal._append.admit(300)
        os.write(journal.streams['gates'].fileno(), b'{"schema_version":1,"payload":{"va')
    elif stage == 'whole-record':  # written but never acknowledged
        journal.append('gates', {'value': 'unacked'})
    os._exit(0)
'''


@pytest.mark.parametrize('stage', ['after-admit', 'partial-record', 'whole-record'])
def test_ledger_lease_append_kill_keeps_committed_prefix_and_live_token(tmp_path, stage):
    child = _run(JOURNAL_CHILD, tmp_path, stage)
    assert child.returncode == 0, child.stderr
    cursor = int(child.stdout.split()[0])
    budget = DiskBudget(tmp_path / 'ledger.sqlite', [tmp_path])
    try:
        assert budget.reserved_bytes > 0 and budget.snapshot()['states']['WRITING']['count'] == 1  # still charged
        journal = Journal(tmp_path / 'journal', disk_budget_getter=lambda: budget)
        journal.protected_cursor = lambda kind: cursor if kind == 'gates' else 0
        assert list(journal.replay('gates', cursor)) == [{'value': 'committed'}]  # committed cut intact
        # A new writer truncates an incomplete, uncommitted tail but never the committed prefix.
        journal.append('gates', {'value': 'next'})
        journal.close()
        assert [p['value'] for p in Journal(tmp_path / 'journal').replay('gates')][:1] == ['committed']
        if stage != 'whole-record': assert [p['value'] for p in Journal(tmp_path / 'journal').replay('gates')] == ['committed', 'next']
    finally: budget.close()
    assert _reconcile_all(tmp_path / 'ledger.sqlite', tmp_path) >= 1


SIDECAR_CHILD = '''
    import os, sys
    from svsim.disk_budget import DiskBudget
    from svsim.distributed_checkpoint import atomic_json
    root, stage = sys.argv[1], sys.argv[2]
    os.makedirs(os.path.join(root, 'd'), exist_ok=True)
    budget = DiskBudget(os.path.join(root, 'ledger.sqlite'), [os.path.join(root, 'd')])
    target = os.path.join(root, 'd', 'ckpt.json')
    atomic_json(target, {'v': 1}, disk_budget=budget)
    fsync, replace = os.fsync, os.replace
    calls = {'fsync': 0}
    def hook_fsync(fd):
        calls['fsync'] += 1
        if stage == 'before-file-fsync' and calls['fsync'] == 1: os._exit(0)
        fsync(fd)
        if stage == 'after-file-fsync' and calls['fsync'] == 1: os._exit(0)
        if stage == 'after-dir-fsync' and calls['fsync'] == 2: os._exit(0)
    def hook_replace(a, b):
        if stage == 'before-rename': os._exit(0)
        replace(a, b)
        if stage == 'after-rename': os._exit(0)
    os.fsync, os.replace = hook_fsync, hook_replace
    atomic_json(target, {'v': 2}, disk_budget=budget)
    print('completed', flush=True)
'''


@pytest.mark.parametrize('stage,outcome', [
    ('before-file-fsync', 1), ('after-file-fsync', 1), ('before-rename', 1),
    ('after-rename', 2), ('after-dir-fsync', 2),
])
def test_sidecar_kill_stage_keeps_target_whole_and_token_until_reconcile(tmp_path, stage, outcome):
    child = _run(SIDECAR_CHILD, tmp_path, stage)
    assert child.returncode == 0 and 'completed' not in child.stdout, child.stderr
    target = tmp_path / 'd' / 'ckpt.json'
    assert json.loads(target.read_text())['v'] == outcome  # whole old or whole new, never torn
    leftovers = list((tmp_path / 'd').glob('.*.tmp'))
    assert len(leftovers) == (0 if stage in ('after-rename', 'after-dir-fsync') else 1)  # partial temporary is kept as evidence
    budget = DiskBudget(tmp_path / 'ledger.sqlite', [tmp_path / 'd'])
    try:  # the killed writer's token is retained (possibly fully materialized), not released
        assert budget.snapshot()['states']['WRITING']['count'] >= 1
    finally: budget.close()
    assert _reconcile_all(tmp_path / 'ledger.sqlite', tmp_path / 'd') >= 1


STRESS_CHILD = '''
    import os, sys
    from svsim.catalog_admission import CatalogConnection
    from svsim.disk_budget import DiskBudget
    root = sys.argv[1]
    budget = DiskBudget(os.path.join(root, 'ledger.sqlite'), [root])
    db = CatalogConnection(os.path.join(root, 'catalog.sqlite'), budget_getter=lambda: budget, require_native=True)
    db.execute('CREATE TABLE items(i INTEGER, value BLOB)')
    print('ready', flush=True)
    i = 0
    while True:
        with db: db.execute('INSERT INTO items VALUES(?,?)', (i, bytes([i % 251]) * 3000))
        print(i, flush=True)  # acknowledged only after the durable commit returned
        i += 1
'''


@pytest.mark.parametrize('delay', [0.25, 0.4, 0.55, 0.7, 0.9, 1.1])
def test_sigkill_during_continuous_catalog_commits_never_loses_an_acknowledged_row(tmp_path, delay):
    proc = subprocess.Popen([sys.executable, '-c', textwrap.dedent(STRESS_CHILD), str(tmp_path)], cwd=ROOT, env=ENV,
                            stdout=subprocess.PIPE, text=True)
    try:
        assert proc.stdout.readline().strip() == 'ready'
        time.sleep(delay)
        proc.send_signal(signal.SIGKILL)
        acked = [int(x) for x in proc.stdout.read().split()]
    finally: proc.kill(); proc.wait()
    budget = DiskBudget(tmp_path / 'ledger.sqlite', [tmp_path])
    try:
        db = CatalogConnection(tmp_path / 'catalog.sqlite', budget_getter=lambda: budget, require_native=True)
        rows = db.execute('SELECT i FROM items ORDER BY i').fetchall()
        assert db.execute('PRAGMA integrity_check').fetchone() == ('ok',)
        got = [r[0] for r in rows]
        assert got == list(range(len(got)))  # contiguous prefix, no torn or reordered commit
        assert len(got) >= len(acked) and len(got) <= len(acked) + 1  # at most the one in-flight commit
        db.close()
    finally: budget.close()
    assert _reconcile_all(tmp_path / 'ledger.sqlite', tmp_path) >= 1


GATE_CHILD = '''
    import os, sys
    import numpy as np
    from mpi4py import MPI
    from svsim.distributed import DistributedConfig, DistributedSimulator
    from svsim.catalog_admission import CatalogConnection
    root, stage = sys.argv[1], sys.argv[2]
    config = DistributedConfig(root, 2, ram_budget_bytes_per_rank=4 * 1024**2, metadata_reserve_bytes=256 * 1024,
                               amplitude_store='file', frame_policy='materialize')
    sim = DistributedSimulator(4, config, comm=MPI.COMM_SELF)
    sim.gate('H', (0,)); sim.materialize(binding=sim.bind()); sim.commit_snapshot()
    np.save(os.path.join(root, '..', 'expected.npy'), sim.state(allow_gather=True))
    for q in (1, 2, 3): sim.gate('RX', (q,), angle=.37)
    if stage == 'after-commit':
        original = CatalogConnection.commit
        def commit(self, *a, **k):
            was_batch = self._batch is not None
            original(self, *a, **k)
            if not was_batch: os._exit(0)
        CatalogConnection.commit = commit
    sim.materialize(binding=sim.bind())
'''


@pytest.mark.parametrize('stage', ['after-commit'])
def test_gate_batch_killed_after_its_durable_commit_still_resumes_from_the_snapshot(tmp_path, stage):
    np = pytest.importorskip('numpy')
    pytest.importorskip('mpi4py.MPI')
    from mpi4py import MPI
    from svsim.distributed import DistributedConfig, DistributedSimulator
    root = tmp_path / 'state'
    child = _run(GATE_CHILD, root, stage)
    assert child.returncode == 0, child.stderr
    expected = np.load(tmp_path / 'expected.npy')
    ledger = root / 'metadata' / 'disk-ledger.sqlite3'
    config = DistributedConfig(str(root), 2, ram_budget_bytes_per_rank=4 * 1024**2, metadata_reserve_bytes=256 * 1024,
                               amplitude_store='file', frame_policy='materialize')
    # Recovery order: owner is gone; settle its tokens, then resume from the last committed snapshot.
    _reconcile_all(ledger, root)
    with DistributedSimulator.resume(config, comm=MPI.COMM_SELF) as sim:
        np.testing.assert_allclose(sim.state(allow_gather=True), expected, atol=1e-12)
        sim.commit_snapshot()
