"""Deterministic fault points inside SQLite's own xSync/xDelete of a catalog batch (P4-009).

The native budget VFS can be built with ``-DSVSIM_TEST_FAULTS`` (the ``SVSIM_VFS_TEST_FAULTS=1`` environment
variable adds the flag in ``SQLiteGuard``). Only such a binary has the ``svsim_test_fault_arm`` SQL function;
a normal build contains no fault code. A child process arms the Nth journal-sync / main-db-sync / journal-delete
(the commit point) of one catalog batch and either dies there (``kill-before`` / ``kill-after``) or gets an I/O
error (``fail``). The reopened catalog must be exactly the old or the new cut, pass the integrity check and
accept new durable work.
"""
import json
import os
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

from svsim.catalog_admission import CatalogConnection
from svsim.disk_budget import DiskBudget

ROOT = Path(__file__).resolve().parents[1]
ENV = {**os.environ, 'OMP_NUM_THREADS': '1', 'SVSIM_VFS_TEST_FAULTS': '1'}

CHILD = '''
    import json, os, sys
    from svsim.catalog_admission import CatalogConnection
    from svsim.disk_budget import DiskBudget
    root, kind, nth, action = sys.argv[1], sys.argv[2], int(sys.argv[3]), sys.argv[4]
    budget = DiskBudget(os.path.join(root, 'ledger.sqlite'), [root])
    try:
        db = CatalogConnection(os.path.join(root, 'catalog.sqlite'), budget_getter=lambda: budget, require_native=True)
        db._conn.execute('SELECT svsim_test_fault_disarm()')
    except Exception as exc:
        print('UNAVAILABLE', repr(exc)); sys.exit(3)
    db.execute('CREATE TABLE items(value BLOB)')
    with db: db.execute('INSERT INTO items VALUES(?)', (b'old',))
    assert db._conn.execute('SELECT svsim_test_fault_arm(?,?,?,?)', ('catalog.sqlite', kind, nth, action)).fetchone()[0] == 1
    error = None
    try:
        with db.batch():
            for i in range(3): db.execute('INSERT INTO items VALUES(?)', (bytes([i + 1]) * 5000,))
    except Exception as exc:
        error = repr(exc)
    db._conn.execute('SELECT svsim_test_fault_disarm()')
    print(json.dumps({'error': error}), flush=True)
    # After a failed journal delete the hot journal is still there: an unadmitted read cannot replay it
    # (the guarded VFS refuses the delete), so the first access is an admitted write, as in recovery.
    with db: db.execute('INSERT INTO items VALUES(?)', (b'after-fault',))
    print(json.dumps({'after': [len(bytes(r[0])) for r in db._conn.execute('SELECT value FROM items')]}), flush=True)
    print('completed', flush=True)
'''


def _run(root, kind, nth, action):
    return subprocess.run([sys.executable, '-c', textwrap.dedent(CHILD), str(root), kind, str(nth), action],
                          cwd=ROOT, env=ENV, capture_output=True, text=True, timeout=120)


@pytest.fixture(scope='module')
def vfs_faults(tmp_path_factory):
    probe = _run(tmp_path_factory.mktemp('probe'), 'main-sync', 99, 'fail')
    if probe.returncode == 3 or 'UNAVAILABLE' in probe.stdout:
        pytest.skip('native budget VFS unavailable on this platform: ' + probe.stdout.strip()[-300:])
    return True


OLD = [b'old']
NEW = [b'old'] + [bytes([i + 1]) * 5000 for i in range(3)]

# kind, nth, action -> committed cut after a process death at that point. A rollback-journal transaction is
# committed by the journal delete: every earlier point (even after the main db was synced) recovers the old cut.
KILLS = [
    ('journal-sync', 1, 'kill-before', 'old'), ('journal-sync', 1, 'kill-after', 'old'),
    ('journal-sync', 2, 'kill-before', 'old'), ('journal-sync', 2, 'kill-after', 'old'),
    ('main-sync', 1, 'kill-before', 'old'), ('main-sync', 1, 'kill-after', 'old'),
    ('journal-delete', 1, 'kill-before', 'old'), ('journal-delete', 1, 'kill-after', 'new'),
]


@pytest.mark.parametrize('kind,nth,action,outcome', KILLS)
def test_kill_at_sqlite_sync_point_leaves_old_or_new_cut(tmp_path, vfs_faults, kind, nth, action, outcome):
    child = _run(tmp_path, kind, nth, action)
    assert child.returncode == 0 and 'completed' not in child.stdout, (child.stdout, child.stderr)
    budget = DiskBudget(tmp_path / 'ledger.sqlite', [tmp_path])
    try:
        db = CatalogConnection(tmp_path / 'catalog.sqlite', budget_getter=lambda: budget, require_native=True)
        assert [bytes(r[0]) for r in db.execute('SELECT value FROM items')] == (OLD if outcome == 'old' else NEW)
        assert db.execute('PRAGMA integrity_check').fetchone() == ('ok',)
        with db: db.execute('INSERT INTO items VALUES(?)', (b'after-recovery',))
        assert not Path(str(db.path) + '-journal').exists()
        db.close()
    finally: budget.close()


FAILS = [('journal-sync', 1), ('journal-sync', 2), ('main-sync', 1), ('journal-delete', 1)]


@pytest.mark.parametrize('kind,nth', FAILS)
def test_io_error_at_sqlite_sync_point_surfaces_and_keeps_a_whole_cut(tmp_path, vfs_faults, kind, nth):
    child = _run(tmp_path, kind, nth, 'fail')
    assert child.returncode == 0 and 'completed' in child.stdout, (child.stdout, child.stderr)
    first = json.loads(child.stdout.splitlines()[0])
    assert 'OperationalError' in first['error']  # the failed sync is never reported as a successful commit
    # the old cut plus the one admitted insert made after the failure (never a partial new cut)
    assert json.loads(child.stdout.splitlines()[1]) == {'after': [3, 11]}
    budget = DiskBudget(tmp_path / 'ledger.sqlite', [tmp_path])
    try:
        db = CatalogConnection(tmp_path / 'catalog.sqlite', budget_getter=lambda: budget, require_native=True)
        rows = [len(bytes(r[0])) for r in db.execute('SELECT value FROM items')]
        assert rows == [3, 11]
        assert db.execute('PRAGMA integrity_check').fetchone() == ('ok',)
        db.close()
    finally: budget.close()


def test_production_build_has_no_fault_functions(tmp_path):
    """Without the build flag the VFS exposes no fault arming function."""
    env = {k: v for k, v in os.environ.items() if k != 'SVSIM_VFS_TEST_FAULTS'}
    script = '''
        import os, sys
        from svsim.catalog_admission import CatalogConnection
        from svsim.disk_budget import DiskBudget
        root = sys.argv[1]
        budget = DiskBudget(os.path.join(root, 'ledger.sqlite'), [root])
        db = CatalogConnection(os.path.join(root, 'catalog.sqlite'), budget_getter=lambda: budget, require_native=True)
        try: db._conn.execute("SELECT svsim_test_fault_arm('x','main-sync',1,'fail')")
        except Exception as exc: print('NOFUNC', exc)
    '''
    out = subprocess.run([sys.executable, '-c', textwrap.dedent(script), str(tmp_path)], cwd=ROOT, env=env,
                         capture_output=True, text=True, timeout=120)
    if 'NOFUNC' not in out.stdout and 'Traceback' in out.stderr:
        pytest.skip('native budget VFS unavailable on this platform')
    assert 'NOFUNC' in out.stdout and 'no such function' in out.stdout
