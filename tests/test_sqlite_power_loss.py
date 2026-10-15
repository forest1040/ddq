"""Lose unsynced SQLite bytes and unsynced journal deletion, rather than just SIGKILL.

The native test-only VFS saves bounded durable images at xSync and at a
directory-synced xDelete. A parent restores those images after the child dies.
The control images are the model oracle, not files subject to the model.
"""
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import textwrap

import pytest

ROOT = Path(__file__).resolve().parents[1]
ENV = {**os.environ, 'SVSIM_VFS_TEST_FAULTS': '1', 'OMP_NUM_THREADS': '1'}

CHILD = '''
    import os,sys
    from pathlib import Path
    from svsim.catalog_admission import CatalogConnection
    from svsim.disk_budget import DiskBudget
    from svsim.volume_coordinator import VolumeCoordinator
    base,subject,kind,nth,action,mode=Path(sys.argv[1]),sys.argv[2],sys.argv[3],int(sys.argv[4]),sys.argv[5],sys.argv[6]
    root=base/'data';root.mkdir();images=base/'images';images.mkdir()
    coord=VolumeCoordinator(root/'coordinator.sqlite')
    budget=DiskBudget(root/'ledger.sqlite',[root],volume_coordinator=coord)
    db=CatalogConnection(root/'catalog.sqlite',budget_getter=lambda:budget,require_native=True)
    db.execute('CREATE TABLE items(value INTEGER)')
    with db:db.execute('INSERT INTO items VALUES(1)')
    if subject=='decrease':token=budget.reserve(10000,operation='test-power',writing=True)
    if mode=='FULL':
        db._conn.execute('PRAGMA synchronous=FULL')
    assert db._conn.execute('SELECT svsim_test_power_arm(?,?)',(str(root),str(images))).fetchone()[0]>=6
    target={'catalog':'catalog.sqlite','increase-coordinator':'coordinator.sqlite','increase-ledger':'ledger.sqlite',
            'decrease-coordinator':'coordinator.sqlite','decrease-ledger':'ledger.sqlite'}.get(subject)
    if subject=='decrease':target=sys.argv[7]+'.sqlite'
    if kind!='none':assert db._conn.execute('SELECT svsim_test_fault_arm(?,?,?,?)',(target,kind,nth,action)).fetchone()[0]==1
    if subject=='catalog':
        with db:db.execute('INSERT INTO items VALUES(2)')
    elif subject.startswith('increase'):budget.reserve(10000,operation='test-power',writing=True)
    elif subject=='decrease':budget.release(token)
    print('ACK',flush=True)
    os._exit(0)
'''


def run_child(base, subject, kind='none', nth=1, action='kill-after', mode='EXTRA', target='ledger'):
    result = subprocess.run([sys.executable, '-c', textwrap.dedent(CHILD), str(base), subject,
                             kind, str(nth), action, mode, target], cwd=ROOT, env=ENV,
                            capture_output=True, text=True, timeout=120)
    assert result.returncode == 0, result.stdout + result.stderr
    return result


def restore_durable_images(base):
    images = base / 'images'
    rows = (images / 'manifest.tsv').read_text().splitlines()
    assert len(rows) >= 6
    for row in rows:
        index, present, name = row.split('\t')
        path = Path(name)
        assert path.is_relative_to(base / 'data')
        if present == '1':
            # In-place restoration preserves the file's incarnation; its
            # contents alone lose volatile writes, as during a power cut.
            path.write_bytes((images / f'{index}.bin').read_bytes())
        else:
            path.unlink(missing_ok=True)


def values(base):
    with sqlite3.connect(base / 'data/catalog.sqlite') as db:
        assert db.execute('PRAGMA integrity_check').fetchone() == ('ok',)
        return [v for v, in db.execute('SELECT value FROM items ORDER BY value')]


@pytest.mark.parametrize('kind,nth,action,expected', [
    ('journal-sync', 1, 'kill-before', [1]), ('journal-sync', 1, 'kill-after', [1]),
    ('journal-sync', 2, 'kill-before', [1]), ('journal-sync', 2, 'kill-after', [1]),
    ('main-sync', 1, 'kill-before', [1]), ('main-sync', 1, 'kill-after', [1]),
    ('journal-delete', 1, 'kill-before', [1]), ('journal-delete', 1, 'kill-after', [1, 2]),
])
def test_catalog_power_cut_at_native_durability_boundaries(tmp_path, kind, nth, action, expected):
    run_child(tmp_path, 'catalog', kind, nth, action)
    restore_durable_images(tmp_path)
    assert values(tmp_path) == expected


@pytest.mark.parametrize('mode,expected', [('FULL', [1]), ('EXTRA', [1, 2])])
def test_model_detects_unsynced_journal_deletion_after_ack(tmp_path, mode, expected):
    result = run_child(tmp_path, 'catalog', mode=mode)
    assert 'ACK' in result.stdout
    restore_durable_images(tmp_path)
    assert values(tmp_path) == expected


@pytest.mark.parametrize('subject,target', [('increase-coordinator', 'coordinator'), ('increase-ledger', 'ledger'),
                                           ('decrease', 'ledger'), ('decrease', 'coordinator')])
@pytest.mark.parametrize('action', ['kill-before', 'kill-after'])
def test_power_cut_never_loses_a_coordinator_claim_below_live_ledger(tmp_path, subject, target, action):
    run_child(tmp_path, subject, 'journal-delete', action=action, target=target)
    restore_durable_images(tmp_path)
    root = tmp_path / 'data'
    with sqlite3.connect(root / 'ledger.sqlite') as db:
        assert db.execute('PRAGMA integrity_check').fetchone() == ('ok',)
        pending = db.execute("SELECT COALESCE(SUM(bytes-materialized),0) FROM reservations "
                             "WHERE state IN ('RESERVED','WRITING')").fetchone()[0]
    with sqlite3.connect(root / 'coordinator.sqlite') as db:
        assert db.execute('PRAGMA integrity_check').fetchone() == ('ok',)
        published = db.execute('SELECT COALESCE(SUM(pending),0) FROM ledgers').fetchone()[0]
    assert published >= pending
    # Reopen is the authority for reducing a conservative leftover claim.
    from svsim.disk_budget import DiskBudget
    from svsim.volume_coordinator import VolumeCoordinator
    coord = VolumeCoordinator(root / 'coordinator.sqlite')
    budget = DiskBudget(root / 'ledger.sqlite', [root], volume_coordinator=coord)
    try:
        assert coord.claim(root / 'ledger.sqlite')['pending'] == budget.reserved_bytes
    finally:
        budget.close()
        coord.close()


def test_production_extension_excludes_power_model(tmp_path):
    env = {k: v for k, v in os.environ.items() if k != 'SVSIM_VFS_TEST_FAULTS'}
    script = '''
        import sys
        from svsim.catalog_admission import SQLiteGuard
        guard=SQLiteGuard()
        for name,args in [('svsim_test_power_arm',"'x','/tmp'"),('svsim_test_power_disarm','')]:
            try:guard.loader.execute('SELECT '+name+'('+args+')')
            except Exception as exc:assert 'no such function' in str(exc)
            else:raise AssertionError(name+' unexpectedly exposed')
    '''
    result = subprocess.run([sys.executable, '-c', textwrap.dedent(script)], cwd=ROOT, env=env,
                            capture_output=True, text=True, timeout=60)
    assert result.returncode == 0, result.stdout + result.stderr
