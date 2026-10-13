"""Exercise the loadable VFS against this process's builtin SQLite library."""
import json
from pathlib import Path
import sqlite3
import subprocess
import sys

import pytest
from svsim.catalog_admission import get_sqlite_guard


@pytest.fixture(scope='module')
def guarded_vfs():
    try:
        guard = get_sqlite_guard()
    except (NotImplementedError, OSError, sqlite3.Error, subprocess.SubprocessError) as exc:
        pytest.skip(f'Production SQLite guard unavailable: {exc}')
    assert guard.library.is_file()
    return guard


def _guarded(guard, path):
    return guard.connect(Path(path).resolve())


def _configure(db, main_cap=1024 * 1024, side_cap=1024 * 1024, active=1):
    db.execute('SELECT svsim_disk_guard_config(?,?,?)', (main_cap, side_cap, active)).fetchone()


def test_unconfigured_guard_denies_mutations_and_reports_sqlite_build(guarded_vfs, tmp_path):
    db = _guarded(guarded_vfs, tmp_path / 'catalog.sqlite')
    with pytest.raises(sqlite3.DatabaseError):
        db.execute('CREATE TABLE x(value)')
    status = json.loads(db.execute('SELECT svsim_disk_guard_status()').fetchone()[0])
    assert status['available'] is True
    assert status['active'] is False
    assert status['sqlite_version'] == sqlite3.sqlite_version
    assert status['denied_writes'] >= 1
    db.close()


def test_guarded_rollback_transaction_and_side_file_cap(guarded_vfs, tmp_path):
    db = _guarded(guarded_vfs, tmp_path / 'catalog.sqlite')
    _configure(db)
    db.execute('CREATE TABLE item(value BLOB)')
    db.commit()
    side_before = json.loads(db.execute('SELECT svsim_disk_guard_status()').fetchone()[0])['side_growth_bytes']
    _configure(db, side_cap=side_before + 8)
    with pytest.raises(sqlite3.DatabaseError):
        db.execute('INSERT INTO item VALUES(?)', (b'x' * 2000,))
    db.rollback()
    assert db.execute('SELECT count(*) FROM item').fetchone()[0] == 0
    status = json.loads(db.execute('SELECT svsim_disk_guard_status()').fetchone()[0])
    assert status['side_growth_cap_bytes'] == side_before + 8
    assert status['denied_writes'] >= 1
    db.close()


def test_inactive_guard_blocks_existing_catalog_writes(guarded_vfs, tmp_path):
    path = tmp_path / 'catalog.sqlite'
    db = _guarded(guarded_vfs, path)
    _configure(db)
    db.execute('CREATE TABLE item(value)')
    db.commit()
    _configure(db, active=0)
    with pytest.raises(sqlite3.DatabaseError):
        db.execute('INSERT INTO item VALUES(1)')
    db.rollback()
    assert db.execute('SELECT count(*) FROM item').fetchone()[0] == 0
    db.close()


def test_main_database_cap_stops_page_growth(guarded_vfs, tmp_path):
    path = tmp_path / 'catalog.sqlite'
    db = _guarded(guarded_vfs, path)
    _configure(db)
    db.execute('CREATE TABLE item(value BLOB)')
    db.commit()
    current_size = path.stat().st_size
    _configure(db, main_cap=current_size, side_cap=1024 * 1024)
    try:
        db.execute('INSERT INTO item VALUES(?)', (b'x' * 20000,))
        db.commit()
    except sqlite3.DatabaseError:
        db.rollback()
    else:
        pytest.fail('main database growth exceeded its configured cap')
    db.rollback()
    assert db.execute('SELECT count(*) FROM item').fetchone()[0] == 0
    db.close()


def test_wal_mode_is_rejected(guarded_vfs, tmp_path):
    db = _guarded(guarded_vfs, tmp_path / 'catalog.sqlite')
    _configure(db)
    with pytest.raises(sqlite3.DatabaseError, match='not authorized'):
        db.execute('PRAGMA journal_mode=WAL').fetchone()
    assert not (tmp_path / 'catalog.sqlite-wal').exists()
    db.close()


def test_connections_to_same_catalog_share_guard_after_loader_close(guarded_vfs, tmp_path):
    path = tmp_path / 'catalog.sqlite'
    first = _guarded(guarded_vfs, path)
    second = _guarded(guarded_vfs, path)
    _configure(first)
    first.execute('CREATE TABLE item(value)')
    first.commit()
    # Guard state belongs to the volume/path account, not one connection.
    _configure(second, active=0)
    with pytest.raises(sqlite3.DatabaseError):
        first.execute('INSERT INTO item VALUES(1)')
    first.rollback()
    assert first.execute('SELECT count(*) FROM item').fetchone()[0] == 0
    second.close()
    first.close()


def test_failed_open_releases_prepared_account(guarded_vfs, tmp_path):
    missing = tmp_path / 'missing-parent' / 'catalog.sqlite'
    with pytest.raises(sqlite3.OperationalError):
        guarded_vfs.connect(missing, main_cap_bytes=1024 * 1024,
                            side_cap_bytes=1024 * 1024)
    path_text = str(missing.resolve())
    guarded_vfs.loader.execute('SELECT svsim_disk_guard_prepare(?,?,?)',
                               (path_text, 1024 * 1024, 1024 * 1024)).fetchone()
    guarded_vfs.loader.execute('SELECT svsim_disk_guard_unprepare(?)', (path_text,)).fetchone()


def test_unprepare_keeps_open_guard_and_hot_journal_recovery_is_admitted(guarded_vfs, tmp_path):
    path = tmp_path / 'catalog.sqlite'
    db = guarded_vfs.connect(path, main_cap_bytes=4 * 1024 * 1024,
                             side_cap_bytes=4 * 1024 * 1024)
    assert json.loads(db.execute('SELECT svsim_disk_guard_status()').fetchone()[0])['active']
    db.execute('CREATE TABLE item(value TEXT)')
    db.execute("INSERT INTO item VALUES('stable')")
    db.commit()
    db.close()

    worker = r'''
import os, sys
from svsim.catalog_admission import get_sqlite_guard
db=get_sqlite_guard().connect(sys.argv[1], main_cap_bytes=4*1024*1024,
                              side_cap_bytes=4*1024*1024)
db.execute("UPDATE item SET value='uncommitted'")
os._exit(0)
'''
    env = dict(__import__('os').environ)
    root = Path(__file__).resolve().parents[1]
    env['PYTHONPATH'] = str(root / 'src') + (':' + env['PYTHONPATH'] if env.get('PYTHONPATH') else '')
    crashed = subprocess.run([sys.executable, '-c', worker, str(path)], env=env,
                             cwd=root, timeout=30, capture_output=True, text=True)
    assert crashed.returncode == 0, crashed.stdout + crashed.stderr
    assert Path(str(path) + '-journal').exists()

    recovered = guarded_vfs.connect(path, main_cap_bytes=4 * 1024 * 1024,
                                    side_cap_bytes=4 * 1024 * 1024)
    assert recovered.execute('SELECT value FROM item').fetchone()[0] == 'stable'
    journal = Path(str(path) + '-journal')
    assert journal.exists()
    assert journal.read_bytes()[:8] != bytes.fromhex('d9d505f920a163d7')
    status = json.loads(recovered.execute('SELECT svsim_disk_guard_status()').fetchone()[0])
    assert status['active'] is True
    assert status['side_growth_bytes'] >= journal.stat().st_size
    _configure(recovered, active=0)
    recovered.close()
