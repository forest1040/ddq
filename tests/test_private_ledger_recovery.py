"""Offline recovery for a store-private ledger (``<store>/metadata/disk-ledger.sqlite3``) after real writer kills (P4-009)."""
import hashlib
import os
import signal
import sqlite3
import subprocess
import sys
import textwrap
from pathlib import Path

import numpy as np
import pytest

from svsim.dataset_lease import DirectoryLease
from svsim.disk_budget import DiskAdmissionError, DiskBudget
from svsim.storage import StorageConfig
from svsim.stored_runtime import StoredSimulator
from svsim.uncommitted_recovery import (UncommittedRecoveryRefused, collect_temporary_artifacts,
                                        recover_uncommitted_reservations)

ROOT = Path(__file__).resolve().parents[1]
ENV = {**os.environ, 'OMP_NUM_THREADS': '1'}
BACKEND = os.environ.get('SVSIM_TEST_BACKEND', 'buffered')

CHILD = '''
    import os, sys
    import numpy as np
    from svsim.storage import StorageConfig
    from svsim.stored_runtime import StoredSimulator
    root, stage, backend = sys.argv[1:4]
    cfg = StorageConfig(root, 3, ram_budget_bytes=512 * 1024, metadata_reserve_bytes=64 * 1024, alignment=4096 if backend == 'direct' else 64, backend=backend)
    sim = StoredSimulator(8, cfg)          # no disk budget given: the store opens its own private ledger
    for q in range(8): sim.gate('H', (q,))
    sim.gate('RZ', (7,), angle=0.37); sim.gate('CX', (7, 0))
    sim.materialize()
    np.save(root + '.expected.npy', sim.state())
    snapshot = sim.commit_snapshot()
    print(snapshot.snapshot_id, flush=True)
    for q in range(8): sim.gate('RX', (q,), angle=0.21 + q)
    if stage == 'mid-materialize':
        from svsim.transport import StorageFile
        original = StorageFile.write_from
        def write_from(self, *a, **k):
            result = original(self, *a, **k)
            os._exit(0)
        StorageFile.write_from = write_from
        sim.materialize(); sim.cache.flush()
    elif stage == 'mid-batch':
        with sim.store.db.batch():
            sim.store.db.execute("INSERT INTO settings VALUES('junk',?)", ('x' * 9000,))
            os._exit(0)
    os._exit(0)
'''


def _cfg(root):
    return StorageConfig(str(root), 3, ram_budget_bytes=512 * 1024, metadata_reserve_bytes=64 * 1024, alignment=4096 if BACKEND == 'direct' else 64, backend=BACKEND)


def _kill(tmp_path, stage):
    root = tmp_path / 'store'
    out = subprocess.run([sys.executable, '-c', textwrap.dedent(CHILD), str(root), stage, BACKEND], cwd=ROOT, env=ENV,
                         capture_output=True, text=True, timeout=120)
    assert out.returncode == 0, out.stderr
    return root, root / 'metadata/disk-ledger.sqlite3', out.stdout.split()[0]


def _artifacts(root):
    """All store files except the private ledger and its journal (metadata that changes by design)."""
    skip = {'disk-ledger.sqlite3', 'disk-ledger.sqlite3-journal'}
    return {str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest()
            for p in sorted(Path(root).rglob('*')) if p.is_file() and p.name not in skip}


def _sha(path): return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def _live(ledger):
    con = sqlite3.connect(Path(ledger).as_uri() + '?mode=ro', uri=True)
    try: return con.execute("SELECT id,operation,state,bytes,materialized FROM reservations WHERE state IN ('RESERVED','WRITING')").fetchall()
    finally: con.close()


def _settle(ledger):
    report = recover_uncommitted_reservations(ledger)
    assert report['status'] == 'dry-run', report['refusals']
    assert report['ledger_layout'] == 'store-private' and report['strict_capacity_guarantee'] is False
    applied = recover_uncommitted_reservations(ledger, apply=True, confirm_plan=report['plan_sha256'])
    assert applied['status'] == 'applied' and applied['artifacts_preserved'] and applied['pending_bytes_after'] == 0
    return report, applied


@pytest.mark.parametrize('stage', ['mid-materialize', 'quiet'])
def test_private_ledger_dry_run_is_read_only_apply_settles_once_and_resume_matches_oracle(tmp_path, stage):
    root, ledger, snapshot = _kill(tmp_path, stage)
    live = _live(ledger)
    assert live and {op for _, op, *_ in live} >= {'trace-append'}
    artifacts, ledger_bytes = _artifacts(root), _sha(ledger)
    report = recover_uncommitted_reservations(ledger)
    assert report['status'] == 'dry-run' and not report['refusals'] and report['ledger_layout'] == 'store-private'
    assert {t['id'] for t in report['tokens']} == {t[0] for t in live}
    assert report['owner_death_proof']['pid_or_timestamp_used'] is False
    assert _artifacts(root) == artifacts and _sha(ledger) == ledger_bytes  # dry run: byte for byte read only
    with pytest.raises(ValueError):
        recover_uncommitted_reservations(ledger, apply=True)
    with pytest.raises(UncommittedRecoveryRefused, match='Plan changed'):
        recover_uncommitted_reservations(ledger, apply=True, confirm_plan='0' * 64)
    assert _sha(ledger) == ledger_bytes
    applied = recover_uncommitted_reservations(ledger, apply=True, confirm_plan=report['plan_sha256'])
    assert applied['status'] == 'applied' and applied['pending_bytes_after'] == 0
    assert applied['observed_usage_after_bytes'] == report['observed_usage_bytes']
    assert _artifacts(root) == artifacts  # nothing deleted, truncated or rewritten
    assert _live(ledger) == []
    with pytest.raises(UncommittedRecoveryRefused):  # exactly once: the reviewed plan cannot be replayed
        recover_uncommitted_reservations(ledger, apply=True, confirm_plan=report['plan_sha256'])
    assert recover_uncommitted_reservations(ledger)['status'] == 'nothing-to-do'
    con = sqlite3.connect(ledger)
    try:
        assert con.execute('SELECT count(*) FROM checkpoint_recovery_receipts').fetchone()[0] == len(live)  # receipt per token
        assert con.execute('SELECT count(*) FROM shared_policy').fetchone()[0] == 0  # no policy was created
        assert con.execute('PRAGMA user_version').fetchone()[0] == 3
    finally: con.close()
    # resume numerics from the last committed snapshot match the oracle
    sim = StoredSimulator(8, _cfg(root), _snapshot=snapshot, _create=False)
    try:
        np.testing.assert_allclose(sim.state(), np.load(str(root) + '.expected.npy'), rtol=0, atol=1e-13)
        sim.commit_snapshot()
    finally: sim.close()


def test_private_ledger_hot_catalog_journal_is_refused_until_admitted_reopen(tmp_path):
    root, ledger, snapshot = _kill(tmp_path, 'mid-batch')
    report = recover_uncommitted_reservations(ledger)
    if report['status'] == 'refused':
        assert any('catalog-journal-present' in r for r in report['refusals'])
        before = _sha(ledger)
        with pytest.raises(UncommittedRecoveryRefused):
            recover_uncommitted_reservations(ledger, apply=True, confirm_plan=report['plan_sha256'])
        assert _sha(ledger) == before and _live(ledger)
        sim = StoredSimulator(8, _cfg(root), _snapshot=snapshot, _create=False)  # admitted reopen replays the journal
        sim.close()
    _settle(ledger)
    assert _live(ledger) == []
    db = sqlite3.connect(root / 'metadata/catalog.sqlite3')
    assert db.execute("SELECT count(*) FROM settings WHERE key='junk'").fetchone() == (0,)


def test_private_ledger_live_holder_blocks_and_death_releases_without_pid_check(tmp_path):
    root, ledger, snapshot = _kill(tmp_path, 'quiet')
    holder = subprocess.Popen([sys.executable, '-c', textwrap.dedent('''
        import sys, time
        from svsim.storage import StorageConfig
        from svsim.stored_runtime import StoredSimulator
        cfg = StorageConfig(sys.argv[1], 3, ram_budget_bytes=512 * 1024, metadata_reserve_bytes=64 * 1024, alignment=int(sys.argv[3]))
        sim = StoredSimulator(8, cfg, _snapshot=sys.argv[2], _create=False)
        print('up', flush=True); time.sleep(300)
    '''), str(root), snapshot, '4096' if BACKEND == 'direct' else '64'], cwd=ROOT, env=ENV, stdout=subprocess.PIPE, text=True)
    try:
        assert holder.stdout.readline().strip() == 'up'
        before = _sha(ledger)
        with pytest.raises(UncommittedRecoveryRefused, match='live'):
            recover_uncommitted_reservations(ledger)
        assert _sha(ledger) == before
    finally:
        holder.send_signal(signal.SIGKILL); holder.wait()
    assert recover_uncommitted_reservations(ledger)['status'] == 'dry-run'


def test_dataset_lease_alone_blocks_private_ledger_recovery(tmp_path):
    root, ledger, _ = _kill(tmp_path, 'quiet')
    lease = DirectoryLease(root)
    try:
        with pytest.raises(UncommittedRecoveryRefused, match='live runtime holds dataset lease'):
            recover_uncommitted_reservations(ledger)
    finally: lease.close()
    assert recover_uncommitted_reservations(ledger)['status'] == 'dry-run'


def _tamper(ledger, sql):
    con = sqlite3.connect(ledger, isolation_level=None)
    try: con.execute(sql)
    finally: con.close()


@pytest.mark.parametrize('case', ['extra-root', 'schema-changed', 'trigger-dropped', 'user-version',
                                  'nonempty-journal', 'wal-file', 'unknown-token', 'moved-out-of-store'])
def test_unknown_private_ledger_states_are_refused_and_change_nothing(tmp_path, case):
    root, ledger, _ = _kill(tmp_path, 'quiet')
    live_before = len(_live(ledger))
    if case == 'extra-root':
        extra = tmp_path / 'other'; extra.mkdir()
        _tamper(ledger, "INSERT INTO roots VALUES('%s')" % extra.resolve())
    elif case == 'schema-changed': _tamper(ledger, 'ALTER TABLE roots ADD COLUMN junk TEXT')
    elif case == 'trigger-dropped':
        name = sqlite3.connect(ledger).execute("SELECT name FROM sqlite_master WHERE type='trigger' LIMIT 1").fetchone()[0]
        _tamper(ledger, f'DROP TRIGGER {name}')
    elif case == 'user-version': _tamper(ledger, 'PRAGMA user_version=2')
    elif case == 'nonempty-journal': Path(str(ledger) + '-journal').write_bytes(b'x' * 512)
    elif case == 'wal-file': Path(str(ledger) + '-wal').write_bytes(b'x' * 512)
    elif case == 'unknown-token':
        _tamper(ledger, "UPDATE reservations SET operation='mystery-writer' WHERE id=(SELECT id FROM reservations WHERE state IN ('RESERVED','WRITING') LIMIT 1)")
    elif case == 'moved-out-of-store':
        moved = tmp_path / 'elsewhere'; moved.mkdir()
        os.rename(ledger, moved / 'disk-ledger.sqlite3'); ledger = moved / 'disk-ledger.sqlite3'
    before, artifacts = _sha(ledger), _artifacts(root)
    try:
        report = recover_uncommitted_reservations(ledger)
    except (DiskAdmissionError, UncommittedRecoveryRefused):
        pass
    else:
        assert report['status'] == 'refused'
        with pytest.raises(UncommittedRecoveryRefused):
            recover_uncommitted_reservations(ledger, apply=True, confirm_plan=report['plan_sha256'])
    assert _sha(ledger) == before and _artifacts(root) == artifacts
    if case not in ('nonempty-journal', 'wal-file'):  # a garbage side file makes even a read-only open fail
        assert len(_live(ledger)) == live_before


def test_private_ledger_recovery_budget_is_recovery_only_and_rejects_policy_and_wrong_layout(tmp_path):
    root, ledger, _ = _kill(tmp_path, 'quiet')
    with pytest.raises(ValueError):
        DiskBudget(ledger, [root], store_private=True)  # not recovery-only
    with pytest.raises(DiskAdmissionError, match='(?i)matching persisted store-private policy'):
        DiskBudget(ledger, [root], recovery_only=True, store_private=True, quota_bytes=1 << 30)  # persisted policy is the default
    with pytest.raises(DiskAdmissionError):
        DiskBudget(ledger, [root, tmp_path], recovery_only=True, store_private=True)
    lease = DirectoryLease(ledger.parent, exclusive=True)
    try:
        budget = DiskBudget(ledger, [root], recovery_only=True, store_private=True)
        try:
            with pytest.raises(DiskAdmissionError, match='(?i)recovery-only'):
                budget.reserve(10)
            with pytest.raises(DiskAdmissionError):
                budget.register_roots([tmp_path])
        finally: budget.close()
    finally: lease.close()


def test_private_ledger_temporary_gc_works_with_the_same_proof(tmp_path):
    root, ledger, _ = _kill(tmp_path, 'quiet')
    stray = root / 'manifests' / ('.snapshot-x.json.' + 'a' * 32 + '.tmp')
    stray.write_bytes(b'partial')
    report = collect_temporary_artifacts(ledger)
    assert report['status'] == 'dry-run' and stray.exists()
    applied = collect_temporary_artifacts(ledger, apply=True, confirm_plan=report['plan_sha256'])
    assert applied['status'] == 'applied' and not stray.exists()


def test_private_ledger_dead_branch_gc_keeps_committed_cut(tmp_path):
    import json
    from test_uncommitted_gc import CHILD as GC_CHILD
    # Same dataset as the shared-ledger GC test, but the store opens its own private ledger.
    script = GC_CHILD.replace("""budget = DiskBudget(os.path.join(root, 'ledger', 'ledger.sqlite3'), [os.path.join(root, 'store')],
                        quota_bytes=%d, shared_policy=True)""" % (64 * 1024**2), "budget = None").replace(
        "store = FileBlockStore(config, 4, disk_budget=budget)", "store = FileBlockStore(config, 4)")
    assert 'budget = None' in script and 'FileBlockStore(config, 4)' in script
    out = subprocess.run([sys.executable, '-c', textwrap.dedent(script), str(tmp_path)], cwd=ROOT, env=ENV,
                         capture_output=True, text=True, timeout=120)
    assert out.returncode == 0, out.stderr
    ids = json.loads(out.stdout.strip().splitlines()[-1])
    ledger = tmp_path / 'store/metadata/disk-ledger.sqlite3'
    from svsim.uncommitted_recovery import collect_dead_branches
    report = collect_dead_branches(ledger)
    assert report['status'] == 'dry-run'
    assert report['stores'][0]['dead_roots'] == sorted([ids['child'], ids['lone']])
    applied = collect_dead_branches(ledger, apply=True, confirm_plan=report['plan_sha256'])
    assert applied['status'] == 'applied'
    con = sqlite3.connect(tmp_path / 'store/metadata/catalog.sqlite3')
    assert con.execute('SELECT id FROM roots').fetchall() == [(ids['committed'],)]
    assert con.execute('PRAGMA integrity_check').fetchone() == ('ok',)
    con.close()
    assert collect_dead_branches(ledger)['status'] == 'nothing-to-do'


def _custom_private_store(tmp_path, *, quota=1 << 30, min_free=1 << 20):
    """A store whose private ledger was created with a non-default policy, and one live dead-owner token."""
    from svsim.storage import FileBlockStore
    root = tmp_path / 'custom'
    root.mkdir()
    budget = DiskBudget(root / 'metadata/disk-ledger.sqlite3', [root], quota_bytes=quota, min_free_bytes=min_free,
                        persist_private_policy=True)
    store = FileBlockStore(StorageConfig(str(root), 2, backend=BACKEND), 4, disk_budget=budget)
    store.close()
    budget.reserve(4096, operation='amplitude-extent', writing=True)
    budget.close()
    return root, root / 'metadata/disk-ledger.sqlite3'


def _policy_row(ledger):
    con = sqlite3.connect(ledger)
    try: return con.execute('SELECT quota_bytes,min_free_bytes FROM private_policy').fetchall()
    finally: con.close()


def test_new_private_ledger_persists_its_policy_and_no_shared_policy(tmp_path):
    root, ledger, _ = _kill(tmp_path, 'quiet')
    con = sqlite3.connect(ledger)
    try:
        assert con.execute('SELECT quota_bytes,min_free_bytes,drain_reserve_bytes,bootstrap_bytes,max_records FROM private_policy').fetchall() \
            == [(None, 0, 0, 8 * 1024 ** 2, 4096)]
        assert con.execute('SELECT count(*) FROM shared_policy').fetchone()[0] == 0
    finally: con.close()


def test_recovery_verifies_a_non_default_private_policy(tmp_path):
    from svsim.uncommitted_recovery import offline_scope
    root, ledger = _custom_private_store(tmp_path)
    assert _policy_row(ledger) == [(1 << 30, 1 << 20)]
    with offline_scope(ledger) as scope:
        assert scope['private_ledger'] and scope['policy']['quota_bytes'] == 1 << 30 and scope['policy']['min_free_bytes'] == 1 << 20
        assert scope['budget'].quota_bytes == 1 << 30 and scope['budget'].min_free_bytes == 1 << 20
    with pytest.raises(DiskAdmissionError, match='conflicts'):  # a caller policy that differs is refused
        with offline_scope(ledger, policy={'quota_bytes': 1 << 31}): pass
    report = recover_uncommitted_reservations(ledger)
    assert report['status'] == 'dry-run' and report['ledger_layout'] == 'store-private' and len(report['tokens']) == 1
    applied = recover_uncommitted_reservations(ledger, apply=True, confirm_plan=report['plan_sha256'])
    assert applied['status'] == 'applied' and applied['pending_bytes_after'] == 0


@pytest.mark.parametrize('tamper', ['page-cap', 'device', 'schema'])
def test_tampered_private_policy_is_refused_and_changes_nothing(tmp_path, tamper):
    root, ledger = _custom_private_store(tmp_path)
    con = sqlite3.connect(ledger)
    try:
        if tamper == 'page-cap': con.execute('UPDATE private_policy SET max_page_count=max_page_count+1')
        elif tamper == 'device': con.execute('UPDATE private_policy SET volume_device=volume_device+1')
        else: con.execute('DROP TABLE private_policy'); con.execute('CREATE TABLE private_policy(singleton INTEGER, quota_bytes INTEGER)')
        con.commit()
    finally: con.close()
    before = _sha(ledger)
    with pytest.raises(DiskAdmissionError):
        recover_uncommitted_reservations(ledger)
    assert _sha(ledger) == before


def test_ledger_without_a_private_policy_row_keeps_the_weaker_default_check(tmp_path):
    root, ledger, _ = _kill(tmp_path, 'quiet')
    con = sqlite3.connect(ledger)
    try:
        con.execute('DROP TABLE private_policy'); con.commit()  # what every pre-existing ledger looks like
    finally: con.close()
    with pytest.raises(DiskAdmissionError, match='persisted none'):
        DiskBudget(ledger, [root], recovery_only=True, store_private=True, quota_bytes=1 << 30)
    report = recover_uncommitted_reservations(ledger)
    assert report['status'] == 'dry-run' and not report['refusals']
    # an existing ledger is never migrated by an ordinary reopen
    from svsim.storage import FileBlockStore
    store = FileBlockStore(_cfg(root), 8, create=False)
    store.close()
    con = sqlite3.connect(ledger)
    try: assert con.execute("SELECT count(*) FROM sqlite_master WHERE name='private_policy'").fetchone()[0] == 0
    finally: con.close()


def test_reopening_a_custom_private_ledger_with_defaults_is_refused(tmp_path):
    from svsim.storage import FileBlockStore
    root, ledger = _custom_private_store(tmp_path)
    recover_uncommitted_reservations(ledger)  # recovery itself is fine; only a differing writer policy is refused
    with pytest.raises(DiskAdmissionError, match='(?i)policy mismatch'):
        FileBlockStore(StorageConfig(str(root), 2, backend=BACKEND), 4, create=False)
