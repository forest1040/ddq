"""Offline reconcile of unlinked reservations after real writer kills (P4-009)."""
import hashlib
import os
import signal
import sqlite3
import subprocess
import sys
import textwrap
import time
from pathlib import Path

import pytest

from svsim.dataset_lease import DirectoryLease
from svsim.disk_budget import DiskAdmissionError, DiskBudget
from svsim.journal import Journal
from svsim.storage import FileBlockStore, StorageConfig
from svsim.uncommitted_recovery import UncommittedRecoveryRefused, recover_uncommitted_reservations

ROOT = Path(__file__).resolve().parents[1]
ENV = {**os.environ, 'OMP_NUM_THREADS': '1'}
QUOTA = 64 * 1024**2
BACKEND = os.environ.get('SVSIM_TEST_BACKEND', 'buffered')  # 'direct' runs the file store in O_DIRECT mode (Linux)

PRELUDE = '''
    import os, sys
    from svsim.disk_budget import DiskBudget
    from svsim.storage import FileBlockStore, StorageConfig
    root, stage = sys.argv[1], sys.argv[2]
    os.makedirs(os.path.join(root, 'store'), exist_ok=True)
    budget = DiskBudget(os.path.join(root, 'ledger', 'ledger.sqlite3'), [os.path.join(root, 'store')],
                        quota_bytes=%d, shared_policy=True)
    store = FileBlockStore(StorageConfig(os.path.join(root, 'store'), 2, backend=%r), 4, disk_budget=budget)
''' % (QUOTA, BACKEND)

STAGES = {
    # allocator claimed, slot token live, no byte written
    'extent-claimed': '''
        version = store.new_version()
        token = budget.reserve(store.slot_bytes, operation='amplitude-extent', writing=True)
        store.reserve_extent(version, token)
    ''',
    # part of a slot reached the segment file, never sealed in the catalog
    'extent-partial': '''
        version = store.new_version()
        token = budget.reserve(store.slot_bytes, operation='amplitude-extent', writing=True)
        segment, offset = store.reserve_extent(version, token)
        fd = os.open(os.path.join(root, 'store/state/segment-%06d.bin' % segment), os.O_WRONLY | os.O_CREAT)
        os.pwrite(fd, b'x' * 300, offset)
    ''',
    # dirty catalog pages and rollback journal, no commit
    'catalog-mid-batch': '''
        with store.db.batch():
            store.db.execute("INSERT INTO settings VALUES('junk',?)", ('x' * 9000,))
            os._exit(0)
    ''',
    # committed journal record, then half of an unacknowledged one
    'journal-partial': '''
        from svsim.journal import Journal
        journal = Journal(os.path.join(root, 'store/journal'), disk_budget_getter=lambda: budget)
        journal.append('gates', {'value': 'committed'})
        journal._append.admit(300)
        os.write(journal.streams['gates'].fileno(), b'{"schema_version":1,"payload":{"va')
    ''',
    'trace-open': '''
        from svsim.telemetry import StreamingExecutionTrace
        trace = StreamingExecutionTrace(os.path.join(root, 'store/traces/run.jsonl'), profile_id='p', plan_id='p',
                                        disk_budget_getter=lambda: budget)
        trace.record('event', metadata={'pad': 'y' * 200})
    ''',
}


def _run(script, *args, timeout=120):
    return subprocess.run([sys.executable, '-c', textwrap.dedent(script), *map(str, args)], cwd=ROOT, env=ENV,
                          capture_output=True, text=True, timeout=timeout)


def _kill_child(root, stage):
    body = textwrap.dedent(PRELUDE) + textwrap.dedent(STAGES[stage]) + '\nos._exit(0)\n'
    child = _run(body, root, stage)
    assert child.returncode == 0, child.stderr
    return Path(root) / 'ledger' / 'ledger.sqlite3'


def _artifact_digest(root):
    """Every file under the dataset, excluding the ledger metadata pool (it changes by design)."""
    out = {}
    for p in sorted(Path(root).rglob('*')):
        if p.is_file() and 'ledger' not in p.relative_to(root).parts:
            out[str(p.relative_to(root))] = hashlib.sha256(p.read_bytes()).hexdigest()
    return out


def _ledger_digest(ledger):
    return hashlib.sha256(Path(ledger).read_bytes()).hexdigest()


def _live(ledger, root):
    budget = DiskBudget(ledger, [Path(root) / 'store'], quota_bytes=QUOTA, shared_policy=True)
    try:
        return budget.live_reservations(linked=False), budget.reserved_bytes
    finally:
        budget.close()


@pytest.mark.parametrize('stage', ['extent-claimed', 'extent-partial', 'journal-partial', 'trace-open'])
def test_dry_run_changes_nothing_and_apply_settles_exactly_once(tmp_path, stage):
    ledger = _kill_child(tmp_path, stage)
    tokens, pending = _live(ledger, tmp_path)
    assert tokens and pending > 0
    artifacts, ledger_before = _artifact_digest(tmp_path), _ledger_digest(ledger)
    report = recover_uncommitted_reservations(ledger)
    assert report['status'] == 'dry-run' and not report['refusals'] and report['strict_capacity_guarantee'] is False
    assert report['owner_death_proof']['pid_or_timestamp_used'] is False
    assert {t['id'] for t in report['tokens']} == {t['id'] for t in tokens}
    assert report['would_release_pending_bytes'] == pending
    # dry run is byte-for-byte read only, including the ledger itself
    assert _artifact_digest(tmp_path) == artifacts and _ledger_digest(ledger) == ledger_before
    # apply needs the reviewed plan
    with pytest.raises(ValueError):
        recover_uncommitted_reservations(ledger, apply=True)
    with pytest.raises(UncommittedRecoveryRefused, match='Plan changed'):
        recover_uncommitted_reservations(ledger, apply=True, confirm_plan='0' * 64)
    assert _ledger_digest(ledger) == ledger_before
    applied = recover_uncommitted_reservations(ledger, apply=True, confirm_plan=report['plan_sha256'])
    assert applied['status'] == 'applied' and applied['released_pending_bytes'] == pending
    assert applied['artifacts_preserved'] and applied['pending_bytes_after'] == 0
    assert _artifact_digest(tmp_path) == artifacts  # no artifact deleted, truncated or rewritten
    assert _live(ledger, tmp_path) == ([], 0)
    # the dead owner's partial bytes remain charged by the observed-usage scan (never freed blindly)
    budget = DiskBudget(ledger, [tmp_path / 'store'], quota_bytes=QUOTA, shared_policy=True)
    try:
        assert budget._observed_usage() == applied['observed_usage_bytes'] > 0
        for token in tokens:
            row = budget.reservation(token['id'])
            assert row['state'] == 'RECONCILED' and row['materialized_bytes'] == token['materialized_bytes']
    finally:
        budget.close()
    # second application: nothing is left, and the old plan is refused
    again = recover_uncommitted_reservations(ledger)
    assert again['status'] == 'nothing-to-do'
    with pytest.raises(UncommittedRecoveryRefused):
        recover_uncommitted_reservations(ledger, apply=True, confirm_plan=report['plan_sha256'])


def test_journal_tail_is_reported_as_residual_and_resume_still_truncates_it(tmp_path):
    ledger = _kill_child(tmp_path, 'journal-partial')
    report = recover_uncommitted_reservations(ledger)
    journal = next(s for s in report['stores'])['journals']['gates']
    assert journal['uncommitted_tail_bytes'] > 0 and journal['valid_prefix_bytes'] > 0
    recover_uncommitted_reservations(ledger, apply=True, confirm_plan=report['plan_sha256'])
    assert (tmp_path / 'store/journal/gates.jsonl').stat().st_size == journal['bytes']  # untouched
    budget = DiskBudget(ledger, [tmp_path / 'store'], quota_bytes=QUOTA, shared_policy=True)
    try:
        j = Journal(tmp_path / 'store/journal', disk_budget_getter=lambda: budget)
        j.append('gates', {'value': 'after'})
        j.close()
    finally:
        budget.close()
    assert [r['value'] for r in Journal(tmp_path / 'store/journal').replay('gates')] == ['committed', 'after']


def test_hot_catalog_journal_is_refused_until_admitted_reopen_then_token_is_attributed(tmp_path):
    ledger = _kill_child(tmp_path, 'catalog-mid-batch')
    tokens, _ = _live(ledger, tmp_path)
    assert any(t['operation'] == 'catalog-transaction' for t in tokens)
    journal = tmp_path / 'store/metadata/catalog.sqlite3-journal'
    report = recover_uncommitted_reservations(ledger)
    assert journal.exists() and journal.stat().st_size, 'stage must leave a non-empty rollback journal'
    assert report['status'] == 'refused' and any('catalog-journal-present' in r for r in report['refusals'])
    with pytest.raises(UncommittedRecoveryRefused):
        recover_uncommitted_reservations(ledger, apply=True, confirm_plan=report['plan_sha256'])
    assert _live(ledger, tmp_path)[0]  # tokens untouched
    # an admitted reopen replays SQLite's rollback journal and consumes it
    budget = DiskBudget(ledger, [tmp_path / 'store'], quota_bytes=QUOTA, shared_policy=True)
    store = FileBlockStore(StorageConfig(str(tmp_path / 'store'), 2, backend=BACKEND), 4, create=False, disk_budget=budget)
    with store.db:
        store.db.execute("INSERT OR REPLACE INTO settings VALUES('probe','1')")
    store.close(); budget.close()
    report = recover_uncommitted_reservations(ledger)
    assert report['status'] == 'dry-run', report['refusals']
    kinds = {t['operation']: t['class'] for t in report['tokens']}
    assert kinds['catalog-transaction'] == 'catalog'
    recover_uncommitted_reservations(ledger, apply=True, confirm_plan=report['plan_sha256'])
    assert _live(ledger, tmp_path) == ([], 0)
    db = sqlite3.connect(tmp_path / 'store/metadata/catalog.sqlite3')
    assert db.execute("SELECT count(*) FROM settings WHERE key='junk'").fetchone() == (0,)  # uncommitted SQL never appeared


def test_live_ledger_holder_blocks_recovery_and_death_releases_it_without_pid_check(tmp_path):
    ledger = _kill_child(tmp_path, 'trace-open')
    holder = subprocess.Popen([sys.executable, '-c', textwrap.dedent('''
        import sys, time
        from svsim.disk_budget import DiskBudget
        b = DiskBudget(sys.argv[1], [sys.argv[2]], quota_bytes=%d, shared_policy=True)
        print('up', flush=True); time.sleep(300)
    ''' % QUOTA), ledger, tmp_path / 'store'], cwd=ROOT, env=ENV, stdout=subprocess.PIPE, text=True)
    try:
        assert holder.stdout.readline().strip() == 'up'
        before = _ledger_digest(ledger)
        with pytest.raises(UncommittedRecoveryRefused, match='live process holds the disk ledger'):
            recover_uncommitted_reservations(ledger)
        assert _ledger_digest(ledger) == before
    finally:
        holder.send_signal(signal.SIGKILL); holder.wait()
    assert recover_uncommitted_reservations(ledger)['status'] == 'dry-run'


def test_live_dataset_lease_blocks_recovery(tmp_path):
    ledger = _kill_child(tmp_path, 'trace-open')
    lease = DirectoryLease(tmp_path / 'store')  # a running store would hold this shared lease
    try:
        with pytest.raises(UncommittedRecoveryRefused, match='live runtime holds dataset lease'):
            recover_uncommitted_reservations(ledger)
    finally:
        lease.close()
    assert recover_uncommitted_reservations(ledger)['status'] == 'dry-run'


def test_new_writer_budget_is_refused_while_recovery_holds_the_ledger(tmp_path, monkeypatch):
    ledger = _kill_child(tmp_path, 'trace-open')
    import svsim.uncommitted_recovery as module
    seen = {}
    original = module._inspect_store

    def inspect(*args, **kwargs):
        with pytest.raises(OSError):  # BlockingIOError/DatasetBusyError: a writer cannot start mid-recovery
            DiskBudget(ledger, [tmp_path / 'store'], quota_bytes=QUOTA, shared_policy=True)
        seen['blocked'] = True
        return original(*args, **kwargs)
    monkeypatch.setattr(module, '_inspect_store', inspect)
    recover_uncommitted_reservations(ledger)
    assert seen['blocked']


def _ledger_with(tmp_path, *tokens):
    (tmp_path / 'store').mkdir()
    budget = DiskBudget(tmp_path / 'ledger/ledger.sqlite3', [tmp_path / 'store'], quota_bytes=QUOTA, shared_policy=True)
    keys = [budget.reserve(n, operation=op, owner=owner, generation=gen, writing=True) for n, op, owner, gen in tokens]
    budget.close()
    return tmp_path / 'ledger/ledger.sqlite3', keys


@pytest.mark.parametrize('token,reason', [
    ((100, 'mystery-writer', None, None), 'unknown-operation'),
    ((100, 'unspecified', None, None), 'unknown-operation'),
    ((100, 'atomic-json', 'a' * 32, 'b' * 32), 'unknown-operation'),   # sidecar tokens must carry an intent
    ((100, 'catalog-transaction', 'f' * 64, 'e' * 64), 'catalog-owner-not-attributable'),
])
def test_unknown_or_unattributable_tokens_refuse_the_whole_run(tmp_path, token, reason):
    ledger, _ = _ledger_with(tmp_path, token, (50, 'journal-append', None, None))
    before = _ledger_digest(ledger)
    report = recover_uncommitted_reservations(ledger)
    assert report['status'] == 'refused' and any(reason in r for r in report['refusals'])
    with pytest.raises(UncommittedRecoveryRefused):
        recover_uncommitted_reservations(ledger, apply=True, confirm_plan=report['plan_sha256'])
    assert _ledger_digest(ledger) == before  # even the attributable token stays charged


def test_interior_journal_corruption_is_refused(tmp_path):
    ledger = _kill_child(tmp_path, 'journal-partial')
    path = tmp_path / 'store/journal/gates.jsonl'
    good = path.read_bytes().split(b'\n')[0] + b'\n'
    path.write_bytes(good + b'garbage line\n' + good)
    report = recover_uncommitted_reservations(ledger)
    assert report['status'] == 'refused' and any('journal-record-after-corruption' in r for r in report['refusals'])


def test_symlink_artifact_and_unreadable_catalog_are_refused(tmp_path):
    ledger = _kill_child(tmp_path, 'trace-open')
    os.symlink(tmp_path / 'store/state', tmp_path / 'store/alias')
    assert 'symlink-artifact-present' in ' '.join(recover_uncommitted_reservations(ledger)['refusals'])
    os.unlink(tmp_path / 'store/alias')
    (tmp_path / 'store/metadata/catalog.sqlite3').write_bytes(b'not a database' * 100)
    report = recover_uncommitted_reservations(ledger)
    assert report['status'] == 'refused' and any('catalog-unreadable' in r for r in report['refusals'])
    with pytest.raises(UncommittedRecoveryRefused):
        recover_uncommitted_reservations(ledger, apply=True, confirm_plan=report['plan_sha256'])


def test_published_extent_beyond_its_segment_is_refused(tmp_path):
    ledger = _kill_child(tmp_path, 'extent-partial')
    store = tmp_path / 'store'
    con = sqlite3.connect(store / 'metadata/catalog.sqlite3')
    con.execute("INSERT INTO versions VALUES('forged',0,1048576,'x')")
    con.commit(); con.close()
    assert any('published-extent-beyond-segment-file' in r for r in recover_uncommitted_reservations(ledger)['refusals'])


def _linked_ledger(tmp_path, stage='prepare', rank=0):
    root = tmp_path / 'store'
    root.mkdir()
    ledger = tmp_path / 'ledger/ledger.sqlite3'
    budget = DiskBudget(ledger, [root], quota_bytes=QUOTA, shared_policy=True)
    sid = 'a1' * 16
    name = f'global/manifests/{sid}.json' if stage == 'decision' else f'rank-{rank:04d}/manifests/{stage.split("-")[0]}-{sid}.json'
    target = root / name
    nonce = 'c' * 32
    linked = budget.reserve(512, writing=True, owner='d' * 32, generation=nonce, operation='atomic-json',
        checkpoint_link={'dataset_root': str(root), 'dataset_id': 'b2' * 16, 'snapshot_id': sid, 'rank': rank, 'stage': stage,
                         'target': str(target), 'temporary': str(target.with_name('.' + target.name + '.' + nonce + '.tmp')),
                         'digest': hashlib.sha256(b'x' * 512).hexdigest()})
    plain = budget.reserve(50, operation='journal-append', writing=True)
    budget.close()
    return root, ledger, sid, linked, plain


def test_checkpoint_linked_token_of_a_committed_cut_is_deferred_not_settled(tmp_path):
    root, ledger, sid, linked, plain = _linked_ledger(tmp_path)
    (root / 'global/manifests').mkdir(parents=True)
    (root / f'global/manifests/{sid}.json').write_text('{}')  # a global decision exists: checkpoint recovery owns it
    report = recover_uncommitted_reservations(ledger)
    assert report['linked_live_tokens_deferred'] == 1 and [t['id'] for t in report['tokens']] == [plain]
    recover_uncommitted_reservations(ledger, apply=True, confirm_plan=report['plan_sha256'])
    budget = DiskBudget(ledger, [root], quota_bytes=QUOTA, shared_policy=True)
    try:
        assert budget.reservation(linked)['state'] == 'WRITING' and budget.reservation(plain)['state'] == 'RECONCILED'
    finally:
        budget.close()


def test_checkpoint_linked_token_of_a_never_committed_snapshot_is_settled_with_a_receipt(tmp_path):
    root, ledger, sid, linked, plain = _linked_ledger(tmp_path)
    temporary = root / f'rank-0000/manifests/.prepare-{sid}.json.{"c" * 32}.tmp'
    temporary.parent.mkdir(parents=True)
    temporary.write_bytes(b'x' * 13)  # partial temporary of the dead writer stays in place
    before = temporary.read_bytes()
    report = recover_uncommitted_reservations(ledger)
    assert report['status'] == 'dry-run' and report['linked_live_tokens_deferred'] == 0
    kinds = {t['id']: t['class'] for t in report['tokens']}
    assert kinds == {linked: 'checkpoint-uncommitted', plain: 'append'}
    recover_uncommitted_reservations(ledger, apply=True, confirm_plan=report['plan_sha256'])
    assert temporary.read_bytes() == before
    budget = DiskBudget(ledger, [root], quota_bytes=QUOTA, shared_policy=True)
    try:
        assert budget.reservation(linked)['state'] == 'RECONCILED' and budget.recovery_receipt(linked) is not None
        assert budget.reserved_bytes == 0 and budget.live_reservations(linked=True) == []
    finally:
        budget.close()
    assert recover_uncommitted_reservations(ledger)['status'] == 'nothing-to-do'


@pytest.mark.parametrize('mutate,reason', [
    ('decision-copy', 'checkpoint-stage-without-committed-decision'),
    ('hardlink', 'checkpoint-temporary-not-an-exclusive-regular-file'),
    ('symlinked-directory', 'checkpoint-directory-aliased'),
])
def test_unsafe_uncommitted_checkpoint_intents_refuse_the_whole_run(tmp_path, mutate, reason):
    stage = 'decision-copy' if mutate == 'decision-copy' else 'prepare'
    root, ledger, sid, linked, plain = _linked_ledger(tmp_path, stage)
    manifests = root / 'rank-0000/manifests'
    if mutate == 'hardlink':
        manifests.mkdir(parents=True)
        (root / 'other').write_bytes(b'x')
        os.link(root / 'other', manifests / f'.prepare-{sid}.json.{"c" * 32}.tmp')
    elif mutate == 'symlinked-directory':
        (root / 'elsewhere').mkdir()
        (root / 'rank-0000').mkdir()
        os.symlink(root / 'elsewhere', manifests)
    before = _ledger_digest(ledger)
    report = recover_uncommitted_reservations(ledger)
    assert report['status'] == 'refused' and any(reason in r for r in report['refusals']), report['refusals']
    with pytest.raises(UncommittedRecoveryRefused):
        recover_uncommitted_reservations(ledger, apply=True, confirm_plan=report['plan_sha256'])
    assert _ledger_digest(ledger) == before


def test_unlinked_cas_api_is_recovery_only_and_unreplayable(tmp_path):
    ledger, (key, _) = _ledger_with(tmp_path, (100, 'journal-append', None, None), (10, 'trace-append', None, None))
    writer = DiskBudget(ledger, [tmp_path / 'store'], quota_bytes=QUOTA, shared_policy=True)
    try:
        with pytest.raises(DiskAdmissionError, match='recovery-only'):
            writer.reconcile_unlinked(key, expected_reservation=writer.reservation(key), materialized_bytes=0,
                                      evidence='x:' + '0' * 64, evidence_document={})
    finally:
        writer.close()
    report = recover_uncommitted_reservations(ledger)
    recover_uncommitted_reservations(ledger, apply=True, confirm_plan=report['plan_sha256'])
    from svsim.dataset_lease import DirectoryLease as DL
    lease = DL(ledger.parent, exclusive=True)
    try:
        budget = DiskBudget(ledger, [tmp_path / 'store'], quota_bytes=QUOTA, shared_policy=True, recovery_only=True)
        try:
            stale = {'id': key, 'bytes': 100, 'materialized_bytes': 0, 'operation': 'journal-append', 'owner': None,
                     'generation': None, 'state': 'WRITING', 'evidence': None}
            proof = {'a': 1}
            import json
            digest = hashlib.sha256(json.dumps(proof, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
            with pytest.raises(DiskAdmissionError):  # stale expectation / already terminal
                budget.reconcile_unlinked(key, expected_reservation=stale, materialized_bytes=0,
                                          evidence='x:' + digest, evidence_document=proof)
            with pytest.raises(ValueError, match='digest'):
                budget.reconcile_unlinked(key, expected_reservation=budget.reservation(key), materialized_bytes=0,
                                          evidence='x:' + '0' * 64, evidence_document=proof)
            assert budget.recovery_receipt(key)['kind'] == 'uncommitted-recovery'
        finally:
            budget.close()
    finally:
        lease.close()


def test_apply_with_a_plan_that_changed_after_review_is_refused(tmp_path):
    ledger, _ = _ledger_with(tmp_path, (100, 'journal-append', None, None))
    report = recover_uncommitted_reservations(ledger)
    writer = DiskBudget(ledger, [tmp_path / 'store'], quota_bytes=QUOTA, shared_policy=True)
    writer.reserve(7, operation='trace-append', writing=True)  # a new token appears after review
    writer.close()
    with pytest.raises(UncommittedRecoveryRefused, match='Plan changed'):
        recover_uncommitted_reservations(ledger, apply=True, confirm_plan=report['plan_sha256'])
    assert len(_live(ledger, tmp_path)[0]) == 2


def _killed_catalog_pair(tmp_path):
    """Two real catalog writers leave independently bound tokens in one ledger."""
    child = _run('''
        import os, sys
        from pathlib import Path
        from svsim.disk_budget import DiskBudget
        from svsim.storage import FileBlockStore, StorageConfig
        root = Path(sys.argv[1]); root.mkdir(exist_ok=True)
        datasets = root / 'datasets'; datasets.mkdir()
        budget = DiskBudget(root / 'ledger/ledger.sqlite3', [datasets],
                            quota_bytes=%d, shared_policy=True)
        stores = []
        for name in ('first', 'second'):
            store = FileBlockStore(StorageConfig(str(datasets / name), 2, backend=%r), 4,
                                   disk_budget=budget)
            with store.db:
                store.db.execute("INSERT INTO settings VALUES('owner', ?)", (name,))
            stores.append(store)
        os._exit(0)
    ''' % (QUOTA, BACKEND), tmp_path)
    assert child.returncode == 0, child.stderr
    return tmp_path / 'ledger/ledger.sqlite3'


def test_multiple_catalog_tokens_keep_file_attribution_in_plan_and_receipt(tmp_path):
    ledger = _killed_catalog_pair(tmp_path)
    before = _artifact_digest(tmp_path)
    report = recover_uncommitted_reservations(ledger)
    assert report['status'] == 'dry-run', report['refusals']
    catalogs = [t for t in report['tokens'] if t['class'] == 'catalog']
    assert len(catalogs) == 2
    expected = {str(tmp_path / f'datasets/{name}/metadata/catalog.sqlite3') for name in ('first', 'second')}
    assert {t['catalog_attribution']['database']['path'] for t in catalogs} == expected
    for token in catalogs:
        db = token['catalog_attribution']['database']
        info = Path(db['path']).stat()
        assert (db['device'], db['inode'], db['bytes']) == (info.st_dev, info.st_ino, info.st_size)
        assert {p['path'] for p in token['catalog_attribution']['side_files']} == {
            db['path'] + suffix for suffix in ('-journal', '-wal', '-shm')}
    recover_uncommitted_reservations(ledger, apply=True, confirm_plan=report['plan_sha256'])
    assert _artifact_digest(tmp_path) == before
    budget = DiskBudget(ledger, [tmp_path / 'datasets'], quota_bytes=QUOTA, shared_policy=True)
    try:
        assert budget.reserved_bytes == 0
        for token in catalogs:
            assert budget.recovery_receipt(token['id'])['catalog_attribution'] == token['catalog_attribution']
    finally:
        budget.close()


@pytest.mark.parametrize('mutation', ['replace', 'exchange', 'legacy-owner'])
def test_catalog_identity_loss_refuses_all_tokens_without_modification(tmp_path, mutation):
    ledger = _killed_catalog_pair(tmp_path)
    first = tmp_path / 'datasets/first/metadata/catalog.sqlite3'
    second = tmp_path / 'datasets/second/metadata/catalog.sqlite3'
    if mutation == 'replace':
        # Keep the original inode alive: replacement is a valid SQLite database with identical bytes.
        saved = first.with_suffix('.saved')
        first.rename(saved)
        first.write_bytes(saved.read_bytes())
    elif mutation == 'exchange':
        saved = first.with_suffix('.saved')
        first.rename(saved); second.rename(first); saved.rename(second)
    else:
        # A genuine old writer recorded the path hash but could not prove a file identity.
        con = sqlite3.connect(ledger)
        con.execute("UPDATE reservations SET owner=? WHERE operation='catalog-transaction'",
                    (hashlib.sha256(str(first).encode()).hexdigest(),))
        con.commit(); con.close()
    before = _artifact_digest(tmp_path), _ledger_digest(ledger)
    report = recover_uncommitted_reservations(ledger)
    assert report['status'] == 'refused'
    assert any('catalog-owner-not-attributable' in r for r in report['refusals'])
    with pytest.raises(UncommittedRecoveryRefused):
        recover_uncommitted_reservations(ledger, apply=True, confirm_plan=report['plan_sha256'])
    assert (_artifact_digest(tmp_path), _ledger_digest(ledger)) == before


def test_new_catalog_startup_token_without_file_identity_is_not_guessed(tmp_path):
    child = _run('''
        import os, sys
        from pathlib import Path
        from svsim.catalog_admission import CatalogConnection
        from svsim.disk_budget import DiskBudget
        root = Path(sys.argv[1]); store = root / 'store'; store.mkdir()
        (store / 'metadata').mkdir()
        budget = DiskBudget(root / 'ledger/ledger.sqlite3', [store], quota_bytes=%d, shared_policy=True)
        db = CatalogConnection(store / 'metadata/catalog.sqlite3', budget_getter=lambda: budget)
        os._exit(0)
    ''' % QUOTA, tmp_path)
    assert child.returncode == 0, child.stderr
    report = recover_uncommitted_reservations(tmp_path / 'ledger/ledger.sqlite3')
    assert report['status'] == 'refused'
    assert any('catalog-owner-not-attributable' in r for r in report['refusals'])


def test_unavailable_birth_identity_never_releases_catalog_tokens(tmp_path, monkeypatch):
    ledger = _killed_catalog_pair(tmp_path)
    import svsim.catalog_admission as catalog_module
    monkeypatch.setattr(catalog_module, 'catalog_birth_identity', lambda *args: None)
    before = _ledger_digest(ledger)
    report = recover_uncommitted_reservations(ledger)
    assert report['status'] == 'refused'
    assert any('catalog-owner-not-attributable' in r for r in report['refusals'])
    with pytest.raises(UncommittedRecoveryRefused):
        recover_uncommitted_reservations(ledger, apply=True, confirm_plan=report['plan_sha256'])
    assert _ledger_digest(ledger) == before
