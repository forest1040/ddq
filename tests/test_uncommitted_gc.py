"""Offline GC of temporaries and dead branches that no committed cut references (P4-009)."""
import hashlib
import json
import os
import sqlite3
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

from svsim.dataset_lease import DirectoryLease
from svsim.disk_budget import DiskBudget
from svsim.storage import FileBlockStore, StorageConfig
from svsim.uncommitted_recovery import (UncommittedRecoveryRefused, collect_dead_branches,
                                        collect_temporary_artifacts, recover_uncommitted_reservations)

ROOT = Path(__file__).resolve().parents[1]
ENV = {**os.environ, 'OMP_NUM_THREADS': '1'}
QUOTA = 64 * 1024**2
BACKEND = os.environ.get('SVSIM_TEST_BACKEND', 'buffered')  # 'direct': O_DIRECT file store (Linux)
HEX = 'ab' * 16

# One extent per segment (segment size == slot size) so segments map 1:1 to versions.
CHILD = '''
    import json, os, sys
    from svsim.disk_budget import DiskBudget
    from svsim.storage import FileBlockStore, StorageConfig, StoredBlockView
    root = sys.argv[1]
    os.makedirs(os.path.join(root, 'store'), exist_ok=True)
    budget = DiskBudget(os.path.join(root, 'ledger', 'ledger.sqlite3'), [os.path.join(root, 'store')],
                        quota_bytes=%d, shared_policy=True)
    probe = StorageConfig(os.path.join(root, 'store'), 2, backend=os.environ.get('SVSIM_TEST_BACKEND', 'buffered'))
    config = StorageConfig(os.path.join(root, 'store'), 2, backend=os.environ.get('SVSIM_TEST_BACKEND', 'buffered'), segment_size_bytes=probe.slot_bytes)
    store = FileBlockStore(config, 4, disk_budget=budget)
    versions = []
    for _ in range(3):
        v = store.new_version(); store.write_version(v, __import__('mmap').mmap(-1, store.slot_bytes)); versions.append(v)
    committed = store.new_root()
    store.publish(committed, [(0, StoredBlockView(versions[0]))]); store.seal(committed)
    with store.db:
        store.db.execute('INSERT INTO snapshots VALUES(?,?)', ('s1', json.dumps({'root': committed})))
    child = store.new_root(committed)                  # dead suffix branch of the committed cut
    store.publish(child, [(1, StoredBlockView(versions[1]))])
    lone = store.new_root()                            # dead, unsealed, parentless
    store.publish(lone, [(2, StoredBlockView(versions[2]))])
    with store.db:
        store.db.execute("INSERT INTO active_runs VALUES('dead-run',?)", (lone,))
    print(json.dumps({'committed': committed, 'child': child, 'lone': lone, 'versions': versions}), flush=True)
    os._exit(0)
''' % QUOTA


def build(tmp_path):
    out = subprocess.run([sys.executable, '-c', textwrap.dedent(CHILD), tmp_path], cwd=ROOT, env=ENV,
                         capture_output=True, text=True, timeout=120)
    assert out.returncode == 0, out.stderr
    return tmp_path / 'ledger/ledger.sqlite3', json.loads(out.stdout.strip().splitlines()[-1])


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def tree(root):
    return {str(p.relative_to(root)): digest(p) for p in sorted(Path(root).rglob('*'))
            if p.is_file() and 'ledger' not in p.relative_to(root).parts}


def catalog(tmp_path, sql):
    con = sqlite3.connect(tmp_path / 'store/metadata/catalog.sqlite3')
    try:
        return con.execute(sql).fetchall()
    finally:
        con.close()


def test_branch_gc_dry_run_is_read_only_and_names_exactly_the_unreferenced_branches(tmp_path):
    ledger, ids = build(tmp_path)
    before, ledger_before = tree(tmp_path), digest(ledger)
    report = collect_dead_branches(ledger)
    assert report['status'] == 'dry-run' and report['strict_capacity_guarantee'] is False
    plan = report['stores'][0]
    assert plan['dead_roots'] == sorted([ids['child'], ids['lone']]) and plan['live_roots'] == 1
    assert plan['stale_active_runs'] == ['dead-run'] and plan['removable_versions'] == 2
    assert [n for n, _ in plan['removable_segments']] == ['segment-000001.bin', 'segment-000002.bin']
    assert tree(tmp_path) == before and digest(ledger) == ledger_before
    assert report['owner_death_proof']['pid_or_timestamp_used'] is False


def test_branch_gc_apply_keeps_the_committed_cut_and_is_not_repeatable(tmp_path):
    ledger, ids = build(tmp_path)
    report = collect_dead_branches(ledger)
    with pytest.raises(ValueError):
        collect_dead_branches(ledger, apply=True)
    with pytest.raises(UncommittedRecoveryRefused, match='Plan changed'):
        collect_dead_branches(ledger, apply=True, confirm_plan='1' * 64)
    assert len(catalog(tmp_path, 'SELECT id FROM roots')) == 3
    applied = collect_dead_branches(ledger, apply=True, confirm_plan=report['plan_sha256'])
    assert applied['status'] == 'applied' and applied['reclaimed_segment_bytes'] == report['would_reclaim_segment_bytes'] > 0
    assert catalog(tmp_path, 'SELECT id FROM roots') == [(ids['committed'],)]
    assert catalog(tmp_path, 'SELECT id FROM versions') == [(ids['versions'][0],)]
    assert catalog(tmp_path, 'SELECT count(*) FROM active_runs') == [(0,)]
    assert catalog(tmp_path, 'PRAGMA integrity_check') == [('ok',)]
    assert sorted(p.name for p in (tmp_path / 'store/state').glob('segment-*.bin')) == ['segment-000000.bin']
    # the committed cut still reads back through a normal reopen
    budget = DiskBudget(ledger, [tmp_path / 'store'], quota_bytes=QUOTA, shared_policy=True)
    try:
        store = FileBlockStore(StorageConfig(str(tmp_path / 'store'), 2, backend=BACKEND, segment_size_bytes=4096), 4, create=False, disk_budget=budget)
        view = store.view(ids['committed'], 0)
        assert view.version_id == ids['versions'][0]
        buffer = __import__('mmap').mmap(-1, store.slot_bytes)  # aligned read buffer (O_DIRECT)
        store.read_version(view.version_id, buffer)  # checksum-verified read of the surviving extent
        assert bytes(buffer) == bytes(store.slot_bytes)
        # an unreferenced branch is gone, not resurrected by the reopen
        with pytest.raises(OSError):
            store.root_chain(ids['child']).__next__()
        store.close()
    finally:
        budget.close()
    # nothing left, and the reviewed plan cannot be replayed
    assert collect_dead_branches(ledger)['status'] == 'nothing-to-do'
    with pytest.raises(UncommittedRecoveryRefused):
        collect_dead_branches(ledger, apply=True, confirm_plan=report['plan_sha256'])


def test_manifest_reference_keeps_a_branch_alive(tmp_path):
    ledger, ids = build(tmp_path)
    (tmp_path / 'store/manifests').mkdir(exist_ok=True)
    (tmp_path / 'store/manifests/decision-x.json').write_text(json.dumps({'manifest': {'root': ids['child']}}))
    plan = collect_dead_branches(ledger)['stores'][0]
    assert plan['dead_roots'] == [ids['lone']]  # child (and its ancestor chain) is committed by name
    assert [n for n, _ in plan['removable_segments']] == ['segment-000002.bin']


def test_store_without_any_snapshot_refuses_unless_explicit(tmp_path):
    ledger, ids = build(tmp_path)
    con = sqlite3.connect(tmp_path / 'store/metadata/catalog.sqlite3')
    con.execute('DELETE FROM snapshots'); con.commit(); con.close()
    report = collect_dead_branches(ledger)
    assert report['status'] == 'refused' and 'no committed snapshot' in report['refusals'][0]
    with pytest.raises(UncommittedRecoveryRefused):
        collect_dead_branches(ledger, apply=True, confirm_plan=report['plan_sha256'])
    assert collect_dead_branches(ledger, allow_no_snapshot=True)['status'] == 'dry-run'


def test_branch_gc_refuses_live_owner_and_unknown_catalog_state(tmp_path):
    ledger, ids = build(tmp_path)
    lease = DirectoryLease(tmp_path / 'store')
    try:
        with pytest.raises(UncommittedRecoveryRefused, match='live runtime holds dataset lease'):
            collect_dead_branches(ledger)
        with pytest.raises(UncommittedRecoveryRefused):
            collect_temporary_artifacts(ledger)
    finally:
        lease.close()
    Path(str(tmp_path / 'store/metadata/catalog.sqlite3') + '-journal').write_bytes(b'hot')
    report = collect_dead_branches(ledger)
    assert report['status'] == 'refused' and 'catalog-journal-present' in report['refusals'][0]


def test_branch_gc_refuses_a_snapshot_naming_an_unknown_root(tmp_path):
    ledger, ids = build(tmp_path)
    con = sqlite3.connect(tmp_path / 'store/metadata/catalog.sqlite3')
    con.execute("INSERT INTO snapshots VALUES('s2',?)", (json.dumps({'root': 'f' * 32}),)); con.commit(); con.close()
    from svsim.disk_budget import DiskAdmissionError
    with pytest.raises(DiskAdmissionError, match='unknown catalog root'):
        collect_dead_branches(ledger)


def _temporaries(tmp_path):
    files = {
        'store/manifests/.prepare.json.%s.tmp' % HEX: b'p' * 100,        # stale writer temporary
        'store/assets/matrix.bin.%s.tmp' % ('cd' * 16): b'm' * 300,       # stale writer temporary
        'store/manifests/keep.tmp': b'k' * 50,                            # not a writer-generation name
    }
    for rel, data in files.items():
        path = tmp_path / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)
    return files


def test_temporary_gc_removes_only_unreferenced_generation_temporaries(tmp_path):
    ledger, _ = build(tmp_path)
    files = _temporaries(tmp_path)
    budget = DiskBudget(ledger, [tmp_path / 'store'], quota_bytes=QUOTA, shared_policy=True)
    sid, nonce = '12' * 16, 'ef' * 16
    target = tmp_path / 'store/rank-0000/manifests' / f'decision-{sid}.json'
    target.parent.mkdir(parents=True, exist_ok=True)
    budget.reserve(70, writing=True, owner='ab' * 16, generation=nonce, operation='atomic-json',
                   checkpoint_link={'dataset_root': str(tmp_path / 'store'), 'dataset_id': '34' * 16, 'snapshot_id': sid,
                                    'rank': 0, 'stage': 'decision-copy', 'target': str(target),
                                    'temporary': str(target.with_name('.' + target.name + '.' + nonce + '.tmp')),
                                    'digest': hashlib.sha256(b'd' * 70).hexdigest()})
    budget.close()
    # the intent exists first; the dead writer's temporary appears afterwards (named by that intent)
    intended = target.with_name('.' + target.name + '.' + nonce + '.tmp')
    intended.write_bytes(b'd' * 70)
    before = tree(tmp_path)
    report = collect_temporary_artifacts(ledger)
    assert report['status'] == 'dry-run' and report['would_reclaim_bytes'] == 400
    verdict = {Path(i['path']).name: (i['disposition'], i['reason']) for i in report['items']}
    assert verdict['keep.tmp'][0] == 'keep' and verdict['.decision-%s.json.%s.tmp' % (sid, nonce)] == ('keep', 'referenced-by-checkpoint-intent')
    assert tree(tmp_path) == before  # dry run is read only
    with pytest.raises(UncommittedRecoveryRefused, match='Plan changed'):
        collect_temporary_artifacts(ledger, apply=True, confirm_plan='2' * 64)
    applied = collect_temporary_artifacts(ledger, apply=True, confirm_plan=report['plan_sha256'])
    assert applied['status'] == 'applied' and applied['reclaimed_bytes'] == 400
    after = tree(tmp_path)
    assert set(before) - set(after) == {'store/manifests/.prepare.json.%s.tmp' % HEX, 'store/assets/matrix.bin.%s.tmp' % ('cd' * 16)}
    assert all(after[k] == before[k] for k in after)  # everything else byte identical
    assert collect_temporary_artifacts(ledger)['status'] == 'nothing-to-do'
    with pytest.raises(UncommittedRecoveryRefused):
        collect_temporary_artifacts(ledger, apply=True, confirm_plan=report['plan_sha256'])


def test_temporary_replaced_after_review_is_not_deleted(tmp_path):
    ledger, _ = build(tmp_path)
    _temporaries(tmp_path)
    report = collect_temporary_artifacts(ledger)
    victim = tmp_path / ('store/manifests/.prepare.json.%s.tmp' % HEX)
    victim.write_bytes(b'z' * 101)  # grew after review: the plan digest no longer matches
    with pytest.raises(UncommittedRecoveryRefused):
        collect_temporary_artifacts(ledger, apply=True, confirm_plan=report['plan_sha256'])
    assert victim.read_bytes() == b'z' * 101


def test_temporary_gc_works_without_admission_credit_on_a_full_volume(tmp_path):
    ledger, _ = build(tmp_path)
    _temporaries(tmp_path)
    con = sqlite3.connect(ledger)
    con.execute('UPDATE shared_policy SET quota_bytes=1'); con.commit(); con.close()
    # a normal budget refuses (over quota) but the shrink-only recovery scope still reclaims space
    with pytest.raises(Exception):
        DiskBudget(ledger, [tmp_path / 'store'], quota_bytes=1, shared_policy=True)
    report = collect_temporary_artifacts(ledger)
    applied = collect_temporary_artifacts(ledger, apply=True, confirm_plan=report['plan_sha256'])
    assert applied['reclaimed_bytes'] == 400


def test_reconcile_then_gc_compose_on_one_killed_dataset(tmp_path):
    ledger, ids = build(tmp_path)
    reservations = recover_uncommitted_reservations(ledger)
    assert reservations['status'] in ('dry-run', 'nothing-to-do')
    if reservations['status'] == 'dry-run':
        recover_uncommitted_reservations(ledger, apply=True, confirm_plan=reservations['plan_sha256'])
    report = collect_dead_branches(ledger)
    collect_dead_branches(ledger, apply=True, confirm_plan=report['plan_sha256'])
    assert recover_uncommitted_reservations(ledger)['status'] == 'nothing-to-do'
    assert collect_dead_branches(ledger)['status'] == 'nothing-to-do'
