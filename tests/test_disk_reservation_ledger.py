"""Persistent disk reservation behavior and bounded ledger admission."""
from pathlib import Path
import subprocess
import sys

import pytest

from svsim.disk_budget import DiskAdmissionError, DiskBudget


def test_crash_token_persists_and_materialization_removes_pending_charge(tmp_path):
    usage = [0]
    common = dict(roots=[tmp_path], quota_bytes=100, usage=lambda: usage[0],
                  free=lambda: 1000, drain_reserve_bytes=10)
    first = DiskBudget(tmp_path / 'ledger.sqlite', **common)
    key = first.reserve(50, operation='catalog-transaction', owner='rank-2', generation=8)
    first.mark_writing(key)
    first.materialize(key, 20)
    usage[0] = 20
    assert first.reserved_bytes == 30
    assert first.reservation(key)['state'] == 'WRITING'
    first.close()

    recovered = DiskBudget(tmp_path / 'ledger.sqlite', **common)
    assert recovered.reserved_bytes == 30
    assert recovered.reservation(key)['materialized_bytes'] == 20
    with pytest.raises(DiskAdmissionError):
        recovered.reserve(61, drain=True)
    recovered.reconcile(key, materialized_bytes=20, evidence='inventory:stable-file')
    assert recovered.reserved_bytes == 0
    assert recovered.reservation(key)['state'] == 'RECONCILED'
    recovered.close()


@pytest.mark.parametrize('amount', [True, 1.5, '1', -1])
def test_reservation_rejects_non_integer_or_negative_amount(tmp_path, amount):
    budget = DiskBudget(tmp_path / 'ledger', [tmp_path], usage=lambda: 0, free=lambda: 100)
    with pytest.raises(ValueError):
        budget.reserve(amount)
    budget.close()


def test_bootstrap_is_admitted_before_creating_ledger(tmp_path):
    ledger = tmp_path / 'ledger.sqlite'
    with pytest.raises(DiskAdmissionError, match='bootstrap'):
        DiskBudget(ledger, [tmp_path], quota_bytes=100, min_free_bytes=0)
    assert not ledger.exists()


def test_full_ledger_keeps_room_for_release(tmp_path):
    budget = DiskBudget(tmp_path / 'ledger', [tmp_path], max_records=8,
                        usage=lambda: 0, free=lambda: 10000)
    keys = [budget.reserve(1) for _ in range(6)]
    with pytest.raises(DiskAdmissionError, match='ledger is full'):
        budget.reserve(1)
    budget.release(keys[0])
    assert budget.reserved_bytes == 5
    assert budget.reserve(1)
    budget.close()


def test_roots_on_different_volumes_are_rejected(tmp_path, monkeypatch):
    one, two = tmp_path / 'one', tmp_path / 'two'
    one.mkdir(); two.mkdir()
    original = Path.stat
    def fake_stat(path, *args, **kwargs):
        result = original(path, *args, **kwargs)
        if path == two:
            return type('Stat', (), {'st_dev': result.st_dev + 1})()
        return result
    monkeypatch.setattr(Path, 'stat', fake_stat)
    with pytest.raises(ValueError, match='one volume'):
        DiskBudget(tmp_path / 'ledger', [one, two], usage=lambda: 0, free=lambda: 100)


def test_production_ledger_pool_is_bounded_and_precharged_once(tmp_path):
    pool = 8 * 1024 * 1024
    quota = pool + 1
    first = DiskBudget(tmp_path / 'ledger.sqlite', [tmp_path], quota_bytes=quota)
    second = DiskBudget(tmp_path / 'ledger.sqlite', [tmp_path], quota_bytes=quota)
    status = first.snapshot()
    assert status['ledger_vfs_enforced'] is True
    assert status['strict_capacity_guarantee'] is False
    assert status['metadata_pool_bytes'] == pool
    assert status['metadata_main_cap_bytes'] + status['metadata_side_cap_bytes'] == pool
    assert first.db.execute('PRAGMA max_page_count').fetchone()[0] == first.max_page_count
    assert first.max_page_count * first.db.execute('PRAGMA page_size').fetchone()[0] <= status['metadata_main_cap_bytes']
    key = first.reserve(1, operation='single-byte', owner='rank-0', generation='g1')
    first.release(key)
    first.close()
    second.close()

    reopened = DiskBudget(tmp_path / 'ledger.sqlite', [tmp_path], quota_bytes=quota)
    key = reopened.reserve(1)
    assert reopened.reserved_bytes == 1
    reopened.release(key)
    reopened.close()


def test_injected_capacity_scope_is_never_strict(tmp_path):
    budget = DiskBudget(tmp_path / 'ledger', [tmp_path], usage=lambda: 0, free=lambda: 100)
    snapshot = budget.snapshot()
    assert snapshot['accounting_scope'] == 'injected'
    assert snapshot['strict_capacity_guarantee'] is False
    budget.close()


@pytest.mark.parametrize('kwargs', [
    {'operation': 'o' * 65},
    {'owner': 'あ' * 86},
    {'generation': 'g' * 129},
])
def test_reservation_labels_have_utf8_byte_limits(tmp_path, kwargs):
    budget = DiskBudget(tmp_path / 'ledger', [tmp_path], usage=lambda: 0, free=lambda: 100)
    with pytest.raises(ValueError, match='bytes'):
        budget.reserve(1, **kwargs)
    budget.close()


def test_reconciliation_evidence_is_bounded(tmp_path):
    budget = DiskBudget(tmp_path / 'ledger', [tmp_path], usage=lambda: 0, free=lambda: 100)
    key = budget.reserve(1)
    with pytest.raises(ValueError, match='bytes'):
        budget.reconcile(key, materialized_bytes=0, evidence='e' * 257)
    budget.release(key)
    budget.close()


def test_child_process_crash_keeps_partial_charge_until_explicit_single_recovery(tmp_path):
    ledger = tmp_path / 'ledger.sqlite'
    partial = tmp_path / 'partial-extent.bin'
    script = """\
import os, sys
from svsim.disk_budget import DiskBudget
ledger, root, output = sys.argv[1:]
budget = DiskBudget(ledger, [root], usage=lambda: 0, free=lambda: 10000)
key = budget.reserve(100, operation='partial-extent', owner='rank-0', generation='crash-1', writing=True)
budget.materialize(key, 40)
with open(output, 'wb') as stream:
    stream.write(b'x' * 40)
    stream.flush()
    os.fsync(stream.fileno())
print(key, flush=True)
os._exit(0)
"""
    child = subprocess.run([sys.executable, '-c', script, str(ledger), str(tmp_path), str(partial)],
                           cwd=Path(__file__).resolve().parents[1], capture_output=True,
                           text=True, timeout=30)
    assert child.returncode == 0, child.stderr
    key = child.stdout.strip()
    assert len(key) == 32
    assert partial.stat().st_size == 40

    recovered = DiskBudget(ledger, [tmp_path], quota_bytes=100,
                           usage=lambda: partial.stat().st_size, free=lambda: 10000)
    row = recovered.reservation(key)
    assert row['state'] == 'WRITING'
    assert row['materialized_bytes'] == 40
    assert recovered.reserved_bytes == 60
    with pytest.raises(ValueError, match='erase'):
        recovered.reconcile(key, materialized_bytes=39, evidence='inventory:partial-extent')
    assert recovered.reserved_bytes == 60

    recovered.reconcile(key, materialized_bytes=40,
                        evidence='owner-exited;inventory:partial-extent;bytes=40')
    assert recovered.reserved_bytes == 0
    row = recovered.reservation(key)
    assert row['state'] == 'RECONCILED'
    assert row['materialized_bytes'] == 40
    assert recovered.snapshot()['states']['RECONCILED']['materialized_bytes'] == 40
    with pytest.raises(DiskAdmissionError, match='quota'):
        recovered.reserve(61, operation='would-overbook-partial-extent')
    with pytest.raises(DiskAdmissionError, match='already terminal'):
        recovered.reconcile(key, materialized_bytes=40,
                             evidence='duplicate-recovery-attempt')
    assert recovered.reserved_bytes == 0
    recovered.close()


def test_shared_policy_connections_serialize_and_share_pending_quota(tmp_path):
    ledger = tmp_path / 'shared.sqlite'
    common = dict(roots=[tmp_path], quota_bytes=100, usage=lambda: 0,
                  free=lambda: 1000, shared_policy=True)
    first = DiskBudget(ledger, **common)
    second = DiskBudget(ledger, **common)
    key = first.reserve(100, operation='shared-client')
    assert second.reserved_bytes == 100
    with pytest.raises(DiskAdmissionError, match='quota'):
        second.reserve(1, operation='must-not-overbook')
    assert second.reservation(key)['bytes'] == 100
    first.close()
    second.close()


def test_shared_policy_mismatch_rejects_before_touching_existing_reservation(tmp_path):
    ledger = tmp_path / 'shared.sqlite'
    original = DiskBudget(ledger, [tmp_path], quota_bytes=100, usage=lambda: 0,
                          free=lambda: 1000, shared_policy=True)
    key = original.reserve(60, operation='keep-existing')
    original.close()

    with pytest.raises(DiskAdmissionError, match='policy mismatch'):
        DiskBudget(ledger, [tmp_path], quota_bytes=99, usage=lambda: 0,
                   free=lambda: 1000, shared_policy=True)

    reopened = DiskBudget(ledger, [tmp_path], quota_bytes=100, usage=lambda: 0,
                          free=lambda: 1000, shared_policy=True)
    assert reopened.reservation(key)['state'] == 'RESERVED'
    assert reopened.reserved_bytes == 60
    reopened.release(key)
    reopened.close()


def test_generic_client_cannot_bypass_an_already_bound_shared_policy(tmp_path):
    ledger = tmp_path / 'shared.sqlite'
    shared = DiskBudget(ledger, [tmp_path], quota_bytes=100, usage=lambda: 0,
                        free=lambda: 1000, shared_policy=True)
    key = shared.reserve(60, operation='protected')
    shared.close()

    with pytest.raises(DiskAdmissionError, match='policy mismatch'):
        DiskBudget(ledger, [tmp_path], quota_bytes=1000, usage=lambda: 0,
                   free=lambda: 1000, shared_policy=False)

    reopened = DiskBudget(ledger, [tmp_path], quota_bytes=100, usage=lambda: 0,
                          free=lambda: 1000, shared_policy=True)
    assert reopened.reservation(key)['state'] == 'RESERVED'
    assert reopened.reserved_bytes == 60
    reopened.release(key)
    reopened.close()


def test_legacy_ledger_with_active_token_cannot_be_bound_to_shared_policy(tmp_path):
    ledger = tmp_path / 'legacy.sqlite'
    legacy = DiskBudget(ledger, [tmp_path], usage=lambda: 0, free=lambda: 1000)
    key = legacy.reserve(5, operation='legacy-active')
    legacy.close()

    shared = dict(roots=[tmp_path], usage=lambda: 0, free=lambda: 1000,
                  shared_policy=True)
    with pytest.raises(DiskAdmissionError, match='legacy reservations are active'):
        DiskBudget(ledger, **shared)

    legacy = DiskBudget(ledger, [tmp_path], usage=lambda: 0, free=lambda: 1000)
    assert legacy.reserved_bytes == 5
    legacy.release(key)
    legacy.close()
    bound = DiskBudget(ledger, **shared)
    assert bound.reserved_bytes == 0
    bound.close()


def test_root_limit_is_global_across_clients_and_constructor(tmp_path):
    roots = []
    for index in range(256):
        root = tmp_path / f'root-{index}'
        root.mkdir()
        roots.append(root)
    common = dict(quota_bytes=10000, usage=lambda: 0, free=lambda: 100000,
                  shared_policy=True)
    budget = DiskBudget(tmp_path / 'roots.sqlite', roots[:250], **common)
    other = DiskBudget(tmp_path / 'roots.sqlite', roots[250:], **common)
    extra = tmp_path / 'root-extra'
    extra.mkdir()
    with pytest.raises(DiskAdmissionError, match='root limit'):
        other.register_roots([extra])
    assert budget.db.execute('SELECT count(*) FROM roots').fetchone()[0] == 256
    budget.close()
    other.close()

    too_many = tmp_path / 'too-many'
    too_many.mkdir()
    with pytest.raises(DiskAdmissionError, match='root limit'):
        DiskBudget(tmp_path / 'too-many-roots.sqlite', roots + [too_many], **common)


def test_shared_root_registration_rejects_aggregate_overquota_and_rolls_back(tmp_path):
    first_root = tmp_path / 'first'
    other_root = tmp_path / 'other'
    first_root.mkdir()
    other_root.mkdir()
    payload = other_root / 'existing.bin'
    payload.write_bytes(b'x' * 101)
    include_other = [False]

    def observed():
        roots = (first_root, other_root) if include_other[0] else (first_root,)
        return sum(item.stat().st_size for root in roots
                   for item in root.rglob('*') if item.is_file())

    budget = DiskBudget(tmp_path / 'ledger.sqlite', [first_root], quota_bytes=100,
                        usage=observed, free=lambda: 10000, shared_policy=True)
    include_other[0] = True
    with pytest.raises(DiskAdmissionError, match='shared disk quota'):
        budget.register_roots([other_root])
    assert budget.db.execute('SELECT path FROM roots').fetchall() == [(str(first_root.resolve()),)]
    budget.close()


def test_nested_child_roots_compact_under_parent_without_losing_usage(tmp_path):
    parent = tmp_path / 'dataset'
    parent.mkdir()
    child_root = parent / 'suffixes'
    child_root.mkdir()
    payload = child_root / 'payload.bin'
    payload.write_bytes(b'payload')
    budget = DiskBudget(tmp_path / 'ledger.sqlite', [parent], quota_bytes=10000,
                        usage=lambda: 0, free=lambda: 100000, shared_policy=True)

    for index in range(300):
        child = child_root / f'shift-{index}' / 'nested'
        child.mkdir(parents=True)
        budget.register_roots([child])

    assert budget.db.execute('SELECT path FROM roots').fetchall() == [(str(parent.resolve()),)]
    assert budget._observed_usage() == len(b'payload')
    budget.close()


def test_unrelated_roots_with_sibling_prefix_names_remain_distinct(tmp_path):
    prefix_root = tmp_path / 'Root.A'
    sibling_prefix_root = tmp_path / 'Root.A2'
    prefix_root.mkdir()
    sibling_prefix_root.mkdir()
    budget = DiskBudget(tmp_path / 'ledger.sqlite', [prefix_root], quota_bytes=10000,
                        usage=lambda: 0, free=lambda: 100000, shared_policy=True)
    budget.register_roots([sibling_prefix_root])
    paths = {Path(path) for (path,) in budget.db.execute('SELECT path FROM roots')}
    assert paths == {prefix_root.resolve(), sibling_prefix_root.resolve()}
    budget.close()


def test_incoming_parent_compacts_descendant_and_keeps_existing_bytes(tmp_path):
    ancestor = tmp_path / 'parent'
    descendant = ancestor / 'child' / 'grandchild'
    descendant.mkdir(parents=True)
    (descendant / 'kept.bin').write_bytes(b'keep these bytes')
    initial = DiskBudget(tmp_path / 'ledger.sqlite', [descendant], quota_bytes=10000,
                         usage=lambda: 0, free=lambda: 100000, shared_policy=True)
    initial.register_roots([ancestor])
    assert initial.db.execute('SELECT path FROM roots').fetchall() == [(str(ancestor.resolve()),)]
    assert initial._observed_usage() == len(b'keep these bytes')
    initial.close()


def test_nonoverlapping_root_limit_still_rejects_257th_root(tmp_path):
    control = tmp_path / 'control'
    control.mkdir()
    budget = DiskBudget(tmp_path / 'ledger.sqlite', [control], quota_bytes=10000,
                        usage=lambda: 0, free=lambda: 100000, shared_policy=True)
    siblings = []
    for index in range(256):
        root = tmp_path / f'unrelated-{index}'
        root.mkdir()
        siblings.append(root)
    budget.register_roots(siblings[:255])
    with pytest.raises(DiskAdmissionError, match='root limit'):
        budget.register_roots([siblings[255]])
    assert budget.db.execute('SELECT count(*) FROM roots').fetchone()[0] == 256  # control + 255 siblings
    budget.close()
