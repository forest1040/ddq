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
