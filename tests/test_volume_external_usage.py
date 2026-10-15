"""Physical external-writer accounting and fail-closed admission boundaries."""
import os
import shutil
import subprocess
import sys

import pytest

from svsim.disk_budget import DiskAdmissionError, DiskBudget
from svsim.volume_coordinator import VolumeCoordinator

POOL = 128 * 1024
LEDGER = 256 * 1024


@pytest.fixture
def setup(tmp_path):
    coord = VolumeCoordinator(tmp_path / 'volume' / 'coordinator.sqlite3',
                              quota_bytes=POOL + LEDGER + 100_000, bootstrap_bytes=POOL)
    dataset = tmp_path / 'dataset'
    dataset.mkdir()
    budget = DiskBudget(dataset / 'ledger.sqlite3', [dataset], bootstrap_bytes=LEDGER,
                        volume_coordinator=coord)
    root, lease = tmp_path / 'external', tmp_path / 'lease'
    root.mkdir()
    lease.mkdir()
    claims = []

    def declare(amount=60_000, **kw):
        claim = coord.declare_external(amount, owner='external writer', lease_dir=lease,
                                       roots=kw.pop('roots', [root]), **kw)
        claims.append(claim)
        return claim

    yield coord, budget, root, declare
    for claim in claims:
        claim.abandon()
    budget.close()
    coord.close()


def test_external_process_growth_consumes_claim_once_and_excess_blocks_mutation(setup):
    coord, budget, root, declare = setup
    claim = declare()
    target = root / 'written.bin'
    subprocess.run([sys.executable, '-c',
                    'import pathlib,sys; pathlib.Path(sys.argv[1]).write_bytes(b"x"*60000)',
                    str(target)], check=True)
    snap = coord.snapshot()
    assert snap['external'][claim.id]['observed_growth_bytes'] == 60_000
    assert snap['external'][claim.id]['usage_monitoring'] == 'net-file-growth'
    assert snap['pending_bytes'] == 0
    key = budget.reserve(40_000)  # 60k physical + 40k pending, not 120k + 40k
    target.write_bytes(b'x' * 60_001)
    rows = budget.db.execute('SELECT count(*) FROM reservations').fetchone()[0]
    for operation in (lambda: budget.reserve(1), coord.snapshot, coord.check_external_usage,
                      claim.release, lambda: claim.resize(60_000)):
        with pytest.raises(DiskAdmissionError, match='exceeded declared bytes'):
            operation()
    assert budget.db.execute('SELECT count(*) FROM reservations').fetchone()[0] == rows
    assert coord._external_row(claim.id)['bytes'] == 60_000
    assert coord._external_row(claim.id)['state'] == 'ACTIVE'
    budget.release(key)  # safe downward ledger publication remains possible
    claim.resize(60_001)  # explicit new credit may cure the excess within the volume budget
    assert coord.check_external_usage()[claim.id]['pending_bytes'] == 0
    claim.release()
    assert coord.snapshot()['external'] == {}
    budget.reserve(39_999)  # released claim does not erase the already written files
    with pytest.raises(DiskAdmissionError, match='quota'):
        budget.reserve(1)


def test_baseline_and_nested_roots_do_not_double_count(setup):
    coord, budget, root, declare = setup
    child = root / 'child'
    child.mkdir()
    (child / 'prior.bin').write_bytes(b'x' * 10_000)
    claim = declare(60_000, roots=[root, child, root])
    (child / 'new.bin').write_bytes(b'x' * 30_000)
    details = coord.snapshot()['external'][claim.id]
    assert details['observed_growth_bytes'] == 30_000
    assert details['pending_bytes'] == 30_000
    budget.reserve(30_000)  # 40k physical + 30k external pending + 30k ledger pending
    with pytest.raises(DiskAdmissionError, match='quota'):
        budget.reserve(1)


@pytest.mark.parametrize('damage', ['removed', 'replaced', 'marker-removed', 'symlink', 'hardlink', 'unreadable'])
def test_unprovable_external_root_blocks_admission(setup, monkeypatch, damage):
    coord, budget, root, declare = setup
    claim = declare()
    if damage == 'removed':
        shutil.rmtree(root)
    elif damage == 'replaced':
        shutil.rmtree(root)
        root.mkdir()  # inode reuse must not restore the missing incarnation marker
    elif damage == 'marker-removed':
        (root / f'.svsim-volume-root-{claim.id}').unlink()
    elif damage == 'symlink':
        (root / 'link').symlink_to(root.parent)
    elif damage == 'hardlink':
        (root / 'file').write_bytes(b'x')
        os.link(root / 'file', root / 'alias')
    else:
        original = os.scandir

        def denied(path):
            if os.fspath(path) == str(root):
                raise PermissionError('external root inaccessible')
            return original(path)

        monkeypatch.setattr(os, 'scandir', denied)
    with pytest.raises(DiskAdmissionError, match='External root'):
        budget.reserve(1)
    with pytest.raises(DiskAdmissionError, match='External root'):
        claim.release()
    assert coord._external_row(claim.id)['state'] == 'ACTIVE'


def test_overlapping_external_and_ledger_roots_are_rejected(setup):
    coord, budget, root, declare = setup
    with pytest.raises(DiskAdmissionError, match='overlaps'):
        declare(roots=[root.parent / 'dataset'])
    with pytest.raises(DiskAdmissionError, match='metadata'):
        declare(roots=[coord.path.parent])
    claim = declare()
    with pytest.raises(DiskAdmissionError, match='overlaps'):
        declare(roots=[root])
    with pytest.raises(DiskAdmissionError, match='overlaps'):
        coord.register_ledger(root / 'other.sqlite3', metadata_bytes=0, roots=[root], pending=0, epoch=0)
    with pytest.raises(DiskAdmissionError, match='overlaps'):
        coord.register_ledger(root / 'other.sqlite3', metadata_bytes=0, roots=[], pending=0, epoch=0)
    assert len(coord.snapshot()['external']) == 1
    assert coord.snapshot()['external'][claim.id]['pending_bytes'] == 60_000


def test_rootless_legacy_claim_retains_full_declaration(setup):
    coord, budget, root, declare = setup
    claim = declare(roots=[])
    (root / 'untracked').write_bytes(b'x' * 60_001)
    details = coord.snapshot()['external'][claim.id]
    assert details['usage_monitoring'] == 'declaration-only'
    assert details['pending_bytes'] == 60_000
    assert coord.snapshot()['strict_capacity_guarantee'] is False
    budget.reserve(40_000)


def test_monitoring_survives_reopen_and_dead_owner_recovery(setup):
    coord, budget, root, declare = setup
    claim = declare()
    (root / 'data').write_bytes(b'x' * 60_001)
    claim.abandon()
    second = VolumeCoordinator.open_bound(coord.path)
    try:
        with pytest.raises(DiskAdmissionError, match='exceeded declared bytes'):
            second.check_external_usage()
        plan = second.plan_external_recovery(claim.id)
        assert second._external_row(claim.id)['state'] == 'ACTIVE'
        second.apply_external_recovery(plan, expected_plan_sha256=plan['plan_sha256'])
        assert second.snapshot()['external'][claim.id]['state'] == 'RECOVERED'
        budget.reserve(39_999)
        with pytest.raises(DiskAdmissionError, match='quota'):
            budget.reserve(1)
    finally:
        second.close()


def test_invalid_root_declaration_rolls_back_and_cleans_markers(setup):
    coord, budget, root, declare = setup
    other = root.parent / 'other'
    other.mkdir()
    (other / 'bad-link').symlink_to(root)
    with pytest.raises(DiskAdmissionError, match='symlink'):
        declare(roots=[root, other])
    assert coord.snapshot()['external'] == {}
    assert not list(root.glob('.svsim-volume-root-*'))
    assert coord.db.execute('SELECT count(*) FROM external_roots').fetchone()[0] == 0
    alias = root.parent / 'alias'
    alias.symlink_to(root, target_is_directory=True)
    with pytest.raises(DiskAdmissionError, match='symlink'):
        declare(roots=[alias])


def test_old_coordinator_without_monitor_table_opens_compatibly(setup):
    coord, budget, root, declare = setup
    claim = declare(roots=[])
    with coord._txn():
        coord.db.execute('DROP TABLE external_roots')
    second = VolumeCoordinator.open_bound(coord.path)
    try:
        assert second.snapshot()['external'][claim.id]['usage_monitoring'] == 'declaration-only'
        assert second.snapshot()['pending_bytes'] == 60_000
    finally:
        second.close()


def test_recovered_and_released_files_remain_observed_and_unreadable_fails_closed(setup, monkeypatch):
    coord, budget, root, declare = setup
    claim = declare()
    (root / 'data').write_bytes(b'x' * 20_000)
    claim.abandon()
    plan = coord.plan_external_recovery(claim.id)
    coord.apply_external_recovery(plan, expected_plan_sha256=plan['plan_sha256'])
    marker = root / f'.svsim-volume-root-{claim.id}'
    assert marker.exists()
    coord.forget_recovered(claim.id)
    assert not marker.exists()
    original = os.scandir

    def denied(path):
        if os.fspath(path) == str(root):
            raise PermissionError('retained external root inaccessible')
        return original(path)

    monkeypatch.setattr(os, 'scandir', denied)
    with pytest.raises(DiskAdmissionError, match='cannot be observed'):
        budget.reserve(1)
    monkeypatch.setattr(os, 'scandir', original)
    budget.reserve(80_000)
    with pytest.raises(DiskAdmissionError, match='quota'):
        budget.reserve(1)
    with pytest.raises(DiskAdmissionError, match='overlaps'):
        declare()  # the original root remains in the observed union after recovery/forget
