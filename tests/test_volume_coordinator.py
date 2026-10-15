"""Per-volume coordination: independent ledgers, declared external jobs, racing processes, lock order, layout refusal."""
import json
import os
import shutil
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
from svsim.volume_coordinator import VolumeCoordinator, filesystem_type

ROOT = Path(__file__).resolve().parents[1]
ENV = {**os.environ, 'OMP_NUM_THREADS': '1'}
COORD_POOL = 128 * 1024
LEDGER_POOL = 256 * 1024
ROOM = 100_000  # bytes of joint credit above the fixed metadata charges
QUOTA = COORD_POOL + 2 * LEDGER_POOL + ROOM
CHUNK = 10_000

pytestmark = pytest.mark.skipif(sys.platform == 'win32', reason='directory flock is supported on Linux/macOS')


def coordinator(tmp_path, **overrides):
    options = dict(quota_bytes=QUOTA, bootstrap_bytes=COORD_POOL)
    options.update(overrides)
    return VolumeCoordinator(tmp_path / 'vol' / 'coordinator.sqlite3', **options)


def ledger(tmp_path, name, coord):
    root = tmp_path / name
    root.mkdir(exist_ok=True)
    return DiskBudget(root / 'ledger.sqlite3', [root], bootstrap_bytes=LEDGER_POOL, volume_coordinator=coord)


def tamper(coord, sql, *args):
    con = sqlite3.connect(coord.path, timeout=30)
    try:
        with con:
            con.execute(sql, args)
    finally:
        con.close()


def count_rows(budget):
    return budget.db.execute('SELECT count(*) FROM reservations').fetchone()[0]


# ---------------------------------------------------------------------------- shared volume budget
def test_two_independent_ledgers_share_the_volume_budget(tmp_path):
    coord = coordinator(tmp_path)
    a, b = ledger(tmp_path, 'a', coord), ledger(tmp_path, 'b', coord)
    try:
        ka = a.reserve(60_000)
        rows_b = count_rows(b)
        with pytest.raises(DiskAdmissionError, match='Volume coordinator quota'):
            b.reserve(50_000)  # each ledger alone has no quota; jointly 110k > 100k
        assert count_rows(b) == rows_b and b.reserved_bytes == 0  # refused before any mutation
        assert coord.snapshot()['pending_bytes'] == 60_000
        kb = b.reserve(40_000)
        assert coord.snapshot()['pending_bytes'] == 100_000
        with pytest.raises(DiskAdmissionError):
            a.reserve(1)
        a.release(ka)
        assert coord.snapshot()['pending_bytes'] == 40_000  # a decrease is visible to the other ledger
        b.reserve(55_000)
        a.reconcile(a.reserve(5_000), materialized_bytes=0, evidence='test')
        assert coord.snapshot()['pending_bytes'] == 95_000
        b.release(kb)
    finally:
        a.close(), b.close(), coord.close()


def test_materialized_bytes_are_not_double_charged_across_ledgers(tmp_path):
    coord = coordinator(tmp_path)
    a, b = ledger(tmp_path, 'a', coord), ledger(tmp_path, 'b', coord)
    try:
        key = a.reserve(50_000)
        (tmp_path / 'a' / 'data.bin').write_bytes(b'x' * 50_000)
        a.materialize(key, 50_000)
        # observed 50k in a's root, pending 0: b may still use the other 50k, not more
        b.reserve(50_000)
        with pytest.raises(DiskAdmissionError):
            b.reserve(1)
    finally:
        a.close(), b.close(), coord.close()


def test_export_inventory_remains_charged_after_materialization_and_reopen(tmp_path):
    coord = coordinator(tmp_path)
    a, b = ledger(tmp_path, 'a', coord), ledger(tmp_path, 'b', coord)
    output = tmp_path / 'output'
    output.mkdir()
    try:
        a.register_export_root(output)
        key = a.reserve(60_000)
        (output / 'state.bin').write_bytes(b'x' * 60_000)
        a.materialize(key, 60_000)
        a.release(key)
        a.close()
        a = ledger(tmp_path, 'a', coord)
        assert coord.snapshot()['pending_bytes'] == 0
        other = b.reserve(40_000)
        with pytest.raises(DiskAdmissionError, match='Volume coordinator quota'):
            b.reserve(1)
        b.release(other)
    finally:
        a.close(), b.close(), coord.close()


def test_export_inventory_quota_refusal_rolls_back_without_mutation(tmp_path):
    coord = coordinator(tmp_path)
    a, b = ledger(tmp_path, 'a', coord), ledger(tmp_path, 'b', coord)
    output = tmp_path / 'output'
    output.mkdir()
    (output / 'old.bin').write_bytes(b'x' * (ROOM + 1))
    try:
        with pytest.raises(DiskAdmissionError, match='Volume coordinator quota'):
            a.register_export_root(output)
        from svsim.export_admission import export_inventory_roots
        assert not export_inventory_roots(a.db)
        assert (output / 'old.bin').stat().st_size == ROOM + 1
        assert not list(output.glob('*.tmp'))
        key = b.reserve(ROOM)
        b.release(key)
    finally:
        a.close(), b.close(), coord.close()


def test_registered_new_dataset_root_is_charged_by_coordinator(tmp_path):
    coord = coordinator(tmp_path)
    a, b = ledger(tmp_path, 'a', coord), ledger(tmp_path, 'b', coord)
    extra = tmp_path / 'extra'
    extra.mkdir()
    (extra / 'data').write_bytes(b'x' * ROOM)
    try:
        a.register_roots([extra])
        with pytest.raises(DiskAdmissionError, match='Volume coordinator quota'):
            b.reserve(1)
    finally:
        a.close(), b.close(), coord.close()


def test_unreadable_inventory_fails_closed_before_reservation(tmp_path, monkeypatch):
    root = tmp_path / 'store'
    root.mkdir()
    budget = DiskBudget(root / 'ledger.sqlite3', [root], bootstrap_bytes=LEDGER_POOL)
    original = os.scandir
    def denied(path):
        if Path(path) == root:
            raise PermissionError('inventory denied')
        return original(path)
    try:
        monkeypatch.setattr(os, 'scandir', denied)
        with pytest.raises(DiskAdmissionError, match='cannot be observed'):
            budget.reserve(1)
        assert count_rows(budget) == 0
    finally:
        budget.close()


def test_bound_ledger_cannot_be_opened_unbound_or_moved(tmp_path):
    coord = coordinator(tmp_path)
    a = ledger(tmp_path, 'a', coord)
    a.close()
    with pytest.raises(DiskAdmissionError, match='bound to a volume coordinator'):
        DiskBudget(tmp_path / 'a' / 'ledger.sqlite3', [tmp_path / 'a'], bootstrap_bytes=LEDGER_POOL)
    other = VolumeCoordinator(tmp_path / 'vol' / 'other.sqlite3', quota_bytes=QUOTA, bootstrap_bytes=COORD_POOL)
    try:
        with pytest.raises(DiskAdmissionError, match='different volume coordinator'):
            DiskBudget(tmp_path / 'a' / 'ledger.sqlite3', [tmp_path / 'a'], bootstrap_bytes=LEDGER_POOL,
                       volume_coordinator=other)
        again = ledger(tmp_path, 'a', coord)
        again.close()
    finally:
        other.close(), coord.close()


def test_policy_and_injected_accounting_are_not_silently_reinterpreted(tmp_path):
    coord = coordinator(tmp_path)
    coord.close()
    with pytest.raises(DiskAdmissionError, match='policy mismatch'):
        coordinator(tmp_path, quota_bytes=QUOTA + 1)
    coord = coordinator(tmp_path)
    try:
        with pytest.raises(ValueError, match='observed accounting'):
            DiskBudget(tmp_path / 'a' / 'l.sqlite3', [tmp_path], usage=lambda: 0, volume_coordinator=coord)
    finally:
        coord.close()


# ---------------------------------------------------------------------------- external jobs
EXTERNAL_CHILD = '''
    import sys, time
    from svsim.volume_coordinator import VolumeCoordinator
    coord_path, quota, pool, lease_dir, amount = sys.argv[1], int(sys.argv[2]), int(sys.argv[3]), sys.argv[4], int(sys.argv[5])
    coord = VolumeCoordinator(coord_path, quota_bytes=quota, bootstrap_bytes=pool)
    claim = coord.declare_external(amount, owner='external-test-job', lease_dir=lease_dir)
    print(claim.id, claim.generation, flush=True)
    time.sleep(120)
'''


def spawn(script, *args):
    return subprocess.Popen([sys.executable, '-c', textwrap.dedent(script), *map(str, args)], cwd=ROOT, env=ENV,
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)


def start_external(tmp_path, amount):
    lease_dir = tmp_path / 'ext-lease'
    lease_dir.mkdir(exist_ok=True)
    child = spawn(EXTERNAL_CHILD, tmp_path / 'vol' / 'coordinator.sqlite3', QUOTA, COORD_POOL, lease_dir, amount)
    line = child.stdout.readline().split()
    assert len(line) == 2, child.stderr.read()
    return child, line[0], lease_dir


def test_external_job_claim_counts_and_is_recovered_after_kill(tmp_path):
    coord = coordinator(tmp_path)
    a, b = ledger(tmp_path, 'a', coord), ledger(tmp_path, 'b', coord)
    child, claim_id, lease_dir = start_external(tmp_path, 70_000)
    try:
        assert coord.snapshot()['pending_bytes'] == 70_000
        with pytest.raises(DiskAdmissionError, match='Volume coordinator quota'):
            a.reserve(40_000)
        with pytest.raises(DiskAdmissionError, match='still holds its lease'):
            coord.plan_external_recovery(claim_id)  # owner alive: no proof of death
        child.send_signal(signal.SIGKILL)
        child.wait(timeout=30)
        # death alone (PID/time) never frees the claim
        with pytest.raises(DiskAdmissionError):
            a.reserve(40_000)
        plan = coord.plan_external_recovery(claim_id)  # dry run changes nothing
        assert coord.snapshot()['external'][claim_id]['state'] == 'ACTIVE'
        with pytest.raises(DiskAdmissionError, match='digest'):
            coord.apply_external_recovery({**plan, 'host': 'tampered'}, expected_plan_sha256=plan['plan_sha256'])
        with pytest.raises(DiskAdmissionError, match='digest'):
            coord.apply_external_recovery(plan, expected_plan_sha256='0' * 64)
        receipt = coord.apply_external_recovery(plan, expected_plan_sha256=plan['plan_sha256'])
        assert receipt['plan_sha256'] == plan['plan_sha256'] and coord.external_receipt(claim_id) == receipt
        assert coord.snapshot()['pending_bytes'] == 0
        with pytest.raises(DiskAdmissionError, match='changed since the plan|not recoverable'):
            coord.apply_external_recovery(plan, expected_plan_sha256=plan['plan_sha256'])  # no double recovery
        a.reserve(90_000)
        coord.forget_recovered(claim_id)
        with pytest.raises(DiskAdmissionError):
            coord.forget_recovered(claim_id)
    finally:
        if child.poll() is None:
            child.kill()
        a.close(), b.close(), coord.close()


def test_external_recovery_refuses_unprovable_or_changed_claims(tmp_path):
    coord = coordinator(tmp_path)
    lease_dir = tmp_path / 'lease'
    lease_dir.mkdir()
    claim = coord.declare_external(10_000, owner='job', lease_dir=lease_dir)
    try:
        claim.abandon()
        plan = coord.plan_external_recovery(claim.id)
        # CAS: any change to the claim after the plan (here a new generation) refuses the plan
        tamper(coord, 'UPDATE external SET generation=generation+1 WHERE id=?', claim.id)
        with pytest.raises(DiskAdmissionError, match='changed since the plan'):
            coord.apply_external_recovery(plan, expected_plan_sha256=plan['plan_sha256'])
        fresh = coord.plan_external_recovery(claim.id)
        # a live holder blocks the proof even after planning
        holder = DirectoryLease(lease_dir)
        with pytest.raises(DiskAdmissionError, match='still holds'):
            coord.apply_external_recovery(fresh, expected_plan_sha256=fresh['plan_sha256'])
        holder.close()
        # a replaced lease directory (different inode) is not the proven one
        shutil.rmtree(lease_dir)
        lease_dir.mkdir()  # may reuse the inode number; the claim marker still proves the incarnation changed
        with pytest.raises(DiskAdmissionError, match='identity changed'):
            coord.plan_external_recovery(claim.id)
        lease_dir.rmdir()
        with pytest.raises(DiskAdmissionError, match='not provable'):
            coord.plan_external_recovery(claim.id)
        # unknown state is refused, not guessed
        tamper(coord, "UPDATE external SET state='WEIRD' WHERE id=?", claim.id)
        lease_dir.mkdir()
        with pytest.raises(DiskAdmissionError, match="state 'WEIRD'"):
            coord.plan_external_recovery(claim.id)
    finally:
        coord.close()


def test_external_claim_resize_release_and_generation(tmp_path):
    coord = coordinator(tmp_path)
    a, b = ledger(tmp_path, 'a', coord), ledger(tmp_path, 'b', coord)
    lease_dir = tmp_path / 'lease'
    lease_dir.mkdir()
    claim = coord.declare_external(30_000, owner='job', lease_dir=lease_dir)
    try:
        generation = claim.generation
        claim.resize(60_000)
        assert claim.generation == generation + 1 and coord.snapshot()['pending_bytes'] == 60_000
        with pytest.raises(DiskAdmissionError, match='quota'):
            claim.resize(ROOM + 1)
        claim.resize(10_000)
        claim.release()
        assert coord.snapshot()['external'] == {} and coord.snapshot()['pending_bytes'] == 0
    finally:
        a.close(), b.close(), coord.close()


# ---------------------------------------------------------------------------- racing processes
RACER = '''
    import json, os, sys, time
    from svsim.volume_coordinator import VolumeCoordinator
    from svsim.disk_budget import DiskAdmissionError, DiskBudget
    kind, coord_path, quota, pool, lpool, work, go, chunk, tag = sys.argv[1:10]
    quota, pool, lpool, chunk = int(quota), int(pool), int(lpool), int(chunk)
    coord = VolumeCoordinator(coord_path, quota_bytes=quota, bootstrap_bytes=pool)
    admitted = 0; held = []
    if kind in ('ledger-a', 'ledger-b'):
        root = os.path.join(work, kind[-1]); os.makedirs(root, exist_ok=True)
        budget = DiskBudget(os.path.join(root, 'ledger.sqlite3'), [root], bootstrap_bytes=lpool, volume_coordinator=coord)
    else:
        lease = os.path.join(work, 'lease-' + tag); os.makedirs(lease, exist_ok=True)
    print('ready', flush=True)
    while not os.path.exists(go): time.sleep(0.001)
    while True:
        try:
            if kind == 'external': held.append(coord.declare_external(chunk, owner='racer-' + tag, lease_dir=lease))
            else: budget.reserve(chunk)
        except DiskAdmissionError: break
        admitted += 1
    print(json.dumps({'admitted': admitted}), flush=True)
    time.sleep(120)  # stay alive holding leases; the parent kills the racer
'''


def run_race(tmp_path, kinds):
    go = tmp_path / 'go'
    coord_path = tmp_path / 'vol' / 'coordinator.sqlite3'
    (tmp_path / 'vol').mkdir(exist_ok=True)
    children = [spawn(RACER, kind, coord_path, QUOTA, COORD_POOL, LEDGER_POOL, tmp_path, go, CHUNK, i)
                for i, kind in enumerate(kinds)]
    try:
        for child in children:
            assert child.stdout.readline().strip() == 'ready', child.stderr.read()
        go.write_text('go')
        admitted = []
        deadline = time.monotonic() + 90
        for child in children:
            admitted.append(json.loads(child.stdout.readline())['admitted'])
            assert time.monotonic() < deadline, 'racers did not finish (lock-order deadlock?)'
        return admitted
    finally:
        for child in children:
            child.kill()
            child.wait(timeout=30)


def test_racing_processes_admit_exactly_the_permitted_amount(tmp_path):
    admitted = run_race(tmp_path, ['ledger-a', 'ledger-a', 'ledger-b', 'ledger-b', 'external', 'external'])
    assert sum(admitted) == ROOM // CHUNK  # exactly the last bytes, never one chunk more
    coord = coordinator(tmp_path)
    try:
        snap = coord.snapshot()
        assert snap['pending_bytes'] == ROOM
        # each ledger claim equals the true pending of its ledger (a killed racer keeps its tokens)
        a, b = ledger(tmp_path, 'a', coord), ledger(tmp_path, 'b', coord)
        try:
            assert a.reserved_bytes + b.reserved_bytes + sum(
                v['bytes'] for v in coord.snapshot()['external'].values()) == ROOM
        finally:
            a.close(), b.close()
    finally:
        coord.close()


def test_lock_order_ledger_catalog_and_coordinator_do_not_deadlock(tmp_path):
    big = 512 * 1024**2  # the catalog lease covers a large page envelope
    coordinator(tmp_path, quota_bytes=big).close()
    script = '''
        import os, sys, time, random
        from svsim.catalog_admission import CatalogConnection
        from svsim.disk_budget import DiskAdmissionError, DiskBudget
        from svsim.volume_coordinator import VolumeCoordinator
        kind, coord_path, quota, pool, lpool, work = sys.argv[1:7]
        coord = VolumeCoordinator(coord_path, quota_bytes=int(quota), bootstrap_bytes=int(pool))
        root = os.path.join(work, kind); os.makedirs(root, exist_ok=True)
        budget = DiskBudget(os.path.join(root, 'ledger.sqlite3'), [root], bootstrap_bytes=int(lpool), volume_coordinator=coord)
        db = CatalogConnection(os.path.join(root, 'catalog.sqlite3'), budget_getter=lambda: budget, require_native=True)
        db.execute('CREATE TABLE IF NOT EXISTS items(value BLOB)')
        rng = random.Random(kind)
        done = refused = 0
        for step in range(150):
            if kind == 'a' or step % 3:
                keys = []
                try:
                    for _ in range(3): keys.append(budget.reserve(rng.choice((500, 2000))))
                except DiskAdmissionError: refused += 1
                for key in keys:
                    budget.materialize(key, 0); budget.release(key)
            if kind == 'a':
                with db: db.execute('INSERT INTO items VALUES(?)', (b'x' * 200,))
                done += 1
        print('done', done, refused, flush=True)
    '''
    work = tmp_path
    children = [spawn(script, kind, tmp_path / 'vol' / 'coordinator.sqlite3', big, COORD_POOL, LEDGER_POOL, work)
                for kind in ('a', 'b', 'b')]
    for child in children:
        out, err = child.communicate(timeout=120)
        assert child.returncode == 0 and out.startswith('done'), err
    coord = coordinator(tmp_path, quota_bytes=big)
    try:
        a, b = ledger(tmp_path, 'a', coord), ledger(tmp_path, 'b', coord)
        try:  # exited catalog writers retain their lease tokens; the republished claims equal the ledgers' truth
            assert coord.snapshot()['pending_bytes'] == a.reserved_bytes + b.reserved_bytes > 0
        finally:
            a.close(), b.close()
    finally:
        coord.close()


def test_dead_ledger_keeps_a_conservative_claim_until_recovered_and_reopened(tmp_path):
    coord = coordinator(tmp_path)
    coord.close()
    script = '''
        import os, sys
        from svsim.disk_budget import DiskBudget
        from svsim.volume_coordinator import VolumeCoordinator
        coord = VolumeCoordinator(sys.argv[1], quota_bytes=int(sys.argv[2]), bootstrap_bytes=int(sys.argv[3]))
        root = sys.argv[4]; os.makedirs(root, exist_ok=True)
        budget = DiskBudget(os.path.join(root, 'ledger.sqlite3'), [root], bootstrap_bytes=int(sys.argv[5]), volume_coordinator=coord)
        budget.reserve(80_000, writing=True)
        os._exit(0)
    '''
    child = spawn(script, tmp_path / 'vol' / 'coordinator.sqlite3', QUOTA, COORD_POOL, tmp_path / 'a', LEDGER_POOL)
    assert child.wait(timeout=60) == 0, child.stderr.read()
    coord = coordinator(tmp_path)
    b = ledger(tmp_path, 'b', coord)
    try:
        assert coord.snapshot()['pending_bytes'] == 80_000
        with pytest.raises(DiskAdmissionError):
            b.reserve(30_000)
        with pytest.raises(DiskAdmissionError, match='pending bytes'):
            coord.forget_ledger(tmp_path / 'a' / 'ledger.sqlite3')
        a = ledger(tmp_path, 'a', coord)  # writable reopen republishes the true state; tokens are not auto-freed
        (key,) = [r[0] for r in a.db.execute("SELECT id FROM reservations WHERE state='WRITING'")]
        assert a.reserved_bytes == 80_000 and coord.snapshot()['pending_bytes'] == 80_000
        a.reconcile(key, materialized_bytes=0, evidence='test-recovery;owner-exited')
        assert coord.snapshot()['pending_bytes'] == 0
        b.reserve(30_000)
        a.close()
        coord.forget_ledger(tmp_path / 'a' / 'ledger.sqlite3')
        assert str(tmp_path / 'a' / 'ledger.sqlite3') not in coord.snapshot()['ledgers']
    finally:
        b.close(), coord.close()


# ---------------------------------------------------------------------------- layout refusal
@pytest.mark.parametrize('fstype', ['nfs4', 'cifs', 'smbfs', 'lustre', 'fuse.sshfs', 'virtiofs', '9p', 'nonlocal:afpfs', '', 'weirdfs'])
def test_unsupported_or_unknown_filesystem_is_refused(tmp_path, fstype):
    with pytest.raises(DiskAdmissionError, match='Unsupported filesystem'):
        coordinator(tmp_path, filesystem_probe=lambda path: fstype)
    assert not (tmp_path / 'vol' / 'coordinator.sqlite3').exists()  # refused before any file is created


def test_undeterminable_layout_is_refused(tmp_path):
    def broken(path):
        raise DiskAdmissionError('Unknown filesystem layout: test')
    with pytest.raises(DiskAdmissionError, match='Unknown filesystem layout'):
        coordinator(tmp_path, filesystem_probe=broken)


def test_real_filesystem_probe_names_a_local_type(tmp_path):
    name = filesystem_type(tmp_path)
    assert isinstance(name, str) and name and not name.startswith('nonlocal:')
    coordinator(tmp_path).close()  # the real probe accepts this host's local temporary filesystem


def test_database_bound_to_another_host_is_refused(tmp_path):
    coordinator(tmp_path, host='node-a').close()
    with pytest.raises(DiskAdmissionError, match='another host'):
        coordinator(tmp_path, host='node-b')
    coordinator(tmp_path, host='node-a').close()


def test_ledger_on_another_volume_is_refused(tmp_path):
    coord = coordinator(tmp_path)
    coord.volume_device += 1  # a coordinator on a different device than the budgeted roots
    try:
        with pytest.raises(DiskAdmissionError, match='another volume'):
            ledger(tmp_path, 'a', coord)
    finally:
        coord.close()
