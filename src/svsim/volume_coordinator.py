"""Per-volume coordination of independent disk ledgers and declared external jobs.

A ``VolumeCoordinator`` is one small SQLite database (on the bounded native VFS) that lives on a
node-local volume. Every participating ``DiskBudget`` keeps its own ledger and its own tokens; the
coordinator only holds a *published claim* per ledger (its pending, unmaterialized bytes) and the
union of the participants' roots. A ledger asks the coordinator before it inserts a reservation, so
the sum of all claims, all declared external jobs and the observed usage of all registered roots is
checked against one volume policy. The coordinator is a leaf in the lock order::

    thread path lock -> ledger BEGIN IMMEDIATE -> coordinator thread lock -> coordinator BEGIN IMMEDIATE

and never opens, locks or reads another ledger.

Claims are published conservatively. An increase is committed to the coordinator before the ledger
commit; a decrease is published after the ledger commit. Each ledger carries a monotone epoch so a
late publish of an older state cannot overwrite a newer one. A crash can leave a claim higher than
the ledger's true pending (never lower); the next writable open of the ledger republishes it.

External jobs are *declared*: an outside process registers a claim with a lease directory it keeps
a shared flock on. After it dies, an exclusive flock on that directory proves no process holds it
(never a PID or clock). Recovery is dry-run -> plan digest -> compare-and-swap on the claim's
generation -> stored receipt. A job that uses more than it declared is not detected here.

The coordinator is single-host. It refuses a filesystem it cannot prove node-local and a database
bound to another hostname; shared filesystems are unsupported rather than guessed.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import socket
import sqlite3
import stat
import sys
import threading
from contextlib import contextmanager
from pathlib import Path
from uuid import uuid4

from .dataset_lease import DatasetBusyError, DirectoryLease
from .disk_budget import DiskAdmissionError, _bounded_text, _integer, _path_lock, _root_antichain

MAX_LEDGERS = 64
MAX_EXTERNALS = 256
MAX_ROOTS = 256
PROOF_MAX_BYTES = 8192

_LOCAL_FILESYSTEMS = frozenset({
    'ext2', 'ext3', 'ext4', 'xfs', 'btrfs', 'tmpfs', 'ramfs', 'overlay', 'overlayfs', 'zfs', 'f2fs',
    'jfs', 'reiserfs', 'apfs', 'hfs'})

_TABLES = {
    'policy': """CREATE TABLE IF NOT EXISTS policy(
                    singleton INTEGER PRIMARY KEY CHECK(singleton=1), quota_bytes INTEGER,
                    min_free_bytes INTEGER NOT NULL, drain_reserve_bytes INTEGER NOT NULL,
                    bootstrap_bytes INTEGER NOT NULL, max_page_count INTEGER NOT NULL,
                    volume_device INTEGER NOT NULL, host TEXT NOT NULL, filesystem TEXT NOT NULL)""",
    'ledgers': """CREATE TABLE IF NOT EXISTS ledgers(
                    path TEXT PRIMARY KEY, metadata_bytes INTEGER NOT NULL,
                    epoch INTEGER NOT NULL, pending INTEGER NOT NULL)""",
    'roots': 'CREATE TABLE IF NOT EXISTS roots(path TEXT PRIMARY KEY)',
    'external': """CREATE TABLE IF NOT EXISTS external(
                    id TEXT PRIMARY KEY, owner TEXT NOT NULL, bytes INTEGER NOT NULL,
                    lease_dir TEXT NOT NULL, lease_dev INTEGER NOT NULL, lease_ino INTEGER NOT NULL,
                    generation INTEGER NOT NULL, state TEXT NOT NULL, proof TEXT)""",
}


def filesystem_type(path):
    """Filesystem type name of the mount holding ``path``; raises if it cannot be determined."""
    path = Path(path).resolve()
    if sys.platform.startswith('linux'):
        best, best_type = None, None
        try:
            with open('/proc/self/mountinfo', encoding='utf-8') as handle:
                lines = handle.read().splitlines()
        except OSError as exc:
            raise DiskAdmissionError(f'Unknown filesystem layout: {exc}') from exc
        for line in lines:
            head, sep, tail = line.partition(' - ')
            fields = head.split()
            if not sep or len(fields) < 5 or not tail.split():
                continue
            mount = Path(re.sub(r'\\([0-7]{3})', lambda m: chr(int(m.group(1), 8)), fields[4]))
            if path == mount or path.is_relative_to(mount):
                if best is None or len(mount.parts) >= len(best.parts):
                    best, best_type = mount, tail.split()[0]
        if best_type is None:
            raise DiskAdmissionError('Unknown filesystem layout: no mount entry covers the path')
        return best_type
    if sys.platform == 'darwin':
        import ctypes

        class _StatFS(ctypes.Structure):
            _fields_ = [('f_bsize', ctypes.c_uint32), ('f_iosize', ctypes.c_int32), ('f_blocks', ctypes.c_uint64),
                        ('f_bfree', ctypes.c_uint64), ('f_bavail', ctypes.c_uint64), ('f_files', ctypes.c_uint64),
                        ('f_ffree', ctypes.c_uint64), ('f_fsid', ctypes.c_int32 * 2), ('f_owner', ctypes.c_uint32),
                        ('f_type', ctypes.c_uint32), ('f_flags', ctypes.c_uint32), ('f_fssubtype', ctypes.c_uint32),
                        ('f_fstypename', ctypes.c_char * 16), ('f_mntonname', ctypes.c_char * 1024),
                        ('f_mntfromname', ctypes.c_char * 1024), ('f_flags_ext', ctypes.c_uint32),
                        ('f_reserved', ctypes.c_uint32 * 7)]
        info = _StatFS()
        libc = ctypes.CDLL(None, use_errno=True)
        if libc.statfs(os.fsencode(str(path)), ctypes.byref(info)) != 0:
            raise DiskAdmissionError('Unknown filesystem layout: statfs failed')
        if not info.f_flags & 0x1000:  # MNT_LOCAL
            return 'nonlocal:' + info.f_fstypename.decode()
        return info.f_fstypename.decode()
    raise DiskAdmissionError(f'Unknown filesystem layout: unsupported platform {sys.platform}')


def _marker(lease_dir, claim_id):
    return Path(lease_dir) / f'.svsim-volume-claim-{claim_id}'


def _canonical(document):
    return json.dumps(document, sort_keys=True, separators=(',', ':'))


class ExternalClaim:
    """A declared reservation of an outside job, tied to a lease directory it keeps open."""

    def __init__(self, coordinator, claim_id, generation, lease):
        self._coordinator, self.id, self.generation, self._lease = coordinator, claim_id, generation, lease

    def resize(self, amount, *, drain=False):
        self.generation = self._coordinator._external_resize(self.id, self.generation, amount, drain)

    def release(self):
        if self._lease is None:
            return
        self._coordinator._external_release(self.id, self.generation)
        self._lease.close()
        self._lease = None

    def abandon(self):
        """Close the lease without releasing: the claim stays until proven dead and recovered."""
        if self._lease is not None:
            self._lease.close()
            self._lease = None


class VolumeCoordinator:
    def __init__(self, path, *, quota_bytes=None, min_free_bytes=0, drain_reserve_bytes=0,
                 bootstrap_bytes=1024 * 1024, filesystem_probe=filesystem_type, host=None):
        _integer(quota_bytes, 'volume quota', minimum=1, optional=True)
        _integer(min_free_bytes, 'volume minimum free bytes')
        _integer(drain_reserve_bytes, 'volume drain reserve bytes')
        _integer(bootstrap_bytes, 'coordinator bootstrap bytes', minimum=8192)
        try:
            from .catalog_admission import get_sqlite_guard
            self._guard = get_sqlite_guard()
        except (ImportError, NotImplementedError, OSError, sqlite3.Error) as exc:
            raise DiskAdmissionError(f'Bounded coordinator VFS unavailable: {exc}') from exc
        self.quota_bytes, self.min_free_bytes = quota_bytes, min_free_bytes
        self.drain_reserve_bytes, self.bootstrap_bytes = drain_reserve_bytes, bootstrap_bytes
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        self.path = path.parent.resolve(strict=True) / path.name
        self.host = host if host is not None else socket.gethostname()
        self._lock = _path_lock(self.path)
        self._probe = filesystem_probe
        self.volume_device = self.path.parent.stat().st_dev
        self.filesystem = self._probe(self.path.parent)
        if self.filesystem not in _LOCAL_FILESYSTEMS:
            raise DiskAdmissionError(f'Unsupported filesystem for volume coordination: {self.filesystem!r} '
                                     '(only node-local filesystems; shared or unknown layouts are refused)')
        if self.path.exists():
            info = self.path.lstat()
            if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
                raise DiskAdmissionError('Coordinator database must be an exclusively named regular file')
        pages = (bootstrap_bytes // 2) // 4096
        self.max_page_count = pages
        self._main_cap, self._side_cap = pages * 4096, bootstrap_bytes - pages * 4096
        self._closed = False
        self.db = self._guard.connect(self.path, main_cap_bytes=self._main_cap, side_cap_bytes=self._side_cap)
        self.db.isolation_level = None
        try:
            with self._lock:
                self._activate(True)
                self.db.execute('PRAGMA synchronous=FULL')
                self.db.execute('PRAGMA journal_mode=DELETE').fetchone()
                size = self.db.execute('PRAGMA page_size').fetchone()[0]
                cap = self._main_cap // size
                if self.db.execute(f'PRAGMA max_page_count={cap}').fetchone()[0] != cap:
                    raise DiskAdmissionError('Coordinator page cap could not be enforced')
                self.max_page_count = cap
                with self._txn(active_already=True):
                    for sql in _TABLES.values():
                        self.db.execute(sql)
                    self._bind_policy()
                self._activate(False)
        except BaseException:
            self.db.close()
            self._closed = True
            raise

    # -- plumbing ---------------------------------------------------------------------------
    def _activate(self, active):
        self.db.execute('SELECT svsim_disk_guard_config(?,?,?)',
                        (self._main_cap, self._side_cap, int(bool(active)))).fetchone()

    @contextmanager
    def _txn(self, *, active_already=False):
        with self._lock:
            if not active_already:
                self._activate(True)
            try:
                self.db.execute('BEGIN IMMEDIATE')
                yield
                self.db.execute('COMMIT')
            except BaseException:
                if self.db.in_transaction:
                    self.db.execute('ROLLBACK')
                raise
            finally:
                if not active_already:
                    self._activate(False)

    def _bind_policy(self):
        expected = (self.quota_bytes, self.min_free_bytes, self.drain_reserve_bytes, self.bootstrap_bytes,
                    self.max_page_count, int(self.volume_device), self.host, self.filesystem)
        row = self.db.execute('''SELECT quota_bytes,min_free_bytes,drain_reserve_bytes,bootstrap_bytes,max_page_count,
            volume_device,host,filesystem FROM policy WHERE singleton=1''').fetchone()
        if row is None:
            self.db.execute('''INSERT INTO policy(singleton,quota_bytes,min_free_bytes,drain_reserve_bytes,
                bootstrap_bytes,max_page_count,volume_device,host,filesystem) VALUES(1,?,?,?,?,?,?,?,?)''', expected)
            return
        stored = tuple(row)
        if stored[6] != self.host:
            raise DiskAdmissionError('Volume coordinator is bound to another host; shared filesystems are unsupported')
        if stored != expected:
            raise DiskAdmissionError('Volume coordinator policy mismatch')

    def close(self):
        if self._closed:
            return
        with self._lock:
            try:
                self._activate(False)
                self.db.close()
            finally:
                self._closed = True

    # -- accounting -------------------------------------------------------------------------
    def _allocated(self, ledger_path):
        total = 0
        for suffix in ('', '-journal'):
            try:
                info = os.lstat(str(ledger_path) + suffix)
            except FileNotFoundError:
                continue
            if stat.S_ISREG(info.st_mode):
                total += info.st_size
        return total

    def _usage(self, excluded):
        roots = sorted((Path(p) for (p,) in self.db.execute('SELECT path FROM roots')), key=lambda p: len(p.parts))
        selected, total = [], 0
        for root in roots:
            if any(root.is_relative_to(parent) for parent in selected):
                continue
            selected.append(root)
            if not root.exists():
                continue
            for path in root.rglob('*'):
                if str(path) in excluded:
                    continue
                try:
                    info = path.stat(follow_symlinks=False)
                except FileNotFoundError:
                    continue
                if stat.S_ISREG(info.st_mode):
                    total += info.st_size
        return total

    def _volume_check(self, *, ledger=None, ledger_pending=0, add_external=0, drain=False):
        """Check the whole volume with one participant's pending replaced (inside a transaction)."""
        rows = self.db.execute('SELECT path,metadata_bytes,pending FROM ledgers').fetchall()
        pending = add_external + self.db.execute(
            "SELECT COALESCE(SUM(bytes),0) FROM external WHERE state='ACTIVE'").fetchone()[0]
        metadata = self.bootstrap_bytes
        unallocated = max(0, self.bootstrap_bytes - self._allocated(self.path))
        excluded = {str(self.path), str(self.path) + '-journal'}
        for path, md, pend in rows:
            pending += ledger_pending if path == ledger else pend
            metadata += md
            unallocated += max(0, md - self._allocated(path))
            excluded.update((path, path + '-journal'))
        margin = 0 if drain else self.drain_reserve_bytes
        if self.quota_bytes is not None and self._usage(excluded) + metadata + pending + margin > self.quota_bytes:
            raise DiskAdmissionError('Volume coordinator quota exceeded')
        st = os.statvfs(self.path.parent)
        if st.f_bavail * st.f_frsize - unallocated - pending - margin < self.min_free_bytes:
            raise DiskAdmissionError('Volume coordinator minimum free space violated')

    def _merge_roots(self, roots):
        existing = [Path(p) for (p,) in self.db.execute('SELECT path FROM roots')]
        selected = _root_antichain(existing + [Path(r) for r in roots])
        if len(selected) > MAX_ROOTS:
            raise DiskAdmissionError('Volume coordinator root limit exceeded')
        for root in selected:
            if root.exists() and root.stat().st_dev != self.volume_device:
                raise DiskAdmissionError('Root is not on the coordinated volume')
        self.db.execute('DELETE FROM roots')
        self.db.executemany('INSERT INTO roots VALUES(?)', [(str(p),) for p in selected])

    # -- ledger participation (called by DiskBudget inside its ledger transaction) ------------
    def register_ledger(self, ledger_path, *, metadata_bytes, roots, pending, epoch):
        """Authoritatively (re)publish a ledger's claim; the caller holds the ledger's write lock."""
        ledger_path = str(ledger_path)
        with self._txn():
            known = self.db.execute('SELECT 1 FROM ledgers WHERE path=?', (ledger_path,)).fetchone()
            if known is None and self.db.execute('SELECT count(*) FROM ledgers').fetchone()[0] >= MAX_LEDGERS:
                raise DiskAdmissionError('Volume coordinator ledger limit exceeded')
            self._merge_roots(roots)
            self.db.execute('''INSERT INTO ledgers(path,metadata_bytes,epoch,pending) VALUES(?,?,?,?)
                ON CONFLICT(path) DO UPDATE SET metadata_bytes=excluded.metadata_bytes,
                epoch=excluded.epoch,pending=excluded.pending''', (ledger_path, metadata_bytes, epoch, pending))
            self._volume_check(ledger=ledger_path, ledger_pending=pending)

    def admit_ledger(self, ledger_path, *, epoch, pending, drain):
        """Admit ``pending`` as this ledger's new total; committed before the ledger's own commit."""
        ledger_path = str(ledger_path)
        with self._txn():
            if self.db.execute('SELECT 1 FROM ledgers WHERE path=?', (ledger_path,)).fetchone() is None:
                raise DiskAdmissionError('Ledger is not registered with the volume coordinator')
            self._volume_check(ledger=ledger_path, ledger_pending=pending, drain=drain)
            self.db.execute('UPDATE ledgers SET epoch=?,pending=? WHERE path=?', (epoch, pending, ledger_path))

    def publish_ledger(self, ledger_path, *, epoch, pending):
        """Publish a (possibly lower) state after the ledger commit; older epochs never overwrite newer."""
        with self._txn():
            self.db.execute('UPDATE ledgers SET epoch=?,pending=? WHERE path=? AND epoch<=?',
                            (epoch, pending, str(ledger_path), epoch))

    def forget_ledger(self, ledger_path):
        """Drop an idle ledger's claim once no process holds the ledger directory (exclusive flock)."""
        ledger_path = Path(ledger_path).resolve()
        with DirectoryLease(ledger_path.parent, exclusive=True):
            with self._txn():
                row = self.db.execute('SELECT pending FROM ledgers WHERE path=?', (str(ledger_path),)).fetchone()
                if row is None:
                    raise KeyError(str(ledger_path))
                if row[0]:
                    raise DiskAdmissionError('Ledger claim still holds pending bytes; recover its tokens first')
                self.db.execute('DELETE FROM ledgers WHERE path=?', (str(ledger_path),))

    # -- declared external jobs ------------------------------------------------------------
    def declare_external(self, amount, *, owner, lease_dir, roots=(), drain=False):
        _integer(amount, 'external bytes')
        owner = _bounded_text(owner, 'owner', 256)
        lease = DirectoryLease(lease_dir)
        try:
            info = os.stat(lease.path)
            claim_id = uuid4().hex
            # A marker file proves the same directory *incarnation* (an inode number can be reused after rmdir).
            os.close(os.open(_marker(lease.path, claim_id), os.O_CREAT | os.O_EXCL | os.O_WRONLY | os.O_NOFOLLOW, 0o600))
            with self._txn():
                if self.db.execute('SELECT count(*) FROM external').fetchone()[0] >= MAX_EXTERNALS:
                    raise DiskAdmissionError('Volume coordinator external claim limit exceeded')
                if roots:
                    self._merge_roots([Path(r).resolve() for r in roots])
                self._volume_check(add_external=amount, drain=drain)
                self.db.execute('INSERT INTO external VALUES(?,?,?,?,?,?,?,?,NULL)',
                                (claim_id, owner, amount, lease.path, info.st_dev, info.st_ino, 1, 'ACTIVE'))
        except BaseException:
            try: os.unlink(_marker(lease.path, claim_id))
            except (OSError, UnboundLocalError): pass
            lease.close()
            raise
        return ExternalClaim(self, claim_id, 1, lease)

    def _external_row(self, claim_id):
        row = self.db.execute('''SELECT id,owner,bytes,lease_dir,lease_dev,lease_ino,generation,state
            FROM external WHERE id=?''', (claim_id,)).fetchone()
        if row is None:
            raise KeyError(claim_id)
        return dict(zip(('id', 'owner', 'bytes', 'lease_dir', 'lease_dev', 'lease_ino', 'generation', 'state'), row))

    def _external_resize(self, claim_id, generation, amount, drain):
        _integer(amount, 'external bytes')
        with self._txn():
            row = self._external_row(claim_id)
            if row['state'] != 'ACTIVE' or row['generation'] != generation:
                raise DiskAdmissionError('External claim generation changed or claim is no longer active')
            if amount > row['bytes']:
                self._volume_check(add_external=amount - row['bytes'], drain=drain)
            self.db.execute('UPDATE external SET bytes=?,generation=generation+1 WHERE id=?', (amount, claim_id))
        return generation + 1

    def _external_release(self, claim_id, generation):
        with self._txn():
            row = self._external_row(claim_id)
            if row['state'] != 'ACTIVE' or row['generation'] != generation:
                raise DiskAdmissionError('External claim generation changed or claim is no longer active')
            self.db.execute('DELETE FROM external WHERE id=?', (claim_id,))
        try: os.unlink(_marker(row['lease_dir'], claim_id))
        except OSError: pass

    def _prove_dead(self, row):
        if row['state'] != 'ACTIVE':
            raise DiskAdmissionError(f"External claim is not recoverable in state {row['state']!r}")
        try:
            lease = DirectoryLease(row['lease_dir'], exclusive=True)
        except DatasetBusyError as exc:
            raise DiskAdmissionError('External claim owner still holds its lease') from exc
        except OSError as exc:
            raise DiskAdmissionError(f'External claim lease directory is not provable: {exc}') from exc
        info = os.stat(lease.path)
        if (info.st_dev, info.st_ino) != (row['lease_dev'], row['lease_ino']):
            lease.close()
            raise DiskAdmissionError('External claim lease directory identity changed')
        try: os.lstat(_marker(lease.path, row['id']))
        except OSError:
            lease.close()
            raise DiskAdmissionError('External claim lease directory identity changed (claim marker missing)') from None
        return lease

    @staticmethod
    def _plan_digest(plan):
        return hashlib.sha256(_canonical(plan).encode()).hexdigest()

    def plan_external_recovery(self, claim_id):
        """Dry run: prove the owner is gone by an exclusive lease and describe the CAS that would apply."""
        with self._lock:
            row = self._external_row(claim_id)
            self._prove_dead(row).close()
            plan = {'version': 1, 'action': 'recover-external', 'claim': row, 'host': self.host,
                    'proof': 'exclusive-directory-flock'}
            return {**plan, 'plan_sha256': self._plan_digest(plan)}

    def apply_external_recovery(self, plan, *, expected_plan_sha256):
        body = {k: v for k, v in plan.items() if k != 'plan_sha256'}
        if self._plan_digest(body) != expected_plan_sha256 or plan.get('plan_sha256') != expected_plan_sha256:
            raise DiskAdmissionError('Recovery plan digest mismatch')
        claim = body['claim']
        if body.get('host') != self.host:
            raise DiskAdmissionError('Recovery plan belongs to another host')
        with self._lock:
            row = self._external_row(claim['id'])
            if row != claim:
                raise DiskAdmissionError('External claim changed since the plan was made')
            lease = self._prove_dead(row)
            try:
                receipt = {'version': 1, 'claim': claim, 'plan_sha256': expected_plan_sha256,
                           'settled': 'declared bytes released from pending credit; written bytes stay observed'}
                proof = _canonical(receipt)
                if len(proof.encode()) > PROOF_MAX_BYTES:
                    raise DiskAdmissionError('Recovery receipt exceeds its byte limit')
                with self._txn():
                    changed = self.db.execute('''UPDATE external SET state='RECOVERED',proof=?,generation=generation+1
                        WHERE id=? AND state='ACTIVE' AND generation=? AND bytes=?''',
                        (proof, claim['id'], claim['generation'], claim['bytes'])).rowcount
                    if changed != 1:
                        raise DiskAdmissionError('External claim changed during recovery')
                try: os.unlink(_marker(lease.path, claim['id']))
                except OSError: pass
            finally:
                lease.close()
            return receipt

    def forget_recovered(self, claim_id):
        with self._txn():
            if self.db.execute("DELETE FROM external WHERE id=? AND state='RECOVERED'", (claim_id,)).rowcount != 1:
                raise DiskAdmissionError('Only a recovered claim can be forgotten')

    def external_receipt(self, claim_id):
        row = self.db.execute('SELECT proof FROM external WHERE id=? AND state=?', (claim_id, 'RECOVERED')).fetchone()
        return None if row is None else json.loads(row[0])

    def snapshot(self):
        with self._lock:
            ledgers = {p: {'metadata_bytes': m, 'epoch': e, 'pending': q}
                       for p, m, e, q in self.db.execute('SELECT path,metadata_bytes,epoch,pending FROM ledgers')}
            external = {i: {'bytes': b, 'state': s, 'generation': g, 'owner': o}
                        for i, b, s, g, o in self.db.execute('SELECT id,bytes,state,generation,owner FROM external')}
            return {'ledgers': ledgers, 'external': external, 'roots': [p for (p,) in self.db.execute('SELECT path FROM roots')],
                    'pending_bytes': sum(v['pending'] for v in ledgers.values())
                    + sum(v['bytes'] for v in external.values() if v['state'] == 'ACTIVE'),
                    'host': self.host, 'filesystem': self.filesystem, 'strict_capacity_guarantee': False}
