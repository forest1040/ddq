"""Persistent node-local disk admission.

Reservations survive process crashes. Materialized bytes move from pending
credit into observed usage, so the same write is not charged twice.
"""
from contextlib import contextmanager
from pathlib import Path
import os
import json
import re
import sqlite3
import stat
import threading
import weakref
from uuid import uuid4

from .dataset_lease import DirectoryLease


class DiskAdmissionError(OSError):
    pass


_LEDGER_REGISTRY = {}
_LEDGER_REGISTRY_LOCK = threading.RLock()
_LEDGER_PATH_LOCKS = {}
_LEDGER_PATH_LOCKS_LOCK = threading.RLock()


_LEDGER_TABLES = {
    'reservations': """CREATE TABLE IF NOT EXISTS reservations(
                        id TEXT PRIMARY KEY, bytes INTEGER NOT NULL, materialized INTEGER NOT NULL DEFAULT 0,
                        operation TEXT NOT NULL DEFAULT 'legacy', owner TEXT, generation TEXT,
                        state TEXT NOT NULL DEFAULT 'RESERVED', evidence TEXT)""",
    'roots': """CREATE TABLE IF NOT EXISTS roots(path TEXT PRIMARY KEY)""",
    'checkpoint_reservations': """CREATE TABLE IF NOT EXISTS checkpoint_reservations(
                        id TEXT PRIMARY KEY, link TEXT NOT NULL CHECK(length(CAST(link AS BLOB))<=4096),
                        FOREIGN KEY(id) REFERENCES reservations(id) ON DELETE CASCADE)""",
    'checkpoint_recovery_receipts': """CREATE TABLE IF NOT EXISTS checkpoint_recovery_receipts(
                        id TEXT PRIMARY KEY, proof TEXT NOT NULL CHECK(length(CAST(proof AS BLOB))<=8192),
                        FOREIGN KEY(id) REFERENCES reservations(id) ON DELETE CASCADE)""",
    'shared_policy': """CREATE TABLE IF NOT EXISTS shared_policy(
                        singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                        quota_bytes INTEGER, min_free_bytes INTEGER NOT NULL,
                        drain_reserve_bytes INTEGER NOT NULL, bootstrap_bytes INTEGER NOT NULL,
                        max_page_count INTEGER NOT NULL, max_records INTEGER NOT NULL,
                        volume_device INTEGER NOT NULL, custom_accounting INTEGER NOT NULL)""",
}

# Optional and additive: only a newly created store-private ledger (persist_private_policy) gets this
# table. Existing ledgers are never migrated and recovery treats a missing table as "no policy persisted".
_PRIVATE_POLICY_TABLE = """CREATE TABLE IF NOT EXISTS private_policy(
                        singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                        quota_bytes INTEGER, min_free_bytes INTEGER NOT NULL,
                        drain_reserve_bytes INTEGER NOT NULL, bootstrap_bytes INTEGER NOT NULL,
                        max_page_count INTEGER NOT NULL, max_records INTEGER NOT NULL,
                        volume_device INTEGER NOT NULL, custom_accounting INTEGER NOT NULL)"""

_LABEL_TRIGGERS = {
    'reservation_labels_insert': """CREATE TRIGGER IF NOT EXISTS reservation_labels_insert
                        BEFORE INSERT ON reservations
                        WHEN length(CAST(NEW.operation AS BLOB))>64
                          OR (NEW.owner IS NOT NULL AND length(CAST(NEW.owner AS BLOB))>256)
                          OR (NEW.generation IS NOT NULL AND length(CAST(NEW.generation AS BLOB))>128)
                          OR (NEW.evidence IS NOT NULL AND length(CAST(NEW.evidence AS BLOB))>256)
                        BEGIN SELECT RAISE(ABORT,'reservation metadata exceeds byte limit'); END""",
    'reservation_labels_update': """CREATE TRIGGER IF NOT EXISTS reservation_labels_update
                        BEFORE UPDATE OF operation,owner,generation,evidence ON reservations
                        WHEN length(CAST(NEW.operation AS BLOB))>64
                          OR (NEW.owner IS NOT NULL AND length(CAST(NEW.owner AS BLOB))>256)
                          OR (NEW.generation IS NOT NULL AND length(CAST(NEW.generation AS BLOB))>128)
                          OR (NEW.evidence IS NOT NULL AND length(CAST(NEW.evidence AS BLOB))>256)
                        BEGIN SELECT RAISE(ABORT,'reservation metadata exceeds byte limit'); END""",
}


def _schema_text(sql):
    return re.sub(r"\s+", "", sql.lower()).replace("ifnotexists", "").rstrip(";")


def _path_lock(path):
    key = str(path)
    with _LEDGER_PATH_LOCKS_LOCK:
        return _LEDGER_PATH_LOCKS.setdefault(key, threading.RLock())


def _bounded_text(value, name, max_bytes, *, optional=False):
    if optional and value is None:
        return None
    if type(value) not in (str, int):
        raise ValueError(f'Invalid {name}')
    result = str(value)
    if len(result.encode('utf-8')) > max_bytes:
        raise ValueError(f'{name} exceeds {max_bytes} UTF-8 bytes')
    return result


def _integer(value, name, *, minimum=0, optional=False):
    if optional and value is None:
        return
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise ValueError(f'Invalid {name}')


def _root_antichain(paths):
    """Return the minimum ancestor set covering all resolved roots."""
    ordered = sorted({Path(path).resolve() for path in paths},
                     key=lambda path: (len(path.parts), str(path)))
    selected = []
    for path in ordered:
        if any(path.is_relative_to(parent) for parent in selected):
            continue
        selected.append(path)
    return tuple(selected)


APPEND_RESERVATION_BYTES = 64 * 1024
APPEND_OPERATIONS = ('trace-append', 'journal-append', 'measurement-append')
LEASE_OPERATIONS = APPEND_OPERATIONS + ('catalog-transaction',)


def register_lease(budget, lease):
    # Budget doubles in tests may not track leases; real budgets release them on close.
    leases = getattr(budget, '_ledger_leases', None)
    if leases is not None: leases.add(lease)


def unregister_lease(budget, lease):
    leases = getattr(budget, '_ledger_leases', None)
    if leases is not None: leases.discard(lease)




class AppendReservation:
    """One durable WRITING token covering many bounded appends by one writer.

    As with a per-record allocation, every appended byte is covered by a live
    token before it is written; the ledger commits once per chunk instead of
    two or three times per record. Written bytes are observed on disk while the
    whole chunk is still pending, so accounting overcounts by at most one chunk
    per open writer. A crash leaves one WRITING token for explicit recovery,
    like a retained per-record token. Near a quota or drain margin the chunk
    falls back to the exact record size.
    """
    def __init__(self, budget_getter, *, operation, drain_getter=lambda: False, chunk_bytes=APPEND_RESERVATION_BYTES):
        if type(chunk_bytes) is not int or chunk_bytes < 1: raise ValueError('Invalid append chunk size')
        self._budget_getter, self._drain_getter = budget_getter, drain_getter
        self.operation, self.chunk_bytes = operation, chunk_bytes
        self.budget = self.key = None
        self.capacity = self.used = 0
        self._lock = threading.RLock()

    def admit(self, nbytes):
        """Ensure the live token covers ``nbytes`` more; returns the token key or None without a budget."""
        _integer(nbytes, 'append bytes')
        with self._lock:
            budget = self._budget_getter()
            if budget is not self.budget or self.used + nbytes > self.capacity: self.release()
            if budget is None: return None
            if self.key is None:
                drain = bool(self._drain_getter())
                amount = max(self.chunk_bytes, nbytes)
                try:
                    key = budget.reserve(amount, drain=drain, operation=self.operation, writing=True)
                except DiskAdmissionError:
                    if amount == nbytes: raise
                    amount = nbytes
                    key = budget.reserve(amount, drain=drain, operation=self.operation, writing=True)
                self.budget, self.key, self.capacity, self.used = budget, key, amount, 0
                register_lease(budget,self)
            return self.key

    def wrote(self, nbytes):
        with self._lock:
            if self.key is None: return  # unbudgeted writer
            if self.used + nbytes > self.capacity: raise ValueError('Append exceeded its admitted reservation')
            self.used += nbytes

    def retain(self):
        """Abandon the token without release after an unrecoverable partial write."""
        with self._lock:
            if self.budget is not None: unregister_lease(self.budget,self)
            self.budget = self.key = None
            self.capacity = self.used = 0

    def release(self):
        with self._lock:
            budget, key = self.budget, self.key
            self.retain()
            if key is not None and not getattr(budget,'_closed',False): budget.release(key)


class DiskBudget:
    """Coordinate byte reservations through a SQLite ledger on one volume."""

    def __init__(self, ledger, roots, *, quota_bytes=None, min_free_bytes=0,
                 drain_reserve_bytes=0, usage=None, free=None,
                 max_page_count=1024, max_records=4096, bootstrap_bytes=8*1024*1024,
                 shared_policy=False, recovery_only=False, ledger_lease=None, store_private=False,
                 persist_private_policy=False, volume_coordinator=None,
                 own_volume_coordinator=False):
        _integer(min_free_bytes, 'minimum free bytes')
        _integer(drain_reserve_bytes, 'drain reserve bytes')
        _integer(quota_bytes, 'disk quota', minimum=1, optional=True)
        _integer(max_page_count, 'maximum ledger pages', minimum=1)
        _integer(max_records, 'maximum ledger records', minimum=8)
        _integer(bootstrap_bytes, 'ledger bootstrap bytes', minimum=1)
        if type(shared_policy) is not bool:
            raise ValueError('Invalid shared ledger policy flag')
        if type(recovery_only) is not bool:raise ValueError('Invalid recovery-only flag')
        if type(store_private) is not bool:raise ValueError('Invalid store-private flag')
        if store_private and (not recovery_only or shared_policy):
            raise ValueError('store_private is a recovery-only layout without shared policy')
        if recovery_only and (not (shared_policy or store_private) or usage is not None or free is not None):
            raise ValueError('Recovery-only requires a bound production shared ledger')
        if type(persist_private_policy) is not bool:raise ValueError('Invalid persist-private-policy flag')
        if persist_private_policy and (shared_policy or recovery_only or store_private):
            raise ValueError('persist_private_policy applies to a writable non-shared ledger only')
        if store_private and max_page_count!=1024:
            raise ValueError('A store-private ledger derives its page cap from the metadata pool')
        if volume_coordinator is not None:
            if usage is not None or free is not None:
                raise ValueError('A volume coordinator requires observed accounting, not injected hooks')
            if recovery_only or store_private:
                raise ValueError('Recovery-only and store-private ledgers do not take a volume coordinator')
        self._coord=volume_coordinator;self._coord_ready=False
        self._coord_owned=bool(own_volume_coordinator) and volume_coordinator is not None
        self.persist_private_policy=persist_private_policy
        self.store_private=store_private
        self.recovery_only=recovery_only
        if ledger_lease is not None and (recovery_only or not isinstance(ledger_lease,DirectoryLease)):
            raise ValueError('ledger_lease must be a FileLease held by an offline maintenance caller')
        self._ledger_lease=None;self._ledger_lease_owned=False
        self.roots = tuple(dict.fromkeys(Path(p).resolve() for p in roots))
        if not self.roots:
            raise ValueError('Disk budget needs a local root')
        # Bound each caller-supplied root list independently. Persistent shared
        # roots have their own 256-entry limit after ancestor compaction.
        if len(self.roots) > 256:
            raise DiskAdmissionError('Disk ledger root limit exceeded (input list)')
        devices = {p.stat().st_dev for p in self.roots}
        if len(devices) != 1:
            raise ValueError('Disk budget roots must be on one volume')
        self.quota_bytes, self.min_free_bytes = quota_bytes, min_free_bytes
        self.drain_reserve_bytes = drain_reserve_bytes
        self._custom_accounting = usage is not None or free is not None
        self._usage = usage if usage is not None else self._observed_usage
        self._free = free if free is not None else self._observed_free
        self.max_records = max_records
        self.shared_policy = shared_policy
        self.bootstrap_bytes = bootstrap_bytes
        # Split one fixed metadata pool between bounded database pages and its
        # rollback journal. The VFS enforces both caps before each write.
        pages_by_pool = (bootstrap_bytes // 2) // 4096
        if pages_by_pool < 1:
            raise ValueError('Ledger metadata pool must be at least 8 KiB')
        self.max_page_count = min(max_page_count, pages_by_pool)
        self._ledger_main_cap = self.max_page_count * 4096
        self._ledger_side_cap = bootstrap_bytes - self._ledger_main_cap
        if self._ledger_side_cap <= 0:
            raise ValueError('Ledger metadata pool must include rollback journal space')
        ledger = Path(ledger)
        if recovery_only:
            info=ledger.lstat()
            if not stat.S_ISREG(info.st_mode) or info.st_nlink!=1:
                raise DiskAdmissionError('Recovery requires an existing exclusively named ledger')
            self._recovery_identity=(info.st_dev,info.st_ino)
        ledger = ledger.resolve()
        if store_private:
            only = self.roots[0] if len(self.roots)==1 else None
            if only is None or ledger != only/'metadata'/'disk-ledger.sqlite3':
                raise DiskAdmissionError('A store-private ledger must be <store>/metadata/disk-ledger.sqlite3 of exactly one store root')
        self._ledger_path = ledger
        if not recovery_only:ledger.parent.mkdir(parents=True, exist_ok=True)
        if ledger.parent.stat().st_dev not in devices:
            raise ValueError('Disk ledger must be on the budgeted volume')
        self._path_lock = _path_lock(ledger)
        self._registry_key = str(ledger)
        self._guard = None
        self._registry_acquired = False
        self._closed = False
        self._ledger_leases = weakref.WeakSet()
        if not self._custom_accounting:
            try:
                from .catalog_admission import get_sqlite_guard
                self._guard = get_sqlite_guard()
            except (ImportError, NotImplementedError, OSError, sqlite3.Error) as exc:
                raise DiskAdmissionError(f'Bounded ledger VFS unavailable: {exc}') from exc
        self.ledger_vfs_enforced = self._guard is not None
        if recovery_only and not self.ledger_vfs_enforced:
            raise DiskAdmissionError('Recovery-only requires the native bounded ledger VFS')
        self.accounting_scope = ('injected' if self._custom_accounting else
                                 'observed-with-bounded-ledger-vfs' if self.ledger_vfs_enforced else
                                 'observed-soft')
        self.strict_capacity_guarantee = False
        self._ledger_preexisting = ledger.exists() and ledger.stat().st_size > 0
        if ledger.exists() and ledger.stat().st_size > self._ledger_main_cap:
            raise DiskAdmissionError('Existing disk ledger exceeds its bounded metadata pool')
        if self._metadata_allocated_bytes()>self.bootstrap_bytes:
            raise DiskAdmissionError('Existing ledger and journal exceed the metadata pool')
        if recovery_only:
            self._open_recovery_ledger()
            return
        # Account for the bounded local metadata pool before its first write.
        # Injected accounting hooks are deterministic test/accounting scopes.
        if not self._custom_accounting:
            if quota_bytes is not None and self._usage() + bootstrap_bytes + drain_reserve_bytes > quota_bytes:
                raise DiskAdmissionError('Ledger bootstrap exceeds host/device disk quota')
            if self._free() - self._unallocated_metadata_pool() - drain_reserve_bytes < min_free_bytes:
                raise DiskAdmissionError('Ledger bootstrap violates minimum free space')
        self.db = self._open_ledger()
        try:
            # Every live writer budget holds a shared flock on the ledger's
            # parent directory inode (never the ledger file: BSD flock and
            # SQLite's fcntl locks conflict) so offline recovery can prove, by
            # an exclusive flock, that no process has the ledger open. A
            # caller-held exclusive lease is adopted for offline maintenance.
            if ledger_lease is not None:
                ledger_lease.assert_current()
                if not ledger_lease.exclusive or Path(ledger_lease.path)!=self._ledger_path.parent:
                    raise ValueError('ledger_lease must be the exclusive lease of this ledger directory')
                self._ledger_lease=ledger_lease
            else:
                self._ledger_lease=DirectoryLease(self._ledger_path.parent);self._ledger_lease_owned=True
            with self._path_lock:
                self._set_guard_active(True)
                self.db.execute('PRAGMA synchronous=EXTRA')
                self.db.execute('PRAGMA journal_mode=DELETE').fetchone()
                page_size = self.db.execute('PRAGMA page_size').fetchone()[0]
                page_cap = self._ledger_main_cap // page_size
                if page_cap < 1:
                    raise DiskAdmissionError('Ledger page size exceeds metadata pool')
                actual_cap = self.db.execute(f'PRAGMA max_page_count={page_cap}').fetchone()[0]
                if actual_cap != page_cap:
                    raise DiskAdmissionError('Ledger page cap could not be enforced')
                # Persist the effective SQLite page limit, rather than the
                # constructor's pre-page-size estimate, in shared policy.
                self.max_page_count = actual_cap
                with self._ledger_transaction(active_already=True):
                    self.db.execute(_LEDGER_TABLES['reservations'])
                    cols = {row[1] for row in self.db.execute('PRAGMA table_info(reservations)')}
                    for name, decl in (('materialized','INTEGER NOT NULL DEFAULT 0'),
                                       ('operation',"TEXT NOT NULL DEFAULT 'legacy'"), ('owner','TEXT'),
                                       ('generation','TEXT'), ('state',"TEXT NOT NULL DEFAULT 'RESERVED'"),
                                       ('evidence','TEXT')):
                        if name not in cols:
                            self.db.execute(f'ALTER TABLE reservations ADD COLUMN {name} {decl}')
                    self.db.execute(_LEDGER_TABLES['roots'])
                    self.db.execute(_LEDGER_TABLES['checkpoint_reservations'])
                    self.db.execute(_LEDGER_TABLES['checkpoint_recovery_receipts'])
                    self.db.execute(_LEDGER_TABLES['shared_policy'])
                    for sql in _LABEL_TRIGGERS.values():self.db.execute(sql)
                    volume_device = devices.pop()
                    self._check_or_bind_shared_policy(volume_device)
                    self._check_or_bind_private_policy(volume_device)
                    self._check_or_bind_volume(volume_device)
                    existing_roots = [Path(path) for (path,) in self.db.execute('SELECT path FROM roots')]
                    selected_roots = _root_antichain(existing_roots + list(self.roots))
                    if len(selected_roots) > 256:
                        raise DiskAdmissionError('Disk ledger root limit exceeded')
                    self.db.execute('DELETE FROM roots')
                    self.db.executemany('INSERT INTO roots VALUES(?)', [(str(p),) for p in selected_roots])
                    if self.shared_policy:
                        self._check_shared_root_capacity()
                    self.db.execute('PRAGMA user_version=3')
                    self._register_with_coordinator()
                self._coord_ready=self._coord is not None
        except BaseException:
            try:
                self._set_guard_active(False)
            except Exception:
                pass
            try: self.db.close()
            finally:
                self._release_registry()
                if self._ledger_lease_owned and self._ledger_lease is not None:self._ledger_lease.close()
            raise
        else:
            self._set_guard_active(False)

    def _open_ledger(self):
        with _LEDGER_REGISTRY_LOCK:
            entry = _LEDGER_REGISTRY.get(self._registry_key)
            if self._guard is not None:
                if entry is None:
                    kwargs={'existing_only':True} if self.recovery_only else {}
                    db = self._guard.connect(self._ledger_path,
                                             main_cap_bytes=self._ledger_main_cap,
                                             side_cap_bytes=self._ledger_side_cap,**kwargs)
                    _LEDGER_REGISTRY[self._registry_key] = [self._guard, 1]
                else:
                    if entry[0] is not self._guard:
                        raise DiskAdmissionError('Ledger is registered with a different SQLite guard')
                    kwargs={'existing_only':True} if self.recovery_only else {}
                    db = self._guard.connect(self._ledger_path,**kwargs)
                    entry[1] += 1
            else:
                if entry is not None and entry[0] is not None:
                    raise DiskAdmissionError('Ledger is already open with a guarded SQLite connection')
                db = sqlite3.connect(self._ledger_path, timeout=30, isolation_level=None)
                if entry is None:
                    _LEDGER_REGISTRY[self._registry_key] = [None, 1]
                else:
                    entry[1] += 1
            self._registry_acquired = True
            return db

    def _require_writer(self):
        if self.recovery_only:
            raise DiskAdmissionError('Recovery-only ledger permits checkpoint CAS with receipts only')

    def _check_recovery_headroom(self, *, check_side_files=True):
        info=self._ledger_path.lstat()
        if (not stat.S_ISREG(info.st_mode) or info.st_nlink!=1 or
                (info.st_dev,info.st_ino)!=self._recovery_identity):
            raise DiskAdmissionError('Recovery ledger identity changed')
        if info.st_size>self._ledger_main_cap or self._metadata_allocated_bytes()>self.bootstrap_bytes:
            raise DiskAdmissionError('Recovery ledger exceeds the precharged metadata pool')
        if check_side_files:
            for suffix in ('-journal','-wal','-shm'):
                try:side=Path(str(self._ledger_path)+suffix).lstat()
                except FileNotFoundError:continue
                if not stat.S_ISREG(side.st_mode) or side.st_nlink!=1 or side.st_size:
                    raise DiskAdmissionError('Recovery ledger side file requires separate recovery')
        # The pool was already charged by the persisted policy. Recovery spends
        # no new data credit and no drain margin, but preserves min-free and
        # requires physical room for the whole remaining metadata envelope.
        if self._free()-self._unallocated_metadata_pool()<self.min_free_bytes:
            raise DiskAdmissionError('Recovery metadata pool lacks physical headroom')

    def _validate_recovery_ledger(self, con):
        if con.execute('PRAGMA user_version').fetchone()[0]!=3:
            raise DiskAdmissionError('Recovery requires the existing ledger schema')
        for table,expected in _LEDGER_TABLES.items():
            row=con.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name=?",(table,)).fetchone()
            if row is None or _schema_text(row[0])!=_schema_text(expected):
                raise DiskAdmissionError('Recovery ledger schema is incomplete or changed')
        triggers=con.execute("SELECT name,sql FROM sqlite_master WHERE type='trigger' AND tbl_name IN ('reservations','roots','checkpoint_reservations','checkpoint_recovery_receipts','shared_policy')").fetchall()
        if {r[0] for r in triggers}!=set(_LABEL_TRIGGERS):
            raise DiskAdmissionError('Recovery ledger trigger schema changed')
        if any(_schema_text(sql)!=_schema_text(_LABEL_TRIGGERS[name]) for name,sql in triggers):
            raise DiskAdmissionError('Recovery ledger label trigger changed')
        if con.execute('PRAGMA journal_mode').fetchone()[0]!='delete':
            raise DiskAdmissionError('Recovery requires existing DELETE journal mode')
        page_size=con.execute('PRAGMA page_size').fetchone()[0]
        self.max_page_count=self._ledger_main_cap//page_size
        if self.max_page_count<1 or con.execute('PRAGMA page_count').fetchone()[0]>self.max_page_count:
            raise DiskAdmissionError('Recovery ledger exceeds its page envelope')
        if self.store_private:
            if not (self.roots[0]/'metadata/catalog.sqlite3').is_file():
                from .catalog_startup import has_private_intent
                if not has_private_intent(con,self.roots[0]):
                    raise DiskAdmissionError('A missing private catalog requires a durable startup intent')
            # No policy row is persisted for the store's own ledger: it must stay unbound, and its only
            # registered root must be the store that owns it.
            if con.execute('SELECT count(*) FROM shared_policy').fetchone()[0]!=0:
                raise DiskAdmissionError('Recovery found a bound shared policy in a store-private ledger')
            registered=[Path(p) for (p,) in con.execute('SELECT path FROM roots LIMIT 2')]
            if registered!=[self.roots[0]]:
                raise DiskAdmissionError('A store-private ledger must register exactly its own store')
            table=con.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='private_policy'").fetchone()
            if table is None:
                # Legacy/unpersisted ledger: only the FileBlockStore defaults can be honored.
                if (self.quota_bytes,self.min_free_bytes,self.drain_reserve_bytes,self.bootstrap_bytes,
                        self.max_records)!=(None,0,0,8*1024*1024,4096):
                    raise DiskAdmissionError('Recovery cannot verify a non-default policy: this store-private ledger persisted none')
                return
            if _schema_text(table[0])!=_schema_text(_PRIVATE_POLICY_TABLE):
                raise DiskAdmissionError('Recovery private policy schema changed')
            expected=(self.quota_bytes,self.min_free_bytes,self.drain_reserve_bytes,self.bootstrap_bytes,
                      self.max_page_count,self.max_records,self.roots[0].stat().st_dev,0)
            policy=con.execute('''SELECT quota_bytes,min_free_bytes,drain_reserve_bytes,bootstrap_bytes,
                max_page_count,max_records,volume_device,custom_accounting FROM private_policy WHERE singleton=1''').fetchone()
            if policy is None or tuple(policy)!=expected:
                raise DiskAdmissionError('Recovery requires a matching persisted store-private policy')
            return
        expected=(self.quota_bytes,self.min_free_bytes,self.drain_reserve_bytes,self.bootstrap_bytes,
                  self.max_page_count,self.max_records,self.roots[0].stat().st_dev,0)
        policy=con.execute('''SELECT quota_bytes,min_free_bytes,drain_reserve_bytes,bootstrap_bytes,
            max_page_count,max_records,volume_device,custom_accounting FROM shared_policy WHERE singleton=1''').fetchone()
        if policy is None or tuple(policy)!=expected:
            raise DiskAdmissionError('Recovery requires a matching bound shared policy')
        registered=[Path(p) for (p,) in con.execute('SELECT path FROM roots LIMIT 257')]
        if not registered or len(registered)>256 or any(not p.is_absolute() or p.resolve()!=p or
                not p.is_dir() or p.stat().st_dev!=expected[-2] for p in registered):
            raise DiskAdmissionError('Recovery registered roots changed or are unavailable')
        if any(not any(root.is_relative_to(p) for p in registered) for root in self.roots):
            raise DiskAdmissionError('Recovery cannot register a new root')

    def _open_recovery_ledger(self):
        # Hold a read snapshot until the guarded existing-only connection is
        # initialized. No schema migration, policy bind, or root rewrite occurs.
        with self._path_lock:
            self._check_recovery_headroom()
            con=sqlite3.connect(self._ledger_path.as_uri()+'?mode=ro',uri=True)
            try:
                con.execute('BEGIN')
                self._validate_recovery_ledger(con)
                if con.execute('PRAGMA quick_check').fetchall()!=[('ok',)]:
                    raise DiskAdmissionError('Recovery ledger integrity check failed')
                self._check_recovery_headroom()
                self.db=self._open_ledger()
                self._set_guard_active(False)
                self.db.execute('PRAGMA synchronous=EXTRA')
                if self.db.execute(f'PRAGMA max_page_count={self.max_page_count}').fetchone()[0]!=self.max_page_count:
                    raise DiskAdmissionError('Recovery ledger page cap could not be enforced')
                self._validate_recovery_ledger(self.db)
            except BaseException:
                if hasattr(self,'db'):
                    try:self.db.close()
                    finally:self._release_registry()
                raise
            finally:con.close()

    def _release_registry(self):
        if not self._registry_acquired:
            return
        with _LEDGER_REGISTRY_LOCK:
            entry = _LEDGER_REGISTRY.get(self._registry_key)
            if entry is not None:
                entry[1] -= 1
                if entry[1] <= 0:
                    _LEDGER_REGISTRY.pop(self._registry_key, None)
        self._registry_acquired = False

    def _check_or_bind_shared_policy(self, volume_device):
        """Verify an existing immutable policy, or bind it when requested.

        A legacy ledger can be bound only while it has no live reservation.
        Terminal/history rows do not prevent binding because they do not
        contribute pending credit. A generic caller may open an unbound legacy
        ledger, but cannot opt out of a policy that is already bound.
        """
        expected = (self.quota_bytes, self.min_free_bytes,
                    self.drain_reserve_bytes, self.bootstrap_bytes,
                    self.max_page_count, self.max_records, int(volume_device),
                    int(self._custom_accounting))
        row = self.db.execute('''SELECT quota_bytes,min_free_bytes,drain_reserve_bytes,
            bootstrap_bytes,max_page_count,max_records,volume_device,custom_accounting
            FROM shared_policy WHERE singleton=1''').fetchone()
        if row is not None:
            if tuple(row) != expected:
                raise DiskAdmissionError('Shared disk ledger policy mismatch')
            self.shared_policy = True
            return
        if not self.shared_policy:
            return
        active = self.db.execute("SELECT 1 FROM reservations WHERE state IN ('RESERVED','WRITING') LIMIT 1").fetchone()
        if active is not None:
            raise DiskAdmissionError('Cannot bind shared policy while legacy reservations are active')
        self.db.execute('''INSERT INTO shared_policy(singleton,quota_bytes,min_free_bytes,
            drain_reserve_bytes,bootstrap_bytes,max_page_count,max_records,volume_device,
            custom_accounting) VALUES(1,?,?,?,?,?,?,?,?)''', expected)

    def _check_or_bind_private_policy(self, volume_device):
        """Persist the policy of a *newly created* store-private ledger and verify it on later opens.

        An existing ledger without the table is not migrated: recovery keeps the documented weaker check
        (defaults only) for it.
        """
        if not self.persist_private_policy or self.shared_policy: return
        expected = (self.quota_bytes, self.min_free_bytes, self.drain_reserve_bytes, self.bootstrap_bytes,
                    self.max_page_count, self.max_records, int(volume_device), int(self._custom_accounting))
        has = self.db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='private_policy'").fetchone()
        if has:
            row = self.db.execute('''SELECT quota_bytes,min_free_bytes,drain_reserve_bytes,bootstrap_bytes,
                max_page_count,max_records,volume_device,custom_accounting FROM private_policy WHERE singleton=1''').fetchone()
            if row is not None and tuple(row) != expected:
                raise DiskAdmissionError('Store-private ledger policy mismatch')
            return
        if self._ledger_preexisting: return
        self.db.execute(_PRIVATE_POLICY_TABLE)
        self.db.execute('''INSERT INTO private_policy(singleton,quota_bytes,min_free_bytes,
            drain_reserve_bytes,bootstrap_bytes,max_page_count,max_records,volume_device,
            custom_accounting) VALUES(1,?,?,?,?,?,?,?,?)''', expected)

    def _check_or_bind_volume(self, volume_device):
        """Bind a ledger to one volume coordinator, or refuse an unbound open of a bound ledger.

        The binding is persisted so a bound ledger cannot be opened without the coordinator (which would let
        it over-commit the volume unseen) and cannot be moved to another coordinator.
        """
        has=self.db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='volume_binding'").fetchone()
        bound=None
        if has: bound=self.db.execute('SELECT coordinator FROM volume_binding WHERE singleton=1').fetchone()
        if self._coord is None:
            if bound is not None: raise DiskAdmissionError('Ledger is bound to a volume coordinator; open it with that coordinator')
            return
        if self._coord.volume_device!=int(volume_device):
            raise DiskAdmissionError('Volume coordinator is on another volume than the budgeted roots')
        if bound is not None and bound[0]!=str(self._coord.path):
            raise DiskAdmissionError('Ledger is bound to a different volume coordinator')
        if not has:
            self.db.execute('''CREATE TABLE IF NOT EXISTS volume_binding(singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                coordinator TEXT NOT NULL, epoch INTEGER NOT NULL)''')
        if bound is None:
            self.db.execute('INSERT INTO volume_binding VALUES(1,?,0)',(str(self._coord.path),))

    @staticmethod
    def read_volume_binding(ledger):
        """The coordinator path a ledger is bound to, or ``None`` (read-only; works on a quiescent ledger)."""
        con = sqlite3.connect(Path(ledger).as_uri() + '?mode=ro', uri=True)
        try:
            con.execute('PRAGMA query_only=ON')
            if not con.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='volume_binding'").fetchone():
                return None
            row = con.execute('SELECT coordinator FROM volume_binding WHERE singleton=1').fetchone()
            return None if row is None else row[0]
        except sqlite3.Error as exc:
            raise DiskAdmissionError(f'Cannot read the ledger volume binding: {exc}') from exc
        finally:
            con.close()

    def republish_volume_claim(self, coordinator, lease):
        """Offline: publish this ledger's true pending to its bound coordinator (idempotent, with a receipt).

        ``lease`` must be the live exclusive lease of the ledger directory, which proves no process holds the
        ledger, so no concurrent publisher can race the reset. The ledger itself is only read.
        """
        lease.assert_current()
        if not lease.exclusive or Path(lease.path) != self._ledger_path.parent:
            raise ValueError('republish needs the exclusive lease of this ledger directory')
        with self._path_lock:
            row = self.db.execute('SELECT coordinator,epoch FROM volume_binding WHERE singleton=1').fetchone() \
                if self.db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='volume_binding'").fetchone() else None
            if row is None:
                return None
            if row[0] != str(coordinator.path):
                raise DiskAdmissionError('Ledger is bound to a different volume coordinator')
            pending = self.db.execute("""SELECT COALESCE(SUM(bytes-materialized),0) FROM reservations
                WHERE state IN ('RESERVED','WRITING')""").fetchone()[0]
        before = coordinator.reset_claim(self._ledger_path, pending=pending, epoch_floor=row[1])
        return {'coordinator': str(coordinator.path), 'ledger': str(self._ledger_path), 'previous_claim_bytes': before['pending'],
                'published_pending_bytes': pending, 'changed': before['pending'] != pending}

    def _register_with_coordinator(self):
        if self._coord is None: return
        self.db.execute('UPDATE volume_binding SET epoch=epoch+1 WHERE singleton=1')
        roots=[Path(p) for (p,) in self.db.execute('SELECT path FROM roots')]
        from .export_admission import export_inventory_roots
        roots.extend(Path(row['path']) for row in export_inventory_roots(self.db))
        self._coord.register_ledger(self._ledger_path,metadata_bytes=self.bootstrap_bytes,roots=roots,
                                    pending=self.reserved_bytes,epoch=self._epoch())

    def _epoch(self):
        return self.db.execute('SELECT epoch FROM volume_binding WHERE singleton=1').fetchone()[0]

    def _publish_claim(self):
        """Publish the committed pending bytes (a decrease) to the coordinator; failure only over-counts."""
        try:
            with self._path_lock:
                epoch,pending=self.db.execute("""SELECT (SELECT epoch FROM volume_binding WHERE singleton=1),
                    COALESCE(SUM(bytes-materialized),0) FROM reservations WHERE state IN ('RESERVED','WRITING')""").fetchone()
                self._coord.publish_ledger(self._ledger_path,epoch=epoch,pending=pending)
        except (sqlite3.Error,OSError): pass

    def _check_shared_root_capacity(self):
        """Check the aggregate registered-root envelope before commit."""
        pending = self.db.execute("SELECT COALESCE(SUM(bytes-materialized),0) FROM reservations WHERE state IN ('RESERVED','WRITING')").fetchone()[0]
        margin = self.drain_reserve_bytes
        observed = self._usage()
        metadata_charge = 0 if self._custom_accounting else self.bootstrap_bytes
        if (self.quota_bytes is not None and
                observed + metadata_charge + pending + margin > self.quota_bytes):
            raise DiskAdmissionError('Registered roots exceed shared disk quota')
        if self._free() - self._unallocated_metadata_pool() - pending - margin < self.min_free_bytes:
            raise DiskAdmissionError('Registered roots violate shared minimum free space')

    def _set_guard_active(self, active):
        if self._guard is not None and hasattr(self, 'db'):
            self.db.execute('SELECT svsim_disk_guard_config(?,?,?)',
                            (self._ledger_main_cap, self._ledger_side_cap, int(bool(active)))).fetchone()

    @contextmanager
    def _ledger_transaction(self, *, active_already=False):
        with self._path_lock:
            if self.recovery_only:self._check_recovery_headroom()
            if not active_already:
                self._set_guard_active(True)
            try:
                self.db.execute('BEGIN IMMEDIATE')
                if self.recovery_only:
                    self._check_recovery_headroom(check_side_files=False)
                    self._validate_recovery_ledger(self.db)
                if self._coord_ready:
                    self.db.execute('UPDATE volume_binding SET epoch=epoch+1 WHERE singleton=1')
                yield
                self.db.execute('COMMIT')
                if self._coord_ready: self._publish_claim()
            except BaseException:
                if self.db.in_transaction:
                    self.db.execute('ROLLBACK')
                raise
            finally:
                if not active_already:
                    self._set_guard_active(False)

    def _metadata_allocated_bytes(self):
        total = 0
        for path in (self._ledger_path, Path(str(self._ledger_path) + '-journal')):
            try:
                info = path.stat(follow_symlinks=False)
            except FileNotFoundError:
                continue
            if stat.S_ISREG(info.st_mode):
                total += info.st_size
        return total

    def _unallocated_metadata_pool(self):
        if self._custom_accounting:
            return 0
        return max(0, self.bootstrap_bytes - self._metadata_allocated_bytes())

    def _observed_usage(self):
        total = 0
        registered = list(Path(p) for (p,) in self.db.execute('SELECT path FROM roots')) if hasattr(self, 'db') else list(self.roots)
        if hasattr(self,'db'):
            from .export_admission import export_inventory_roots
            registered.extend(Path(r['path']) for r in export_inventory_roots(self.db))
        roots=sorted(registered,key=lambda p:len(p.parts))
        selected=[]
        metadata_paths = {self._ledger_path, Path(str(self._ledger_path) + '-journal')}
        for root in roots:
            if any(root.is_relative_to(parent) for parent in selected): continue
            selected.append(root)
            # pathlib's recursive glob can suppress traversal errors. Unreadable
            # managed files must never become apparently available quota.
            stack = [root]
            try:
                device = self.roots[0].stat().st_dev
                root_info = root.lstat()
                if not stat.S_ISDIR(root_info.st_mode) or root_info.st_dev != device:
                    raise DiskAdmissionError('Disk ledger root identity or volume changed')
                while stack:
                    with os.scandir(stack.pop()) as entries:
                        for entry in entries:
                            if Path(entry.path) in metadata_paths:
                                continue
                            try: info = Path(entry.path).stat(follow_symlinks=False)
                            except FileNotFoundError: continue
                            if info.st_dev != device:
                                raise DiskAdmissionError('Disk ledger root spans another volume')
                            if stat.S_ISDIR(info.st_mode): stack.append(Path(entry.path))
                            elif stat.S_ISREG(info.st_mode): total += info.st_size
            except OSError as exc:
                raise DiskAdmissionError(f'Disk ledger root cannot be observed: {exc}') from exc
        return total

    def _observed_free(self):
        st = os.statvfs(self.roots[0])
        return st.f_bavail * st.f_frsize

    def register_roots(self,roots):
        """Charge local stores; persistent roots are kept as a bounded antichain."""
        self._require_writer()
        incoming=tuple(dict.fromkeys(Path(p).resolve() for p in roots))
        if len(incoming) > 256:
            raise DiskAdmissionError('Disk ledger root limit exceeded (input list)')
        device=self.roots[0].stat().st_dev
        if any(p.stat().st_dev!=device for p in incoming):raise ValueError('Roots must stay on one volume')
        with self._ledger_transaction():
            existing = [Path(path) for (path,) in self.db.execute('SELECT path FROM roots')]
            selected = _root_antichain(existing + list(incoming))
            if len(selected) > 256:
                raise DiskAdmissionError('Disk ledger root limit exceeded')
            self.db.execute('DELETE FROM roots')
            self.db.executemany('INSERT INTO roots VALUES(?)', [(str(p),) for p in selected])
            if self.shared_policy:
                self._check_shared_root_capacity()
            if self._coord_ready:
                self._register_with_coordinator()

    def register_export_root(self,path):
        from .export_admission import register_export_root
        return register_export_root(self,path)

    @property
    def reserved_bytes(self):
        with self._path_lock:
            row = self.db.execute("SELECT COALESCE(SUM(bytes-materialized),0) FROM reservations WHERE state IN ('RESERVED','WRITING')").fetchone()
            return row[0]

    @property
    def lease_reserved_bytes(self):
        """Unmaterialized bytes held by reusable trace/journal/catalog ledger tokens."""
        with self._path_lock:
            marks = ','.join('?' * len(LEASE_OPERATIONS))
            row = self.db.execute(f"SELECT COALESCE(SUM(bytes-materialized),0) FROM reservations WHERE state IN ('RESERVED','WRITING') AND operation IN ({marks})",
                                  LEASE_OPERATIONS).fetchone()
            return row[0]

    def _checkpoint_link(self, link, amount):
        if type(link) is not dict:
            raise ValueError('Invalid checkpoint link')
        if link.get('purpose') == 'state-export':
            from .export_admission import normalize_intent
            return normalize_intent(self,link,amount)
        if link.get('purpose') == 'catalog-startup':
            from .catalog_startup import normalize_intent
            return normalize_intent(self,link,amount)
        keys = {'dataset_root','dataset_id','snapshot_id','rank','stage','target','temporary','digest'}
        if set(link) != keys:
            raise ValueError('Invalid checkpoint link fields')
        raw_root = Path(link['dataset_root']) if isinstance(link['dataset_root'], str) else None
        if raw_root is None or not raw_root.is_absolute(): raise ValueError('dataset_root must be absolute')
        def reject_symlinks(path, base=None):
            path = Path(os.path.abspath(path))
            parts = [path, *path.parents]
            if base is not None:
                base=Path(os.path.abspath(base))
                parts=parts[:parts.index(base)+1]
            for item in reversed(parts):
                try: mode = item.lstat().st_mode
                except FileNotFoundError: continue
                if stat.S_ISLNK(mode): raise ValueError('Checkpoint paths cannot contain symlinks')
        try:
            if stat.S_ISLNK(raw_root.lstat().st_mode): raise ValueError('dataset_root cannot be a symlink')
        except FileNotFoundError:
            pass
        if not raw_root.is_dir(): raise ValueError('dataset_root must be an existing directory')
        root = raw_root.resolve(strict=True)
        for name in ('dataset_id','snapshot_id'):
            if not isinstance(link[name],str) or re.fullmatch(r'[0-9a-f]{32}',link[name]) is None:
                raise ValueError('Invalid checkpoint UUID')
        rank=link['rank']
        if isinstance(rank,bool) or not isinstance(rank,int) or rank<0 or rank>1024: raise ValueError('Invalid checkpoint rank')
        stage=link['stage']
        if stage not in ('prepare','decision','decision-copy','metadata-payload','metadata-chunk'):
            raise ValueError('Invalid checkpoint stage')
        if not isinstance(link['digest'],str) or re.fullmatch(r'[0-9a-f]{64}',link['digest']) is None:
            raise ValueError('Invalid checkpoint digest')
        if stage=='prepare': rel=Path(f'rank-{rank:04d}/manifests/prepare-{link["snapshot_id"]}.json')
        elif stage=='decision':
            if rank != 0: raise ValueError('Decision checkpoint rank must be zero')
            rel=Path(f'global/manifests/{link["snapshot_id"]}.json')
        elif stage=='decision-copy': rel=Path(f'rank-{rank:04d}/manifests/decision-{link["snapshot_id"]}.json')
        elif stage=='metadata-payload': rel=Path(f'rank-{rank:04d}/manifests/payload-{link["snapshot_id"]}.json')
        else: rel=Path(f'rank-{rank:04d}/manifests/chunk-{link["digest"]}.json')
        target_input=link['target']; temp_input=link['temporary']
        if not isinstance(target_input,str) or not isinstance(temp_input,str): raise ValueError('Invalid checkpoint paths')
        target=Path(target_input); temp=Path(temp_input)
        def in_root_lexical(path):
            if not path.is_absolute():
                relative=path
            else:
                relative=None
                for base in (raw_root,root):
                    try:
                        relative=path.relative_to(base)
                        break
                    except ValueError:
                        pass
                if relative is None:
                    raise ValueError('Checkpoint path is outside dataset_root')
            if '..' in relative.parts:
                raise ValueError('Checkpoint path cannot traverse dataset_root')
            return relative
        target_relative=in_root_lexical(target)
        temp_relative=in_root_lexical(temp)
        if target_relative != rel: raise ValueError('Checkpoint target does not match stage')
        target_candidate=target if target.is_absolute() else raw_root/target
        temp_candidate=temp if temp.is_absolute() else raw_root/temp
        expected=root/rel
        temp_name=temp_candidate.name
        if re.fullmatch(r'\.'+re.escape(expected.name)+r'\.[0-9a-f]{32}\.tmp',temp_name) is None:
            raise ValueError('Invalid checkpoint temporary path')
        if temp_relative.parent != rel.parent: raise ValueError('Invalid checkpoint temporary path')
        temp_abs=expected.parent/temp_name
        reject_symlinks(expected,root); reject_symlinks(temp_abs,root)
        # Resolve each candidate's parent with filesystem semantics, including
        # root-external symlink and '..' aliases, then require the resulting
        # location to equal the canonical stage path. Keep the final component
        # intact so the lstat checks below can reject a symlink leaf.
        target_resolved=target_candidate.parent.resolve(strict=False)/target_candidate.name
        temp_resolved=temp_candidate.parent.resolve(strict=False)/temp_candidate.name
        if target_resolved != expected: raise ValueError('Checkpoint target does not resolve under dataset_root')
        if temp_resolved != temp_abs: raise ValueError('Checkpoint temporary path does not resolve under dataset_root')
        target_existed=False
        try:
            mode=expected.lstat().st_mode
            if stat.S_ISLNK(mode): raise ValueError('Checkpoint target cannot be a symlink')
            if not stat.S_ISREG(mode): raise ValueError('Existing checkpoint target must be a regular file')
            target_existed=True
        except FileNotFoundError: pass
        try:
            temp_mode=temp_abs.lstat().st_mode
            if stat.S_ISLNK(temp_mode): raise ValueError('Checkpoint temporary path cannot be a symlink')
            if not stat.S_ISREG(temp_mode): raise ValueError('Existing checkpoint temporary path must be a regular file')
            raise ValueError('Checkpoint temporary path already exists')
        except FileNotFoundError: pass
        dev=root.stat().st_dev
        roots=[Path(p) for (p,) in self.db.execute('SELECT path FROM roots')]
        if not any(root.is_relative_to(p) for p in roots) or any(p.stat().st_dev != dev for p in roots if root.is_relative_to(p)):
            raise ValueError('dataset_root must be covered by a registered root on the same volume')
        root_info=root.stat()
        canonical={'dataset_root':str(root),'dataset_device':root_info.st_dev,'dataset_inode':root_info.st_ino,
                   'dataset_id':link['dataset_id'],'snapshot_id':link['snapshot_id'],
                   'rank':rank,'stage':stage,'target':str(expected.relative_to(root)),
                   'temporary':str(temp_abs.relative_to(root)),'digest':link['digest'],
                   'target_existed':target_existed,'expected_bytes':amount}
        if stage.startswith('metadata-'):
            if target_existed: raise ValueError('Checkpoint metadata is write-once; target already exists')
            from .catalog_admission import catalog_birth_identity
            canonical['dataset_birth_identity']=catalog_birth_identity(root,root_info)
        encoded=json.dumps(canonical,sort_keys=True,separators=(',',':'),ensure_ascii=False)
        if len(encoded.encode('utf-8'))>4096: raise ValueError('Checkpoint link exceeds 4096 UTF-8 bytes')
        return encoded

    def reserve(self, amount, *, drain=False, operation='unspecified', owner=None, generation=None, writing=False, checkpoint_link=None):
        self._require_writer()
        _integer(amount, 'reservation bytes')
        if not isinstance(drain, bool): raise ValueError('Invalid drain flag')
        if type(writing) is not bool: raise ValueError('Invalid writing flag')
        operation = _bounded_text(operation, 'operation', 64)
        owner = _bounded_text(owner, 'owner', 256, optional=True)
        generation = _bounded_text(generation, 'generation', 128, optional=True)
        link_json = None if checkpoint_link is None else self._checkpoint_link(checkpoint_link, amount)
        key = uuid4().hex
        with self._ledger_transaction():
            if checkpoint_link is not None and checkpoint_link.get('purpose')=='catalog-startup':
                # Serialize creation ownership with the token insertion, not just
                # the earlier read-only normalization.
                link_json=self._checkpoint_link(checkpoint_link,amount)
            count = self.db.execute('SELECT count(*) FROM reservations').fetchone()[0]
            if count >= self.max_records - 2:  # retain terminal slots for release/recovery bookkeeping
                raise DiskAdmissionError('Disk reservation ledger is full')
            pending = self.reserved_bytes
            margin = 0 if drain else self.drain_reserve_bytes
            observed = self._usage()
            metadata_charge = 0 if self._custom_accounting else self.bootstrap_bytes
            if self.quota_bytes is not None and observed + metadata_charge + pending + amount + margin > self.quota_bytes:
                raise DiskAdmissionError('Host/device disk quota exceeded')
            if self._free()-self._unallocated_metadata_pool()-pending-amount-margin < self.min_free_bytes:
                raise DiskAdmissionError('Host/device minimum free space violated')
            if self._coord_ready:
                # Leaf lock: the joint volume check commits before this ledger's own commit (an over-count on a
                # later failure, never an under-count).
                self._coord.admit_ledger(self._ledger_path,epoch=self._epoch(),pending=pending+amount,drain=drain)
            self.db.execute('INSERT INTO reservations(id,bytes,operation,owner,generation,state) VALUES(?,?,?,?,?,?)',
                             (key, amount, operation, owner, generation, 'WRITING' if writing else 'RESERVED'))
            if link_json is not None:
                self.db.execute('INSERT INTO checkpoint_reservations(id,link) VALUES(?,?)',(key,link_json))
        return key

    def mark_writing(self, key):
        self._require_writer()
        with self._ledger_transaction():
            cur = self.db.execute("UPDATE reservations SET state='WRITING' WHERE id=? AND state='RESERVED'", (key,))
            if cur.rowcount == 0 and self.reservation(key) is None:
                raise KeyError(key)

    def materialize(self, key, amount):
        self._require_writer()
        _integer(amount, 'materialized bytes')
        with self._ledger_transaction():
            row = self.db.execute("SELECT bytes,materialized,state FROM reservations WHERE id=?", (key,)).fetchone()
            if row is None: raise KeyError(key)
            if row[2] not in ('RESERVED','WRITING'): raise ValueError('Reservation is terminal')
            if amount < row[1] or amount > row[0]: raise ValueError('Materialized bytes exceed reservation')
            self.db.execute("UPDATE reservations SET materialized=?,state='WRITING' WHERE id=?", (amount,key))

    def reservation(self, key):
        with self._path_lock:
            row = self.db.execute('SELECT id,bytes,materialized,operation,owner,generation,state,evidence FROM reservations WHERE id=?',(key,)).fetchone()
        if row is None: return None
        names=('id','bytes','materialized_bytes','operation','owner','generation','state','evidence')
        return dict(zip(names,row))

    def snapshot(self):
        with self._path_lock:
            rows = self.db.execute("SELECT state,count(*),COALESCE(sum(bytes),0),COALESCE(sum(materialized),0) FROM reservations GROUP BY state").fetchall()
            return {'reserved_bytes':self.reserved_bytes,'states':{r[0]:{'count':r[1],'bytes':r[2],'materialized_bytes':r[3]} for r in rows},'max_records':self.max_records,
                    'shared_policy':self.shared_policy,'store_private':self.store_private,'recovery_only':self.recovery_only,
                    'registered_root_count':self.db.execute('SELECT count(*) FROM roots').fetchone()[0],
                    'lease_reserved_bytes':self.lease_reserved_bytes,'metadata_pool_bytes':self.bootstrap_bytes,'metadata_main_cap_bytes':self._ledger_main_cap,
                    'metadata_side_cap_bytes':self._ledger_side_cap,'ledger_vfs_enforced':self.ledger_vfs_enforced,
                    'ledger_vfs_capability':self._guard.capability() if self._guard else None,
                    'accounting_scope':self.accounting_scope,'strict_capacity_guarantee':False}

    def reconcile(self, key, *, materialized_bytes, evidence, terminal_state='RECONCILED'):
        """Explicitly settle one live reservation after external recovery checks.

        This method does not infer owner death from a PID or lease and does not
        inspect catalog cuts. The caller must supply bounded evidence from its
        recovery procedure. Only RESERVED/WRITING rows may be settled, and the
        state transition is conditional so concurrent or repeated recovery is
        rejected rather than silently releasing the same token twice.
        """
        self._require_writer()
        _integer(materialized_bytes, 'materialized bytes')
        if not isinstance(evidence,str) or not evidence.strip(): raise ValueError('Explicit recovery evidence is required')
        evidence = _bounded_text(evidence, 'recovery evidence', 256)
        if terminal_state not in ('DURABLE','ROLLED_BACK','RECONCILED'): raise ValueError('Invalid terminal state')
        with self._ledger_transaction():
            self._reconcile_row(key,materialized_bytes,evidence,terminal_state)

    def _reconcile_row(self,key,materialized_bytes,evidence,terminal_state):
        row=self.db.execute('SELECT bytes,materialized,state FROM reservations WHERE id=?',(key,)).fetchone()
        if row is None: raise KeyError(key)
        reserved,previous_materialized,state=row
        if state not in ('RESERVED','WRITING'): raise DiskAdmissionError('Reservation is already terminal; recovery was already applied')
        if materialized_bytes>reserved: raise ValueError('Materialized bytes exceed reservation')
        if materialized_bytes<previous_materialized: raise ValueError('Recovery cannot erase previously materialized byte accounting')
        changed=self.db.execute("UPDATE reservations SET materialized=?,state=?,evidence=? WHERE id=? AND state IN ('RESERVED','WRITING')",
                                (materialized_bytes,terminal_state,evidence,key)).rowcount
        if changed!=1: raise DiskAdmissionError('Reservation changed during recovery')

    def checkpoint_link(self,key):
        """The persisted checkpoint intent of one reservation, or None."""
        with self._path_lock:
            row=self.db.execute('SELECT link FROM checkpoint_reservations WHERE id=?',(key,)).fetchone()
        return None if row is None else json.loads(row[0])

    def bind_catalog_startup(self,key,*,expected_reservation,expected_link,phase):
        """CAS-bind an admitted catalog creation to its durable inode generation."""
        from .catalog_startup import cas_bind
        return cas_bind(self,key,expected_reservation=expected_reservation,expected_link=expected_link,phase=phase)

    def checkpoint_links(self,dataset_root,limit=128):
        _integer(limit,'checkpoint link limit',minimum=1)
        root=str(Path(dataset_root).resolve())
        names=('id','bytes','materialized_bytes','operation','owner','generation','state','evidence')
        result=[]
        with self._path_lock:
            cursor=self.db.execute('''SELECT r.id,r.bytes,r.materialized,r.operation,r.owner,r.generation,r.state,r.evidence,c.link
                FROM checkpoint_reservations c JOIN reservations r ON r.id=c.id ORDER BY r.id''')
            for row in cursor:
                link=json.loads(row[8])
                if link.get('dataset_root')!=root: continue
                result.append({'reservation':dict(zip(names,row[:8])),'link':link})
                if len(result)>limit: raise DiskAdmissionError('Checkpoint link result exceeds limit')
        return result

    def reconcile_checkpoint(self,key,*,expected_reservation,expected_link,materialized_bytes,evidence,terminal_state='RECONCILED',evidence_document=None):
        if self.recovery_only and evidence_document is None:
            raise DiskAdmissionError('Recovery-only checkpoint CAS requires a persisted proof receipt')
        _integer(materialized_bytes,'materialized bytes')
        if not isinstance(evidence,str) or not evidence.strip(): raise ValueError('Explicit recovery evidence is required')
        evidence=_bounded_text(evidence,'recovery evidence',256)
        if terminal_state not in ('DURABLE','ROLLED_BACK','RECONCILED'): raise ValueError('Invalid terminal state')
        proof_json=None if evidence_document is None else self._proof_document(evidence,evidence_document)
        with self._ledger_transaction():
            current=self.reservation(key)
            linkrow=self.db.execute('SELECT link FROM checkpoint_reservations WHERE id=?',(key,)).fetchone()
            if current is None or linkrow is None: raise DiskAdmissionError('Checkpoint reservation intent is missing')
            link=json.loads(linkrow[0])
            if current != expected_reservation or link != expected_link: raise DiskAdmissionError('Checkpoint reservation CAS mismatch')
            if current['state'] not in ('RESERVED','WRITING'): raise DiskAdmissionError('Reservation is already terminal')
            self._reconcile_row(key,materialized_bytes,evidence,terminal_state)
            if proof_json is not None:
                self.db.execute('INSERT INTO checkpoint_recovery_receipts(id,proof) VALUES(?,?)',(key,proof_json))

    @staticmethod
    def _proof_document(evidence,document):
        if type(document) is not dict: raise ValueError('Recovery evidence document must be an object')
        proof_json=json.dumps(document,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False)
        proof_bytes=proof_json.encode('utf-8')
        if len(proof_bytes)>8192: raise ValueError('Recovery evidence document exceeds 8192 UTF-8 bytes')
        import hashlib
        if not evidence.endswith(':'+hashlib.sha256(proof_bytes).hexdigest()):
            raise ValueError('Recovery evidence digest does not match evidence document')
        return proof_json

    def live_reservations(self,*,linked,limit=256):
        """Live RESERVED/WRITING rows with (linked=True) or without a checkpoint intent."""
        _integer(limit,'live reservation limit',minimum=1)
        names=('id','bytes','materialized_bytes','operation','owner','generation','state','evidence')
        with self._path_lock:
            rows=self.db.execute(f"""SELECT r.id,r.bytes,r.materialized,r.operation,r.owner,r.generation,r.state,r.evidence
                FROM reservations r LEFT JOIN checkpoint_reservations c ON c.id=r.id
                WHERE r.state IN ('RESERVED','WRITING') AND c.id IS {'NOT NULL' if linked else 'NULL'} ORDER BY r.id LIMIT ?""",(limit+1,)).fetchall()
        if len(rows)>limit: raise DiskAdmissionError('Live reservation result exceeds limit')
        return [dict(zip(names,row)) for row in rows]

    def reconcile_unlinked(self,key,*,expected_reservation,materialized_bytes,evidence,evidence_document,terminal_state='RECONCILED'):
        """Offline CAS settlement of a live token that has no checkpoint intent.

        Only a recovery-only budget opened under an exclusive ledger lease may
        call this. The settlement is conditional on the exact inspected row,
        never lowers materialized bytes, and stores the proof receipt in the
        same transaction, so a second application is refused.
        """
        if not self.recovery_only: raise DiskAdmissionError('Unlinked settlement requires a recovery-only ledger')
        _integer(materialized_bytes,'materialized bytes')
        if not isinstance(evidence,str) or not evidence.strip(): raise ValueError('Explicit recovery evidence is required')
        evidence=_bounded_text(evidence,'recovery evidence',256)
        if terminal_state not in ('RECONCILED','ROLLED_BACK'): raise ValueError('Invalid terminal state')
        proof_json=self._proof_document(evidence,evidence_document)
        with self._ledger_transaction():
            current=self.reservation(key)
            if current is None: raise KeyError(key)
            if self.db.execute('SELECT 1 FROM checkpoint_reservations WHERE id=?',(key,)).fetchone():
                raise DiskAdmissionError('Reservation has a checkpoint intent; use checkpoint recovery')
            if current!=expected_reservation: raise DiskAdmissionError('Unlinked reservation CAS mismatch')
            self._reconcile_row(key,materialized_bytes,evidence,terminal_state)
            self.db.execute('INSERT INTO checkpoint_recovery_receipts(id,proof) VALUES(?,?)',(key,proof_json))

    def recovery_receipt(self,key):
        with self._path_lock:
            row=self.db.execute('SELECT proof FROM checkpoint_recovery_receipts WHERE id=?',(key,)).fetchone()
        return None if row is None else json.loads(row[0])

    def release(self, key):
        self._require_writer()
        # Idempotent for old callers; completed tokens are removed only at this
        # explicit quiescent boundary. A process crash never invokes this path.
        with self._ledger_transaction():
            self.db.execute("UPDATE reservations SET state='RECONCILED',evidence='release' WHERE id=? AND state IN ('RESERVED','WRITING')",(key,))
            removed=self.db.execute("DELETE FROM reservations WHERE id=? AND state='RECONCILED'",(key,)).rowcount
            if removed:
                self.db.execute('DELETE FROM checkpoint_reservations WHERE id=?',(key,))
                self.db.execute('DELETE FROM checkpoint_recovery_receipts WHERE id=?',(key,))

    @contextmanager
    def allocation(self, amount, *, drain=False, operation='allocation', owner=None, generation=None,
                   retain_on_error=False):
        """Reserve an operation, optionally retaining failed-write evidence.

        A caller using retain_on_error may release explicitly after verified
        cleanup. Neither this context nor release performs cut-based recovery.
        """
        if type(retain_on_error) is not bool:raise ValueError('Invalid retain-on-error flag')
        key = self.reserve(amount, drain=drain, operation=operation, owner=owner, generation=generation,writing=True)
        release=True
        try:
            yield key
        except BaseException:
            release=not retain_on_error
            raise
        finally:
            if release:self.release(key)

    def close(self):
        if self._closed:return
        # Append writers are quiescent once their owner closes the budget; their
        # written bytes are on disk and observed, so the shared token is released.
        for lease in list(self._ledger_leases): lease.release()
        with self._path_lock:
            try:
                self._set_guard_active(False)
                self.db.close()
            finally:
                self._release_registry()
                if self._ledger_lease_owned and self._ledger_lease is not None:self._ledger_lease.close()
                self._closed=True
                if self._coord_owned:self._coord.close()
