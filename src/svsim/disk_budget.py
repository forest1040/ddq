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
from uuid import uuid4


class DiskAdmissionError(OSError):
    pass


_LEDGER_REGISTRY = {}
_LEDGER_REGISTRY_LOCK = threading.RLock()
_LEDGER_PATH_LOCKS = {}
_LEDGER_PATH_LOCKS_LOCK = threading.RLock()


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


class DiskBudget:
    """Coordinate byte reservations through a SQLite ledger on one volume."""

    def __init__(self, ledger, roots, *, quota_bytes=None, min_free_bytes=0,
                 drain_reserve_bytes=0, usage=None, free=None,
                 max_page_count=1024, max_records=4096, bootstrap_bytes=8*1024*1024,
                 shared_policy=False):
        _integer(min_free_bytes, 'minimum free bytes')
        _integer(drain_reserve_bytes, 'drain reserve bytes')
        _integer(quota_bytes, 'disk quota', minimum=1, optional=True)
        _integer(max_page_count, 'maximum ledger pages', minimum=1)
        _integer(max_records, 'maximum ledger records', minimum=8)
        _integer(bootstrap_bytes, 'ledger bootstrap bytes', minimum=1)
        if type(shared_policy) is not bool:
            raise ValueError('Invalid shared ledger policy flag')
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
        ledger = Path(ledger).resolve()
        self._ledger_path = ledger
        ledger.parent.mkdir(parents=True, exist_ok=True)
        if ledger.parent.stat().st_dev not in devices:
            raise ValueError('Disk ledger must be on the budgeted volume')
        self._path_lock = _path_lock(ledger)
        self._registry_key = str(ledger)
        self._guard = None
        self._registry_acquired = False
        self._closed = False
        if not self._custom_accounting:
            try:
                from .catalog_admission import get_sqlite_guard
                self._guard = get_sqlite_guard()
            except (ImportError, NotImplementedError, OSError, sqlite3.Error) as exc:
                raise DiskAdmissionError(f'Bounded ledger VFS unavailable: {exc}') from exc
        self.ledger_vfs_enforced = self._guard is not None
        self.accounting_scope = ('injected' if self._custom_accounting else
                                 'observed-with-bounded-ledger-vfs' if self.ledger_vfs_enforced else
                                 'observed-soft')
        self.strict_capacity_guarantee = False
        if ledger.exists() and ledger.stat().st_size > self._ledger_main_cap:
            raise DiskAdmissionError('Existing disk ledger exceeds its bounded metadata pool')
        if self._metadata_allocated_bytes()>self.bootstrap_bytes:
            raise DiskAdmissionError('Existing ledger and journal exceed the metadata pool')
        # Account for the bounded local metadata pool before its first write.
        # Injected accounting hooks are deterministic test/accounting scopes.
        if not self._custom_accounting:
            if quota_bytes is not None and self._usage() + bootstrap_bytes + drain_reserve_bytes > quota_bytes:
                raise DiskAdmissionError('Ledger bootstrap exceeds host/device disk quota')
            if self._free() - self._unallocated_metadata_pool() - drain_reserve_bytes < min_free_bytes:
                raise DiskAdmissionError('Ledger bootstrap violates minimum free space')
        self.db = self._open_ledger()
        try:
            with self._path_lock:
                self._set_guard_active(True)
                self.db.execute('PRAGMA synchronous=FULL')
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
                    self.db.execute('''CREATE TABLE IF NOT EXISTS reservations(
                        id TEXT PRIMARY KEY, bytes INTEGER NOT NULL, materialized INTEGER NOT NULL DEFAULT 0,
                        operation TEXT NOT NULL DEFAULT 'legacy', owner TEXT, generation TEXT,
                        state TEXT NOT NULL DEFAULT 'RESERVED', evidence TEXT)''')
                    cols = {row[1] for row in self.db.execute('PRAGMA table_info(reservations)')}
                    for name, decl in (('materialized','INTEGER NOT NULL DEFAULT 0'),
                                       ('operation',"TEXT NOT NULL DEFAULT 'legacy'"), ('owner','TEXT'),
                                       ('generation','TEXT'), ('state',"TEXT NOT NULL DEFAULT 'RESERVED'"),
                                       ('evidence','TEXT')):
                        if name not in cols:
                            self.db.execute(f'ALTER TABLE reservations ADD COLUMN {name} {decl}')
                    self.db.execute('CREATE TABLE IF NOT EXISTS roots(path TEXT PRIMARY KEY)')
                    self.db.execute('''CREATE TABLE IF NOT EXISTS checkpoint_reservations(
                        id TEXT PRIMARY KEY, link TEXT NOT NULL CHECK(length(CAST(link AS BLOB))<=4096),
                        FOREIGN KEY(id) REFERENCES reservations(id) ON DELETE CASCADE)''')
                    self.db.execute('''CREATE TABLE IF NOT EXISTS checkpoint_recovery_receipts(
                        id TEXT PRIMARY KEY, proof TEXT NOT NULL CHECK(length(CAST(proof AS BLOB))<=8192),
                        FOREIGN KEY(id) REFERENCES reservations(id) ON DELETE CASCADE)''')
                    self.db.execute('''CREATE TABLE IF NOT EXISTS shared_policy(
                        singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                        quota_bytes INTEGER, min_free_bytes INTEGER NOT NULL,
                        drain_reserve_bytes INTEGER NOT NULL, bootstrap_bytes INTEGER NOT NULL,
                        max_page_count INTEGER NOT NULL, max_records INTEGER NOT NULL,
                        volume_device INTEGER NOT NULL, custom_accounting INTEGER NOT NULL)''')
                    self.db.execute('''CREATE TRIGGER IF NOT EXISTS reservation_labels_insert
                        BEFORE INSERT ON reservations
                        WHEN length(CAST(NEW.operation AS BLOB))>64
                          OR (NEW.owner IS NOT NULL AND length(CAST(NEW.owner AS BLOB))>256)
                          OR (NEW.generation IS NOT NULL AND length(CAST(NEW.generation AS BLOB))>128)
                          OR (NEW.evidence IS NOT NULL AND length(CAST(NEW.evidence AS BLOB))>256)
                        BEGIN SELECT RAISE(ABORT,'reservation metadata exceeds byte limit'); END''')
                    self.db.execute('''CREATE TRIGGER IF NOT EXISTS reservation_labels_update
                        BEFORE UPDATE OF operation,owner,generation,evidence ON reservations
                        WHEN length(CAST(NEW.operation AS BLOB))>64
                          OR (NEW.owner IS NOT NULL AND length(CAST(NEW.owner AS BLOB))>256)
                          OR (NEW.generation IS NOT NULL AND length(CAST(NEW.generation AS BLOB))>128)
                          OR (NEW.evidence IS NOT NULL AND length(CAST(NEW.evidence AS BLOB))>256)
                        BEGIN SELECT RAISE(ABORT,'reservation metadata exceeds byte limit'); END''')
                    self._check_or_bind_shared_policy(devices.pop())
                    existing_roots = [Path(path) for (path,) in self.db.execute('SELECT path FROM roots')]
                    selected_roots = _root_antichain(existing_roots + list(self.roots))
                    if len(selected_roots) > 256:
                        raise DiskAdmissionError('Disk ledger root limit exceeded')
                    self.db.execute('DELETE FROM roots')
                    self.db.executemany('INSERT INTO roots VALUES(?)', [(str(p),) for p in selected_roots])
                    if self.shared_policy:
                        self._check_shared_root_capacity()
                    self.db.execute('PRAGMA user_version=3')
        except BaseException:
            try:
                self._set_guard_active(False)
            except Exception:
                pass
            try: self.db.close()
            finally: self._release_registry()
            raise
        else:
            self._set_guard_active(False)

    def _open_ledger(self):
        with _LEDGER_REGISTRY_LOCK:
            entry = _LEDGER_REGISTRY.get(self._registry_key)
            if self._guard is not None:
                if entry is None:
                    db = self._guard.connect(self._ledger_path,
                                             main_cap_bytes=self._ledger_main_cap,
                                             side_cap_bytes=self._ledger_side_cap)
                    _LEDGER_REGISTRY[self._registry_key] = [self._guard, 1]
                else:
                    if entry[0] is not self._guard:
                        raise DiskAdmissionError('Ledger is registered with a different SQLite guard')
                    db = self._guard.connect(self._ledger_path)
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
            if not active_already:
                self._set_guard_active(True)
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
        registered = (Path(p) for (p,) in self.db.execute('SELECT path FROM roots')) if hasattr(self, 'db') else self.roots
        roots=sorted(registered,key=lambda p:len(p.parts))
        selected=[]
        metadata_paths = {self._ledger_path, Path(str(self._ledger_path) + '-journal')}
        for root in roots:
            if any(root.is_relative_to(parent) for parent in selected): continue
            selected.append(root)
            for path in root.rglob('*'):
                if path in metadata_paths:
                    continue
                try: info=path.stat(follow_symlinks=False)
                except FileNotFoundError: continue
                if stat.S_ISREG(info.st_mode): total += info.st_size
        return total

    def _observed_free(self):
        st = os.statvfs(self.roots[0])
        return st.f_bavail * st.f_frsize

    def register_roots(self,roots):
        """Charge local stores; persistent roots are kept as a bounded antichain."""
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

    @property
    def reserved_bytes(self):
        with self._path_lock:
            row = self.db.execute("SELECT COALESCE(SUM(bytes-materialized),0) FROM reservations WHERE state IN ('RESERVED','WRITING')").fetchone()
            return row[0]

    def _checkpoint_link(self, link, amount):
        if type(link) is not dict:
            raise ValueError('Invalid checkpoint link')
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
        if stage not in ('prepare','decision','decision-copy'): raise ValueError('Invalid checkpoint stage')
        if not isinstance(link['digest'],str) or re.fullmatch(r'[0-9a-f]{64}',link['digest']) is None:
            raise ValueError('Invalid checkpoint digest')
        if stage=='prepare': rel=Path(f'rank-{rank:04d}/manifests/prepare-{link["snapshot_id"]}.json')
        elif stage=='decision':
            if rank != 0: raise ValueError('Decision checkpoint rank must be zero')
            rel=Path(f'global/manifests/{link["snapshot_id"]}.json')
        else: rel=Path(f'rank-{rank:04d}/manifests/decision-{link["snapshot_id"]}.json')
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
        encoded=json.dumps(canonical,sort_keys=True,separators=(',',':'),ensure_ascii=False)
        if len(encoded.encode('utf-8'))>4096: raise ValueError('Checkpoint link exceeds 4096 UTF-8 bytes')
        return encoded

    def reserve(self, amount, *, drain=False, operation='unspecified', owner=None, generation=None, writing=False, checkpoint_link=None):
        _integer(amount, 'reservation bytes')
        if not isinstance(drain, bool): raise ValueError('Invalid drain flag')
        if type(writing) is not bool: raise ValueError('Invalid writing flag')
        operation = _bounded_text(operation, 'operation', 64)
        owner = _bounded_text(owner, 'owner', 256, optional=True)
        generation = _bounded_text(generation, 'generation', 128, optional=True)
        link_json = None if checkpoint_link is None else self._checkpoint_link(checkpoint_link, amount)
        key = uuid4().hex
        with self._ledger_transaction():
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
            self.db.execute('INSERT INTO reservations(id,bytes,operation,owner,generation,state) VALUES(?,?,?,?,?,?)',
                             (key, amount, operation, owner, generation, 'WRITING' if writing else 'RESERVED'))
            if link_json is not None:
                self.db.execute('INSERT INTO checkpoint_reservations(id,link) VALUES(?,?)',(key,link_json))
        return key

    def mark_writing(self, key):
        with self._ledger_transaction():
            cur = self.db.execute("UPDATE reservations SET state='WRITING' WHERE id=? AND state='RESERVED'", (key,))
            if cur.rowcount == 0 and self.reservation(key) is None:
                raise KeyError(key)

    def materialize(self, key, amount):
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
                    'shared_policy':self.shared_policy,
                    'registered_root_count':self.db.execute('SELECT count(*) FROM roots').fetchone()[0],
                    'metadata_pool_bytes':self.bootstrap_bytes,'metadata_main_cap_bytes':self._ledger_main_cap,
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
        _integer(materialized_bytes,'materialized bytes')
        if not isinstance(evidence,str) or not evidence.strip(): raise ValueError('Explicit recovery evidence is required')
        evidence=_bounded_text(evidence,'recovery evidence',256)
        if terminal_state not in ('DURABLE','ROLLED_BACK','RECONCILED'): raise ValueError('Invalid terminal state')
        proof_json=None
        if evidence_document is not None:
            if type(evidence_document) is not dict: raise ValueError('Recovery evidence document must be an object')
            proof_json=json.dumps(evidence_document,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False)
            proof_bytes=proof_json.encode('utf-8')
            if len(proof_bytes)>8192: raise ValueError('Recovery evidence document exceeds 8192 UTF-8 bytes')
            import hashlib
            if not evidence.endswith(':'+hashlib.sha256(proof_bytes).hexdigest()):
                raise ValueError('Recovery evidence digest does not match evidence document')
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

    def recovery_receipt(self,key):
        with self._path_lock:
            row=self.db.execute('SELECT proof FROM checkpoint_recovery_receipts WHERE id=?',(key,)).fetchone()
        return None if row is None else json.loads(row[0])

    def release(self, key):
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
        with self._path_lock:
            try:
                self._set_guard_active(False)
                self.db.close()
            finally:
                self._release_registry()
                self._closed=True
