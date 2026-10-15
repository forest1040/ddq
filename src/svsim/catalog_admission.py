"""Pre-transaction catalog reservations and optional same-SQLite VFS enforcement.

All mutation paths pass through this proxy, including cursors and scripts.
Native loading/building happens only when a writable catalog is opened.
"""
from __future__ import annotations

from contextlib import contextmanager
from time import perf_counter
from hashlib import sha256
from copy import deepcopy
import json
import os
from pathlib import Path
import shutil
import sqlite3
import stat
import struct
import subprocess
import sys
import tempfile
import threading

from .disk_budget import DiskAdmissionError, register_lease, unregister_lease


class CatalogAdmissionError(DiskAdmissionError):
    pass


_GUARD = None
_ACTIVE_CATALOGS = {}


class SQLiteGuard:
    def __init__(self):
        if not hasattr(sqlite3.Connection,'enable_load_extension'):
            raise NotImplementedError('Python SQLite cannot load a same-library VFS')
        source = Path(__file__).with_name('native')/'sqlite_budget_vfs.c'
        compiler = shutil.which('cc') or shutil.which('clang')
        if not compiler: raise NotImplementedError('Installed C compiler is required for catalog VFS')
        includes = [Path('/usr/include'),Path('/usr/local/include'),Path('/opt/homebrew/include'),
                    Path('/Library/Developer/CommandLineTools/SDKs/MacOSX.sdk/usr/include')]
        if shutil.which('brew'):
            result=subprocess.run(['brew','--prefix','sqlite'],capture_output=True,text=True,timeout=5)
            if result.returncode==0:includes.insert(0,Path(result.stdout.strip())/'include')
        header = next((p for p in includes if all((p/name).is_file() for name in ('sqlite3ext.h','sqlite3.h'))),None)
        if header is None: raise NotImplementedError('Installed sqlite3ext.h is required for catalog VFS')
        self._directory = tempfile.TemporaryDirectory(prefix='svsim-sqlite-vfs-')
        self.library = Path(self._directory.name)/('svsim_disk.dylib' if sys.platform=='darwin' else 'svsim_disk.so')
        command = [compiler,'-dynamiclib' if sys.platform=='darwin' else '-shared','-fPIC','-O2',
                   '-I',str(header),str(source),'-o',str(self.library)]
        # Test-only deterministic xSync/xDelete fault points exist only in a binary built with this flag.
        if os.environ.get('SVSIM_VFS_TEST_FAULTS')=='1': command[4:4]=['-DSVSIM_TEST_FAULTS']
        inputs = {str(path): sha256(path.read_bytes()).hexdigest()
                  for path in (source,header/'sqlite3ext.h',header/'sqlite3.h')}
        self.source_hash = inputs[str(source)]
        try:
            result = subprocess.run(command,capture_output=True,text=True,timeout=30)
            if result.returncode: raise NotImplementedError('Catalog VFS build unavailable: '+result.stderr[-1500:])
            if any(sha256(Path(path).read_bytes()).hexdigest()!=digest for path,digest in inputs.items()):
                raise CatalogAdmissionError('SQLite VFS build inputs changed during compilation')
            self._binary_hash = sha256(self.library.read_bytes()).hexdigest()
            compiler_version = subprocess.run([compiler,'--version'],capture_output=True,text=True,timeout=5)
            if compiler_version.returncode: raise NotImplementedError('Cannot identify SQLite VFS compiler')
            self.loader = sqlite3.connect(':memory:')
            identity = self._load(self.loader)
            self._capability = {
                'schema_version':1, 'vfs':'svsim-budget-v1', 'source_sha256':self.source_hash,
                'header_directory':str(header.resolve()),
                'headers':{name:inputs[str(header/name)] for name in ('sqlite3ext.h','sqlite3.h')},
                'compiler':str(Path(compiler).resolve()), 'compiler_version':compiler_version.stdout[:8192],
                'build_flags':command[1:-2], 'binary_sha256':self._binary_hash, **identity,
                'identity_scope':'connection-runtime-and-extension-dispatch',
                'header_runtime_build_match_claimed':False,
            }
            raw=json.dumps(self._capability,sort_keys=True,separators=(',',':')).encode()
            self._capability['fingerprint']=sha256(raw).hexdigest()
        except BaseException:
            if hasattr(self,'loader'):self.loader.close()
            self._directory.cleanup()
            raise

    def capability(self):
        return deepcopy(self._capability)

    @staticmethod
    def _dispatch_identity(connection):
        identity={key:connection.execute('SELECT svsim_disk_guard_identity(?)',(key,)).fetchone()[0]
                  for key in ('runtime_version','runtime_source_id','header_version','header_source_id')}
        options=[]
        for index in range(4096):
            value=connection.execute('SELECT svsim_disk_guard_compile_option(?)',(index,)).fetchone()[0]
            if value is None:break
            options.append(value)
        else:raise CatalogAdmissionError('SQLite compile option registry exceeds limit')
        identity['runtime_compile_options']=sorted(options)
        expected_version,expected_source=connection.execute('SELECT sqlite_version(),sqlite_source_id()').fetchone()
        expected_options=sorted(row[0] for row in connection.execute('PRAGMA compile_options'))
        if (identity['runtime_version']!=expected_version or identity['runtime_source_id']!=expected_source or
                identity['runtime_compile_options']!=expected_options or expected_version!=sqlite3.sqlite_version):
            raise CatalogAdmissionError('SQLite extension dispatch identity mismatch')
        return identity

    def _load(self,connection):
        if sha256(self.library.read_bytes()).hexdigest()!=self._binary_hash:
            raise CatalogAdmissionError('SQLite VFS binary changed after compilation')
        connection.enable_load_extension(True)
        try: connection.execute('SELECT load_extension(?,?)',(str(self.library),'sqlite3_svsimdisk_init')).fetchone()
        finally: connection.enable_load_extension(False)
        identity=self._dispatch_identity(connection)
        if hasattr(self,'_capability'):
            for key,value in identity.items():
                if value!=self._capability[key]:
                    raise CatalogAdmissionError('SQLite VFS identity changed between connections')
        return identity

    def connect(self,path,*,main_cap_bytes=None,side_cap_bytes=None,existing_only=False):
        path = Path(path).resolve()
        prepared = main_cap_bytes is not None
        if prepared:
            self.loader.execute('SELECT svsim_disk_guard_prepare(?,?,?)',
                                (str(path),main_cap_bytes,side_cap_bytes)).fetchone()
        try:
            mode='&mode=rw' if existing_only else ''
            db = sqlite3.connect(path.as_uri()+'?vfs=svsim-budget-v1'+mode,uri=True,timeout=30)
            try: self._load(db)
            except BaseException: db.close(); raise
            return db
        finally:
            if prepared: self.loader.execute('SELECT svsim_disk_guard_unprepare(?)',(str(path),)).fetchone()


def get_sqlite_guard():
    global _GUARD
    if _GUARD is None: _GUARD = SQLiteGuard()
    return _GUARD


def _payload_bytes(value):
    if value is None: return 0
    if isinstance(value,str): return len(value.encode())
    if isinstance(value,(bytes,bytearray,memoryview)): return len(value)
    if isinstance(value,(tuple,list)): return sum(_payload_bytes(v) for v in value)
    if isinstance(value,dict): return sum(_payload_bytes(k)+_payload_bytes(v) for k,v in value.items())
    if type(value) in (int,float): return 32
    raise CatalogAdmissionError('Unsupported SQL parameter type')


class AdmissionCursor:
    def __init__(self,owner,cursor=None): self.owner,self._cursor=owner,cursor
    def execute(self,sql,parameters=()):
        self._cursor=self.owner.execute(sql,parameters)._cursor
        return self
    def executemany(self,sql,parameters):
        self._cursor=self.owner.executemany(sql,parameters)._cursor
        return self
    def executescript(self,sql):
        self._cursor=self.owner.executescript(sql)._cursor
        return self
    def __iter__(self): return iter(self._cursor)
    def __getattr__(self,name):
        if name=='connection': return self.owner
        return getattr(self._cursor,name)


CATALOG_LEASE_SLACK_BYTES = 1024 * 1024


def catalog_birth_identity(path, info):
    """Read stable file birth time, or decline recovery attribution.

    Linux statx has a fixed 256-byte ABI, independent of libc's struct stat.
    Identity fields are cross-checked against the caller's lstat snapshot.
    """
    if hasattr(info, 'st_birthtime_ns'):
        return f'ns:{info.st_birthtime_ns}'
    if hasattr(info, 'st_birthtime'):
        return 'seconds:' + info.st_birthtime.hex()
    if sys.platform != 'linux':
        return None
    import ctypes
    try:
        statx = ctypes.CDLL(None, use_errno=True).statx
        statx.argtypes = [ctypes.c_int, ctypes.c_char_p, ctypes.c_int, ctypes.c_uint, ctypes.c_void_p]
        statx.restype = ctypes.c_int
        buffer = ctypes.create_string_buffer(256)
        if statx(-100, os.fsencode(path), 0x100, 0xfff, buffer) != 0:
            return None
        raw = buffer.raw
        mask = struct.unpack_from('=I', raw)[0]
        inode = struct.unpack_from('=Q', raw, 32)[0]
        major, minor = struct.unpack_from('=II', raw, 136)
        seconds, nanos = struct.unpack_from('=qI', raw, 80)
        if (mask & 0x900 != 0x900 or nanos >= 10**9 or
                (inode, major, minor) != (info.st_ino, os.major(info.st_dev), os.minor(info.st_dev))):
            return None
        return f'ns:{seconds * 10**9 + nanos}'
    except (AttributeError, OSError, ValueError):
        return None


def catalog_file_owner(path, info=None):
    """Bounded, versioned attribution to one canonical database inode.

    Rollback side files belong to the fixed SQLite suffixes of this path;
    they remain unsafe to settle until admitted SQLite recovery consumes them.
    A startup token for an absent database deliberately carries no identity.
    """
    path = Path(path).absolute()
    if path.resolve() != path or path.is_symlink():
        raise CatalogAdmissionError('Catalog path must not contain symlinks')
    if info is None:
        try: info = path.lstat()
        except FileNotFoundError:
            return 'catalog-unbound-v3:' + sha256(str(path).encode()).hexdigest()
    if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
        raise CatalogAdmissionError('Catalog requires an exclusively named regular file')
    birth = catalog_birth_identity(path, info)
    identity = json.dumps([str(path), info.st_dev, info.st_ino, birth], separators=(',', ':'))
    prefix = 'catalog-v3:' if birth is not None else 'catalog-unbound-v3:'
    return prefix + sha256(identity.encode()).hexdigest()


class CatalogLedgerLease:
    """One durable ledger token reused across catalog transactions.

    A transaction's envelope is main-file growth plus transient rollback
    journal/side bytes. Main growth is cumulative, so the token's remaining
    cover is its bytes minus growth since it was taken; once that cannot cover
    the next envelope a new token with slack is reserved. Per-transaction VFS
    caps are unchanged. Already-grown bytes are also observed on disk, so the
    overcount is bounded by one token. A crash leaves one WRITING token for the
    explicit recovery path, as a crash inside a transaction always did.
    """
    def __init__(self):
        self.budget=self.key=None
        self.owner=None
        self.bytes=self.base_size=0
        self.ledger_reservation_count=0

    def cover(self,budget,size,needed,*,drain,owner):
        if (self.key is not None and self.budget is budget and not getattr(budget,'_closed',False)
                and self.owner == owner
                and self.bytes-max(0,size-self.base_size)>=needed):
            return self.key
        self.release()
        amount=needed+CATALOG_LEASE_SLACK_BYTES
        arguments=dict(drain=drain,operation='catalog-transaction',owner=owner,writing=True)
        try: key=budget.reserve(amount,generation=sha256(os.urandom(16)).hexdigest(),**arguments)
        except DiskAdmissionError:
            amount=needed
            key=budget.reserve(amount,generation=sha256(os.urandom(16)).hexdigest(),**arguments)
        self.budget,self.key,self.bytes,self.base_size=budget,key,amount,size
        self.owner=owner
        self.ledger_reservation_count+=1
        register_lease(budget,self)
        return key

    def release(self):
        budget,key=self.budget,self.key
        if budget is not None: unregister_lease(budget,self)
        self.budget=self.key=None
        self.owner=None
        self.bytes=self.base_size=0
        if key is not None and not getattr(budget,'_closed',False): budget.release(key)


class CatalogBatch:
    def __init__(self):
        self.on_commit,self.on_rollback,self.after=[],[],[]
        self.broken=False
        self.absorbed=self.splits=0

    def run(self,name):
        hooks=getattr(self,name)
        setattr(self,name,[])
        for hook in (reversed(hooks) if name=='on_rollback' else hooks): hook()


class CatalogConnection:
    """A main-thread connection that reserves before BEGIN, not after SQL.

    The per-transaction growth allowance is deliberately finite. Existing
    catalog/journal bytes are observed; main growth and side growth are reserved.
    Larger operations must request an explicit envelope before starting.
    """
    def __init__(self,path,*,budget_getter,drain_getter=lambda:False,
                 growth_bytes=256*1024,require_native=False):
        if type(growth_bytes) is not int or growth_bytes<4096:
            raise ValueError('Invalid catalog growth envelope')
        self.path=Path(path).absolute()
        catalog_file_owner(self.path)  # Refuse aliases before SQLite opens anything.
        self._file_identity=None
        self._budget_getter,self._drain_getter=budget_getter,drain_getter
        self.growth_bytes=growth_bytes
        self._thread=threading.get_ident()
        self._token=self._budget=None
        self._lease=CatalogLedgerLease()
        self._batch=None
        self.batch_count=self.absorbed_commit_count=self.batch_split_count=0
        self.last_batch=None
        self._depth=0
        self._guard=None
        self._closed=False
        self._active=False
        self.reservation_count=0
        self.mutation_count=0
        self.guard_reason=None
        # Reserve startup/recovery before SQLite can open/write a hot journal.
        main,side=self._reserve()
        try:
            try: self._guard=get_sqlite_guard()
            except CatalogAdmissionError:
                # An identity failure is evidence of a mismatch, rather than
                # an unavailable optional compiler/capability. Never downgrade it.
                raise
            except (NotImplementedError,OSError,sqlite3.Error,subprocess.SubprocessError) as exc:
                self.guard_reason=str(exc)
                if require_native: raise CatalogAdmissionError(self.guard_reason) from exc
            self._conn = (self._guard.connect(self.path,main_cap_bytes=main,side_cap_bytes=side)
                          if self._guard else sqlite3.connect(self.path,timeout=30))
            info=self.path.lstat()
            self._file_identity=(info.st_dev,info.st_ino,catalog_file_owner(self.path,info))
            if self._guard:
                state=self._status()
                if state['sqlite_version']!=sqlite3.sqlite_version: raise CatalogAdmissionError('SQLite library identity mismatch')
            self._conn.execute('PRAGMA synchronous=FULL')
            self._conn.execute('PRAGMA journal_mode=DELETE').fetchone()
            self._conn.execute('PRAGMA mmap_size=0')
            # Native VFS rejects unsupported disk temp files; current small TEMP
            # tables remain cache-resident. Large temp spills fail closed.
            self._conn.setlimit(sqlite3.SQLITE_LIMIT_LENGTH,16*1024**2)
            self._conn.setlimit(sqlite3.SQLITE_LIMIT_SQL_LENGTH,1024**2)
            self._conn.setlimit(sqlite3.SQLITE_LIMIT_ATTACHED,0)
            self._finish()
        except BaseException:
            if '_conn' in self.__dict__: self._conn.close()
            self._release()
            self._lease.release()
            raise

    def _check(self):
        if self._closed: raise ValueError('Closed catalog connection')
        if threading.get_ident()!=self._thread: raise RuntimeError('Catalog mutation must use its creating main thread')

    def _reserve(self,minimum_main_bytes=0):
        owner=_ACTIVE_CATALOGS.get(str(self.path))
        if owner is not None and owner is not self:
            raise CatalogAdmissionError('Another connection owns the active catalog envelope')
        owner=catalog_file_owner(self.path)
        info=self.path.lstat() if self.path.exists() else None
        if self._file_identity is not None and (info is None or (info.st_dev,info.st_ino,owner)!=self._file_identity):
            raise CatalogAdmissionError('Catalog file identity changed while connection was open')
        size=info.st_size if info else 0
        main_cap=((max(size,minimum_main_bytes)+self.growth_bytes+4095)//4096)*4096
        side_cap=2*max(size,minimum_main_bytes,4096)+self.growth_bytes+65536
        budget=self._budget_getter()
        if budget is not None:
            device=info.st_dev if info else (self.path.parent.stat().st_dev if self.path.parent.exists() else None)
            if device is not None and hasattr(budget,'roots') and device!=budget.roots[0].stat().st_dev:
                raise CatalogAdmissionError('Catalog and budget must use the same volume')
            self._token=self._lease.cover(budget,size,main_cap-size+side_cap,
                                          drain=bool(self._drain_getter()),owner=owner)
            self._budget=budget
        self._main_cap,self._side_cap=main_cap,side_cap
        self._input_bytes=0
        _ACTIVE_CATALOGS[str(self.path)]=self
        self._active=True
        self.reservation_count+=1
        return main_cap,side_cap

    def _start(self,minimum_main_bytes=0):
        if self._active: return
        main,side=self._reserve(minimum_main_bytes)
        try:
            if self._guard:
                self._conn.execute('SELECT svsim_disk_guard_config(?,?,1)',(main,side)).fetchone()
            page=self._conn.execute('PRAGMA page_size').fetchone()[0]
            cap=main//page
            actual=self._conn.execute(f'PRAGMA max_page_count={cap}').fetchone()[0]
            if actual!=cap: raise CatalogAdmissionError('SQLite page cap could not be enforced')
        except BaseException:
            self._finish()
            raise

    def _status(self):
        if not self._guard: return {'available':False,'reason':self.guard_reason}
        return json.loads(self._conn.execute('SELECT svsim_disk_guard_status()').fetchone()[0])

    def admission_status(self):
        return {**self._status(),'scope':'managed-catalog-and-rollback-journal',
                'capability':self._guard.capability() if self._guard else None,
                'strict_capacity_guarantee':False,'reservation_count':self.reservation_count,
                'ledger_reservation_count':self._lease.ledger_reservation_count,
                'batch_count':self.batch_count,'absorbed_commit_count':self.absorbed_commit_count,
                'batch_split_count':self.batch_split_count,
                'mutation_count':self.mutation_count,'temp_file_support':False}

    def _release(self):
        # The ledger token outlives the transaction; only the envelope ends here.
        self._token=self._budget=None
        if _ACTIVE_CATALOGS.get(str(self.path)) is self: _ACTIVE_CATALOGS.pop(str(self.path),None)
        self._active=False

    def _finish(self):
        if self._active and self._guard:
            self._conn.execute('SELECT svsim_disk_guard_config(?,?,0)',(self._main_cap,self._side_cap)).fetchone()
        self._release()

    def execute(self,sql,parameters=()):
        self._check()
        if not isinstance(sql,str) or len(sql.encode())>1024**2: raise CatalogAdmissionError('SQL statement exceeds limit')
        stripped=sql.strip()
        if 'svsim_disk_guard_' in stripped.lower() or 'load_extension' in stripped.lower():
            raise CatalogAdmissionError('Catalog guard controls are internal')
        if not stripped: return AdmissionCursor(self,self._conn.execute(sql,parameters))
        keyword=stripped.split(None,1)[0].upper()
        if self._batch is not None:
            if self._batch.broken: raise CatalogAdmissionError('Catalog batch was rolled back')
            if keyword=='BEGIN': return AdmissionCursor(self,self._conn.cursor())
        if keyword in ('COMMIT','END'):
            self.commit(); return AdmissionCursor(self,self._conn.cursor())
        if keyword=='ROLLBACK':
            self.rollback(); return AdmissionCursor(self,self._conn.cursor())
        if keyword in ('ATTACH','DETACH','VACUUM','SAVEPOINT','RELEASE','WITH'):
            raise CatalogAdmissionError('Unsupported catalog statement: '+keyword)
        pragma=keyword=='PRAGMA'
        if pragma:
            name=stripped[6:].strip().split('=',1)[0].split('(',1)[0].rstrip(';').strip().lower()
            allowed={'synchronous','journal_mode','mmap_size','cache_size','temp_store',
                     'user_version','page_size','page_count','max_page_count','table_info',
                     'integrity_check','quick_check','foreign_keys'}
            if name not in allowed: raise CatalogAdmissionError('Unsupported catalog pragma: '+name)
            if '=' in stripped:
                value=stripped.split('=',1)[1].strip().rstrip(';').upper()
                fixed={'synchronous':{'FULL','2'},'mmap_size':{'0'},'journal_mode':{'DELETE'},
                       'temp_store':{'FILE','1'}}
                if name in fixed and value not in fixed[name]:
                    raise CatalogAdmissionError('Unsupported catalog pragma value: '+name)
                if name in {'page_size','max_page_count'}:
                    raise CatalogAdmissionError('Catalog page limits are managed by admission')
        if pragma and ('journal_mode' in stripped.lower()) and '=' in stripped and stripped.split('=',1)[1].strip().rstrip(';').upper()!='DELETE':
            raise CatalogAdmissionError('Only DELETE journal mode has a catalog envelope')
        mutation=keyword not in ('SELECT','EXPLAIN') and (not pragma or 'user_version' in stripped.lower() and '=' in stripped)
        if keyword=='BEGIN': mutation=True
        if mutation:
            amount=len(sql.encode())+_payload_bytes(parameters)
            if amount>self.growth_bytes//4: raise CatalogAdmissionError('SQL input exceeds predeclared transaction envelope')
            self._start()
            if self._input_bytes+amount>self.growth_bytes//4 and self._batch is not None and self._input_bytes:
                self._split_batch()
            if self._input_bytes+amount>self.growth_bytes//4:
                raise CatalogAdmissionError('Transaction input exceeds its reserved envelope')
            self._input_bytes+=amount
        try:
            cur=self._conn.execute(sql,parameters)
            if mutation:
                self.mutation_count+=1
                if self._depth==0 and not self._conn.in_transaction: self._finish()
            return AdmissionCursor(self,cur)
        except BaseException:
            if self._active: self.rollback()
            raise

    def executemany(self,sql,parameters):
        # Consume bounded caller iterators one row at a time; no giant tuple.
        last=AdmissionCursor(self,self._conn.cursor())
        for item in parameters: last=self.execute(sql,item)
        return last

    def executescript(self,script):
        # sqlite3.executescript commits behind the caller; split complete SQL
        # statements and route each through admission instead.
        if not isinstance(script,str) or len(script.encode())>1024**2: raise CatalogAdmissionError('SQL script exceeds limit')
        self.commit()
        pending=''
        last=AdmissionCursor(self,self._conn.cursor())
        for character in script:
            pending+=character
            if character==';' and sqlite3.complete_statement(pending):
                last=self.execute(pending)
                self.commit()
                pending=''
        if pending.strip(): last=self.execute(pending); self.commit()
        return last

    def cursor(self): return AdmissionCursor(self,self._conn.cursor())
    def backup_from(self,source):
        """Reserve a complete destination before SQLite's native backup writes."""
        self._check()
        if self._depth or self.in_transaction:
            raise CatalogAdmissionError('Backup requires a quiescent destination')
        pages=source.execute('PRAGMA page_count').fetchone()[0]
        page_size=source.execute('PRAGMA page_size').fetchone()[0]
        self._start(pages*page_size)
        try:
            source.backup(self._conn)
            self._conn.commit()
            self.mutation_count+=1
        except BaseException:
            self._conn.rollback()
            raise
        finally: self._finish()
    @property
    def in_transaction(self): return self._conn.in_transaction
    def commit(self):
        self._check()
        if self._batch is not None:
            # Inner commits join the enclosing gate batch; only its exit is durable.
            self.absorbed_commit_count+=1;self._batch.absorbed+=1
            return
        try: self._conn.commit()
        except BaseException:
            self._conn.rollback()
            self._finish()
            raise
        self._finish()
    def rollback(self):
        self._check()
        if self._batch is not None: self._batch.broken=True
        try: self._conn.rollback()
        finally: self._finish()

    @contextmanager
    def batch(self):
        """Group a gate stage's catalog writes into durable transactions.

        Recovery resumes only from committed snapshots, so intermediate commits
        inside a stage add fsync cost without recovery value. Statements keep
        their envelope checks; when the input envelope fills, the work so far
        commits and a new transaction continues (split). ``on_commit`` hooks run
        after each durable commit, ``on_rollback`` hooks restore process state
        when the batch is rolled back, and ``after`` hooks always run once the
        batch transaction has ended, in their own transaction.
        """
        self._check()
        if self._batch is not None:
            yield self._batch
            return
        if self._conn.in_transaction or self._depth: raise CatalogAdmissionError('Catalog batch needs a quiescent connection')
        state=CatalogBatch()
        self._start()
        try: self._conn.execute('BEGIN IMMEDIATE')
        except BaseException:
            self._finish(); raise
        self._batch=state;self._depth+=1;self.batch_count+=1
        ok=False
        try:
            yield state
            ok=not state.broken
        finally:
            self._depth-=1;self._batch=None
            try:
                if ok:
                    started=perf_counter()
                    try:
                        self.commit()
                    except BaseException:
                        state.run('on_rollback'); raise
                    self.last_batch={'commit_seconds':perf_counter()-started,'absorbed':state.absorbed,
                                     'splits':state.splits,'main_bytes':self.path.stat().st_size if self.path.exists() else 0}
                    state.run('on_commit')
                else:
                    try:
                        if self._conn.in_transaction: self._conn.rollback()
                    finally:
                        self._finish(); state.run('on_rollback')
            finally:
                if state.after:
                    with self:
                        for hook in state.after: hook()

    def _split_batch(self):
        self._conn.commit()
        self._finish()
        self._batch.run('on_commit')
        self._batch.on_rollback.clear()
        self.batch_split_count+=1;self._batch.splits+=1
        self._start()
        self._conn.execute('BEGIN IMMEDIATE')

    def on_commit(self,hook):
        """Run now outside a batch, else after the batch's durable commit."""
        if self._batch is None: hook()
        else: self._batch.on_commit.append(hook)

    def on_rollback(self,hook):
        if self._batch is not None: self._batch.on_rollback.append(hook)

    def after_batch(self,hook):
        """Run now outside a batch, else once the batch transaction has ended (commit or rollback)."""
        if self._batch is None:
            with self: hook()
        else: self._batch.after.append(hook)
    def close(self):
        if self._closed:return
        try: self.rollback()
        finally:
            self._conn.close();self._closed=True
            self._lease.release()
    def __enter__(self):
        self._check();self._start();self._depth+=1
        return self
    def __exit__(self,kind,value,tb):
        self._depth-=1
        if kind is not None: self.rollback()
        elif self._depth==0:self.commit()
    def __getattr__(self,name):
        if name.startswith('_'): raise AttributeError(name)
        if name in {'backup','deserialize','blobopen','create_function','create_aggregate','set_authorizer','set_progress_handler','load_extension','enable_load_extension'}:
            raise CatalogAdmissionError('Unsupported writable catalog escape: '+name)
        return getattr(self._conn,name)
