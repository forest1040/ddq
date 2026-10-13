"""Pre-transaction catalog reservations and optional same-SQLite VFS enforcement.

All mutation paths pass through this proxy, including cursors and scripts.
Native loading/building happens only when a writable catalog is opened.
"""
from __future__ import annotations

from hashlib import sha256
import json
import os
from pathlib import Path
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import threading

from .disk_budget import DiskAdmissionError


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
        header = next((p for p in includes if (p/'sqlite3ext.h').is_file()),None)
        if header is None: raise NotImplementedError('Installed sqlite3ext.h is required for catalog VFS')
        self._directory = tempfile.TemporaryDirectory(prefix='svsim-sqlite-vfs-')
        self.library = Path(self._directory.name)/('svsim_disk.dylib' if sys.platform=='darwin' else 'svsim_disk.so')
        command = [compiler,'-dynamiclib' if sys.platform=='darwin' else '-shared','-fPIC','-O2',
                   '-I',str(header),str(source),'-o',str(self.library)]
        result = subprocess.run(command,capture_output=True,text=True,timeout=30)
        if result.returncode: raise NotImplementedError('Catalog VFS build unavailable: '+result.stderr[-1500:])
        self.loader = sqlite3.connect(':memory:')
        self._load(self.loader)
        self.source_hash = sha256(source.read_bytes()).hexdigest()

    def _load(self,connection):
        connection.enable_load_extension(True)
        try: connection.execute('SELECT load_extension(?,?)',(str(self.library),'sqlite3_svsimdisk_init')).fetchone()
        finally: connection.enable_load_extension(False)

    def connect(self,path,*,main_cap_bytes=None,side_cap_bytes=None):
        path = Path(path).resolve()
        prepared = main_cap_bytes is not None
        if prepared:
            self.loader.execute('SELECT svsim_disk_guard_prepare(?,?,?)',
                                (str(path),main_cap_bytes,side_cap_bytes)).fetchone()
        try:
            db = sqlite3.connect(path.as_uri()+'?vfs=svsim-budget-v1',uri=True,timeout=30)
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
        self.path=Path(path).resolve()
        self._budget_getter,self._drain_getter=budget_getter,drain_getter
        self.growth_bytes=growth_bytes
        self._thread=threading.get_ident()
        self._token=self._budget=None
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
            except (NotImplementedError,OSError,sqlite3.Error,subprocess.SubprocessError) as exc:
                self.guard_reason=str(exc)
                if require_native: raise CatalogAdmissionError(self.guard_reason) from exc
            self._conn = (self._guard.connect(self.path,main_cap_bytes=main,side_cap_bytes=side)
                          if self._guard else sqlite3.connect(self.path,timeout=30))
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
            raise

    def _check(self):
        if self._closed: raise ValueError('Closed catalog connection')
        if threading.get_ident()!=self._thread: raise RuntimeError('Catalog mutation must use its creating main thread')

    def _reserve(self,minimum_main_bytes=0):
        owner=_ACTIVE_CATALOGS.get(str(self.path))
        if owner is not None and owner is not self:
            raise CatalogAdmissionError('Another connection owns the active catalog envelope')
        size=self.path.stat().st_size if self.path.exists() else 0
        main_cap=((max(size,minimum_main_bytes)+self.growth_bytes+4095)//4096)*4096
        side_cap=2*max(size,minimum_main_bytes,4096)+self.growth_bytes+65536
        budget=self._budget_getter()
        if budget is not None:
            self._token=budget.reserve(main_cap-size+side_cap,drain=bool(self._drain_getter()),
                                       operation='catalog-transaction',owner=sha256(str(self.path).encode()).hexdigest(),generation=sha256(os.urandom(16)).hexdigest(),writing=True)
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
                'strict_capacity_guarantee':False,'reservation_count':self.reservation_count,
                'mutation_count':self.mutation_count,'temp_file_support':False}

    def _release(self):
        if self._token is not None:
            self._budget.release(self._token)
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
        try: self._conn.commit()
        except BaseException:
            self._conn.rollback()
            self._finish()
            raise
        self._finish()
    def rollback(self):
        self._check()
        try: self._conn.rollback()
        finally: self._finish()
    def close(self):
        if self._closed:return
        try: self.rollback()
        finally: self._conn.close();self._closed=True
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
