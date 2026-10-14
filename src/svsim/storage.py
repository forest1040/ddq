"""Append-only state segments and disk-resident COW version catalog."""
from dataclasses import dataclass
from hashlib import sha256
import json
import os
from pathlib import Path
import shutil
import sqlite3
from time import perf_counter
from uuid import uuid4
import numpy as np
from .transport import open_storage
from .journal import canonical
from .dataset_lease import DirectoryLease


@dataclass(frozen=True)
class StorageConfig:
    directory: str
    block_bits: int
    ram_budget_bytes: int = 64*1024**2
    backend: str = 'buffered'
    alignment: int = 4096
    segment_size_bytes: int = 1 << 30
    metadata_reserve_bytes: int = 65536
    root_depth_limit: int = 16

    def __post_init__(self):
        if self.block_bits < 1: raise ValueError('Invalid block_bits')
        if self.alignment < 1 or self.alignment & (self.alignment-1): raise ValueError('Invalid alignment')
        if self.backend not in {'buffered','direct'}: raise ValueError('Invalid backend')
        if not 0 <= self.metadata_reserve_bytes < self.ram_budget_bytes: raise ValueError('Invalid metadata reserve')
        if self.segment_size_bytes < self.slot_bytes or self.root_depth_limit < 2: raise ValueError('Invalid segment/depth limit')

    @property
    def payload_bytes(self): return 16*(1 << self.block_bits)
    @property
    def slot_bytes(self): return ((self.payload_bytes+self.alignment-1)//self.alignment)*self.alignment


@dataclass(frozen=True)
class BlockRef:
    version_id: str
    segment: int | None
    offset: int | None
    payload_bytes: int
    storage_bytes: int
    checksum: str | None


@dataclass(frozen=True)
class StoredBlockView:
    version_id: str
    base_frontier: int = -1
    logical_frontier: int = -1
    frame_id: int = 0
    phase: complex = 1+0j
    binding_id: str = ''
    layout_id: int = 0
    parameter_dependencies: frozenset[str] = frozenset()
    frame_asset_id: str = ''
    phase_value: object = None
    base_log: tuple = ()
    base_coordinate_id: str = ''


class FileBlockStore:
    def __init__(self, config, num_qubits, *, trace=None, create=True, transport_factory=open_storage, disk_budget=None):
        if not 1 <= config.block_bits <= num_qubits: raise ValueError('Invalid state/block width')
        self.config, self.num_qubits, self.trace = config, num_qubits, trace
        self.num_blocks = 1 << (num_qubits-config.block_bits)
        self.payload_bytes, self.slot_bytes = config.payload_bytes, config.slot_bytes
        self.directory = Path(config.directory)
        self.files = {}
        self._last_offsets = {}
        self.guard = None
        self.transport_factory = transport_factory
        if create:
            self.directory.mkdir(parents=True,exist_ok=True)
            if (self.directory/'metadata/catalog.sqlite3').exists(): raise FileExistsError('Storage directory already contains a state')
        elif not (self.directory/'metadata/catalog.sqlite3').is_file(): raise FileNotFoundError('Missing state catalog')
        self._directory_lease = DirectoryLease(self.directory)
        try:
            self._open_store(config, num_qubits, create, disk_budget)
        except BaseException:
            self._cleanup_failed_open()
            raise

    def _open_store(self, config, num_qubits, create, disk_budget):
        for sub in ('state','metadata','assets','journal','traces','manifests'): (self.directory/sub).mkdir(exist_ok=True)
        from .disk_budget import DiskBudget
        from .catalog_admission import CatalogConnection
        self._owned_disk_budget = None
        if disk_budget is None:
            self._owned_disk_budget=DiskBudget(self.directory/'metadata/disk-ledger.sqlite3',[self.directory])
            disk_budget=self._owned_disk_budget
        self.disk_budget=disk_budget
        self.db=CatalogConnection(self.directory/'metadata/catalog.sqlite3',
            budget_getter=lambda:self.disk_budget,drain_getter=lambda:getattr(self,'_disk_drain',False),
            require_native=self.disk_budget.quota_bytes is not None)
        self._initialize_catalog(config,num_qubits,create)

    def _cleanup_failed_open(self):
        # Construction has not exposed the store. Keep the inode lease if any
        # resource refuses to close, so recovery remains blocked until exit.
        cleanup_ok = True
        for handle in getattr(self, 'files', {}).values():
            try: handle.close()
            except BaseException: cleanup_ok = False
        db = getattr(self, 'db', None)
        if db is not None:
            try: db.close()
            except BaseException: cleanup_ok = False
        budget = getattr(self, '_owned_disk_budget', None)
        if budget is not None:
            try:
                budget.close()
                self._owned_disk_budget = None
            except BaseException:
                cleanup_ok = False
        lease = getattr(self, '_directory_lease', None)
        if cleanup_ok and lease is not None:
            lease.close()
            self._directory_lease = None

    def _initialize_catalog(self,config,num_qubits,create):
        self.db.execute('PRAGMA synchronous=FULL')
        self.db.execute(f'PRAGMA cache_size=-{max(1,min(16,config.metadata_reserve_bytes//4096))}')
        self.db.execute('PRAGMA temp_store=FILE')
        version = self.db.execute('PRAGMA user_version').fetchone()[0]
        if version not in {0,1,2}:
            self.db.close()
            raise OSError('Unsupported catalog schema')
        self.db.executescript('''
          CREATE TABLE IF NOT EXISTS roots(id TEXT PRIMARY KEY,parent TEXT,sealed INTEGER DEFAULT 0);
          CREATE TABLE IF NOT EXISTS versions(id TEXT PRIMARY KEY,segment INTEGER,offset INTEGER,checksum TEXT);
          CREATE TABLE IF NOT EXISTS views(root TEXT,h INTEGER,version TEXT,base INTEGER,frontier INTEGER,frame INTEGER,re REAL,im REAL,PRIMARY KEY(root,h));
          CREATE TABLE IF NOT EXISTS frames(id INTEGER PRIMARY KEY,data TEXT UNIQUE);
          CREATE TABLE IF NOT EXISTS snapshots(id TEXT PRIMARY KEY,data TEXT NOT NULL);
          CREATE TABLE IF NOT EXISTS settings(key TEXT PRIMARY KEY,data TEXT);
          CREATE TABLE IF NOT EXISTS active_runs(id TEXT PRIMARY KEY,root TEXT);
          CREATE TABLE IF NOT EXISTS root_edges(root TEXT PRIMARY KEY,data TEXT NOT NULL);
          CREATE TABLE IF NOT EXISTS frames_v2(id TEXT PRIMARY KEY,data TEXT NOT NULL);
          CREATE TABLE IF NOT EXISTS views_v2(root TEXT,h INTEGER,data TEXT NOT NULL,PRIMARY KEY(root,h));
          CREATE TABLE IF NOT EXISTS root_log_bounds(root TEXT PRIMARY KEY,rotations INTEGER NOT NULL);
          CREATE TABLE IF NOT EXISTS catalog_checksums(key TEXT PRIMARY KEY,checksum TEXT NOT NULL);
        ''')
        self.db.execute('PRAGMA user_version=2')
        self.db.execute("INSERT OR IGNORE INTO frames VALUES(0,'[]')")
        if create:
            self.db.execute('INSERT INTO settings VALUES(?,?)',('config',canonical({'num_qubits':num_qubits,'block_bits':config.block_bits,'alignment':config.alignment})))
            self.db.execute('INSERT INTO settings VALUES(?,?)',('allocator',canonical([0,0])))
            self.db.commit()
        else:
            saved = json.loads(self.db.execute("SELECT data FROM settings WHERE key='config'").fetchone()[0])
            if saved != {'num_qubits':num_qubits,'block_bits':config.block_bits,'alignment':config.alignment}:
                self.db.close()
                raise ValueError('Storage dimensions/alignment mismatch')
        # Record creation and current-open capability separately. A compatible
        # SQLite upgrade may reopen a dataset; it does not rewrite creation evidence.
        capability=self.db.admission_status()['capability']
        with self.db:
            self.db.execute('INSERT OR IGNORE INTO settings VALUES(?,?)',
                            ('catalog_vfs_creation_capability',canonical(capability)))
            self.db.execute('INSERT OR REPLACE INTO settings VALUES(?,?)',
                            ('catalog_vfs_open_capability',canonical(capability)))
        # Slots orphaned by failed writes are never reused.
        paths = sorted((self.directory/'state').glob('segment-*.bin'))
        self.segment = int(paths[-1].stem.split('-')[1]) if paths else 0
        self.offset = ((paths[-1].stat().st_size+self.slot_bytes-1)//self.slot_bytes)*self.slot_bytes if paths else 0

    def _file(self, segment):
        if segment not in self.files:
            # Descriptor count is bounded even for very large states.
            if len(self.files) >= 4:
                _, handle = self.files.popitem()
                handle.flush(); handle.close()
            path = self.directory/'state'/f'segment-{segment:06d}.bin'
            self.files[segment] = self.transport_factory(path,'r+b' if path.exists() else 'w+b',backend=self.config.backend,alignment=self.config.alignment)
        return self.files[segment]

    def new_root(self, parent=None):
        root = uuid4().hex
        self.db.execute('INSERT INTO roots(id,parent) VALUES(?,?)',(root,parent)); self.db.commit()
        self.db.execute('INSERT INTO root_log_bounds VALUES(?,?)',(root,self.log_bound(parent)))
        self.db.commit()
        return root

    def log_bound(self,root):
        if not root: return 0
        for r in self.root_chain(root):
            row = self.db.execute('SELECT rotations FROM root_log_bounds WHERE root=?',(r,)).fetchone()
            if row: return row[0]
        return 0

    def seal(self, root):
        self.db.execute('UPDATE roots SET sealed=1 WHERE id=?',(root,)); self.db.commit()

    def root_chain(self, root):
        seen = set()
        while root:
            if root in seen: raise OSError('Cycle in root catalog')
            seen.add(root)
            row = self.db.execute('SELECT parent FROM roots WHERE id=?',(root,)).fetchone()
            if row is None: raise OSError('Unknown catalog root')
            yield root
            root = row[0]

    def view(self, root, h):
        if not 0 <= h < self.num_blocks: raise ValueError('Block ID out of range')
        if getattr(self,'phase2',None): return self.phase2.view(root,h)
        for r in self.root_chain(root):
            if self.db.execute('SELECT 1 FROM root_edges WHERE root=?',(r,)).fetchone():
                raise ValueError('Phase-aware roots require StoredSimulator view resolution')
            row = self.db.execute('SELECT version,base,frontier,frame,re,im FROM views WHERE root=? AND h=?',(r,h)).fetchone()
            if row is not None: return StoredBlockView(*row[:4],complex(*row[4:]))
        return StoredBlockView('initial-one' if h == 0 else 'zero')

    def publish(self, root, views):
        row = self.db.execute('SELECT sealed FROM roots WHERE id=?',(root,)).fetchone()
        if row is None or row[0]: raise ValueError('Cannot mutate immutable/unknown root')
        with self.db:
            for h, view in views:
                self.db.execute('INSERT OR REPLACE INTO views VALUES(?,?,?,?,?,?,?,?)',
                                (root,h,view.version_id,view.base_frontier,view.logical_frontier,view.frame_id,view.phase.real,view.phase.imag))
                if getattr(self,'phase2',None): self.phase2.publish_extra(root,h,view)

    def flatten(self, root):
        if getattr(self,'phase2',None): return root # Explicit compaction avoids hidden full-label scans.
        if sum(1 for _ in self.root_chain(root)) < self.config.root_depth_limit: return root
        flattened = self.new_root()
        for h in range(self.num_blocks): self.publish(flattened,((h,self.view(root,h)),))
        return flattened

    def frame(self, frame_id):
        from .journal import decode_gate
        row = self.db.execute('SELECT data FROM frames WHERE id=?',(frame_id,)).fetchone()
        if row is None: raise OSError('Missing Clifford frame')
        return tuple(decode_gate(g) for g in json.loads(row[0]))

    def intern_frame(self, gates):
        from .journal import gate_data
        if any(not g.clifford for g in gates): raise ValueError('Non-Clifford in local Frame')
        data = canonical([gate_data(g) for g in gates])
        inserted = self.db.execute('INSERT OR IGNORE INTO frames(data) VALUES(?)',(data,)).rowcount
        frame_id = self.db.execute('SELECT id FROM frames WHERE data=?',(data,)).fetchone()[0]
        self.db.commit()
        if inserted and getattr(self,'journal',None):
            self.journal.append('frame',{'frame_id':frame_id,'gates':json.loads(data)})
        return frame_id

    def new_version(self):
        version = uuid4().hex
        self.db.execute('INSERT INTO versions(id) VALUES(?)',(version,)); self.db.commit()
        return version

    def reserve_extent(self, version, reservation):
        """Claim a private immutable slot after the caller reserved its full cost."""
        token = self.disk_budget.reservation(reservation)
        if token is None or token['bytes'] < self.slot_bytes or token['state'] not in {'RESERVED','WRITING'}:
            raise ValueError('A live full-slot disk reservation is required before allocator SQL')
        ref = self.ref(version)
        if ref.segment is not None: raise ValueError('Cannot overwrite immutable Block extent')
        if shutil.disk_usage(self.directory).free < self.slot_bytes: raise OSError('Insufficient disk capacity')
        with self.db:
            self.db.execute('BEGIN IMMEDIATE')
            segment, offset = json.loads(self.db.execute("SELECT data FROM settings WHERE key='allocator'").fetchone()[0])
            if offset+self.slot_bytes > self.config.segment_size_bytes:
                segment += 1; offset = 0
            self.db.execute("UPDATE settings SET data=? WHERE key='allocator'",(canonical([segment,offset+self.slot_bytes]),))
        self.segment,self.offset = segment,offset+self.slot_bytes
        return segment, offset

    def seal_extent(self, version, segment, offset, checksum):
        """Catalog a completed private slot only after all bytes and checksum exist."""
        ref = self.ref(version)
        if ref.segment is not None: raise ValueError('Cannot overwrite immutable Block extent')
        self.db.execute('UPDATE versions SET segment=?,offset=?,checksum=? WHERE id=?',
                        (segment,offset,checksum,version)); self.db.commit()

    def delete_unwritten(self, version):
        self.db.execute('DELETE FROM versions WHERE id=? AND segment IS NULL',(version,)); self.db.commit()

    def ref(self, version):
        row = self.db.execute('SELECT segment,offset,checksum FROM versions WHERE id=?',(version,)).fetchone()
        if row is None: raise OSError('Missing Block version')
        return BlockRef(version,row[0],row[1],self.payload_bytes,self.slot_bytes,row[2])

    def _event(self, operation, version, elapsed, *, handle, segment, offset, **extra):
        from .telemetry import storage_condition
        from .lifecycle import BenchmarkSample
        from dataclasses import asdict
        key = operation,segment
        previous = self._last_offsets.get(key)
        pattern = None if previous is None else 'sequential' if offset == previous+self.slot_bytes else 'random'
        self._last_offsets[key] = offset
        # Retain a bounded access history; descriptors themselves are bounded.
        if len(self._last_offsets) > 8:
            self._last_offsets.pop(next(iter(self._last_offsets)))
        backend = handle.backend_identity
        device_id = f'device-{os.stat(self.directory).st_dev}'
        condition = storage_condition(operation,self.slot_bytes,backend=backend,device_id=device_id,
                                      pattern=pattern,alignment=self.config.alignment,flush='deferred')
        if self.trace:
            self.trace.record(version,performance_class=f'storage-{operation}',bytes=self.slot_bytes,
                              transfer_ms=elapsed*1000, tier='disk', device_id=device_id,
                              observed_bandwidth=self.slot_bytes/elapsed if elapsed > 0 else None,
                              metadata={'operation':operation,'version_id':version,'payload_bytes':self.payload_bytes,
                                        'block_bytes':self.slot_bytes,'concurrency':1,'backend':backend,
                                        'condition':asdict(condition),**extra})
            if self.guard and elapsed > 0:
                report = self.guard.observe(BenchmarkSample(condition,bandwidth_bytes_per_second=self.slot_bytes/elapsed,
                                                             latency_ms=elapsed*1000))
                self.trace.record(version+'-health',performance_class='health',health_state=report.health_state,
                                  expected_bandwidth=report.expected_bandwidth,observed_bandwidth=report.observed_bandwidth,
                                  decision_reason=report.reason,metadata={'operation':'observe','mode':'observe',
                                                                        'degradation_ratio':report.degradation_ratio})

    def read_version(self, version, buffer):
        if version in {'zero','initial-one'}:
            array = np.frombuffer(buffer,dtype='<c16')
            array.fill(0)
            if version == 'initial-one': array[0] = 1
            return
        ref = self.ref(version)
        if ref.segment is None: raise OSError('Unwritten Block version lost from cache')
        handle = self._file(ref.segment)
        start = perf_counter()
        count = handle.read_into(ref.offset,buffer)
        elapsed = getattr(handle,'last_duration_seconds',perf_counter()-start)
        if count != self.slot_bytes: raise OSError('Truncated Block slot')
        if sha256(buffer[:self.payload_bytes]).hexdigest() != ref.checksum: raise OSError('Block checksum mismatch')
        self._event('read',version,elapsed,handle=handle,segment=ref.segment,offset=ref.offset)

    def write_version(self, version, buffer):
        budget=getattr(self,'disk_budget',None)
        if budget is not None:
            token=budget.reserve(self.slot_bytes,drain=getattr(self,'_disk_drain',False),operation='amplitude-extent',writing=True)
            try:
                return self._write_version(version,buffer,reservation=token)
            finally: budget.release(token)
        return self._write_version(version,buffer)

    def _write_version(self, version, buffer, *, reservation=None):
        ref = self.ref(version)
        if ref.segment is not None: raise ValueError('Cannot overwrite immutable Block extent')
        if shutil.disk_usage(self.directory).free < self.slot_bytes: raise OSError('Insufficient disk capacity')
        # Claim an extent under the shared catalog lock. Active suffix forks
        # have separate caches/connections and must never reuse each other's slots.
        with self.db:
            self.db.execute('BEGIN IMMEDIATE')
            segment, offset = json.loads(self.db.execute("SELECT data FROM settings WHERE key='allocator'").fetchone()[0])
            if offset+self.slot_bytes > self.config.segment_size_bytes:
                segment += 1; offset = 0
            self.db.execute("UPDATE settings SET data=? WHERE key='allocator'",(canonical([segment,offset+self.slot_bytes]),))
        self.segment,self.offset = segment,offset+self.slot_bytes
        checksum = sha256(buffer[:self.payload_bytes]).hexdigest()
        handle = self._file(segment)
        start = perf_counter()
        count = handle.write_from(offset,buffer)
        elapsed = getattr(handle,'last_duration_seconds',perf_counter()-start)
        if count != self.slot_bytes: raise OSError('Incomplete Block write')
        if reservation is not None:self.disk_budget.materialize(reservation,self.slot_bytes)
        self.db.execute('UPDATE versions SET segment=?,offset=?,checksum=? WHERE id=?',(segment,offset,checksum,version)); self.db.commit()
        self._event('write',version,elapsed,handle=handle,segment=segment,offset=offset)

    def flush(self):
        for handle in self.files.values():
            start = perf_counter(); handle.flush()
            if self.trace: self.trace.record('flush',performance_class='storage-flush',transfer_ms=(perf_counter()-start)*1000,metadata={'operation':'flush'})
        # Closed segments were fsynced before eviction from the descriptor cache.
        for sub in ('state','assets','journal','metadata'):
            fd = os.open(self.directory/sub,os.O_RDONLY)
            try: os.fsync(fd)
            finally: os.close(fd)

    def close(self):
        for handle in self.files.values(): handle.close()
        self.db.close()
        if getattr(self,'_owned_disk_budget',None):
            self._owned_disk_budget.close();self._owned_disk_budget=None
        self.files.clear()
        lease = getattr(self, '_directory_lease', None)
        if lease is not None:
            lease.close()
            self._directory_lease = None
