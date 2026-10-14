"""Bounded RAM payloads and read-only access to immutable file stores."""
from hashlib import sha256
import json
from pathlib import Path
import sqlite3
from time import perf_counter

from .cache import BufferLease
from .dataset_lease import DirectoryLease
from .storage import FileBlockStore
from .transport import open_storage


class MemoryBlockStore(FileBlockStore):
    """Disk catalog with explicitly charged, volatile RAM payload extents.

    Ordinary flush cannot make resident amplitudes durable. Call
    ``flush_to_file`` before publishing a restartable checkpoint.
    """
    def __init__(self, config, num_qubits, budget, *, create=True, trace=None, disk_budget=None):
        super().__init__(config, num_qubits, create=create, trace=trace,disk_budget=disk_budget)
        self.budget = budget
        self.resident = {}
        self.db.execute('CREATE TABLE IF NOT EXISTS resident_versions(id TEXT PRIMARY KEY)')
        self.db.commit()
        if not create and self.db.execute('SELECT 1 FROM resident_versions LIMIT 1').fetchone():
            super().close()
            raise OSError('Volatile RAM amplitudes cannot be resumed')

    @property
    def durable(self): return not self.resident

    @property
    def resident_bytes(self): return sum(v.cost for v in self.resident.values())

    def _memory_event(self, operation, version, elapsed):
        if self.trace:
            self.trace.record(version, performance_class='memory-'+operation,
                              bytes=self.slot_bytes, transfer_ms=elapsed*1000,
                              tier='ram', metadata={'operation':operation, 'version_id':version})

    def read_version(self, version, buffer):
        lease = self.resident.get(version)
        if lease is None:
            return super().read_version(version, buffer)
        if len(buffer) != self.slot_bytes: raise ValueError('Invalid Block slot size')
        start = perf_counter()
        buffer[:] = lease.view
        if sha256(buffer[:self.payload_bytes]).hexdigest() != self.ref(version).checksum:
            raise OSError('Block checksum mismatch')
        self._memory_event('read', version, perf_counter()-start)

    def write_version(self, version, buffer):
        ref = self.ref(version)
        if version in self.resident or ref.segment is not None:
            raise ValueError('Cannot overwrite immutable Block extent')
        if len(buffer) != self.slot_bytes: raise ValueError('Invalid Block slot size')
        lease = BufferLease(self.budget, self.slot_bytes, self.config.alignment)
        try:
            start = perf_counter()
            lease.view[:] = buffer
            checksum = sha256(lease.view[:self.payload_bytes]).hexdigest()
            with self.db:
                self.db.execute('UPDATE versions SET checksum=? WHERE id=?',(checksum,version))
                self.db.execute('INSERT INTO resident_versions VALUES(?)',(version,))
            self.resident[version] = lease
            self.db.on_rollback(lambda: self._drop_resident(version))
        except BaseException:
            lease.close()
            raise
        self._memory_event('write',version,perf_counter()-start)

    def _drop_resident(self, version):
        lease = self.resident.pop(version, None)
        if lease is not None: lease.close()

    def delete_unwritten(self, version):
        if version in self.resident: return
        super().delete_unwritten(version)

    def flush(self):
        if self.resident:
            raise OSError('RAM amplitudes are volatile; flush_to_file is required for durability')
        super().flush()

    def flush_to_file(self):
        # Keep RAM copies until every payload and directory is durable. A
        # failed flush is retryable without allocating another state vector.
        for version, lease in self.resident.items():
            if self.ref(version).segment is None:
                super().write_version(version, lease.view)
        super().flush()
        with self.db:
            self.db.execute('DELETE FROM resident_versions')
        for lease in self.resident.values(): lease.close()
        self.resident.clear()

    spill = flush_to_file

    def reclaim_versions(self, live_versions):
        """Release resident versions outside a caller-provided reference closure.

        The caller must mark current, rollback, checkpoint and remote roots,
        plus cache entries. This method deliberately does not infer liveness
        from catalog ancestors: inherited view rows can be superseded, while
        immutable prefix roots may still be used by another runtime.
        Returns the number of managed RAM bytes released. Durable file extents
        are never reclaimed here.
        """
        live = frozenset(live_versions)
        doomed = [version for version in self.resident if version not in live]
        released = 0
        for version in doomed:
            lease = self.resident[version]
            released += lease.cost
            # The store never exports resident arrays, so closing this owned
            # lease does not invalidate any caller-owned read buffer. Inside a
            # gate batch the release waits for the durable commit, so a
            # rollback still finds the catalogued payload.
            self.db.on_commit(lambda version=version: self._drop_resident(version))
            with self.db:
                self.db.execute('DELETE FROM resident_versions WHERE id=?',(version,))
                # A spill may already have written an extent before fsync
                # failed. Such durable-address versions remain catalogued.
                self.db.execute('DELETE FROM versions WHERE id=? AND segment IS NULL',(version,))
        return released

    def close(self):
        for lease in self.resident.values(): lease.close()
        self.resident.clear()
        super().close()


class ReadOnlyBlockStore(FileBlockStore):
    """Open an existing prefix catalog without DDL or writable descriptors."""
    def __init__(self, config, num_qubits, *, trace=None):
        if not 1 <= config.block_bits <= num_qubits: raise ValueError('Invalid state/block width')
        self.config, self.num_qubits, self.trace = config, num_qubits, trace
        self.num_blocks = 1 << (num_qubits-config.block_bits)
        self.payload_bytes, self.slot_bytes = config.payload_bytes, config.slot_bytes
        self.directory = Path(config.directory)
        self.files, self._last_offsets = {}, {}
        self.guard = None
        self.transport_factory = open_storage
        self._directory_lease = DirectoryLease(self.directory)
        catalog = self.directory/'metadata/catalog.sqlite3'
        try:
            self.db = sqlite3.connect(catalog.resolve().as_uri()+'?mode=ro', uri=True)
            self.db.execute('PRAGMA query_only=ON')
            version = self.db.execute('PRAGMA user_version').fetchone()[0]
            if version not in {1,2}: raise OSError('Unsupported catalog schema')
            saved = json.loads(self.db.execute("SELECT data FROM settings WHERE key='config'").fetchone()[0])
            if saved != {'num_qubits':num_qubits,'block_bits':config.block_bits,'alignment':config.alignment}:
                raise ValueError('Storage dimensions/alignment mismatch')
        except BaseException:
            cleanup_ok = True
            if hasattr(self, 'db'):
                try: self.db.close()
                except BaseException: cleanup_ok = False
            if cleanup_ok:
                self._directory_lease.close()
                self._directory_lease = None
            raise

    def _file(self, segment):
        if segment not in self.files:
            if len(self.files) >= 4:
                _, handle = self.files.popitem()
                handle.close()
            self.files[segment] = open_storage(self.directory/'state'/f'segment-{segment:06d}.bin',
                                              'rb',backend=self.config.backend,alignment=self.config.alignment)
        return self.files[segment]

    def _deny(self, *args, **kwargs): raise PermissionError('Read-only Block store')

    new_root = seal = publish = flatten = intern_frame = new_version = _deny
    delete_unwritten = write_version = _deny

    def flush(self): pass
