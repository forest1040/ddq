"""Main-thread extent allocation/publication around bounded asynchronous I/O."""
from hashlib import sha256
import json
from .journal import canonical
from .async_io import BackpressureError


class AsyncStoreIO:
    def __init__(self, store, engine):
        self.store, self.engine = store, engine
        self.pending = {}

    def _handle(self, segment, writing=False):
        path = self.store.directory/'state'/f'segment-{segment:06d}.bin'
        return self.store.transport_factory(path, 'w+b' if writing and not path.exists() else 'r+b',
                    backend=self.store.config.backend, alignment=self.store.config.alignment)

    def _credit(self):
        self.engine._check()
        if len(self.engine.pending) >= self.engine.max_requests: raise BackpressureError('I/O queue is full')

    def submit_read(self, version, lease):
        self._credit()
        if version in {'zero', 'initial-one'}:
            # Virtual initial blocks have no physical extent.
            self.store.read_version(version, lease.view)
            return None
        ref = self.store.ref(version)
        if ref.segment is None: raise OSError('Unwritten Block version lost from cache')
        handle = self._handle(ref.segment)
        try: request = self.engine.submit_read(handle, ref.offset, lease)
        except BaseException: handle.close(); raise
        def complete(req):
            try:
                if req.error is None:
                    if sha256(lease.view[:self.store.payload_bytes]).hexdigest() != ref.checksum:
                        raise OSError('Block checksum mismatch')
                    self.store._event('read', version, req.elapsed_seconds, handle=handle,
                                      segment=ref.segment, offset=ref.offset)
            finally:
                handle.close()
                self.pending.pop(req.request_id, None)
        request.add_done_callback(complete)
        self.pending[request.request_id] = request
        return request

    def submit_write(self, version, lease):
        self._credit()
        store = self.store
        ref = store.ref(version)
        if ref.segment is not None: raise ValueError('Cannot overwrite immutable Block extent')
        if any(v == version for _, v in getattr(self, '_writing', {}).items()):
            raise ValueError('Block write is already pending')
        import shutil
        if shutil.disk_usage(store.directory).free < store.slot_bytes: raise OSError('Insufficient disk capacity')
        checksum = sha256(lease.view[:store.payload_bytes]).hexdigest()
        with store.db:
            store.db.execute('BEGIN IMMEDIATE')
            segment, offset = json.loads(store.db.execute("SELECT data FROM settings WHERE key='allocator'").fetchone()[0])
            if offset + store.slot_bytes > store.config.segment_size_bytes: segment += 1; offset = 0
            store.db.execute("UPDATE settings SET data=? WHERE key='allocator'", (canonical([segment, offset+store.slot_bytes]),))
        store.segment, store.offset = segment, offset + store.slot_bytes
        disk_budget = getattr(store, 'disk_budget', None)
        reservation = disk_budget.reserve(store.slot_bytes) if disk_budget is not None else None
        handle = None
        try:
            handle = self._handle(segment, writing=True)
            request = self.engine.submit_write(handle, offset, lease)
        except BaseException:
            if handle is not None: handle.close()
            if disk_budget is not None: disk_budget.release(reservation)
            raise
        if not hasattr(self, '_writing'): self._writing = {}
        self._writing[request.request_id] = version
        def complete(req):
            try:
                if req.error is None:
                    with store.db:
                        store.db.execute('UPDATE versions SET segment=?,offset=?,checksum=? WHERE id=?',
                                         (segment, offset, checksum, version))
                    store._event('write', version, req.elapsed_seconds, handle=handle, segment=segment, offset=offset)
            finally:
                handle.close()
                self.pending.pop(req.request_id, None)
                self._writing.pop(req.request_id, None)
                if disk_budget is not None: disk_budget.release(reservation)
        request.add_done_callback(complete)
        self.pending[request.request_id] = request
        return request

    def flush(self):
        self.engine.drain()
        # Engine-owned descriptors were closed after completion. fsync every
        # written segment explicitly; store.files may not contain these files.
        for path in sorted((self.store.directory/'state').glob('segment-*.bin')):
            handle = self._handle(int(path.stem.split('-')[1]))
            try: self.engine.wait(self.engine.submit_flush(handle))
            finally: handle.close()
        self.store.flush()
