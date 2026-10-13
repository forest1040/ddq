"""Private aligned extent writer with explicit coverage and logical checksum."""
from hashlib import sha256


class TileExtentWriter:
    """Write one immutable slot incrementally; seal only after exact coverage."""
    def __init__(self, store, version, *, tile_bytes):
        self.store, self.version = store, version
        self.tile_bytes = tile_bytes
        self.payload_bytes, self.slot_bytes = store.payload_bytes, store.slot_bytes
        if tile_bytes <= 0 or tile_bytes % store.config.alignment:
            raise ValueError('Tile size must be storage-aligned')
        # Reserve the complete physical slot before the allocator/catalog SQL.
        self.reservation = store.disk_budget.reserve(self.slot_bytes,
            drain=getattr(store, '_disk_drain', False), operation='tile-amplitude-extent', writing=True)
        try:
            self.segment, self.offset = store.reserve_extent(version, self.reservation)
        except BaseException:
            store.disk_budget.release(self.reservation)
            raise
        self.covered = 0
        self.closed = False

    def write(self, offset, data):
        if self.closed: raise ValueError('Tile writer is closed')
        view = memoryview(data).cast('B')
        if offset < 0 or len(view) == 0 or offset % self.store.config.alignment or len(view) % self.store.config.alignment:
            raise ValueError('Tile writes must be nonempty and aligned')
        end = offset + len(view)
        if end > self.slot_bytes: raise ValueError('Tile exceeds output slot')
        if offset != self.covered:
            if offset < self.covered: raise ValueError('Overlapping or out-of-order tile write')
            raise ValueError('Tile output coverage has a hole')
        handle = self.store._file(self.segment)
        count = handle.write_from(self.offset + offset, view)
        if count != len(view): raise OSError('Incomplete tile write')
        self.covered = end

    def seal(self, scratch=None):
        if self.closed: raise ValueError('Tile writer is closed')
        if self.covered != self.slot_bytes: raise ValueError('Tile output coverage has a hole')
        if scratch is None: raise ValueError('Checksum scratch buffer is required')
        scratch_view = memoryview(scratch).cast('B')
        try:
            if len(scratch_view) < self.tile_bytes: raise ValueError('Checksum scratch is smaller than tile size')
            # Payload checksum is in logical byte order; slot padding is excluded.
            handle = self.store._file(self.segment)
            digest = sha256()
            offset = 0
            alignment = self.store.config.alignment
            while offset < self.payload_bytes:
                valid = min(self.tile_bytes, self.payload_bytes-offset)
                size = ((valid+alignment-1)//alignment)*alignment
                if size > len(scratch_view): raise ValueError('Checksum scratch is too small for aligned read')
                count = handle.read_into(self.offset+offset, scratch_view[:size])
                if count != size: raise OSError('Short checksum read')
                digest.update(scratch_view[:valid])
                offset += valid
            self.store.disk_budget.materialize(self.reservation, self.slot_bytes)
            self.store.seal_extent(self.version, self.segment, self.offset, digest.hexdigest())
            self.store.disk_budget.release(self.reservation)
            self.closed = True
            return self.version
        finally:
            scratch_view.release()

    def abort(self):
        if not self.closed:
            self.store.disk_budget.release(self.reservation)
            self.closed = True

    def __enter__(self): return self
    def __exit__(self, typ, value, tb):
        if not self.closed: self.abort()
