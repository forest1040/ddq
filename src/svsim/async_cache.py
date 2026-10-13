"""Version-keyed raw prefetch and bounded background immutable writeback."""
from contextlib import contextmanager
from time import sleep

from .async_io import BackpressureError
from .async_store_io import AsyncStoreIO
from .cache import BlockCache, BufferLease, CacheEntry


class AsyncBlockCache(BlockCache):
    def __init__(self, store, budget, engine, *, prefetch_blocks=2):
        super().__init__(store, budget)
        self.engine = engine
        self.io = AsyncStoreIO(store, engine)
        self.prefetch_blocks = prefetch_blocks
        self.reads, self.writes = {}, {}
        self.errors = []
        self.prefetch_hits = self.prefetch_submitted = 0
        self.future_use = {}

    def set_future_versions(self, versions):
        """Bounded raw-version next-use hints from the current task window."""
        self.future_use.clear()
        for ordinal, version in enumerate(versions):
            if ordinal >= 64:
                break
            self.future_use.setdefault(version, ordinal)

    def poll(self):
        completed = self.engine.poll()
        if self.errors:
            raise self.errors[0]
        if not completed and self.engine.pending:
            sleep(0.0001)
        return completed

    def _track(self, version, request, mapping, *, write=False):
        if request is None:
            return
        mapping[version] = request
        entry = self.entries[version]
        entry.pins += 1
        def complete(req):
            entry.pins -= 1
            mapping.pop(version, None)
            if req.error is not None:
                self.errors.append(req.error)
            elif write:
                entry.dirty = False
        request.add_done_callback(complete)

    def prefetch(self, version):
        """Fetch immutable raw bytes; Frame/log resolution stays on the caller."""
        self.poll()
        if version in self.entries:
            return False
        if len(self.reads) >= self.prefetch_blocks or not self.prefetch_blocks:
            return False
        if len(self.engine.pending) >= self.engine.max_requests:
            return False
        # Speculation never evicts a foreground working set to make room.
        if self.budget.used + self.buffer_cost > self.budget.limit:
            return False
        lease = BufferLease(self.budget, self.store.slot_bytes, self.store.config.alignment)
        self.entries[version] = CacheEntry(lease)
        try:
            request = self.io.submit_read(version, lease)
            self._track(version, request, self.reads)
        except BaseException:
            del self.entries[version]
            lease.close()
            raise
        self.prefetch_submitted += 1
        return True

    def writeback(self, version):
        entry = self.entries[version]
        if not entry.dirty or version in self.writes:
            return False
        if version in self.reads:
            return False
        request = self.io.submit_write(version, entry.buffer)
        self._track(version, request, self.writes, write=True)
        return True

    def ensure(self, additional):
        if additional > self.budget.limit - self.store.config.metadata_reserve_bytes:
            raise MemoryError('Minimum task exceeds managed RAM budget')
        self.poll()
        while self.budget.used + additional > self.budget.limit:
            eligible = [k for k, e in self.entries.items() if e.pins == 0 and e.buffer.pins == 0]
            if self.future_use and eligible:
                # Farther next use wins; ties prefer clean extents and LRU.
                candidate = max(eligible, key=lambda k: (self.future_use.get(k, 65),
                                                         not self.entries[k].dirty))
            else:
                candidate = next(iter(eligible), None)
            if candidate is None:
                if not self.engine.pending:
                    raise MemoryError('Insufficient unpinned RAM for task')
                self.poll()
                continue
            entry = self.entries[candidate]
            if entry.dirty:
                try:
                    self.writeback(candidate)
                except BackpressureError:
                    self.poll()
                continue
            self._evict(candidate)

    def _evict(self, key):
        entry = self.entries[key]
        if entry.pins or entry.buffer.pins:
            raise RuntimeError('Cannot evict pending I/O or foreground Block')
        if entry.dirty:
            while True:
                try:
                    self.writeback(key)
                    break
                except BackpressureError:
                    self.poll()
            request = self.writes.get(key)
            if request is not None:
                self.engine.wait(request)
            self.poll()
        entry.buffer.close()
        del self.entries[key]
        self.evictions += 1

    @contextmanager
    def pin(self, version):
        self.poll()
        if version not in self.entries:
            self.misses += 1
            self.ensure(self.buffer_cost)
            lease = BufferLease(self.budget, self.store.slot_bytes, self.store.config.alignment)
            self.entries[version] = CacheEntry(lease)
            try:
                while True:
                    try:
                        request = self.io.submit_read(version, lease)
                        break
                    except BackpressureError:
                        self.poll()
                self._track(version, request, self.reads)
            except BaseException:
                del self.entries[version]
                lease.close()
                raise
        else:
            self.hits += 1
            if version in self.reads:
                self.prefetch_hits += 1
        request = self.reads.get(version)
        if request is not None:
            self.engine.wait(request)
        self.poll()
        entry = self.entries[version]
        self.entries.move_to_end(version)
        entry.pins += 1
        try:
            yield entry.buffer
        finally:
            entry.pins -= 1

    def flush(self):
        for version, entry in list(self.entries.items()):
            if entry.dirty:
                while True:
                    try:
                        self.writeback(version)
                        break
                    except BackpressureError:
                        self.poll()
        self.io.flush()
        self.poll()

    def discard_unwritten(self):
        # Never delete a version while a write still owns its extent/buffer.
        self.engine.drain()
        super().discard_unwritten()
