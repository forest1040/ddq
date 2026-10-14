"""Version-keyed LRU cache and explicit owned-buffer reservations."""
from collections import OrderedDict
from contextlib import contextmanager
from dataclasses import dataclass
import mmap
import json
import numpy as np
from .transport import _buffer_address


class MemoryBudget:
    def __init__(self, limit, metadata_bytes=0):
        if limit < 1 or not 0 <= metadata_bytes <= limit:
            raise ValueError('Invalid RAM budget/metadata reserve')
        self.limit, self.used, self.peak = limit, 0, 0
        self.reserve(metadata_bytes)

    def reserve(self, count):
        if count < 0: raise ValueError('Negative reservation')
        if self.used + count > self.limit:
            raise MemoryError(f'Task needs {self.used + count} managed bytes; RAM budget is {self.limit}')
        self.used += count
        self.peak = max(self.peak, self.used)

    def release(self, count):
        if not 0 <= count <= self.used: raise RuntimeError('Unbalanced RAM reservation')
        self.used -= count


class ManagedMetadata:
    """Bounded estimated metadata charges against the shared managed-RAM budget.

    Charges use conservative fixed overhead and encoded-size multipliers. They
    are capacity controls, not a measurement or guarantee of process RSS.
    """
    MAX_ALLOCATIONS = 512
    MAX_KEY_LENGTH = 128

    def __init__(self, budget):
        self.budget = budget
        self.allocations = {}
        self.closed = False

    @staticmethod
    def conservative_size(value):
        encoded = json.dumps(value, sort_keys=True, separators=(',', ':'),
                             ensure_ascii=True, allow_nan=False).encode('ascii')
        return 4096 + 8 * len(encoded)

    def reserve(self, key, count):
        if self.closed: raise RuntimeError('Metadata accounting is closed')
        if not isinstance(key, str) or not key or len(key) > self.MAX_KEY_LENGTH:
            raise ValueError('Invalid metadata allocation key')
        if isinstance(count, bool) or not isinstance(count, int) or count < 0:
            raise ValueError('Metadata allocation must be a nonnegative integer')
        if key not in self.allocations and len(self.allocations) >= self.MAX_ALLOCATIONS:
            raise MemoryError('Managed metadata allocation capacity exceeded')
        previous = self.allocations.get(key, 0)
        delta = count - previous
        if delta > 0:
            self.budget.reserve(delta)
        elif delta < 0:
            self.budget.release(-delta)
        if count:
            self.allocations[key] = count
        else:
            self.allocations.pop(key, None)
        return True

    def release(self, key):
        previous = self.allocations.pop(key, 0)
        if previous:
            self.budget.release(previous)
        return previous

    @property
    def used(self):
        return sum(self.allocations.values())

    def close(self):
        if self.closed:
            return
        amount = self.used
        self.allocations.clear()
        if amount:
            self.budget.release(amount)
        self.closed = True


class BufferLease:
    """Aligned mmap allocation. Array views must not outlive their lease."""
    def __init__(self, budget, size, alignment=4096):
        self.budget, self.size, self.cost = budget, size, self.allocation_cost(size,alignment)
        budget.reserve(self.cost)
        try:
            self.region = mmap.mmap(-1, self.cost)
            whole = memoryview(self.region)
            shift = (-_buffer_address(whole)) % alignment
            self.view = whole[shift:shift+size]
            whole.release()
        except BaseException:
            budget.release(self.cost)
            raise
        self.closed = False
        self.cached = False
        self.pins = 0
        self._close_requested = False

    @staticmethod
    def allocation_cost(size, alignment):
        # mmap consumes complete OS pages even for a tiny logical Block. Count
        # those pages, rather than letting small-block caches exceed RAM limits.
        return ((size+alignment+mmap.PAGESIZE-1)//mmap.PAGESIZE)*mmap.PAGESIZE

    def array(self, payload_bytes=None, *, readonly=False):
        if self.closed: raise ValueError('Closed buffer')
        array = np.frombuffer(self.view, dtype='<c16', count=(payload_bytes or self.size)//16)
        if readonly: array.setflags(write=False)
        return array

    def acquire_pin(self):
        if self.closed or self._close_requested: raise ValueError("Closed buffer owner")
        self.pins += 1

    def release_pin(self):
        if self.pins <= 0: raise RuntimeError("Unbalanced buffer pin")
        self.pins -= 1
        if not self.pins and self._close_requested: self.close()

    def close(self):
        self._close_requested = True
        if self.pins: return
        if not self.closed:
            self.view.release()
            self.region.close()
            self.budget.release(self.cost)
            self.closed = True

    def __enter__(self): return self
    def __exit__(self,*args):
        if not self.cached: self.close()


@dataclass
class CacheEntry:
    buffer: BufferLease
    dirty: bool = False
    pins: int = 0


class BlockCache:
    def __init__(self, store, budget):
        self.store, self.budget = store, budget
        self.entries = OrderedDict()
        self.hits = self.misses = self.evictions = 0

    @property
    def buffer_cost(self): return BufferLease.allocation_cost(self.store.slot_bytes,self.store.config.alignment)

    def ensure(self, additional):
        if additional > self.budget.limit - self.store.config.metadata_reserve_bytes:
            raise MemoryError(f'Minimum task RAM is {additional + self.store.config.metadata_reserve_bytes} bytes')
        while self.budget.used + additional > self.budget.limit:
            key = next((k for k, e in self.entries.items() if e.pins == 0), None)
            if key is None: raise MemoryError('Insufficient unpinned RAM for task')
            self._evict(key)

    def _evict(self, key):
        entry = self.entries[key]
        if entry.pins: raise RuntimeError('Cannot evict pinned Block')
        if entry.dirty:
            self.store.write_version(key, entry.buffer.view)
            entry.dirty = False
        entry.buffer.close()
        del self.entries[key]
        self.evictions += 1

    @contextmanager
    def pin(self, version):
        if version in self.entries:
            self.hits += 1
            self.entries.move_to_end(version)
        else:
            self.misses += 1
            self.ensure(self.buffer_cost)
            buffer = BufferLease(self.budget, self.store.slot_bytes, self.store.config.alignment)
            try: self.store.read_version(version, buffer.view)
            except BaseException:
                buffer.close()
                raise
            self.entries[version] = CacheEntry(buffer)
        entry = self.entries[version]
        entry.pins += 1
        try: yield entry.buffer
        finally: entry.pins -= 1

    @contextmanager
    def outputs(self, count):
        self.ensure(count*self.buffer_cost)
        buffers = []
        try:
            for _ in range(count): buffers.append(BufferLease(self.budget, self.store.slot_bytes, self.store.config.alignment))
            yield buffers
        finally:
            for buffer in buffers:
                if not buffer.closed and not buffer.cached: buffer.close()

    def adopt(self, buffer):
        version = self.store.new_version()
        buffer.cached = True
        self.entries[version] = CacheEntry(buffer, dirty=True)
        hook = getattr(self.store.db, 'on_rollback', None)
        if hook is not None: hook(lambda: self._forget(version))
        return version

    def _forget(self, version):
        """Drop a version whose catalog row was rolled back with its gate batch."""
        entry = self.entries.pop(version, None)
        if entry is not None and not entry.pins: entry.buffer.close()

    def flush(self):
        for key, entry in self.entries.items():
            if entry.dirty:
                self.store.write_version(key, entry.buffer.view)
                entry.dirty = False

    def clear(self):
        for key in list(self.entries): self._evict(key)

    def discard_unwritten(self):
        for key, entry in list(self.entries.items()):
            if entry.dirty:
                entry.dirty = False
                self.store.delete_unwritten(key)
            self._evict(key)
