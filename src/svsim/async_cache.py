"""Version-keyed raw prefetch and bounded background immutable writeback."""
from contextlib import contextmanager
from time import sleep

from .async_io import BackpressureError
from .async_store_io import AsyncStoreIO
from .cache import BlockCache, BufferLease, CacheEntry, ManagedMetadata
from .cache_cost import FutureUseSummary, MAX_HINTS, MAX_CONTEXT_ITEMS, MAX_ID_LENGTH, resolve_cost


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
        self.metadata = ManagedMetadata(budget)
        self.cost_summary = None
        self.cost_context = None
        self.context_getter = None
        self.cost_resolver = None
        self.cost_trace = []
        self.cost_reasons = {}
        self.cost_trace_dropped = 0
        self.metadata_shed_count = 0

    def set_future_versions(self, versions):
        """Install bounded recency hints. They never become measured cost evidence."""
        result = {}
        iterator = iter(versions)
        for ordinal in range(MAX_HINTS):
            try:
                version = next(iterator)
            except StopIteration:
                break
            if not isinstance(version, str) or not version or len(version) > MAX_ID_LENGTH:
                return False
            result.setdefault(version, ordinal)
        amount = ManagedMetadata.conservative_size(result)
        try:
            self.metadata.reserve('legacy-future-use', amount if result else 0)
        except MemoryError:
            return False
        self.future_use = result
        return True

    def set_cost_hints(self, summary, *, context=(), cost_resolver=None, context_getter=None):
        """Install strict cost hints. Context must include all policy/root/layout/binding/profile versions."""
        if not isinstance(summary, FutureUseSummary):
            raise TypeError('summary must be a FutureUseSummary')
        pairs = tuple(sorted(dict(context).items()))
        if len(pairs) > MAX_CONTEXT_ITEMS or any(
                not isinstance(key, str) or not isinstance(value, str) or
                not key or not value or len(key) > MAX_ID_LENGTH or len(value) > MAX_ID_LENGTH
                for key, value in pairs):
            raise ValueError('Cache cost context exceeds bounded limits')
        amount = summary.managed_bytes + ManagedMetadata.conservative_size(pairs)
        try:
            self.metadata.reserve('cost-summary', amount)
        except MemoryError:
            return False
        self.cost_summary = summary
        self.cost_context = pairs
        self.cost_resolver = cost_resolver
        self.context_getter = context_getter
        return True

    def clear_cost_hints(self):
        self.metadata.release('cost-summary')
        self.cost_summary = self.cost_context = None
        self.cost_resolver = self.context_getter = None

    def _discard_stale_hints(self, live_context):
        if self.cost_summary is None:
            return None
        if self.context_getter is None:
            reason = 'missing_context_getter'
        else:
            try:
                current = tuple(sorted(dict(live_context).items())) if live_context is not None else None
                fresh = (current is not None and current == self.cost_context and
                         self.cost_summary.is_current(live_context))
            except Exception:
                fresh = False
            if fresh:
                return None
            reason = 'stale_context'
        self.clear_cost_hints()
        return reason

    def _cost_candidate(self, key, entry, live_context):
        summary = self.cost_summary
        if summary is None: return None, 'no_summary'
        if live_context is None: return None, 'missing_context_getter'
        if not summary.is_current(live_context): return None, 'stale_context'
        if self.cost_context != tuple(sorted(dict(live_context).items())): return None, 'stale_context'
        hint = summary.get(key)
        if hint is None: return None, 'missing_hint'
        cost = resolve_cost(hint, self.cost_resolver, key, entry)
        if cost is None: return None, 'unknown_cost'
        score = cost.eviction_score(entry.dirty, hint.reuse_count)
        if score is None: return None, 'unvalidated_cost'
        return (score, cost.units), 'known'

    def _record_policy(self, reason, candidate=None, *, headroom=0):
        if not isinstance(reason, str) or not reason or len(reason) > MAX_ID_LENGTH:
            self._note_trace_dropped()
            return
        if reason not in self.cost_reasons:
            if len(self.cost_reasons) >= 32:
                self._note_trace_dropped()
                return
            try:
                self._reserve_optional('cost-reasons-pool', 4096, headroom=headroom)
            except MemoryError:
                self._note_trace_dropped()
                return
            self.cost_reasons[reason] = 1
        else:
            self.cost_reasons[reason] = min(self.cost_reasons[reason] + 1, (1 << 31) - 1)
        if len(self.cost_trace) < 256:
            candidate = str(candidate)[:MAX_ID_LENGTH] if candidate is not None else None
            snapshot = self.cost_summary.snapshot_version if self.cost_summary else None
            row = {'reason': reason, 'candidate': candidate, 'snapshot': snapshot}
            try:
                self._reserve_optional('cost-trace-pool', 4096, headroom=headroom)
                # conservative_size includes its fixed pool; account the encoded
                # payload multiplier per row while charging the fixed pool once.
                encoded_bytes = ManagedMetadata.conservative_size(row) - 4096
                amount = 128 + encoded_bytes
            except MemoryError:
                self._note_trace_dropped()
                return
            key = f'cost-trace:{len(self.cost_trace)}'
            try:
                self._reserve_optional(key, amount, headroom=headroom)
            except MemoryError:
                self._note_trace_dropped()
                return
            self.cost_trace.append(row)
        else:
            self._note_trace_dropped()

    def _note_trace_dropped(self):
        self.cost_trace_dropped = min(self.cost_trace_dropped + 1, (1 << 31) - 1)

    def _reserve_optional(self, key, amount, *, headroom=0):
        """Reserve trace metadata only from RAM beyond the foreground requirement."""
        previous = self.metadata.allocations.get(key, 0)
        if self.budget.used + max(0, amount - previous) + headroom > self.budget.limit:
            raise MemoryError('Optional cache metadata would consume foreground RAM')
        return self.metadata.reserve(key, amount)

    def _shed_optional_metadata(self, required):
        """Drop bounded hints/traces only when they block a required allocation."""
        if self.budget.used + required <= self.budget.limit:
            return True
        if self.metadata.used:
            self.metadata_shed_count = min(self.metadata_shed_count + 1, (1 << 31) - 1)
        self.cost_trace_dropped = min(self.cost_trace_dropped + len(self.cost_trace), (1 << 31) - 1)
        for index in range(len(self.cost_trace)):
            self.metadata.release(f'cost-trace:{index}')
        self.cost_trace.clear()
        if self.budget.used + required <= self.budget.limit:
            return True
        self.metadata.release('cost-trace-pool')
        self.metadata.release('cost-reasons-pool')
        self.cost_reasons.clear()
        if self.budget.used + required <= self.budget.limit:
            return True
        self.metadata.release('legacy-future-use')
        self.future_use.clear()
        if self.budget.used + required <= self.budget.limit:
            return True
        self.clear_cost_hints()
        return self.budget.used + required <= self.budget.limit

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
        try:
            initial_context = self.context_getter() if self.context_getter is not None else None
        except Exception:
            initial_context = None
        stale_reason = self._discard_stale_hints(initial_context)
        if stale_reason:
            self._record_policy('stale_hints_discarded', headroom=additional)
        while self.budget.used + additional > self.budget.limit:
            eligible = [k for k, e in self.entries.items() if e.pins == 0 and e.buffer.pins == 0]
            if eligible:
                # Let one cache eviction plus only the necessary metadata relief
                # satisfy the request; otherwise optional metadata would cause
                # avoidable repeated evictions of the foreground working set.
                reclaimable = min(self.entries[k].buffer.cost for k in eligible)
                if self.budget.used + additional - reclaimable > self.budget.limit:
                    self._shed_optional_metadata(max(0, additional - reclaimable))
            try:
                live_context = self.context_getter() if self.context_getter is not None else None
            except Exception:
                live_context = None
            stale_reason = self._discard_stale_hints(live_context) or stale_reason
            evaluations = ([(None, stale_reason)] * len(eligible) if stale_reason else
                           [self._cost_candidate(k, self.entries[k], live_context) for k in eligible])
            units = {value[0][1] for value in evaluations if value[0] is not None}
            if eligible and evaluations and all(value[0] is not None for value in evaluations) and len(units) == 1:
                # Lower retained benefit is cheaper to evict; equal cost prefers clean then LRU.
                candidate = min(zip(eligible, evaluations),
                                key=lambda item: (item[1][0][0], self.entries[item[0]].dirty))[0]
                self._record_policy('known_cost', candidate, headroom=additional)
            else:
                candidate = next(iter(eligible), None)
                reason = next((v[1] for v in evaluations if v[0] is None),
                              'mixed_cost_units' if len(units) > 1 else 'no_eligible')
                self._record_policy('lru_fallback:' + reason, candidate, headroom=additional)
            if candidate is None:
                if self._shed_optional_metadata(additional):
                    continue
                if self.errors:
                    self.poll()
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
        try:
            self.engine.drain()
            super().discard_unwritten()
        finally:
            self.close_metadata()

    def close_metadata(self):
        """Release hint/trace reservations after cache shutdown; safe to call repeatedly."""
        if self.metadata.closed:
            return
        self.metadata.close()
        self.future_use.clear()
        self.cost_summary = self.cost_context = None
        self.context_getter = self.cost_resolver = None
        self.cost_trace.clear()
        self.cost_reasons.clear()
