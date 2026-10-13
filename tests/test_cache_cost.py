import pytest

from svsim.cache_cost import CacheCost, FutureUseHint, FutureUseSummary
from svsim.async_cache import AsyncBlockCache
from svsim.async_io import WorkerIOEngine
from svsim.cache import BufferLease, MemoryBudget
from svsim.storage import FileBlockStore, StorageConfig
from svsim.transport import open_storage


def test_summary_is_bounded_and_context_versioned():
    summary = FutureUseSummary.from_versions(range(80), snapshot_version='snap',
                                            context_versions={'root': 'r1'}, window=32)
    assert len(summary.hints) == 32
    assert summary.get('0').next_use == 0
    assert summary.is_current({'root': 'r1'})
    assert not summary.is_current({'root': 'r2'})


def test_cache_cost_combines_once_and_requires_known_dirty_writeback():
    assert CacheCost(raw_read=2, rematerialize=3, evidence='measured', validated=True,
                     units='seconds', source='profile-a', scope='local-read').eviction_score(False, 2) == 10
    assert CacheCost(whole_fetch=8, raw_read=2, rematerialize=3,
                     component_fetch=True, evidence='measured', validated=True,
                     source='profile-a', scope='whole-fetch').eviction_score(False, 2) == 16
    assert CacheCost(raw_read=2, rematerialize=3).eviction_score(False) is None
    assert CacheCost(raw_read=2, rematerialize=3, dirty_writeback=4,
                     evidence='measured', validated=True, source='profile-a',
                     scope='dirty-read').eviction_score(True) == 9


def test_summary_rejects_duplicate_hints():
    try:
        FutureUseSummary((FutureUseHint('x', 0), FutureUseHint('x', 1)), 's', ())
    except ValueError as exc:
        assert 'Duplicate' in str(exc)
    else:
        raise AssertionError('duplicate hint accepted')


def test_hint_and_context_limits_are_strict():
    for kwargs in ({'next_use': True}, {'reuse_count': 1.5}):
        with pytest.raises(ValueError):
            FutureUseHint('x', **({'next_use': 0} | kwargs))
    with pytest.raises(ValueError, match='window'):
        FutureUseSummary((FutureUseHint('x', 2),), 's', (), window=2)
    with pytest.raises(ValueError, match='future window'):
        FutureUseSummary.from_versions(range(5), window=True)
    with pytest.raises(ValueError, match='context'):
        FutureUseSummary((), 's', tuple((str(i), 'v') for i in range(17)))
    with pytest.raises(ValueError, match='Duplicate context'):
        FutureUseSummary((), 's', (('root', 'r1'), ('root', 'r2')))


def test_cost_inputs_reject_bool_numeric_fields_and_nonbool_flags():
    with pytest.raises(ValueError, match='Costs'):
        CacheCost(raw_read=True)
    with pytest.raises(ValueError, match='Costs'):
        FutureUseHint('v', 0, raw_read=False)
    with pytest.raises(ValueError, match='confidence'):
        FutureUseHint('v', 0, confidence=True)
    with pytest.raises(ValueError, match='confidence'):
        FutureUseHint('v', 0, confidence=10**10000)
    with pytest.raises(TypeError, match='component_fetch'):
        CacheCost(component_fetch=1)
    with pytest.raises(TypeError, match='component_fetch'):
        FutureUseHint('v', 0, component_fetch=1)
    with pytest.raises(TypeError, match='cost_validated'):
        FutureUseHint('v', 0, cost_validated=1)
    with pytest.raises(TypeError, match='validated'):
        CacheCost(validated=1)


def test_score_rejects_invalid_reuse_inputs_and_returns_unknown_on_overflow():
    cost = _measured(1.0)
    for dirty, reuse_count in ((1, 1), (False, True), (False, 1.5), (False, 0)):
        with pytest.raises(ValueError):
            cost.eviction_score(dirty, reuse_count)
    assert cost.eviction_score(False, 10**10000) is None
    overflowing_sum = _measured(1e308, remat=1e308)
    assert overflowing_sum.eviction_score(False) is None
    with pytest.raises(ValueError, match='Costs'):
        CacheCost(raw_read=10**10000)


def _cache(tmp_path, *, transport_factory=None):
    config = StorageConfig(str(tmp_path), block_bits=2, alignment=64,
                           ram_budget_bytes=64 + 2 * BufferLease.allocation_cost(64, 64),
                           metadata_reserve_bytes=64)
    store = FileBlockStore(config, 4, transport_factory=transport_factory or open_storage)
    engine = WorkerIOEngine(engine='cooperative')
    budget = MemoryBudget(config.ram_budget_bytes, config.metadata_reserve_bytes)
    return config, store, engine, budget, AsyncBlockCache(store, budget, engine, prefetch_blocks=1)


def _measured(raw, remat=0, writeback=None):
    return CacheCost(raw_read=raw, rematerialize=remat, dirty_writeback=writeback,
                     evidence='measured', validated=True, units='seconds',
                     source='validated-test-profile', scope='single-node-cache')


def test_async_cache_cost_choice_and_stale_context_lru_fallback(tmp_path):
    config, store, engine, budget, cache = _cache(tmp_path)
    # Install two clean cached versions in LRU order.
    for key in ('zero', 'initial-one'):
        with cache.pin(key): pass
    hints = FutureUseSummary((FutureUseHint('zero', 1), FutureUseHint('initial-one', 2)),
                             'snap', (('root', 'r1'),))
    cache.set_cost_hints(hints, context={'root': 'r1'},
                         context_getter=lambda: {'root': 'r1'},
                         cost_resolver=lambda version, entry, hint:
                         _measured(100 if version == 'zero' else 1))
    cache.ensure(cache.buffer_cost)
    assert list(cache.entries) == ['zero']
    assert cache.cost_trace[-1]['reason'] == 'known_cost'
    cache.clear()

    # A live context change invalidates every hint and restores deterministic LRU.
    for key in ('zero', 'initial-one'):
        with cache.pin(key): pass
    current = {'root': 'r2'}
    cache.set_cost_hints(hints, context={'root': 'r1'}, context_getter=lambda: current,
                         cost_resolver=lambda *_: CacheCost(raw_read=100, rematerialize=0))
    cache.ensure(cache.buffer_cost)
    assert list(cache.entries) == ['initial-one']
    assert cache.cost_trace[-1]['reason'] == 'lru_fallback:stale_context'
    cache.clear(); engine.close(); store.close()
    assert budget.used == config.metadata_reserve_bytes


def test_unvalidated_or_missing_freshness_evidence_always_falls_back_to_lru(tmp_path):
    config, store, engine, budget, cache = _cache(tmp_path)
    for key in ('zero', 'initial-one'):
        with cache.pin(key): pass
    hints = FutureUseSummary((FutureUseHint('zero', 1), FutureUseHint('initial-one', 2)),
                             'snap', (('root', 'r'),))
    cache.set_cost_hints(hints, context={'root': 'r'},
                         cost_resolver=lambda *_: CacheCost(raw_read=100, rematerialize=0,
                                                            evidence='injected', validated=False,
                                                            source='manual', scope='test'))
    cache.ensure(cache.buffer_cost)
    assert list(cache.entries) == ['initial-one']
    assert cache.cost_trace[-1]['reason'] == 'lru_fallback:missing_context_getter'
    cache.clear(); engine.close(); store.close()
    assert budget.used == config.metadata_reserve_bytes


def test_cost_oracle_keeps_costly_dirty_version_over_cheap_clean(tmp_path):
    config, store, engine, budget, cache = _cache(tmp_path)
    with cache.outputs(1) as outputs:
        dirty = cache.adopt(outputs[0])
    with cache.pin('zero'): pass
    hints = FutureUseSummary((FutureUseHint(dirty, 1), FutureUseHint('zero', 2)),
                             'snap', (('root', 'r'),))
    cache.set_cost_hints(hints, context={'root': 'r'}, context_getter=lambda: {'root': 'r'},
                         cost_resolver=lambda version, entry, hint:
                         _measured(1 if version == dirty else 2, writeback=100 if entry.dirty else 0))
    cache.ensure(cache.buffer_cost)
    assert dirty in cache.entries and 'zero' not in cache.entries
    cache.clear(); engine.close(); store.close()
    assert budget.used == config.metadata_reserve_bytes


def test_prefetch_respects_budget_and_pending_io_pins_entry(tmp_path):
    config, store, engine, budget, cache = _cache(tmp_path)
    with cache.pin('zero'): pass
    with cache.pin('initial-one'): pass
    assert not cache.prefetch('zero')
    assert not cache.prefetch('later')
    # Free one slot, submit a read, and verify its pending ownership pins the entry.
    cache._evict('zero')
    version = store.new_version()
    store.write_version(version, bytearray(store.slot_bytes))
    assert cache.prefetch(version)
    entry = cache.entries[version]
    assert entry.pins == 1
    with pytest.raises(RuntimeError, match='pending I/O'):
        cache._evict(version)
    engine.wait(cache.reads[version])
    cache.poll()
    assert entry.pins == 0
    cache.clear(); engine.close(); store.close()
    assert budget.used == config.metadata_reserve_bytes


def test_failed_dirty_writeback_keeps_owned_buffer_and_does_not_evict(tmp_path):
    class ShortWriteTransport:
        def __init__(self, path, mode, **kwargs):
            self.delegate = open_storage(path, mode, backend='buffered', alignment=kwargs['alignment'])
        def write_from(self, offset, view): return len(view) - 1
        def read_into(self, offset, view): return self.delegate.read_into(offset, view)
        def flush(self): return self.delegate.flush()
        def close(self): return self.delegate.close()

    config, store, engine, budget, cache = _cache(tmp_path, transport_factory=ShortWriteTransport)
    with cache.outputs(1) as outputs:
        dirty = cache.adopt(outputs[0])
    with cache.pin('zero'): pass
    cache.ensure(cache.buffer_cost)
    with pytest.raises(OSError, match='Incomplete write'):
        engine.wait(cache.writes[dirty])
    assert dirty in cache.entries and cache.entries[dirty].dirty
    assert cache.entries[dirty].pins == 0 and not cache.entries[dirty].buffer.closed
    cache.discard_unwritten(); engine.close(); store.close()
    assert budget.used == config.metadata_reserve_bytes
