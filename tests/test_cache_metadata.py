import pytest

from svsim.async_cache import AsyncBlockCache
from svsim.async_io import WorkerIOEngine
from svsim.cache import BufferLease, ManagedMetadata, MemoryBudget
from svsim.cache_cost import FutureUseSummary
from svsim.storage import FileBlockStore, StorageConfig


def test_managed_metadata_replace_rejects_capacity_and_close_settles_exactly():
    budget = MemoryBudget(1000, metadata_bytes=100)
    charge = ManagedMetadata(budget)
    charge.reserve("summary", 500)
    charge.reserve("summary", 600)
    assert budget.used == 700
    with pytest.raises(MemoryError):
        charge.reserve("summary", 1000)
    assert charge.allocations == {"summary": 600}
    charge.release("summary")
    assert budget.used == 100
    charge.reserve("trace:0", 100)
    charge.close()
    charge.close()
    assert budget.used == 100
    with pytest.raises(RuntimeError, match="closed"):
        charge.reserve("late", 1)


def _cache(tmp_path, extra=0):
    slot_cost = BufferLease.allocation_cost(64, 64)
    config = StorageConfig(str(tmp_path), block_bits=2, alignment=64,
                           ram_budget_bytes=64 + 2 * slot_cost + extra,
                           metadata_reserve_bytes=64)
    store = FileBlockStore(config, 4)
    engine = WorkerIOEngine(engine="cooperative")
    budget = MemoryBudget(config.ram_budget_bytes, config.metadata_reserve_bytes)
    cache = AsyncBlockCache(store, budget, engine, prefetch_blocks=1)
    return config, store, engine, budget, cache


def _summary():
    return FutureUseSummary.from_pending_versions(("zero", "initial-one", "zero"),
        snapshot_version="cut-1", context_versions=(("root", "r1"),), window=3)


def test_async_hints_and_policy_trace_are_charged_and_released_on_close(tmp_path):
    config, store, engine, budget, cache = _cache(tmp_path, extra=32768)
    for key in ("zero", "initial-one"):
        with cache.pin(key):
            pass
    before = budget.used
    assert cache.set_cost_hints(_summary(), context={"root": "r1"},
                                context_getter=lambda: {"root": "r1"})
    assert budget.used > before
    assert cache.set_future_versions(("zero", "initial-one", "zero"))
    assert budget.used > before
    cache._record_policy("test_reason", "zero")
    cache.ensure(cache.buffer_cost)
    assert cache.cost_trace
    assert any(key.startswith("cost-trace:") for key in cache.metadata.allocations)
    cache.discard_unwritten()
    assert budget.used == config.metadata_reserve_bytes
    engine.close()
    store.close()


def test_hint_capacity_refusal_keeps_previous_state_and_length_is_bounded(tmp_path):
    config, store, engine, budget, cache = _cache(tmp_path)
    for key in ("zero", "initial-one"):
        with cache.pin(key):
            pass
    before = budget.used
    assert not cache.set_cost_hints(_summary(), context={"root": "r1"},
                                    context_getter=lambda: {"root": "r1"})
    assert cache.cost_summary is None and budget.used == before
    assert not cache.set_future_versions(("zero",))
    assert not cache.future_use and budget.used == before
    budget.limit += 20000
    assert cache.set_future_versions(tuple(f"v{i}" for i in range(65)))
    assert len(cache.future_use) == 64
    assert not cache.set_future_versions(("x" * 257,))
    cache.discard_unwritten()
    engine.close()
    store.close()
    assert budget.used == config.metadata_reserve_bytes


def test_stale_cost_summary_is_deleted_before_lru_fallback(tmp_path):
    config, store, engine, budget, cache = _cache(tmp_path, extra=24576)
    for key in ("zero", "initial-one"):
        with cache.pin(key):
            pass
    live = {"root": "r1"}
    assert cache.set_cost_hints(_summary(), context=live, context_getter=lambda: live)
    assert "cost-summary" in cache.metadata.allocations
    live["root"] = "r2"
    cache.ensure(0)
    assert cache.cost_summary is None
    assert "cost-summary" not in cache.metadata.allocations
    budget.limit = budget.used
    cache.ensure(cache.buffer_cost)
    assert any(row["reason"] == "stale_hints_discarded" for row in cache.cost_trace)
    cache.discard_unwritten()
    engine.close()
    store.close()
    assert budget.used == config.metadata_reserve_bytes
