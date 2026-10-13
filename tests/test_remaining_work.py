from dataclasses import asdict
from types import SimpleNamespace

import pytest

MPI = pytest.importorskip("mpi4py.MPI")

from svsim import AsyncDistributedSimulator, AsyncPolicy, DistributedConfig
from svsim.remaining_work import CaptureLimits, RemainingWorkExtractor
from svsim.runtime_cost import CostSample
from svsim.runtime_eta import CountedTask, RuntimeETA
from svsim.journal import canonical, content_hash


def make_sim(path, *, swap_policy="S0", lookahead=4):
    config = DistributedConfig(str(path), 2, ram_budget_bytes_per_rank=16 * 1024**2,
                               metadata_reserve_bytes=256 * 1024,
                               amplitude_store="file")
    policy = AsyncPolicy(io_engine="cooperative", swap_policy=swap_policy,
                         lookahead_gates=lookahead, prefetch_blocks=0)
    return AsyncDistributedSimulator(4, config, comm=MPI.COMM_SELF, policy=policy)


def append_circuit(sim, count=8):
    for index in range(count):
        # Vary both the operation and target so tail classes are distinct.
        if index % 2:
            sim.gate("RX", (index % 4,), angle=0.1 * (index + 1))
        else:
            sim.gate("H", (index % 4,))


def test_capture_bounds_window_scan_groups_and_metadata(tmp_path):
    with make_sim(tmp_path / "bounded", lookahead=8) as sim:
        append_circuit(sim, 12)
        extractor = RemainingWorkExtractor(CaptureLimits(
            window=2, max_groups=1, scan_items=2, scan_seconds=0.5, max_bytes=65536))
        snapshot = extractor.capture(sim)
        assert len(snapshot.window) <= 3  # two source groups plus publication
        assert snapshot.scanned_sources == 2
        assert snapshot.unscanned_sources > 0
        assert not snapshot.complete
        assert len(snapshot.tail) <= 2  # one class plus an overflow summary
        assert snapshot.metadata_bytes <= 65536


def test_s2_group_preserves_each_source_id_once(tmp_path):
    with make_sim(tmp_path / "s2", swap_policy="S2", lookahead=4) as sim:
        for gate, target in (("H", 0), ("RX", 1), ("H", 2), ("RZ", 3)):
            if gate in {"RX", "RZ"}:
                sim.gate(gate, (target,), angle=0.25)
            else:
                sim.gate(gate, (target,))
        snapshot = RemainingWorkExtractor().capture(sim)
        source_tasks = [task for task in snapshot.window if task.source_ids]
        flattened = [source_id for task in source_tasks for source_id in task.source_ids]
        expected = [op.op_id for op in sim.sequencer.operations[:4]]
        assert sorted(flattened) == sorted(expected)
        assert len(flattened) == len(set(flattened))
        assert any(len(task.source_ids) > 1 for task in source_tasks)


def test_tail_scan_advances_incrementally_then_completes(tmp_path):
    with make_sim(tmp_path / "incremental", lookahead=1) as sim:
        append_circuit(sim, 8)
        extractor = RemainingWorkExtractor(CaptureLimits(
            window=1, max_groups=64, scan_items=2, scan_seconds=0.5, max_bytes=8192))
        first = extractor.capture(sim)
        second = extractor.capture(sim)
        assert first.scanned_sources == 2
        assert first.unscanned_sources == 5
        assert not first.complete
        assert second.scanned_sources == 4
        assert second.unscanned_sources == 3
        final = None
        for _ in range(4):
            final = extractor.capture(sim)
            if final.complete:
                break
        assert final.complete
        assert final.unscanned_sources == 0
        assert final.scanned_sources == 7
        # Suffix groups cover only the scanned suffix; source-window coverage stays separate.
        assert sum(task.count for task in final.tail) == final.scanned_sources


def test_private_staged_delta_keeps_published_cut_and_marks_unknown(tmp_path):
    with make_sim(tmp_path / "private") as sim:
        append_circuit(sim, 4)
        extractor = RemainingWorkExtractor()
        published = extractor.capture(sim)
        first_source = next(task.source_ids[0] for task in published.window if task.source_ids)
        sim._private_epoch = True
        sim._active_epoch_ledger = SimpleNamespace(tasks={
            first_source: SimpleNamespace(state=SimpleNamespace(value="staged"))})
        private = extractor.capture(sim)
        assert private.private
        assert private.published_frontier == published.published_frontier
        assert private.snapshot_id != published.snapshot_id
        assert private.staged_sources == 1
        assert "PRIVATE_EPOCH_RESIDUAL_UNCALIBRATED" in private.unknown_reasons
        assert all(first_source not in task.source_ids for task in private.window)
        sim._private_epoch = False
        sim._active_epoch_ledger = None


def test_private_snapshot_marks_profile_context_change_stale(tmp_path):
    with make_sim(tmp_path / "private-stale") as sim:
        append_circuit(sim, 2)
        extractor = RemainingWorkExtractor()
        published = extractor.capture(sim)
        sim._private_epoch = True
        sim.runtime.profile_version += 1
        private = extractor.capture(sim)
        assert private.private
        assert private.stale
        assert private.published_frontier == published.published_frontier
        assert "PRIVATE_EPOCH_RESIDUAL_UNCALIBRATED" in private.unknown_reasons
        sim._private_epoch = False


def test_empty_endpoint_with_dirty_cache_is_unknown_not_zero_eta(tmp_path):
    with make_sim(tmp_path / "dirty") as sim:
        key = ("unpublished", 0)
        sim.cache.entries[key] = SimpleNamespace(dirty=True)
        try:
            snapshot = RemainingWorkExtractor().capture(sim)
            assert snapshot.endpoint_frontier == -1
            assert snapshot.complete
            assert "DIRTY_WRITEBACK_REMAINS" in snapshot.unknown_reasons
            report = RuntimeETA().evaluate(snapshot)
            assert report.expected_seconds is None
            assert "DIRTY_WRITEBACK_REMAINS" in report.unknown_reasons
        finally:
            sim.cache.entries.pop(key, None)


def test_unknown_suffix_and_huge_count_never_claim_unjustified_eta(tmp_path):
    with make_sim(tmp_path / "unknown", swap_policy="S1", lookahead=1) as sim:
        append_circuit(sim, 3)
        snapshot = RemainingWorkExtractor().capture(sim)
        assert "FUTURE_S1_LAYOUT_UNKNOWN" in {
            reason for task in snapshot.window for reason in task.unknown_reasons
        }
        result = sim.runtime.update_eta()
        assert result["snapshot_id"] == sim.runtime.latest_eta["snapshot_id"]
        assert result["eligible_for_control"] is False
        assert result["views"]["baseline"]["expected_seconds"] is None
        assert "FUTURE_S1_LAYOUT_UNKNOWN" in result["views"]["baseline"]["unknown_reasons"]

    # Counted evaluation accepts arbitrary-size counts without expansion, while
    # retaining the evaluator's explicit unknown boundary for semantic suffixes.
    task = CountedTask("huge", {"operation": "semantic-suffix"}, {}, 10**1000,
                       cost_key="huge", unknown_reasons=("FUTURE_EXECUTOR_SHAPE_UNKNOWN",))
    report = RuntimeETA().evaluate(SimpleNamespace(snapshot_id="huge", complete=True,
        stale=False, unknown_reasons=(), window=(), tail=(task,), in_flight=(), context={}),
        {"huge": CostSample("matched", 1, 1, 1)})
    assert report.lower_seconds is None
    assert report.expected_seconds is None
    assert report.known_work_count == 0
    assert report.total_work_count == 10**1000
    assert report.coverage == 0
    assert "FUTURE_EXECUTOR_SHAPE_UNKNOWN" in report.unknown_reasons


def test_huge_count_with_finite_weighted_cost_remains_compact():
    count = 10**300
    huge_task = CountedTask("huge-known", {"operation": "repeat"}, {}, count,
                             cost_key="huge-known")
    snapshot = SimpleNamespace(snapshot_id="finite-huge", complete=True, stale=False,
        unknown_reasons=(), window=(), tail=(huge_task,), in_flight=(), context={})
    report = RuntimeETA().evaluate(snapshot,
        {"huge-known": CostSample("matched", 1e-300, 1e-300, 1e-300)})
    assert report.expected_seconds == pytest.approx(1.0)
    assert report.total_work_count == count
    assert report.coverage == 1


def test_capture_limit_rejects_unbounded_settings():
    with pytest.raises(ValueError, match="max_groups"):
        CaptureLimits(max_groups=257)
    with pytest.raises(ValueError, match="window"):
        CaptureLimits(window=33)


def test_private_request_delta_cannot_overrun_snapshot_budget(tmp_path):
    with make_sim(tmp_path/'private-request-limit',lookahead=2) as sim:
        append_circuit(sim,2)
        extractor=RemainingWorkExtractor(CaptureLimits(max_bytes=8192))
        published=extractor.capture(sim)
        sim._private_epoch=True
        sim.io_engine.pending={i:SimpleNamespace(request_id=i,operation='read') for i in range(64)}
        try:
            private=extractor.capture(sim)
            assert private.private
            assert private.published_frontier==published.published_frontier
            assert private.metadata_bytes<=extractor.limits.max_bytes
            assert not private.complete
            assert 'REQUEST_DETAILS_OMITTED' in private.unknown_reasons
            assert RuntimeETA().evaluate(private).expected_seconds is None
        finally:
            sim.io_engine.pending.clear()
            sim._private_epoch=False
    with pytest.raises(ValueError, match="1024-byte"):
        CaptureLimits(max_bytes=1023)


def test_window_metadata_fallback_stays_inside_status_budget(tmp_path):
    with make_sim(tmp_path / "window-budget", lookahead=8) as sim:
        append_circuit(sim, 8)
        budget = 1024
        snapshot = RemainingWorkExtractor(CaptureLimits(
            window=8, max_groups=8, scan_items=1, scan_seconds=0.5, max_bytes=budget)).capture(sim)
        assert snapshot.metadata_bytes <= budget
        assert snapshot.metadata_bytes == len(canonical(asdict(snapshot)).encode())
        assert not snapshot.complete
        assert "CAPTURE_METADATA_LIMIT" in snapshot.unknown_reasons


def test_oversized_request_details_are_omitted_and_capture_stays_incomplete(tmp_path):
    with make_sim(tmp_path / "request-budget") as sim:
        pending = sim.io_engine.pending
        rows = {}
        for index in range(16):
            key = f"synthetic-{index}"
            rows[key] = SimpleNamespace(request_id=(key + "x" * 100), operation="read")
        pending.update(rows)
        try:
            budget = 1024
            snapshot = RemainingWorkExtractor(CaptureLimits(max_bytes=budget)).capture(sim)
            assert snapshot.metadata_bytes <= budget
            assert snapshot.metadata_bytes == len(canonical(asdict(snapshot)).encode())
            assert snapshot.in_flight == ()
            assert not snapshot.complete
            assert "REQUEST_DETAILS_OMITTED" in snapshot.unknown_reasons
        finally:
            for key in rows:
                pending.pop(key, None)


def test_private_minimal_fallback_hashes_published_frontier(tmp_path):
    with make_sim(tmp_path / "private-fallback") as sim:
        sim._private_epoch = True
        sim._rollback_root = "published-checksum"
        extractor = RemainingWorkExtractor()
        snapshot = extractor.capture(sim)
        assert snapshot.private
        assert snapshot.published_frontier == sim.frontier
        assert snapshot.context["root"] == "published-checksum"
        assert snapshot.snapshot_id == content_hash(snapshot.context)
        assert snapshot.metadata_bytes == len(canonical(asdict(snapshot)).encode())
        sim._private_epoch = False
