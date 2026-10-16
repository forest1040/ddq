from dataclasses import asdict
import time
from types import SimpleNamespace

import pytest

MPI = pytest.importorskip("mpi4py.MPI")

from svsim import AsyncDistributedSimulator, AsyncPolicy, DistributedConfig
from svsim.remaining_work import CaptureLimits, RemainingWorkExtractor, future_shape_reasons, task as make_task
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


def test_runtime_snapshot_emits_explicit_route_work_and_unknown_details(tmp_path):
    with make_sim(tmp_path / "coverage-input", lookahead=4) as sim:
        append_circuit(sim, 8)
        snapshot = RemainingWorkExtractor().capture(sim)
        tasks = snapshot.window + snapshot.future
        executors = [t for t in tasks if t.stage_route]
        assert executors and all(t.payload_bytes_per_unit == 0 for t in executors)
        assert any(t.kernel_work_per_unit is not None for t in executors)
        assert any(node.unknown_reasons for t in executors for node in t.stage_route.stages)
        report = RuntimeETA().evaluate(snapshot, {})
        assert report.expected_seconds is None
        assert report.executor_stage_coverage
        assert not any(row["included_in_priced_parent"] for row in report.executor_stage_coverage)
        assert all(row["independently_calibrated_stages"] == 0 for row in report.executor_stage_coverage)
        assert "excludes storage" in report.payload_byte_coverage.scope
        assert report.eligible_for_control is False


def test_runtime_task_accepts_only_explicit_exact_work_metadata():
    condition = {"operation": "mpi-transfer", "payload_bytes": 4096,
                 "kernel_work_units": 12}
    entry = make_task("transfer", condition, {}, count=3)
    assert entry.payload_bytes_per_unit == 4096
    assert entry.kernel_work_per_unit == 12
    assert entry.payload_bytes_per_unit * entry.count == 12288

    coarse = make_task("coarse", {"n": 40, "ranks": 8, "gate_count": 100}, {})
    assert coarse.payload_bytes_per_unit is None
    assert coarse.kernel_work_per_unit is None


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
        assert all(first_source not in task.source_ids for task in private.window)
        # Between calls nothing runs: requests are inside the measured publication.
        assert private.in_flight_covered
        assert "PRIVATE_EPOCH_RESIDUAL_UNCALIBRATED" not in private.unknown_reasons
        # A running call becomes one measured residual task for its elapsed time.
        running = next(task for task in private.window if task.source_ids)
        sim._executor_in_flight = (running.source_ids, time.perf_counter() - 0.5)
        residual = extractor.capture(sim)
        task = next(task for task in residual.window if task.kind == "residual")
        assert task.source_ids == running.source_ids and task.condition["operation"] == "call-residual"
        assert task.condition["elapsed_seconds"] >= 0.5 and "op_id" not in task.condition["class"]["operations"][0]
        assert residual.in_flight_covered
        # A running call the window cannot price leaves the epoch residual unknown.
        sim._executor_in_flight = ((10**6,), time.perf_counter())
        unknown = extractor.capture(sim)
        assert not unknown.in_flight_covered
        assert "PRIVATE_EPOCH_RESIDUAL_UNCALIBRATED" in unknown.unknown_reasons
        sim._executor_in_flight = None
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
        assert RuntimeETA().evaluate(private).expected_seconds is None
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
    # S1 layout decisions are replayed (tests/mpi_s1_eta_worker.py); nothing is left unknown for them.
    assert future_shape_reasons(SimpleNamespace(policy=AsyncPolicy(swap_policy="S1"), size=2)) == ()
    with make_sim(tmp_path / "unknown", swap_policy="S1", lookahead=1) as sim:
        append_circuit(sim, 3)
        # A single rank never moves S1 layout bits: the shape is exact.
        snapshot = RemainingWorkExtractor().capture(sim)
        assert not any(task.unknown_reasons for task in snapshot.window)
        assert snapshot.future_complete and not snapshot.future_reasons
        result = sim.runtime.update_eta()
        assert result["snapshot_id"] == sim.runtime.latest_eta["snapshot_id"]
        assert result["eligible_for_control"] is False
        # No measured cost exists yet: unknown, never a guessed value.
        assert result["views"]["observed"]["expected_seconds"] is None
        assert any(r.startswith("MISSING_COST:") for r in result["views"]["observed"]["unknown_reasons"])

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


def _layers(sim, layers):
    for _ in range(layers):
        sim.gate("H", (0,)); sim.gate("RX", (1,), angle=0.3); sim.gate("H", (3,)); sim.gate("RX", (2,), angle=0.2)


@pytest.mark.parametrize("swap_policy", ["S0", "S2"])
def test_future_shape_matches_scheduler_calls_and_epochs(tmp_path, swap_policy):
    from svsim.replan_cost_producer import remaining_calls
    with make_sim(tmp_path / swap_policy, swap_policy=swap_policy, lookahead=3) as sim:
        _layers(sim, 6)
        sim.materialize(frontier=4)
        end = len(sim.sequencer.operations) - 1
        snap = RemainingWorkExtractor().capture(sim, endpoint_frontier=end)
        assert snap.future_complete and not snap.future_reasons
        executors = [t for t in snap.window + snap.future if t.kind in {"executor", "executor-count"}]
        assert sum(t.count for t in executors) == remaining_calls(sim, sim.policy, sim.frontier + 1, end)
        epochs = -(-(end - sim.frontier) // 3)
        publish = {t.task_id: t.count for t in snap.window + snap.future if t.kind == "publication"}
        assert publish == {"window:publish": 1, "future:publish": epochs - 1}
        assert next(t.count for t in snap.future if t.kind == "epoch-boundary") == epochs
        # Classes aggregate repeated shapes and carry no per-occurrence ids.
        assert all("op_id" not in op for t in snap.future if t.kind == "executor-count"
                   for op in t.condition["operations"])
        assert len(snap.future) < sum(t.count for t in snap.future)


def test_future_shape_is_scanned_by_whole_epochs_within_budget(tmp_path):
    with make_sim(tmp_path / "budget", lookahead=2) as sim:
        _layers(sim, 4)
        extractor = RemainingWorkExtractor(CaptureLimits(scan_items=2, scan_seconds=0.5))
        first = extractor.capture(sim)
        assert not first.future_complete and not first.future_reasons
        while not (snap := extractor.capture(sim)).future_complete:
            pass
        assert next(t.count for t in snap.future if t.kind == "publication") == 7
        # Over its class share the future is dropped with a reason; the tail stays.
        small = RemainingWorkExtractor(CaptureLimits(max_groups=3, scan_seconds=0.5)).capture(sim)
        assert small.future == () and small.future_reasons == ("FUTURE_CLASS_LIMIT",) and small.tail


def test_observed_view_prices_full_remaining_from_measured_epoch_stages(tmp_path):
    config = DistributedConfig(str(tmp_path / "observed"), 2, ram_budget_bytes_per_rank=64 * 1024**2,
                               metadata_reserve_bytes=8 * 1024**2, amplitude_store="file")
    policy = AsyncPolicy(io_engine="cooperative", swap_policy="S2", lookahead_gates=4, prefetch_blocks=0)
    with AsyncDistributedSimulator(4, config, comm=MPI.COMM_SELF, policy=policy) as sim:
        sim.runtime.configure_measurements(max_bytes=2 * 1024**2)
        _layers(sim, 30)
        end = len(sim.sequencer.operations) - 1
        sim.materialize(frontier=79)
        stages = {r.stage for r in sim.runtime.measurements.cost_observations()}
        assert {"epoch-publication", "epoch-boundary"} <= stages
        observed = sim.runtime.update_eta(endpoint_frontier=end)["views"]["observed"]
        reasons = observed["unknown_reasons"]
        # Only measured, certified classes may answer; otherwise the reason is a missing cost.
        assert observed["expected_seconds"] is not None or all(r.startswith("MISSING_COST:") for r in reasons)
        if observed["expected_seconds"] is not None:
            assert observed["coverage"] == 1.0
            assert observed["lower_seconds"] <= observed["expected_seconds"] <= observed["upper_seconds"]
        sim.materialize()


def test_cost_rows_cache_follows_collector_revision():
    from svsim.runtime_measurement import RuntimeMeasurementCollector
    collector = RuntimeMeasurementCollector(max_bytes=64 * 1024)
    def add(i):
        collector.complete(f"e{i}", context={}, condition={"operation": "x"}, stage="x", units="calls",
                           quantity=1, protocol_seconds=0.1, success=True, validated=True, evidence="measured")
    add(0)
    first = collector.cost_rows()
    assert collector.cost_rows() is first and len(first) == 1
    copies = collector.cost_observations()
    assert copies[0] == first[0] and copies[0] is not first[0]
    add(1)
    assert len(collector.cost_rows()) == 2


def _shape(snapshot):
    return ({(t.kind, canonical(t.condition)): t.count for t in snapshot.future},
            {canonical(t.condition): t.count for t in snapshot.tail})


def test_scans_survive_whole_epoch_advances_and_match_a_fresh_scan(tmp_path):
    with make_sim(tmp_path / "advance", swap_policy="S2", lookahead=2) as sim:
        _layers(sim, 10)
        sim.materialize(frontier=1)  # binds parameters; a Binding change rescans
        extractor = RemainingWorkExtractor(CaptureLimits(scan_items=8, scan_seconds=0.5))
        while not (snap := extractor.capture(sim)).future_complete or not snap.complete:
            pass
        for frontier in (3, 7, 13):  # whole epochs of 2
            sim.materialize(frontier=frontier)
            incremental = extractor.capture(sim)
            fresh = RemainingWorkExtractor(CaptureLimits(scan_items=4096, scan_seconds=1)).capture(sim)
            # One bounded capture suffices: nothing already scanned is rescanned.
            assert incremental.complete and incremental.future_complete
            assert _shape(incremental) == _shape(fresh)
        # A non-aligned advance rescans from the new epoch alignment.
        sim.materialize(frontier=16)
        assert not extractor.capture(sim).future_complete


def test_call_residual_prices_the_remainder_of_a_certified_class():
    from svsim.runtime_measurement import RuntimeMeasurementCollector
    from svsim.runtime_observed_costs import observed_task_cost
    collector = RuntimeMeasurementCollector(max_bytes=256 * 1024)
    condition = {"operation": "epoch-publication", "n": 4}
    for i in range(20):
        collector.complete(f"p{i}", context={"policy_epoch": 0, "plan_version": 0}, condition=condition,
                           stage="epoch-publication", units="calls", quantity=1, protocol_seconds=1.0 + 0.01 * (i % 3),
                           success=True, validated=True, evidence="measured")
    whole = observed_task_cost(collector, condition, 0, 0)
    assert whole is not None and whole.measured
    residual = observed_task_cost(collector, {"operation": "call-residual", "class": condition,
                                              "elapsed_seconds": 0.4}, 0, 0)
    assert residual.mean_seconds == pytest.approx(whole.mean_seconds - 0.4, abs=0.02)
    assert residual.lower_seconds <= residual.mean_seconds <= residual.upper_seconds
    decisions = {}
    overrun = observed_task_cost(collector, {"operation": "call-residual", "class": condition,
                                             "elapsed_seconds": 5.0}, 0, 0, decisions=decisions)
    assert overrun is None and "RESIDUAL_OVERRUN" in decisions.values()


def test_distinct_angles_certify_through_the_angle_pooled_class():
    from svsim.runtime_measurement import RuntimeMeasurementCollector
    from svsim.runtime_observed_costs import observed_task_cost
    collector = RuntimeMeasurementCollector(max_bytes=512 * 1024)
    def op(angle):
        return {"kind": "RX", "targets": [1], "controls": [], "angle": {"op": "const", "value": angle},
                "clifford": False, "diagonal": False, "permutation": False}
    def condition(angle):
        return {"operation": "executor-gate", "executor": "single-gate", "operations": [op(angle)], "n": 4}
    for i in range(20):
        collector.complete(f"g{i}", context={"policy_epoch": 0, "plan_version": 0}, condition=condition(0.1 * i),
                           stage="executor-gate", units="groups", quantity=1, protocol_seconds=0.01,
                           success=True, validated=True, evidence="measured")
    decisions = {}
    cost = observed_task_cost(collector, condition(9.9), 0, 0, decisions=decisions)
    assert cost.measured and cost.condition.startswith("observed-pooled-angle:")
    assert "INSUFFICIENT_SAMPLES" in decisions.values() and "CERTIFIED" in decisions.values()
    # Pooling never crosses a different gate shape (here: another target).
    other = condition(0.5); other["operations"][0]["targets"] = [2]
    assert observed_task_cost(collector, other, 0, 0) is None


def test_angle_pooling_keeps_chronological_holdout_when_exact_angles_repeat():
    from svsim.runtime_measurement import RuntimeMeasurementCollector
    from svsim.runtime_observed_costs import observed_task_cost, DRIFTING, CERTIFIED
    collector = RuntimeMeasurementCollector(max_bytes=512 * 1024)

    def condition(angle):
        return {'operation': 'executor-gate', 'executor': 'single-gate', 'n': 4,
                'operations': [{'kind': 'RX', 'targets': [1], 'controls': [],
                                'angle': angle, 'clifford': False, 'diagonal': False,
                                'permutation': False}]}

    # Neither exact class has 8 post-burn-in samples. Condition-ring order
    # interleaves the two time regimes in both halves and used to certify this
    # 3x slowdown incorrectly; true chronological holdout must reject it.
    for i, seconds in enumerate([1.0] * 9 + [3.0] * 6):
        collector.complete(f'event-{15-i}', context={'policy_epoch': 0, 'plan_version': 0},
                           condition=condition(0.1 if i % 2 else 0.2), stage='executor-gate',
                           units='groups', quantity=1, protocol_seconds=seconds,
                           success=True, validated=True, evidence='measured')
    decisions = {}
    assert observed_task_cost(collector, condition(0.3), 0, 0, decisions=decisions) is None
    pooled = [v for k, v in decisions.items() if k.startswith('pooled-angle:')]
    assert pooled == [DRIFTING]
    assert CERTIFIED not in decisions.values()


def test_future_classes_differing_only_in_angle_are_pooled(tmp_path):
    import random
    rng = random.Random(7)
    config = DistributedConfig(str(tmp_path / "angles"), 2, ram_budget_bytes_per_rank=16 * 1024**2,
                               metadata_reserve_bytes=4 * 1024**2, amplitude_store="file")
    policy = AsyncPolicy(io_engine="cooperative", swap_policy="S2", lookahead_gates=2, prefetch_blocks=0)
    with AsyncDistributedSimulator(4, config, comm=MPI.COMM_SELF, policy=policy) as sim:
        for _ in range(300):
            sim.gate("RX", (1,), angle=rng.uniform(-3, 3)); sim.gate("H", (3,))
        snap = RemainingWorkExtractor(CaptureLimits(scan_items=4096, scan_seconds=1)).capture(sim)
        # 300 distinct angles would exceed the class limit; one shape class remains.
        assert snap.future_complete and not snap.future_reasons
        from svsim.replan_cost_producer import remaining_calls
        classes = [t for t in snap.future if t.kind == "executor-count"]
        assert len(classes) <= 4
        window = sum(1 for t in snap.window if t.kind == "executor")
        assert window + sum(t.count for t in classes) == remaining_calls(sim, sim.policy, 0, len(sim.sequencer.operations) - 1)
        assert any(op["angle"] == {"op": "pooled-angle"} for t in classes for op in t.condition["operations"])


def test_overflowed_tail_stays_incremental_and_exact(tmp_path):
    with make_sim(tmp_path / "overflow", swap_policy="S0", lookahead=2) as sim:
        for index in range(24):
            sim.gate("RX", (index % 4,), angle=0.01 * index)  # 24 distinct classes
        sim.materialize(frontier=1)
        extractor = RemainingWorkExtractor(CaptureLimits(max_groups=5, scan_items=4096, scan_seconds=1))
        first = extractor.capture(sim)
        assert first.complete and any(t.task_id == "tail:overflow" for t in first.tail)
        for frontier in (3, 9):
            sim.materialize(frontier=frontier)
            snap = extractor.capture(sim)
            assert snap.complete  # no rescan needed
            tail = {t.task_id: t.count for t in snap.tail}
            assert sum(tail.values()) == snap.scanned_sources == len(sim.sequencer.operations) - frontier - 3


def test_endpoint_stages_price_an_explicit_end_to_end_eta(tmp_path):
    from svsim.observables import ObservableIR, PauliTerm
    from svsim.parameters import ParameterSymbol
    from svsim.remaining_work import expectation_condition, gradient_condition, checkpoint_condition
    from svsim.runtime_observed_costs import _class
    config = DistributedConfig(str(tmp_path / "endpoint"), 2, ram_budget_bytes_per_rank=64 * 1024**2,
                               metadata_reserve_bytes=8 * 1024**2, amplitude_store="file")
    policy = AsyncPolicy(io_engine="cooperative", swap_policy="S2", lookahead_gates=4, prefetch_blocks=0)
    with AsyncDistributedSimulator(4, config, comm=MPI.COMM_SELF, policy=policy) as sim:
        sim.runtime.configure_measurements(max_bytes=2 * 1024**2)
        obs = ObservableIR((PauliTerm(0, 1), PauliTerm(0, 2)))
        sim.runtime.set_eta_endpoint(expectation=obs, checkpoint=True)
        for _ in range(14):  # Trotter-like steps: append a layer, run, measure, checkpoint
            _layers(sim, 1)
            sim.expectation(obs)
            sim.commit_snapshot()
        _layers(sim, 1)
        eta = sim.runtime.update_eta(endpoint_frontier=len(sim.sequencer.operations) - 1)
        assert eta["endpoint"] == "gates+expectation+checkpoint"
        observed = eta["views"]["observed"]
        rows = sim.runtime.measurements.cost_observations()
        # The endpoint task conditions are exactly the recorded stage classes.
        assert {_class(r.condition) for r in rows if r.stage == "expectation"} == {
            _class(expectation_condition(sim, obs))}
        checkpoint_rows = [r for r in rows if r.stage == "checkpoint-commit"]
        # Cost identity includes the pre-commit format-2 payload shape, so
        # changing source/history sizes cannot borrow the cost of another size.
        assert len({_class(r.condition) for r in checkpoint_rows}) > 1
        latest_shape = checkpoint_condition(sim)
        assert latest_shape['payload_format'] == 2
        assert latest_shape['gates_entries'] == len(sim.sequencer.operations)
        assert latest_shape['gates_pending_chunks'] >= 0
        assert all('history_pending_entries' in r.condition for r in checkpoint_rows)
        # A stage that is not certified (here: checkpoint cost grows with the history) leaves the total unknown.
        if observed["expected_seconds"] is None:
            assert any(r.startswith("MISSING_COST:") for r in observed["unknown_reasons"])
            assert "COST_UNCERTIFIED:INSUFFICIENT_SAMPLES:endpoint:checkpoint" in observed["unknown_reasons"]
        sim.runtime.set_eta_endpoint()
        assert sim.runtime.update_eta()["endpoint"] == "gates-only"
        # Gradient: the recorded stage class equals the endpoint condition for the same request.
        theta = ParameterSymbol("endpoint-theta", parameter_id="endpoint-theta")
        sim.gate("RY", (0,), angle=theta)
        sim.gradient(obs, binding=sim.bind({theta: 0.3}), method="adjoint")
        recorded = [r for r in sim.runtime.measurements.cost_observations() if r.stage == "gradient"]
        assert recorded and _class(recorded[-1].condition) == _class(
            gradient_condition(sim, obs, ["endpoint-theta"], "adjoint"))
