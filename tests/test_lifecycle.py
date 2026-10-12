from dataclasses import FrozenInstanceError
import socket

import pytest

from svsim.lifecycle import (
    AdaptiveRuntimeScheduler,
    BenchmarkCondition,
    BenchmarkSample,
    ChangeCategory,
    ExecutionTrace,
    HardwareProfile,
    HardwareProfiler,
    MonitorMode,
    StaticCircuitOptimizer,
)


def profile_with_baseline(condition):
    return HardwareProfile(
        profile_id="profile-a", version=1, created_at="now", hostname="host",
        os_name="Linux", os_release="test", python_version="3.12", cpu_count=8,
        cpu_model="test cpu", ram_total_bytes=1000, ram_available_bytes=500,
        benchmarks=(BenchmarkSample(condition, bandwidth_bytes_per_second=1000, latency_ms=2),),
    )


def test_hardware_profiler_discovers_portable_basics(tmp_path):
    profile = HardwareProfiler([tmp_path]).discover()
    assert profile.version == 1
    assert profile.hostname
    assert profile.cpu_count and profile.cpu_count > 0
    assert profile.ram_total_bytes is None or profile.ram_total_bytes > 0
    assert profile.devices[0].capacity_bytes == 0 or profile.devices[0].capacity_bytes > 0
    assert profile.network_interfaces == tuple(name for _, name in socket.if_nameindex())


def test_static_plan_and_trace_preserve_versions_and_conditions():
    condition = BenchmarkCondition("ssd_read", block_bytes=4096, io_depth=2)
    profile = profile_with_baseline(condition)
    plan = StaticCircuitOptimizer().create_plan(
        "circuit-hash", profile, plan_id="plan-a", alternatives=("path-b",),
        replanning_frontiers=("task-4",), expected_conditions=(condition,),
    )
    assert plan.profile_id == profile.profile_id
    assert plan.expected_conditions == (condition,)
    with pytest.raises(FrozenInstanceError):
        plan.version = 3

    trace = ExecutionTrace("trace-a", 1, profile.profile_id, plan.plan_id, plan_version=plan.version)
    event = trace.record("read-1", bytes=4096, observed_bandwidth=800)
    assert event.plan_version == 1
    assert trace.schema_version == 1


def test_guard_matches_load_conditions_and_recommends_without_applying():
    condition = BenchmarkCondition("ssd_read", block_bytes=4096, io_depth=2)
    profile = profile_with_baseline(condition)
    plan = StaticCircuitOptimizer().create_plan(
        "circuit-hash", profile, plan_id="plan-a", alternatives=("path-b",),
        replanning_frontiers=("task-4",),
    )
    scheduler = AdaptiveRuntimeScheduler(MonitorMode.RECOMMEND, degradation_threshold=0.8)
    report = scheduler.evaluate(profile, plan, BenchmarkSample(condition, bandwidth_bytes_per_second=500))
    assert report.health_state == "DEGRADED"
    candidates = scheduler.recommend(plan, report, safe_frontier="task-4")
    assert [candidate.category for candidate in candidates] == [
        ChangeCategory.LIVE_TUNE, ChangeCategory.FRONTIER_REPLAN,
    ]
    assert plan.version == 1  # proposals do not commit or mutate the plan


def test_guard_does_not_compare_mismatched_load_and_observe_mode_is_quiet():
    baseline_condition = BenchmarkCondition("ssd_read", block_bytes=4096, io_depth=1)
    observed_condition = BenchmarkCondition("ssd_read", block_bytes=4096, io_depth=8)
    profile = profile_with_baseline(baseline_condition)
    plan = StaticCircuitOptimizer().create_plan("circuit", profile, plan_id="plan")
    scheduler = AdaptiveRuntimeScheduler(MonitorMode.OBSERVE)
    report = scheduler.evaluate(profile, plan, BenchmarkSample(observed_condition, bandwidth_bytes_per_second=1))
    assert report.health_state == "INSUFFICIENT_BASELINE"
    assert scheduler.recommend(plan, report) == ()


def test_benchmark_condition_validates_ranges():
    with pytest.raises(ValueError):
        BenchmarkCondition("mpi", read_fraction=1.5)


@pytest.mark.parametrize("value", [float("nan"), float("inf"), float("-inf")])
def test_benchmark_sample_rejects_non_finite_metrics(value):
    with pytest.raises(ValueError):
        BenchmarkSample(BenchmarkCondition("ssd"), bandwidth_bytes_per_second=value)


def test_benchmark_sample_rejects_invalid_confidence_interval():
    with pytest.raises(ValueError):
        BenchmarkSample(BenchmarkCondition("ssd"), confidence_interval=(2.0, 1.0))
