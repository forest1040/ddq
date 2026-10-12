import json

from svsim.lifecycle import BenchmarkCondition, BenchmarkSample, HardwareProfile, PlanTemplate
from svsim.telemetry import ObservePerformanceGuard, StreamingExecutionTrace


def _profile(condition):
    baseline = BenchmarkSample(condition, bandwidth_bytes_per_second=1000, latency_ms=10)
    return HardwareProfile("hw", 1, "now", "host", "test", "1", "3.12", 1, None,
        1000, 500, benchmarks=(baseline,))


def test_streaming_trace_appends_and_iterates_without_event_list(tmp_path):
    path = tmp_path / "trace.jsonl"
    trace = StreamingExecutionTrace(path, profile_id="hw", plan_id="plan", plan_version=3)
    ev = trace.record("task-a", bytes=32, metadata={"operation": "read", "transfer_bytes": 32})
    trace.record("task-b", plan_version=4, metadata={"operation": "flush"})
    assert ev.plan_version == 3
    assert trace.read_count == 1 and trace.read_bytes == 32 and trace.flush_count == 1
    assert not hasattr(trace, "events")
    trace.close()
    rows = list(trace.iter_events())
    assert [r["task_id"] for r in rows] == ["task-a", "task-b"]
    assert rows[1]["plan_version"] == 4 and rows[0]["profile_id"] == "hw"
    assert all(isinstance(json.loads(line), dict) for line in path.read_text().splitlines())


def test_guard_requires_persistent_matched_windows_and_tracks_runtime_profile():
    condition = BenchmarkCondition("ssd_read", block_bytes=4096, io_depth=1)
    profile = _profile(condition)
    plan = PlanTemplate("p", 7, "circuit", "hw")
    guard = ObservePerformanceGuard(profile, plan, window_size=2, consecutive_windows=2)
    degraded = BenchmarkSample(condition, bandwidth_bytes_per_second=500)
    assert guard.observe(degraded).health_state == "OBSERVING"
    assert guard.observe(degraded).health_state == "HEALTHY"
    assert guard.observe(degraded).health_state == "HEALTHY"
    report = guard.observe(degraded)
    assert report.health_state == "DEGRADED"
    assert report.plan_version == 7 and report.degradation_ratio == 0.5
    assert guard.runtime_profile.hardware_profile_id == "hw"
    assert guard.runtime_profile.version == 3
    assert len(guard.runtime_profile.samples) == 1


def test_guard_ignores_unmatched_conditions():
    condition = BenchmarkCondition("ssd_read", block_bytes=4096)
    guard = ObservePerformanceGuard(_profile(condition), PlanTemplate("p", 1, "c", "hw"))
    report = guard.observe(BenchmarkSample(BenchmarkCondition("ssd_read", block_bytes=8192), bandwidth_bytes_per_second=1))
    assert report.health_state == "INSUFFICIENT_BASELINE"
    assert not guard.runtime_profile.samples


def test_guard_neutral_and_insufficient_windows_reset_persistence():
    condition = BenchmarkCondition("ssd_read", block_bytes=4096, io_depth=1)
    guard = ObservePerformanceGuard(_profile(condition), PlanTemplate("p", 1, "c", "hw"),
                                   window_size=1, consecutive_windows=2)
    poor = BenchmarkSample(condition, bandwidth_bytes_per_second=500)
    healthy = BenchmarkSample(condition, bandwidth_bytes_per_second=1000)
    assert guard.observe(poor).health_state == "HEALTHY"
    assert guard.observe(healthy).health_state == "HEALTHY"
    assert guard.observe(poor).health_state == "HEALTHY"
    assert guard.observe(poor).health_state == "DEGRADED"
    insufficient = BenchmarkSample(condition)
    assert guard.observe(insufficient).health_state == "INSUFFICIENT_METRICS"
    assert len(guard.runtime_profile.samples) == 1
