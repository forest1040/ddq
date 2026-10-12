"""Small, versioned schemas for hardware discovery, planning, and runtime health.

This module deliberately stops at observation and recommendation. It does not
change scheduler settings or commit a replacement execution plan.
"""

from __future__ import annotations

import os
import platform
import shutil
import socket
import subprocess
import sys
import math
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Iterable


SCHEMA_VERSION = 1


class MonitorMode(str, Enum):
    OBSERVE = "observe"
    RECOMMEND = "recommend"
    AUTO = "auto"


class ChangeCategory(str, Enum):
    LIVE_TUNE = "LIVE_TUNE"
    FRONTIER_REPLAN = "FRONTIER_REPLAN"
    RESTART_REQUIRED = "RESTART_REQUIRED"


@dataclass(frozen=True)
class Device:
    device_id: str
    kind: str
    path: str | None = None
    capacity_bytes: int | None = None
    free_bytes: int | None = None
    filesystem: str | None = None
    details: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class BenchmarkCondition:
    """Dimensions that must match before a sample is compared to a baseline."""

    performance_class: str
    block_bytes: int | None = None
    io_depth: int | None = None
    read_fraction: float | None = None
    mpi_concurrent: bool = False
    ranks: int = 1
    details: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.block_bytes is not None and self.block_bytes < 0:
            raise ValueError("block_bytes must be non-negative")
        if self.io_depth is not None and self.io_depth < 1:
            raise ValueError("io_depth must be positive")
        if self.read_fraction is not None and not 0 <= self.read_fraction <= 1:
            raise ValueError("read_fraction must be between zero and one")
        if self.ranks < 1:
            raise ValueError("ranks must be positive")


@dataclass(frozen=True)
class BenchmarkSample:
    condition: BenchmarkCondition
    bandwidth_bytes_per_second: float | None = None
    latency_ms: float | None = None
    measured_at: str = field(default_factory=lambda: _now())
    sample_count: int = 1
    confidence_interval: tuple[float, float] | None = None
    source: str = "measured"

    def __post_init__(self) -> None:
        for name, value in (("bandwidth", self.bandwidth_bytes_per_second), ("latency", self.latency_ms)):
            if value is not None and (not math.isfinite(value) or value < 0):
                raise ValueError(f"{name} must be finite and non-negative")
        if self.sample_count < 1:
            raise ValueError("sample_count must be positive")
        if self.confidence_interval is not None:
            low, high = self.confidence_interval
            if not math.isfinite(low) or not math.isfinite(high) or low > high:
                raise ValueError("confidence_interval must have finite, ordered bounds")


@dataclass(frozen=True)
class HardwareProfile:
    profile_id: str
    version: int
    created_at: str
    hostname: str
    os_name: str
    os_release: str
    python_version: str
    cpu_count: int | None
    cpu_model: str | None
    ram_total_bytes: int | None
    ram_available_bytes: int | None
    devices: tuple[Device, ...] = ()
    network_interfaces: tuple[str, ...] = ()
    benchmarks: tuple[BenchmarkSample, ...] = ()
    configuration_hash: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def matching_benchmark(self, condition: BenchmarkCondition) -> BenchmarkSample | None:
        return next((sample for sample in self.benchmarks if sample.condition == condition), None)


@dataclass(frozen=True)
class PlanTemplate:
    plan_id: str
    version: int
    circuit_hash: str
    profile_id: str
    layout_id: str | None = None
    frontier: str | int | None = None
    task_dependencies: tuple[tuple[str, tuple[str, ...]], ...] = ()
    expected_conditions: tuple[BenchmarkCondition, ...] = ()
    alternatives: tuple[str, ...] = ()
    replanning_frontiers: tuple[str | int, ...] = ()
    created_at: str = field(default_factory=lambda: _now())
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class RuntimeProfile:
    profile_id: str
    version: int
    hardware_profile_id: str
    created_at: str
    samples: tuple[BenchmarkSample, ...] = ()
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class HealthReport:
    health_state: str
    profile_id: str
    plan_version: int
    condition: BenchmarkCondition | None
    expected_bandwidth: float | None
    observed_bandwidth: float | None
    expected_latency_ms: float | None = None
    observed_latency_ms: float | None = None
    degradation_ratio: float | None = None
    bottleneck: str | None = None
    reason: str = ""
    sample_count: int = 0
    created_at: str = field(default_factory=lambda: _now())


@dataclass(frozen=True)
class PlanCandidate:
    candidate_id: str
    source_plan_id: str
    source_plan_version: int
    proposed_plan_version: int
    category: ChangeCategory
    reason: str
    estimated_remaining_seconds: float | None = None
    estimated_switch_cost_seconds: float = 0.0
    safe_frontier: str | int | None = None
    actions: tuple[str, ...] = ()
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class TraceEvent:
    task_id: str
    timestamp: str
    rank: int | None = None
    tier: str | None = None
    device_id: str | None = None
    peer_rank: int | None = None
    bytes: int | None = None
    queue_wait_ms: float | None = None
    transfer_ms: float | None = None
    latency_p95: float | None = None
    performance_class: str | None = None
    expected_bandwidth: float | None = None
    observed_bandwidth: float | None = None
    health_state: str | None = None
    bottleneck: str | None = None
    candidate_plan_id: str | None = None
    plan_version: int = 1
    decision_reason: str | None = None
    estimated_remaining_seconds: float | None = None
    actual_switch_cost: float | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class ExecutionTrace:
    trace_id: str
    version: int
    profile_id: str
    plan_id: str
    plan_version: int = 1
    schema_version: int = SCHEMA_VERSION
    events: list[TraceEvent] = field(default_factory=list)

    def record(self, task_id: str, **values: Any) -> TraceEvent:
        event_version = values.pop("plan_version", self.plan_version)
        event = TraceEvent(task_id=task_id, timestamp=_now(), plan_version=event_version, **values)
        self.events.append(event)
        return event


class HardwareProfiler:
    """Portable, read-only discovery of basic CPU, memory, disk, and NIC facts."""

    def __init__(self, storage_paths: Iterable[str | os.PathLike[str]] = ("/",)) -> None:
        self.storage_paths = tuple(Path(p) for p in storage_paths)

    def discover(self) -> HardwareProfile:
        total, available = _memory_bytes()
        devices: list[Device] = []
        for path in self.storage_paths:
            try:
                usage = shutil.disk_usage(path)
                devices.append(Device(
                    device_id=str(path), kind="disk", path=str(path),
                    capacity_bytes=usage.total, free_bytes=usage.free,
                    details={"used_bytes": usage.used},
                ))
            except OSError:
                continue
        try:
            interfaces = tuple(name for _, name in socket.if_nameindex())
        except (AttributeError, OSError):
            interfaces = ()
        return HardwareProfile(
            profile_id=f"hw-{_now().replace(':', '').replace('-', '')}",
            version=1,
            created_at=_now(),
            hostname=socket.gethostname(),
            os_name=platform.system(),
            os_release=platform.release(),
            python_version=sys.version.split()[0],
            cpu_count=os.cpu_count(),
            cpu_model=_cpu_model(),
            ram_total_bytes=total,
            ram_available_bytes=available,
            devices=tuple(devices),
            network_interfaces=interfaces,
            metadata={"machine": platform.machine(), "processor": platform.processor()},
        )


class StaticCircuitOptimizer:
    """Creates an immutable plan schema; circuit analysis is supplied by callers."""

    def create_plan(
        self, circuit_hash: str, profile: HardwareProfile, *, plan_id: str,
        layout_id: str | None = None, frontier: str | int | None = None,
        task_dependencies: Iterable[tuple[str, Iterable[str]]] = (),
        expected_conditions: Iterable[BenchmarkCondition] = (),
        alternatives: Iterable[str] = (), replanning_frontiers: Iterable[str | int] = (),
    ) -> PlanTemplate:
        return PlanTemplate(
            plan_id=plan_id, version=1, circuit_hash=circuit_hash,
            profile_id=profile.profile_id, layout_id=layout_id, frontier=frontier,
            task_dependencies=tuple((task, tuple(deps)) for task, deps in task_dependencies),
            expected_conditions=tuple(expected_conditions), alternatives=tuple(alternatives),
            replanning_frontiers=tuple(replanning_frontiers),
        )


class AdaptiveRuntimeScheduler:
    """Compares matched telemetry and returns recommendations without applying them."""

    def __init__(self, mode: MonitorMode = MonitorMode.RECOMMEND, degradation_threshold: float = 0.8) -> None:
        if not 0 < degradation_threshold <= 1:
            raise ValueError("degradation_threshold must be in (0, 1]")
        self.mode = MonitorMode(mode)
        self.degradation_threshold = degradation_threshold

    def evaluate(
        self, hardware_profile: HardwareProfile, plan: PlanTemplate,
        observed: BenchmarkSample,
    ) -> HealthReport:
        baseline = hardware_profile.matching_benchmark(observed.condition)
        if baseline is None:
            return HealthReport("INSUFFICIENT_BASELINE", hardware_profile.profile_id, plan.version,
                                observed.condition, None, observed.bandwidth_bytes_per_second,
                                reason="No benchmark with matching load conditions", sample_count=observed.sample_count)
        expected_bw, actual_bw = baseline.bandwidth_bytes_per_second, observed.bandwidth_bytes_per_second
        expected_lat, actual_lat = baseline.latency_ms, observed.latency_ms
        ratios: list[float] = []
        if expected_bw and actual_bw is not None:
            ratios.append(actual_bw / expected_bw)
        if expected_lat and actual_lat is not None:
            ratios.append(expected_lat / actual_lat if actual_lat > 0 else 1.0)
        if not ratios:
            state, ratio = "INSUFFICIENT_METRICS", None
        else:
            ratio = min(ratios)
            state = "DEGRADED" if ratio < self.degradation_threshold else "HEALTHY"
        return HealthReport(state, hardware_profile.profile_id, plan.version, observed.condition,
                            expected_bw, actual_bw, expected_lat, actual_lat, ratio,
                            bottleneck=observed.condition.performance_class if state == "DEGRADED" else None,
                            reason="Matched baseline comparison", sample_count=observed.sample_count)

    def recommend(self, plan: PlanTemplate, report: HealthReport, *, safe_frontier: str | int | None = None,
                  estimated_remaining_seconds: float | None = None,
                  estimated_switch_cost_seconds: float = 0.0) -> tuple[PlanCandidate, ...]:
        if self.mode == MonitorMode.OBSERVE or report.health_state != "DEGRADED":
            return ()
        candidates = [PlanCandidate(
            candidate_id=f"{plan.plan_id}-v{plan.version + 1}-live",
            source_plan_id=plan.plan_id, source_plan_version=plan.version,
            proposed_plan_version=plan.version, category=ChangeCategory.LIVE_TUNE,
            reason=report.reason, estimated_remaining_seconds=estimated_remaining_seconds,
            actions=("adjust_io_depth", "adjust_prefetch", "adjust_background_concurrency"),
        )]
        if safe_frontier is not None and safe_frontier in plan.replanning_frontiers:
            candidates.append(PlanCandidate(
                candidate_id=f"{plan.plan_id}-v{plan.version + 1}-frontier",
                source_plan_id=plan.plan_id, source_plan_version=plan.version,
                proposed_plan_version=plan.version + 1, category=ChangeCategory.FRONTIER_REPLAN,
                reason=report.reason, estimated_remaining_seconds=estimated_remaining_seconds,
                estimated_switch_cost_seconds=estimated_switch_cost_seconds,
                safe_frontier=safe_frontier, actions=plan.alternatives,
            ))
        return tuple(candidates)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _memory_bytes() -> tuple[int | None, int | None]:
    # Linux exposes both physical and currently available memory here.
    try:
        values: dict[str, int] = {}
        with open("/proc/meminfo", encoding="ascii") as stream:
            for line in stream:
                key, raw = line.split(":", 1)
                if key in {"MemTotal", "MemAvailable"}:
                    values[key] = int(raw.strip().split()[0]) * 1024
        if values:
            return values.get("MemTotal"), values.get("MemAvailable")
    except (OSError, ValueError):
        pass
    try:
        page = os.sysconf("SC_PAGE_SIZE")
        return int(page * os.sysconf("SC_PHYS_PAGES")), int(page * os.sysconf("SC_AVPHYS_PAGES"))
    except (AttributeError, OSError, ValueError):
        pass
    if sys.platform == "darwin":
        try:
            result = subprocess.run(
                ["sysctl", "-n", "hw.memsize"], check=True, capture_output=True,
                text=True, timeout=2,
            )
            return int(result.stdout.strip()), None
        except (OSError, subprocess.SubprocessError, ValueError):
            pass
    return None, None


def _cpu_model() -> str | None:
    if platform.processor():
        return platform.processor()
    if sys.platform.startswith("linux"):
        try:
            with open("/proc/cpuinfo", encoding="utf-8", errors="replace") as stream:
                for line in stream:
                    if line.lower().startswith(("model name", "hardware")):
                        return line.split(":", 1)[1].strip()
        except OSError:
            pass
    return None
