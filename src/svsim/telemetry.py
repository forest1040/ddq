"""Streaming execution telemetry and passive performance observation."""

from __future__ import annotations

import json
import math
import os
import uuid
from contextlib import nullcontext
from dataclasses import asdict
from pathlib import Path
from statistics import median
from typing import Any, Iterable, Iterator

from .lifecycle import (
    BenchmarkCondition, BenchmarkSample, HardwareProfile, HealthReport,
    PlanTemplate, RuntimeProfile, TraceEvent, _now,
)


def storage_condition(operation: str, block_bytes: int, *, backend: str,
                      device_id: str, pattern: str | None = None,
                      concurrency: int = 1, alignment: int | None = None,
                      warmup: int | None = None, flush: str | None = None) -> BenchmarkCondition:
    """Build a canonical matched condition for benchmark and runtime storage I/O."""
    operation = operation.lower()
    if operation not in {"read", "write"}: raise ValueError("operation must be read or write")
    if block_bytes < 0 or concurrency < 1: raise ValueError("invalid storage condition size or concurrency")
    return BenchmarkCondition(
        f"storage-{operation}", block_bytes=block_bytes, io_depth=concurrency,
        read_fraction=1.0 if operation == "read" else 0.0,
        details={"backend": backend, "device_id": device_id, "operation": operation,
                 "pattern": pattern, "concurrency": concurrency, "alignment": alignment,
                 "warmup": warmup, "flush": flush},
    )


def storage_device_id(path: str | os.PathLike[str]) -> str:
    """Identify the filesystem device, independent of a run's directory name."""
    return f"device-{os.stat(path).st_dev}"


class StreamingExecutionTrace:
    """Append each event directly to JSONL; memory use does not grow with event count."""

    def __init__(self, path: str | Path, *, profile_id: str, plan_id: str,
                 plan_version: int = 1, trace_id: str | None = None, version: int = 1,
                 disk_budget_getter=lambda:None, drain_getter=lambda:False) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.trace_id = trace_id or f"trace-{uuid.uuid4().hex}"
        self.version = version
        self.profile_id = profile_id
        self.plan_id = plan_id
        self.plan_version = plan_version
        self.schema_version = 1
        self.event_count = 0
        self.read_bytes = self.write_bytes = 0
        self.read_count = self.write_count = self.flush_count = 0
        self._disk_budget_getter,self._drain_getter=disk_budget_getter,drain_getter
        self._stream = self.path.open("a", encoding="utf-8")

    def record(self, task_id: str, **kwargs: Any) -> TraceEvent:
        """Write a lifecycle TraceEvent. Timing values are passed through as supplied."""
        values = dict(kwargs)
        timestamp = values.pop("timestamp", _now())
        plan_version = values.pop("plan_version", self.plan_version)
        event = TraceEvent(task_id=task_id, timestamp=timestamp,
                           plan_version=plan_version, **values)
        row = {"trace_id": self.trace_id, "trace_version": self.version,
               "profile_id": self.profile_id, "plan_id": self.plan_id,
               **asdict(event)}
        raw=json.dumps(row, separators=(",", ":"), default=_json_default) + "\n"
        nbytes=len(raw.encode('utf-8'))
        if nbytes>1024**2: raise ValueError('Trace record exceeds bounded 1 MiB envelope')
        budget=self._disk_budget_getter()
        with (budget.allocation(nbytes,drain=bool(self._drain_getter())) if budget else nullcontext()):
            self._stream.write(raw)
            # Release credit only after buffered bytes have reached the file.
            self._stream.flush()
        self.event_count += 1
        meta = event.metadata
        operation = str(meta.get("operation", "")).lower()
        nbytes = meta.get("transfer_bytes", event.bytes)
        if isinstance(nbytes, int) and nbytes >= 0:
            if operation == "read": self.read_count += 1; self.read_bytes += nbytes
            elif operation == "write": self.write_count += 1; self.write_bytes += nbytes
        if operation == "flush": self.flush_count += 1
        return event

    def iter_events(self) -> Iterator[dict[str, Any]]:
        if not self._stream.closed:
            self._stream.flush()
        with self.path.open("r", encoding="utf-8") as source:
            for line in source:
                if line.strip():
                    yield json.loads(line)

    def close(self) -> None:
        if not self._stream.closed:
            self._stream.flush()
            self._stream.close()

    def __enter__(self) -> StreamingExecutionTrace: return self
    def __exit__(self, *_: object) -> None: self.close()


def _json_default(value: Any) -> Any:
    if hasattr(value, "value"): return value.value
    raise TypeError(f"not JSON serializable: {type(value).__name__}")


class ObservePerformanceGuard:
    """Detect sustained matched-condition performance changes without changing plans."""

    def __init__(self, hardware_profile: HardwareProfile, plan: PlanTemplate, *,
                 window_size: int = 8, consecutive_windows: int = 3,
                 degradation_threshold: float = 0.8, recovery_threshold: float = 0.9) -> None:
        if window_size < 1 or consecutive_windows < 1: raise ValueError("window and persistence counts must be positive")
        if not 0 < degradation_threshold <= recovery_threshold <= 1:
            raise ValueError("thresholds must satisfy 0 < degradation <= recovery <= 1")
        self.hardware_profile, self.plan = hardware_profile, plan
        self.window_size, self.consecutive_windows = window_size, consecutive_windows
        self.degradation_threshold, self.recovery_threshold = degradation_threshold, recovery_threshold
        self._samples: dict[str, list[BenchmarkSample]] = {}
        self._bad: dict[str, int] = {}
        self._good: dict[str, int] = {}
        self._state: dict[str, str] = {}
        self._runtime_samples: dict[str, BenchmarkSample] = {}
        self._version = 1

    def observe(self, sample: BenchmarkSample) -> HealthReport:
        baseline = self.hardware_profile.matching_benchmark(sample.condition)
        if baseline is None:
            return self._report("INSUFFICIENT_BASELINE", sample, None, None, "No benchmark with matching load conditions")
        if sample.source != "measured" or sample.sample_count < 1:
            key = _condition_key(sample.condition)
            self._reset_pending(key)
            return self._report("INSUFFICIENT_METRICS", sample, baseline, None, "Sample is not a successful measurement")
        key = _condition_key(sample.condition)
        if not ((baseline.bandwidth_bytes_per_second is not None and sample.bandwidth_bytes_per_second is not None)
                or (baseline.latency_ms is not None and sample.latency_ms is not None)):
            self._reset_pending(key)
            return self._report("INSUFFICIENT_METRICS", sample, baseline, None,
                                "No observed metric matches an available baseline metric")
        bucket = self._samples.setdefault(key, [])
        bucket.append(sample)
        window_reports: list[HealthReport] = []
        while len(bucket) >= self.window_size:
            current = bucket[:self.window_size]
            del bucket[:self.window_size]
            ratios = []
            if baseline.bandwidth_bytes_per_second and all(x.bandwidth_bytes_per_second is not None for x in current):
                ratios.append(median(x.bandwidth_bytes_per_second for x in current if x.bandwidth_bytes_per_second is not None) / baseline.bandwidth_bytes_per_second)
            if baseline.latency_ms and all(x.latency_ms is not None for x in current):
                lat = median(x.latency_ms for x in current if x.latency_ms is not None)
                ratios.append(baseline.latency_ms / lat if lat else 1.0)
            ratio = min(ratios) if ratios else None
            state = self._state.get(key, "HEALTHY")
            if ratio is not None and ratio < self.degradation_threshold:
                self._bad[key] = self._bad.get(key, 0) + 1
                self._good[key] = 0
                if self._bad[key] >= self.consecutive_windows: state = "DEGRADED"
            elif ratio is not None and ratio >= self.recovery_threshold:
                self._good[key] = self._good.get(key, 0) + 1
                self._bad[key] = 0
                if self._good[key] >= self.consecutive_windows: state = "HEALTHY"
            else:
                # Neutral measurements break persistence in either direction.
                self._bad[key] = self._good[key] = 0
            self._state[key] = state
            aggregate = BenchmarkSample(sample.condition,
                bandwidth_bytes_per_second=_median_optional(x.bandwidth_bytes_per_second for x in current),
                latency_ms=_median_optional(x.latency_ms for x in current), sample_count=len(current))
            self._runtime_samples[key] = aggregate
            self._version += 1
            window_reports.append(self._report(state, aggregate, baseline, ratio, "Matched benchmark window", len(current)))
        if window_reports: return window_reports[-1]
        return self._report(self._state.get(key, "OBSERVING"), sample, baseline, None,
                            "Waiting for a complete observation window", len(bucket))

    @property
    def runtime_profile(self) -> RuntimeProfile:
        return RuntimeProfile(profile_id=f"{self.hardware_profile.profile_id}-runtime-v{self._version}",
            version=self._version, hardware_profile_id=self.hardware_profile.profile_id,
            created_at=_now(), samples=tuple(self._runtime_samples.values()),
            metadata={"plan_id": self.plan.plan_id, "plan_version": self.plan.version, "mode": "observe"})

    def _report(self, state: str, sample: BenchmarkSample, baseline: BenchmarkSample | None,
                ratio: float | None, reason: str, count: int | None = None) -> HealthReport:
        return HealthReport(state, self.hardware_profile.profile_id, self.plan.version, sample.condition,
            baseline.bandwidth_bytes_per_second if baseline else None, sample.bandwidth_bytes_per_second,
            baseline.latency_ms if baseline else None, sample.latency_ms, ratio,
            bottleneck=sample.condition.performance_class if state == "DEGRADED" else None,
            reason=reason, sample_count=count if count is not None else sample.sample_count)

    def _reset_pending(self, key: str | None = None) -> None:
        keys = (key,) if key is not None else tuple(self._samples)
        for item in keys:
            self._samples.pop(item, None)
            self._bad.pop(item, None)
            self._good.pop(item, None)


def _condition_key(condition: BenchmarkCondition) -> str:
    return json.dumps(asdict(condition), sort_keys=True, separators=(",", ":"))


def _median_optional(values: Iterable[float | None]) -> float | None:
    present = [v for v in values if v is not None]
    return median(present) if present else None
