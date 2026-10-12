"""Bounded, reproducible RAM and storage microbenchmarks."""

from __future__ import annotations

import os
import math
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Sequence

from .lifecycle import BenchmarkCondition, BenchmarkSample, HardwareProfile, HardwareProfiler
from .telemetry import storage_condition, storage_device_id
from .transport import StorageFile
from .cache import BufferLease, MemoryBudget


@dataclass(frozen=True)
class BenchmarkReport:
    profile: HardwareProfile
    scratch_path: str
    buffer_bytes: int
    sample_count: int
    warmup_count: int
    elapsed_seconds: float
    measurements: tuple[BenchmarkSample, ...]


class MockTransport:
    """StorageFile adapter with deterministic virtual timing and no real sleeps."""

    def __init__(self, storage: StorageFile, *, bytes_per_second: float = 100_000_000,
                 fixed_latency_seconds: float = 0.0, delay_multipliers: Sequence[float] = ()) -> None:
        if not math.isfinite(bytes_per_second) or bytes_per_second <= 0 or not math.isfinite(fixed_latency_seconds) or fixed_latency_seconds < 0:
            raise ValueError("invalid mock timing")
        if any(not math.isfinite(x) or x < 0 for x in delay_multipliers): raise ValueError("delay multipliers must be finite and non-negative")
        self.storage = storage
        self.bytes_per_second = bytes_per_second
        self.fixed_latency_seconds = fixed_latency_seconds
        self.delay_multipliers = tuple(delay_multipliers)
        self._operation_index = 0
        self.virtual_elapsed_seconds = 0.0
        self.last_duration_seconds = 0.0

    def _duration(self, nbytes: int) -> None:
        multiplier = self.delay_multipliers[min(self._operation_index, len(self.delay_multipliers)-1)] if self.delay_multipliers else 1.0
        self._operation_index += 1
        self.last_duration_seconds = self.fixed_latency_seconds + nbytes / self.bytes_per_second * multiplier
        self.virtual_elapsed_seconds += self.last_duration_seconds

    def read(self, offset: int, size: int) -> bytes:
        value = self.storage.read(offset, size); self._duration(len(value)); return value

    def read_into(self, offset: int, target: bytearray | memoryview) -> int:
        count = self.storage.read_into(offset, target)
        self._duration(count)
        return count

    def write(self, offset: int, data: bytes | bytearray | memoryview) -> int:
        count = self.storage.write(offset, data); self._duration(count); return count

    def write_from(self, offset: int, source: bytes | bytearray | memoryview) -> int:
        count = self.storage.write_from(offset, source)
        self._duration(count)
        return count

    def flush(self) -> None:
        self.storage.flush(); self._duration(0)

    @property
    def backend_identity(self) -> str: return self.storage.backend_identity

    def __getattr__(self, name: str) -> Any: return getattr(self.storage, name)


def run_benchmarks(directory: str | os.PathLike[str], *, block_bytes: int = 64 * 1024,
                   samples: int = 3, backend: str = "buffered", alignment: int = 4096,
                   ram_copy_bytes: int | None = None, warmup: int = 1) -> BenchmarkReport:
    """Measure bounded RAM copies and sequential/random storage I/O.

    The scratch file is uniquely named, created beneath ``directory``, and removed
    before return. Sample values reflect actual monotonic wall time.
    """
    if samples < 1 or warmup < 0: raise ValueError("samples must be positive and warmup non-negative")
    if block_bytes < 1: raise ValueError("block_bytes must be positive")
    if alignment < 1 or alignment & (alignment-1): raise ValueError('Invalid alignment')
    if backend == "direct": block_bytes = ((block_bytes + alignment - 1) // alignment) * alignment
    root = Path(directory); root.mkdir(parents=True, exist_ok=True)
    profile = HardwareProfiler([root]).discover()
    copy_bytes = min(block_bytes, 1024 * 1024) if ram_copy_bytes is None else ram_copy_bytes
    if copy_bytes < 0: raise ValueError("ram_copy_bytes must be non-negative")
    src, dst = bytearray(copy_bytes), bytearray(copy_bytes)
    measured: list[BenchmarkSample] = []
    started = time.monotonic()
    def record(name: str, operation: str, pattern: str, nbytes: int, fn: Any) -> None:
        if name == "ram_copy":
            condition = BenchmarkCondition(name, block_bytes=nbytes, io_depth=1,
                details={"backend": "ram", "operation": operation, "pattern": pattern,
                         "warmup": warmup, "concurrency": 1})
        else:
            condition = storage_condition(operation, nbytes, backend=backend_identity,
                device_id=storage_device_id(root), pattern=pattern, alignment=alignment,
                warmup=None, flush="deferred", concurrency=1)
        for _ in range(warmup): fn()
        durations = []
        for _ in range(samples):
            begin = time.monotonic(); fn(); durations.append(max(time.monotonic() - begin, 1e-12))
        total = sum(durations)
        measured.append(BenchmarkSample(condition,
            bandwidth_bytes_per_second=(nbytes * samples / total if nbytes else None),
            latency_ms=total * 1000 / samples, sample_count=samples,
            source="measured"))

    try:
        record("ram_copy", "copy", "sequential", copy_bytes, lambda: dst.__setitem__(slice(None), src))
        fd, raw_path = tempfile.mkstemp(prefix="svsim-bench-", suffix=".bin", dir=root)
        os.close(fd)
        path = Path(raw_path)
        file_size = block_bytes * 4
        io_buffer = BufferLease(MemoryBudget(BufferLease.allocation_cost(block_bytes,alignment)),block_bytes,alignment)
        with StorageFile(path, "w+b", backend=backend, alignment=alignment) as store:
            backend_identity = store.backend_identity
            for offset in range(0, file_size, block_bytes): store.write_from(offset,io_buffer.view)
            store.flush()
        with StorageFile(path, "r+b", backend=backend, alignment=alignment) as store:
            seq_offsets = [0, block_bytes, block_bytes * 2, block_bytes * 3]
            rand_offsets = [block_bytes * 2, 0, block_bytes * 3, block_bytes]
            for op in ("read", "write"):
                for pattern, offsets in (("sequential", seq_offsets), ("random", rand_offsets)):
                    index = [0]
                    def io_once(op: str = op, offsets: list[int] = offsets, index: list[int] = index) -> None:
                        offset = offsets[index[0] % len(offsets)]; index[0] += 1
                        count = store.read_into(offset,io_buffer.view) if op == 'read' else store.write_from(offset,io_buffer.view)
                        if count != block_bytes: raise OSError('Incomplete benchmark transfer')
                    record(f"storage_{op}", op, pattern, block_bytes, io_once)
            # Keep transfer measurements separate from durability sync cost.
            store.flush()
        path.unlink(missing_ok=True)
    finally:
        if 'io_buffer' in locals(): io_buffer.close()
        if 'path' in locals(): path.unlink(missing_ok=True)
    hardware = HardwareProfile(**{**profile.__dict__, "benchmarks": tuple(measured), "version": profile.version + 1})
    return BenchmarkReport(hardware, str(root), 2*copy_bytes+BufferLease.allocation_cost(block_bytes,alignment), samples,
                           warmup, time.monotonic() - started, tuple(measured))


benchmark = run_benchmarks
