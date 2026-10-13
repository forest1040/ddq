"""Small, explicitly budgeted same-host calibration campaigns."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import math
import os
from pathlib import Path
import shutil
import statistics
import time
import uuid

import numpy as np

from .calibration_conditions import condition_key, kernel_condition, storage_condition
from .ir import GateNormalizer
from .pauli_rotation import SignedPauli, apply_pauli_rotation
from .profile_document import ProfileDocument
from .telemetry import storage_device_id
from .streaming_kernels import apply_local
from .transport import StorageFile
from .distributed_kernels import pauli_inner
from .kernels import eager_reference
from .cache import BufferLease, MemoryBudget


@dataclass(frozen=True)
class BenchmarkOptions:
    block_bits: int = 12
    repetitions: int = 5
    warmup: int = 1
    ram_budget_bytes: int = 256 * 1024 * 1024
    disk_budget_bytes: int = 16 * 1024 * 1024
    walltime_seconds: float = 30.0
    backend: str = "buffered"
    suites: tuple[str, ...] = ("ram", "kernel", "storage", "mixed")
    seed: int = 0

    def __post_init__(self):
        for name in ("block_bits", "repetitions", "warmup", "ram_budget_bytes", "disk_budget_bytes", "seed"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int):
                raise ValueError(f"{name} must be an integer")
        if not 1 <= self.block_bits <= 20 or not 1 <= self.repetitions <= 1000 or not 0 <= self.warmup <= 1000:
            raise ValueError("invalid block size or trial counts")
        if self.ram_budget_bytes < 1 or self.disk_budget_bytes < 1:
            raise ValueError("RAM and disk budgets must be positive")
        if not math.isfinite(self.walltime_seconds) or self.walltime_seconds <= 0:
            raise ValueError("walltime_seconds must be positive and finite")
        if self.backend not in {"buffered", "direct"}:
            raise ValueError("backend must be buffered or direct")
        if not self.suites or len(set(self.suites)) != len(self.suites):
            raise ValueError("suites must be non-empty and unique")
        if set(self.suites) - {"ram", "kernel", "storage", "mixed"}:
            raise ValueError("unknown benchmark suite")
        if "mixed" in self.suites and not {"kernel", "storage"}.issubset(self.suites):
            raise ValueError("mixed suite requires both kernel and storage suites")
        if not 0 <= self.seed <= (1 << 64) - 1:
            raise ValueError("seed must fit in an unsigned 64-bit integer")


def _timestamp():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _record(operation, condition, durations, *, warmup, nbytes, provenance):
    vals = sorted(item[0] for item in durations)
    p95_index = (len(vals) - 1) * 0.95
    low_index, high_index = math.floor(p95_index), math.ceil(p95_index)
    p95 = vals[low_index] + (vals[high_index] - vals[low_index]) * (p95_index - low_index)
    return {"operation": operation, "condition": condition, "condition_key": condition_key(condition),
            "evidence": "measured", "scope": "same-host", "quality": "validated", "units": "calls",
            "summary": {"median_seconds": statistics.median(vals), "mean_seconds": statistics.mean(vals),
                        "min_seconds": vals[0], "max_seconds": vals[-1],
                        "p95_seconds": p95},
            "trials": [{"wall_seconds": value, "cpu_seconds": cpu, "bytes": nbytes,
                        "count": 1, "timestamp": stamp}
                       for value, cpu, stamp in durations],
            "timestamp": _timestamp(), "warmup_count": warmup, "sample_count": len(durations),
            "provenance": provenance}


def _assert_close(actual, expected, message):
    if not np.allclose(actual, expected, rtol=1e-12, atol=1e-12):
        raise ArithmeticError(message)


def _assert_scalar_close(actual, expected, message):
    if not np.isclose(actual, expected, rtol=1e-12, atol=1e-12):
        raise ArithmeticError(message)


def _assert_bytes(actual, expected, message):
    if bytes(actual) != expected:
        raise ArithmeticError(message)


def _dense_pauli_inner(state, x_mask, z_mask, block_bits):
    """Independent source-index oracle for a Pauli expectation."""
    phase_y = 1j ** (x_mask & z_mask).bit_count()
    low_x = x_mask & ((1 << block_bits) - 1)
    total = 0j
    for i, value in enumerate(state):
        phase_z = -1 if (z_mask & i).bit_count() & 1 else 1
        total += np.conjugate(state[i ^ low_x]) * phase_y * phase_z * value
    return total


def run_calibration(profile: ProfileDocument, scratch: Path, options: BenchmarkOptions) -> ProfileDocument:
    """Run bounded local RAM/kernel/storage probes and return a new profile."""
    if not isinstance(profile, ProfileDocument):
        raise TypeError("profile must be a ProfileDocument")
    root = Path(scratch).expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    block_bytes = 16 * (1 << options.block_bits)
    # Include every suite's state/work arrays and storage buffers in admission.
    ram_need = 6 * block_bytes + (1 << options.block_bits) * 32 + 65536
    case_count = (int("ram" in options.suites) +
                  (5 if "kernel" in options.suites else 0) +
                  (3 if "storage" in options.suites else 0) +
                  int("mixed" in options.suites))
    # Reserve conservatively for raw trial dictionaries, timestamps, summaries, and list growth.
    raw_sample_reserve = case_count * options.repetitions * 512
    total_ram_need = ram_need + raw_sample_reserve
    if total_ram_need > options.ram_budget_bytes:
        raise ValueError(f"RAM preflight needs {total_ram_need} bytes including raw trials, budget is {options.ram_budget_bytes}")
    alignment = 4096
    disk_need = ((block_bytes + alignment - 1) // alignment) * alignment if options.backend == "direct" else block_bytes
    if "storage" in options.suites and disk_need > options.disk_budget_bytes:
        raise ValueError(f"storage preflight needs {disk_need} bytes, budget is {options.disk_budget_bytes}")
    job = root / f"svbench-{uuid.uuid4().hex}"
    job.mkdir(mode=0o700)
    deadline = time.monotonic() + options.walltime_seconds
    rng = np.random.default_rng(options.seed)
    data = profile.data
    records = list(profile.calibrations)
    original_record_count = len(records)
    omitted = []
    cpu_before = os.times()
    load_before = os.getloadavg() if hasattr(os, "getloadavg") else None
    backend_id = options.backend

    def measure(op, condition, fn, nbytes, prov, *, prepare=None, validate=None):
        durations = []
        for _ in range(options.warmup):
            if time.monotonic() >= deadline:
                omitted.append({"operation": op, "reason": "walltime deadline before warmup"})
                return
            if prepare: prepare()
            if time.monotonic() >= deadline:
                omitted.append({"operation": op, "reason": "walltime deadline during warmup preparation"})
                return
            result = fn()
            if validate: validate(result)
            if time.monotonic() >= deadline:
                omitted.append({"operation": op, "reason": "walltime deadline exceeded during warmup"})
                return
        for _ in range(options.repetitions):
            if time.monotonic() >= deadline:
                omitted.append({"operation": op, "reason": "walltime deadline before trial"})
                return
            if prepare: prepare()
            if time.monotonic() >= deadline:
                omitted.append({"operation": op, "reason": "walltime deadline during trial preparation"})
                return
            c0, t0 = time.process_time(), time.perf_counter()
            result = fn()
            elapsed = max(time.perf_counter()-t0, 1e-12)
            cpu = max(time.process_time()-c0, 0.0)
            if validate: validate(result)
            if time.monotonic() >= deadline:
                omitted.append({"operation": op, "reason": "walltime deadline exceeded during trial"})
                return
            durations.append((elapsed, cpu, _timestamp()))
        # A partial/truncated case is omitted; it cannot masquerade as a full validated sample.
        if len(durations) == options.repetitions:
            records.append(_record(op, condition, durations, warmup=options.warmup,
                                   nbytes=nbytes, provenance=prov))
        elif not any(row["operation"] == op for row in omitted):
            omitted.append({"operation": op, "reason": "incomplete trial set"})

    try:
        if "ram" in options.suites:
            size = block_bytes
            src = rng.standard_normal(size // 16).astype(np.complex128)
            dst = np.zeros_like(src)
            cond = {"operation": "ram-copy", "bytes": size, "dtype": "complex128", "backend": "ram"}
            measure("ram-copy", cond, lambda: np.copyto(dst, src), size,
                    {"seed": options.seed, "oracle": "source-destination-equality"},
                    validate=lambda _result: _assert_close(dst, src, "RAM copy mismatch"))

        if "kernel" in options.suites:
            state = (rng.standard_normal(1 << options.block_bits) +
                     1j * rng.standard_normal(1 << options.block_bits)).astype(np.complex128)
            state /= np.linalg.norm(state)
            normalizer = GateNormalizer(options.block_bits)
            h = normalizer.gate("H", (0,))
            rx = normalizer.gate("RX", (0,), angle=0.37)
            for name, gate in (("kernel-h", h), ("kernel-rx", rx)):
                condition = kernel_condition(name, options.block_bits, target=0,
                                             angle_class="generic-0.37" if name == "kernel-rx" else None)
                base = state.copy()
                expected = eager_reference([gate], options.block_bits, None, initial_state=base)
                prepare = lambda base=base: np.copyto(state, base)
                validate = lambda _result, expected=expected, name=name: _assert_close(state, expected, f"{name} oracle check failed")
                measure(name, condition, lambda g=gate: apply_local(state, g, None, block_bits=options.block_bits),
                        block_bytes, {"seed": options.seed, "oracle": "independent-eager-reference"},
                        prepare=prepare, validate=validate)
            if options.block_bits >= 3:
                cond = kernel_condition("kernel-pauli-rotation", options.block_bits,
                                       target=(0, 1, 2), paulis=("Y", "X", "Z"), angle_class="generic-0.37")
                pauli = SignedPauli(3, 5)
                base = state.copy()
                pauli_gate = GateNormalizer(options.block_bits).gate("PAULI_ROT", (0, 1, 2), paulis=("Y", "X", "Z"), angle=.37)
                expected = eager_reference([pauli_gate], options.block_bits, None, initial_state=base)
                measure("kernel-pauli-rotation", cond,
                        lambda: apply_pauli_rotation(state, pauli, .37, block_bits=options.block_bits),
                        block_bytes, {"seed": options.seed, "oracle": "independent-eager-reference"},
                        prepare=lambda: np.copyto(state, base),
                        validate=lambda _result: _assert_close(state, expected, "Pauli rotation oracle check failed"))
                for x_mask in (3, 5):
                    cond = kernel_condition("kernel-pauli-inner", options.block_bits,
                                            target=(0, 1, 2), paulis=("Y", "X", "Z") if x_mask == 3 else ("Y", "I", "Y"))
                    cond["x_mask"] = x_mask
                    cond["z_mask"] = 5
                    cond["source_block"] = 0
                    inner_expected = _dense_pauli_inner(state.copy(), x_mask, 5, options.block_bits)
                    measure("kernel-pauli-inner", cond,
                            lambda x=x_mask: pauli_inner(state, state, x, 5, 0, options.block_bits),
                            block_bytes, {"seed": options.seed, "oracle": "independent-source-index-sum"},
                            validate=lambda result, expected=inner_expected: _assert_scalar_close(
                                result, expected, "Pauli inner-product oracle check failed"))

        if "storage" in options.suites and time.monotonic() >= deadline:
            omitted.append({"operation": "storage", "reason": "walltime deadline before suite"})
        if "storage" in options.suites and time.monotonic() < deadline:
            size = disk_need
            file_path = job / "probe.bin"
            io_lease = BufferLease(MemoryBudget(BufferLease.allocation_cost(size, alignment)), size, alignment)
            verify_lease = BufferLease(MemoryBudget(BufferLease.allocation_cost(size, alignment)), size, alignment)
            buf, verify = io_lease.view, verify_lease.view
            buf[:] = rng.bytes(size)
            expected_bytes = bytes(buf)
            try:
                with StorageFile(file_path, "w+b", backend=options.backend, alignment=alignment) as store:
                    backend_id = str(store.backend_identity)
                    store.write_from(0, buf)
                    store.flush()
                with StorageFile(file_path, "r+b", backend=options.backend, alignment=alignment) as store:
                    device_id = storage_device_id(job)
                    for op in ("read", "write"):
                        condition = storage_condition(op, size, backend=backend_id, alignment=alignment,
                                                      device_id=device_id, pattern="sequential", flush="deferred")
                        condition.update(working_set_bytes=size, cache_state="single-slot-unspecified", io_depth=1)
                        if op == "read":
                            prepare = lambda: buf.__setitem__(slice(None), bytes(size))
                            fn = lambda: store.read_into(0, buf)
                            def validate(_result):
                                if _result != size:
                                    raise OSError("short storage read")
                                _assert_bytes(buf, expected_bytes, "storage read mismatch")
                        else:
                            prepare = None
                            fn = lambda: store.write_from(0, buf)
                            def validate(_result):
                                if _result != size:
                                    raise OSError("short storage write")
                                if store.read_into(0, verify) != size:
                                    raise OSError("short storage verification read")
                                _assert_bytes(verify, expected_bytes, "storage write mismatch")
                        measure("storage-" + op, condition, fn, size,
                                {"backend_requested": options.backend, "sync": "synchronous"},
                                prepare=prepare, validate=validate)
                    condition = {"operation": "storage-fsync", "block_bytes": size, "backend": backend_id,
                                 "device_id": device_id, "flush": "fsync", "scope": "local"}
                    measure("storage-fsync", condition, lambda: os.fsync(store.fd), 0,
                            {"backend_requested": options.backend},
                            prepare=lambda: store.write_from(0, buf))
                    if "mixed" in options.suites and "kernel" in options.suites:
                        mix_state = np.ones(1 << min(options.block_bits, 6), dtype=np.complex128)
                        mix_state /= np.linalg.norm(mix_state)
                        mix_gate = GateNormalizer(min(options.block_bits, 6)).gate("H", (0,))
                        mix_base = mix_state.copy()
                        mix_expected = eager_reference([mix_gate], min(options.block_bits, 6), None, initial_state=mix_base)
                        mix_condition = {"operation": "mixed-kernel-io", "block_bytes": size,
                                         "block_bits": min(options.block_bits, 6), "backend": backend_id,
                                         "device_id": device_id, "kernel": "local-h", "placement": "same-main-thread",
                                         "io": "sequential-read-write", "flush": "deferred"}
                        def mixed_once():
                            apply_local(mix_state, mix_gate, None, block_bits=min(options.block_bits, 6))
                            written = store.write_from(0, buf)
                            read = store.read_into(0, buf)
                            return written, read
                        def validate_mixed(result):
                            written, read = result
                            if written != size or read != size:
                                raise OSError("incomplete mixed benchmark transfer")
                            _assert_bytes(buf, expected_bytes, "mixed storage content mismatch")
                            _assert_close(mix_state, mix_expected, "mixed kernel result mismatch")
                        measure("mixed-kernel-io", mix_condition, mixed_once, size * 2,
                                {"backend_requested": options.backend, "components_included": ["local-h", "read", "write"],
                                 "validation": "counts-and-content-outside-timed-region"},
                                prepare=lambda: np.copyto(mix_state, mix_base), validate=validate_mixed)
            finally:
                file_path.unlink(missing_ok=True)
                io_lease.close()
                verify_lease.close()

        cpu_after = os.times()
        load_after = os.getloadavg() if hasattr(os, "getloadavg") else None
        expected_operations = set()
        if "ram" in options.suites: expected_operations.add("ram-copy")
        if "kernel" in options.suites:
            expected_operations.update({"kernel-h", "kernel-rx"})
            if options.block_bits >= 3: expected_operations.update({"kernel-pauli-rotation", "kernel-pauli-inner"})
        if "storage" in options.suites: expected_operations.update({"storage-read", "storage-write", "storage-fsync"})
        if "mixed" in options.suites: expected_operations.add("mixed-kernel-io")
        recorded = {row["operation"] for row in records[original_record_count:]}
        omitted_names = {row["operation"] for row in omitted}
        omitted.extend({"operation":op,"reason":"suite not reached before walltime deadline"}
                       for op in sorted(expected_operations - recorded - omitted_names))
        discovery = dict(data["discovery"])
        discovery["calibration_budgets"] = {"managed_ram_reference_bytes":total_ram_need,
            "ram_budget_bytes":options.ram_budget_bytes,"disk_budget_bytes":options.disk_budget_bytes,
            "walltime_seconds":options.walltime_seconds,"ram_scope":"managed arrays and raw records; process/library RSS separate",
            "deadline_scope":"cooperative between cases/trials; in-flight syscall and oracle cannot be preempted"}
        discovery["omitted_calibrations"] = omitted
        discovery["calibration_observation"] = {"cpu_before": cpu_before.user + cpu_before.system,
            "cpu_after": cpu_after.user + cpu_after.system, "load_average_before": load_before,
            "load_average_after": load_after, "thread_environment": {k: os.environ.get(k) for k in
            ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS", "VECLIB_MAXIMUM_THREADS")}}
        return ProfileDocument.create(data["hardware"], data["software"], data["allocation"],
                                      calibrations=records, discovery=discovery,
                                      provenance={**data["provenance"], "calibration_parent_profile_id": profile.profile_id,
                                                  "campaign": "svbench-local-v1"})
    finally:
        shutil.rmtree(job, ignore_errors=True)
