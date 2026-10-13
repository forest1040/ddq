"""Stable condition documents and keys for calibration records."""

from __future__ import annotations

import hashlib
import json
import math
from typing import Any


_VOLATILE_KEYS = {
    "timestamp", "measured_at", "created_at", "started_at", "finished_at",
    "wall_seconds", "cpu_seconds", "elapsed_seconds", "cpu_wall_ratio",
    "cpu_percent", "load_average", "trial_id", "sample_id",
}


def canonical_condition(condition: dict[str, Any]) -> dict[str, Any]:
    """Return a deterministic condition without volatile result observations.

    Conditions should describe the workload and environment, not a particular
    measured result. Volatile result fields are recursively omitted so callers
    cannot accidentally create a different key for each trial.
    """
    if not isinstance(condition, dict):
        raise TypeError("condition must be a dict")
    stack = [(condition, 0)]
    nodes = 0
    while stack:
        item, depth = stack.pop()
        nodes += 1
        if depth > 32 or nodes > 50_000:
            raise ValueError("condition exceeds nesting or node limit")
        if isinstance(item, dict):
            stack.extend((child, depth + 1) for child in item.values())
        elif isinstance(item, (list, tuple)):
            stack.extend((child, depth + 1) for child in item)

    def clean(value: Any) -> Any:
        if isinstance(value, dict):
            return {str(k): clean(v) for k, v in sorted(value.items(), key=lambda item: str(item[0]))
                    if str(k).lower() not in _VOLATILE_KEYS}
        if isinstance(value, (list, tuple)):
            return [clean(v) for v in value]
        if isinstance(value, float) and not math.isfinite(value):
            raise ValueError("condition numbers must be finite")
        if value is None or isinstance(value, (str, int, float, bool)):
            return value
        raise TypeError(f"condition contains non-JSON value: {type(value).__name__}")

    result = clean(condition)
    # Validate canonical JSON representation and reject unsupported values.
    json.dumps(result, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)
    return result


def condition_key(condition: dict[str, Any]) -> str:
    """Return a stable SHA-256 key for a canonical condition."""
    raw = json.dumps(canonical_condition(condition), sort_keys=True,
                     separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def kernel_condition(operation: str, block_bits: int, *, target: int = 0,
                     controls=(), control_values=(), paulis=(), angle_class=None) -> dict[str, Any]:
    if not isinstance(block_bits, int) or isinstance(block_bits, bool) or not 0 <= block_bits <= 1024:
        raise ValueError("block_bits must be an integer between 0 and 1024")
    targets = [target] if isinstance(target, int) and not isinstance(target, bool) else list(target)
    if any(not isinstance(q, int) or isinstance(q, bool) or q < 0 for q in targets):
        raise ValueError("targets must be non-negative integer indices")
    controls = list(controls)
    control_values = list(control_values)
    paulis = [str(p).upper() for p in paulis]
    if len(control_values) not in (0, len(controls)):
        raise ValueError("control_values must be empty or match controls")
    if any(v not in (0, 1) or isinstance(v, bool) for v in control_values):
        raise ValueError("control values must be 0 or 1")
    if any(not isinstance(q, int) or isinstance(q, bool) or q < 0 for q in controls):
        raise ValueError("controls must be non-negative integer indices")
    condition = {
        "operation": str(operation).lower(),
        "block_bits": block_bits,
        "block_bytes": 16 * (1 << block_bits),
        "dtype": "complex128",
        "backend": "ram",
        "path": "local",
        "targets": targets,
        "controls": controls,
        "control_values": control_values,
        "paulis": paulis,
        "angle_class": angle_class,
    }
    return canonical_condition(condition)


def storage_condition(operation: str, block_bytes: int, backend: str, alignment: int,
                      device_id: str, pattern: str = "sequential", flush: str = "deferred") -> dict[str, Any]:
    op = str(operation).lower()
    if op not in {"read", "write"}:
        raise ValueError("operation must be read or write")
    for name, value in (("block_bytes", block_bytes), ("alignment", alignment)):
        if not isinstance(value, int) or isinstance(value, bool) or value < 1:
            raise ValueError(f"{name} must be a positive integer")
    return canonical_condition({
        "operation": op,
        "block_bytes": block_bytes,
        "backend": str(backend),
        "alignment": alignment,
        "device_id": str(device_id),
        "pattern": str(pattern),
        "flush": str(flush),
        "scope": "local",
    })
