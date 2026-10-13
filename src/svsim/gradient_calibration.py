"""Explicit bounded calibration of complete gradient methods.

Calibration is opt-in. It executes each requested method through caller supplied
fresh-trial and validation callbacks; normal gradient calls never enter here.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
from contextlib import nullcontext
import hashlib
import json
import math
import os
from pathlib import Path
import time
from typing import Any, Callable, Mapping


METHODS = ("adjoint", "recompute", "parameter-shift")
REPORT_SCHEMA_VERSION = 1
REPORT_PRODUCER = "svsim.gradient_calibration"


class _FrozenDict(dict):
    def _immutable(self, *args, **kwargs):
        raise TypeError("calibration contexts are immutable")
    __setitem__ = __delitem__ = __ior__ = clear = pop = popitem = setdefault = update = _immutable


def _freeze(value):
    if isinstance(value, Mapping):
        return _FrozenDict({str(key): _freeze(child) for key, child in value.items()})
    if isinstance(value, (list, tuple)):
        return tuple(_freeze(child) for child in value)
    return value


def _thaw(value):
    if isinstance(value, Mapping):
        return {str(key): _thaw(child) for key, child in value.items()}
    if isinstance(value, tuple):
        return [_thaw(child) for child in value]
    return value


def context_fingerprint(context: Mapping[str, Any]) -> str:
    if not isinstance(context, Mapping):
        raise TypeError("context must be a mapping")
    encoded = json.dumps(context, sort_keys=True, separators=(",", ":"), allow_nan=False)
    return hashlib.sha256(encoded.encode()).hexdigest()


@dataclass(frozen=True)
class GradientCalibrationSpec:
    """Resource limits and exact execution identity for an explicit calibration."""
    context: Mapping[str, Any]
    methods: tuple[str, ...] = METHODS
    warmup_repetitions: int = 1
    repetitions: int = 5
    wall_limit_seconds: float = 3600.0
    ram_limit_bytes: int = 1 << 40
    disk_limit_bytes: int = 1 << 40
    scratch_dir: str | None = None

    def __post_init__(self):
        if not isinstance(self.context, Mapping):
            raise TypeError("context must be a mapping")
        normalized = _thaw(self.context)
        encoded = json.dumps(normalized, sort_keys=True, separators=(",", ":"), allow_nan=False)
        if len(encoded.encode()) > 256 * 1024:
            raise ValueError("context exceeds calibration size limit")
        object.__setattr__(self, "context", _freeze(normalized))
        if not self.context:
            raise ValueError("exact execution context is required")
        if not isinstance(self.methods, (tuple, list)) or any(not isinstance(m, str) for m in self.methods):
            raise TypeError("methods must be a sequence of method names")
        methods = tuple(self.methods)
        object.__setattr__(self, "methods", methods)
        if not methods or len(set(methods)) != len(methods) or any(m not in METHODS for m in methods):
            raise ValueError("methods must be unique supported gradient methods")
        for name in ("warmup_repetitions", "repetitions", "ram_limit_bytes", "disk_limit_bytes"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value < (1 if name == "repetitions" else 0):
                raise ValueError(f"{name} is outside its allowed range")
        if self.repetitions > 100 or self.warmup_repetitions > 100:
            raise ValueError("warmup and measured repetitions are bounded at 100")
        if isinstance(self.wall_limit_seconds, bool) or not math.isfinite(self.wall_limit_seconds) or self.wall_limit_seconds <= 0:
            raise ValueError("wall_limit_seconds must be positive and finite")
        if self.scratch_dir is not None and (not isinstance(self.scratch_dir, str) or not self.scratch_dir):
            raise ValueError("scratch_dir must be a nonempty path")


@dataclass(frozen=True)
class GradientTrialObservation:
    method: str
    repetition: int
    warmup: bool
    wall_seconds: float
    validated: bool
    success: bool
    raw_ref: str
    setup_seconds: float = 0.0
    runner_seconds: float = 0.0
    peak_ram_bytes: int | None = None
    peak_disk_bytes: int | None = None
    error: str | None = None
    raw_hash: str = ""
    context_before_hash: str | None = None
    context_after_hash: str | None = None
    resources_measured: bool = False


@dataclass(frozen=True)
class GradientCalibrationReport:
    context: Mapping[str, Any]
    context_hash: str
    observations: tuple[GradientTrialObservation, ...]
    diagnostic: Mapping[str, Any] = field(default_factory=dict)
    certified_envelopes: Mapping[str, Mapping[str, Any]] = field(default_factory=dict)

    def __post_init__(self):
        object.__setattr__(self, "context", _freeze(self.context))
        object.__setattr__(self, "diagnostic", _freeze(self.diagnostic))
        object.__setattr__(self, "certified_envelopes", _freeze(self.certified_envelopes))

    def to_dict(self) -> dict[str, Any]:
        payload = {"schema_version": REPORT_SCHEMA_VERSION, "producer": REPORT_PRODUCER,
                   "evidence": "measured", "calibration_id": self.context_hash,
                   "context": _thaw(self.context), "context_hash": self.context_hash,
                   "observations": [asdict(row) for row in self.observations],
                   "diagnostic": _thaw(self.diagnostic),
                   "certified_envelopes": _thaw(self.certified_envelopes)}
        payload["report_hash"] = hashlib.sha256(json.dumps(
            payload, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()).hexdigest()
        return payload

def _tree_bytes(path: str | None) -> int | None:
    if path is None or not os.path.exists(path):
        return 0 if path is not None else None
    total = 0
    for root, _, files in os.walk(path):
        for name in files:
            try:
                total += os.path.getsize(os.path.join(root, name))
            except OSError:
                pass
    return total


def _contains_injected(value):
    if isinstance(value, Mapping):
        for key, child in value.items():
            if str(key).lower() == "injected" and child is True:
                return True
            if str(key).lower() in {"evidence", "provenance"} and isinstance(child, str) and child.lower() in {
                    "injected", "manual", "assumed"}:
                return True
            if _contains_injected(child):
                return True
    elif isinstance(value, (tuple, list)):
        return any(_contains_injected(child) for child in value)
    return False


def calibrate_gradient_methods(
    spec: GradientCalibrationSpec,
    trial_factory: Callable[[str, int, str | None], Any],
    runner: Callable[[Any, str], Any],
    oracle_validator: Callable[[Any, str, Any], bool],
    *,
    resource_probe: Callable[[Any, str], Mapping[str, int]] | None = None,
    actual_context_getter: Callable[[Any, str], Mapping[str, Any]] | None = None,
    envelope_certifier: Callable[[str, tuple[GradientTrialObservation, ...], str], Mapping[str, Any]] | None = None,
    clock: Callable[[], float] = time.perf_counter,
) -> GradientCalibrationReport:
    """Run actual whole-method trials under explicit limits.

    The factory should return a context manager when it owns resources; its
    ``__exit__`` cleanup is included in whole-trial wall time. Plain trial values
    remain the caller's cleanup responsibility. The runner must return after
    whole gradient completion. Runner time and whole-trial time are separate;
    stage durations are never added together. Wall limits are soft for a
    synchronous callback: an overrun is recorded after that callback returns.
    """
    if not callable(trial_factory) or not callable(runner) or not callable(oracle_validator):
        raise TypeError("trial_factory, runner and oracle_validator must be callable")
    if resource_probe is not None and not callable(resource_probe):
        raise TypeError("resource_probe must be callable")
    identity = context_fingerprint(spec.context)
    if actual_context_getter is not None and not callable(actual_context_getter):
        raise TypeError("actual_context_getter must be callable")
    if envelope_certifier is not None and not callable(envelope_certifier):
        raise TypeError("envelope_certifier must be callable")
    scratch = spec.scratch_dir
    if scratch is not None:
        Path(scratch).mkdir(parents=True, exist_ok=True)
    last_clock = None
    def now():
        nonlocal last_clock
        value = clock()
        if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
            raise ValueError("clock must return finite numeric values")
        value = float(value)
        if last_clock is not None and value < last_clock:
            raise ValueError("clock moved backwards")
        last_clock = value
        return value
    started = now()
    rows: list[GradientTrialObservation] = []
    previous_trial: Any = None
    over_limit = False
    for method in spec.methods:
        count = spec.warmup_repetitions + spec.repetitions
        for repetition in range(count):
            warmup = repetition < spec.warmup_repetitions
            if now() - started >= spec.wall_limit_seconds:
                over_limit = True
                break
            trial_started = now()
            trial_resource = None
            result = None
            error = None
            success = validated = False
            ram = disk = None
            resources_measured = False
            runner_elapsed = 0.0
            context_before_hash = context_after_hash = None
            try:
                trial_resource = trial_factory(method, repetition, scratch)
                if trial_resource is None or trial_resource is previous_trial:
                    raise ValueError("trial_factory must return a fresh, non-null trial for every invocation")
                managed = hasattr(trial_resource, "__enter__") and hasattr(trial_resource, "__exit__")
                with (trial_resource if managed else nullcontext(trial_resource)) as trial:
                    if actual_context_getter is not None:
                        context_before_hash = context_fingerprint(actual_context_getter(trial, method))
                        if context_before_hash != identity:
                            raise ValueError("actual pre-run context does not match declared exact context")
                    runner_started = now()
                    result = runner(trial, method)
                    runner_elapsed = now() - runner_started
                    if actual_context_getter is not None:
                        context_after_hash = context_fingerprint(actual_context_getter(trial, method))
                        if context_after_hash != identity:
                            raise ValueError("actual trial context does not match declared exact context")
                    validated = oracle_validator(trial, method, result) is True
                    success = validated
                    if resource_probe is not None:
                        measured = resource_probe(trial, method)
                        if not isinstance(measured, Mapping):
                            raise ValueError("resource probe must return a mapping")
                        ram, disk = measured.get("peak_ram_bytes"), measured.get("peak_disk_bytes")
                    if ram is not None and (isinstance(ram, bool) or not isinstance(ram, int) or ram < 0):
                        raise ValueError("resource probe returned invalid peak_ram_bytes")
                    if disk is not None and (isinstance(disk, bool) or not isinstance(disk, int) or disk < 0):
                        raise ValueError("resource probe returned invalid peak_disk_bytes")
                    resources_measured = ram is not None and disk is not None
                after_disk = _tree_bytes(scratch)
                if after_disk is not None:
                    disk = max(disk or 0, after_disk)
                if disk is not None and (isinstance(disk, bool) or not isinstance(disk, int) or disk < 0):
                    raise ValueError("resource probe returned invalid peak_disk_bytes")
                if not validated:
                    error = "oracle numerical validation failed"
                if (ram is not None and ram > spec.ram_limit_bytes) or (disk is not None and disk > spec.disk_limit_bytes):
                    over_limit = True
                    success = False
                    error = "observed resource peak exceeded calibration limit"
                    over_limit = True
            except Exception as exc:
                error = f"{type(exc).__name__}: {exc}"[:512]
                success = validated = False
                after_disk = _tree_bytes(scratch)
                if after_disk is not None:
                    disk = max(disk or 0, after_disk)
            previous_trial = trial_resource
            elapsed = now() - trial_started
            ref = f"gradient-calibration:{identity}:{method}:{repetition}"
            row = GradientTrialObservation(method, repetition, warmup, elapsed, validated,
                                           success, ref, elapsed-runner_elapsed,
                                           runner_elapsed, ram, disk, error, "",
                                           context_before_hash, context_after_hash, resources_measured)
            raw_fields = asdict(row)
            raw_fields.pop("raw_hash", None)
            row_hash = hashlib.sha256(json.dumps(raw_fields, sort_keys=True, separators=(",", ":"),
                                                allow_nan=False).encode()).hexdigest()
            rows.append(GradientTrialObservation(**{**asdict(row), "raw_hash": row_hash}))
            if over_limit:
                break
            if now() - started > spec.wall_limit_seconds:
                over_limit = True
                break
        if over_limit:
            break

    frozen_rows = tuple(rows)
    certified: dict[str, Mapping[str, Any]] = {}
    if (envelope_certifier is not None and actual_context_getter is not None and not over_limit and
            not _contains_injected(spec.context)):
        for method in spec.methods:
            held = tuple(row for row in frozen_rows if row.method == method and not row.warmup and row.success)
            if len(held) < 5:
                continue
            declaration = envelope_certifier(method, held, identity)
            if not isinstance(declaration, Mapping):
                continue
            try:
                lower, expected, upper = (float(declaration[k]) for k in
                                          ("lower_seconds", "expected_seconds", "upper_seconds"))
                confidence = float(declaration["confidence"])
                refs = tuple(declaration["held_out_raw_refs"])
                training_refs = tuple(declaration["training_raw_refs"])
                resource_ram, resource_disk = declaration["ram_bytes"], declaration["disk_bytes"]
                measurement = declaration["measurement"]
                held_rows = [row for row in held if row.raw_ref in refs]
                measured_times = [row.wall_seconds if measurement == "whole-trial" else row.runner_seconds
                                  for row in held_rows]
                within = sum(lower <= value <= upper for value in measured_times)
                coverage = within / len(held_rows) if held_rows else 0.0
                if (not all(math.isfinite(v) for v in (lower, expected, upper, confidence)) or
                        not 0 <= lower <= expected <= upper or not 0.95 <= confidence <= 1 or
                        len(refs) < 5 or len(set(refs)) != len(refs) or
                        not set(refs).issubset({row.raw_ref for row in held}) or
                        not declaration.get("model_id") or not training_refs or
                        declaration.get("evidence") != "measured" or
                        declaration.get("scope") not in {"same-host", "cross-host"} or
                        declaration.get("units") != "seconds/gradient" or
                        measurement not in {"whole-trial", "runner"} or
                        declaration.get("held_out_count") != len(refs) or
                        declaration.get("held_out_success_count") != len(held_rows) or
                        declaration.get("held_out_coverage") != coverage or coverage < confidence or
                        any(not row.resources_measured or row.peak_ram_bytes is None or row.peak_disk_bytes is None
                            for row in held_rows) or
                        any(isinstance(v, bool) or not isinstance(v, int) or v < 0
                            for v in (resource_ram, resource_disk)) or
                        any(resource_ram < row.peak_ram_bytes or resource_disk < row.peak_disk_bytes
                            for row in held_rows) or
                        set(refs).intersection(training_refs) or
                        declaration.get("validation") != "held-out" or
                        not isinstance(declaration.get("method"), str) or not declaration["method"]):
                    continue
            except (KeyError, TypeError, ValueError):
                continue
            certified[method] = {
                "lower_seconds": lower, "expected_seconds": expected, "upper_seconds": upper,
                "confidence": confidence, "method": declaration["method"],
                "validation": "held-out", "held_out_raw_refs": refs,
                "held_out_count": len(refs), "condition_key": identity,
                "model_id": declaration["model_id"], "training_raw_refs": training_refs,
                "evidence": "measured", "scope": declaration["scope"],
                "units": "seconds/gradient", "ram_bytes": resource_ram,
                "disk_bytes": resource_disk, "measurement": measurement,
                "held_out_success_count": len(held_rows), "held_out_coverage": coverage,
            }
    diagnostic: dict[str, Any] = {"over_limit": over_limit, "wall_seconds": now() - started,
                                  "resource_estimates_are_observed_peaks_not_guarantees": True}
    for method in spec.methods:
        measured = [row for row in frozen_rows if row.method == method and not row.warmup and row.success]
        ram_peaks = [row.peak_ram_bytes for row in measured if row.peak_ram_bytes is not None]
        disk_peaks = [row.peak_disk_bytes for row in measured if row.peak_disk_bytes is not None]
        diagnostic[method] = {
            "successful_repetitions": len(measured),
            "wall_seconds_min": min((row.wall_seconds for row in measured), default=None),
            "wall_seconds_max": max((row.wall_seconds for row in measured), default=None),
            "peak_ram_bytes": max(ram_peaks) if len(ram_peaks) == len(measured) and measured else None,
            "peak_disk_bytes": max(disk_peaks) if len(disk_peaks) == len(measured) and measured else None,
        }
    report = GradientCalibrationReport(_freeze(spec.context), identity, frozen_rows, diagnostic, certified)
    if len(json.dumps(report.to_dict(), sort_keys=True, separators=(",", ":"), allow_nan=False).encode()) > 4 * 1024 * 1024:
        raise ValueError("gradient calibration report exceeds 4 MiB cap")
    return report
