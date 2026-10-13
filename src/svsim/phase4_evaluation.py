"""Bounded, paired performance comparisons for Phase 4 evidence.

This module stores observations and computes descriptive paired summaries. It
does not fit a predictive model or extrapolate beyond measured cases.
"""
from __future__ import annotations

from dataclasses import dataclass, field
import hashlib
import json
import math
import random
from typing import Any, Mapping

from .journal import canonical, content_hash

STATUSES = {"completed", "failed", "unsupported", "insufficient-budget", "unmeasured"}
MAX_VARIANTS = 16
MAX_TRIALS = 100
MAX_REPORT_BYTES = 4 * 1024 * 1024
MAX_TREE_BYTES = 256 * 1024
MAX_RAW_BYTES = 16 * 1024
_IDENTITY_FIELDS = ("source", "initial_state", "binding", "exact_witness", "observable",
                    "endpoint", "n", "b", "ranks", "resources", "backend", "build",
                    "seed", "tolerances", "warmups", "trials", "budget_seconds")


class _FrozenDict(dict):
    """JSON-serializable immutable dict used for defensive copies."""
    def _immutable(self, *args, **kwargs):
        raise TypeError("manifest and evidence mappings are immutable")

    __setitem__ = __delitem__ = __ior__ = clear = pop = popitem = setdefault = update = _immutable


def _freeze(value):
    if isinstance(value, dict):
        result = _FrozenDict()
        dict.update(result, {key: _freeze(item) for key, item in value.items()})
        return result
    if isinstance(value, list):
        return tuple(_freeze(item) for item in value)
    return value


def _tree(value: Any, name: str, *, max_bytes: int = MAX_TREE_BYTES) -> dict:
    if not isinstance(value, Mapping):
        raise ValueError(f"{name} must be a mapping")
    try:
        encoded = canonical(dict(value))
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError(f"{name} must contain finite JSON values") from exc
    if len(encoded.encode("utf-8")) > max_bytes:
        raise ValueError(f"{name} exceeds size limit")
    try:
        result = json.loads(encoded)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError(f"{name} must contain finite JSON values") from exc
    if not result:
        raise ValueError(f"{name} must not be empty")
    return _freeze(result)


def _finite(value: Any, label: str, *, positive=False, nonnegative=False) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{label} must be finite")
    try:
        value = float(value)
    except (OverflowError, ValueError) as exc:
        raise ValueError(f"{label} must be finite") from exc
    if not math.isfinite(value):
        raise ValueError(f"{label} must be finite")
    if positive and value <= 0 or nonnegative and value < 0:
        raise ValueError(f"{label} is out of range")
    return value


def _equivalent(left: Any, right: Any) -> bool:
    """Compare decoded JSON structures while treating 1 and 1.0 as equal."""
    if isinstance(left, bool) or isinstance(right, bool):
        return type(left) is type(right) and left == right
    if isinstance(left, Mapping) and isinstance(right, Mapping):
        return set(left) == set(right) and all(_equivalent(left[key], right[key]) for key in left)
    if isinstance(left, (tuple, list)) and isinstance(right, (tuple, list)):
        return len(left) == len(right) and all(_equivalent(a, b) for a, b in zip(left, right))
    if type(left) in (int, float) and type(right) in (int, float):
        return left == right
    return type(left) is type(right) and left == right


@dataclass(frozen=True)
class CaseManifest:
    """Conditions shared by every variant in one comparison."""
    case_id: str
    source: Mapping[str, Any]
    initial_state: Mapping[str, Any]
    binding: Mapping[str, Any]
    exact_witness: Mapping[str, Any]
    observable: Mapping[str, Any]
    endpoint: str
    n: int
    b: int
    ranks: int
    resources: Mapping[str, Any]
    backend: str
    build: Mapping[str, Any]
    seed: int
    tolerances: Mapping[str, Any]
    warmups: int = 2
    trials: int = 7
    budget_seconds: float = 3600.0

    def __post_init__(self):
        if not isinstance(self.case_id, str) or not self.case_id or len(self.case_id) > 128:
            raise ValueError("case_id must be a non-empty short string")
        for key in ("source", "initial_state", "binding", "exact_witness", "observable",
                    "resources", "build", "tolerances"):
            object.__setattr__(self, key, _tree(getattr(self, key), key))
        for key in ("endpoint", "backend"):
            value = getattr(self, key)
            if not isinstance(value, str) or not value or len(value) > 256:
                raise ValueError(f"{key} must be a non-empty short string")
        for key in ("n", "b", "ranks", "seed", "warmups", "trials"):
            value = getattr(self, key)
            if type(value) is not int or value < 0:
                raise ValueError(f"{key} must be a non-negative integer")
        if self.n < 1 or not 1 <= self.b <= self.n or self.ranks < 1 or self.warmups > 100 or not 1 <= self.trials <= MAX_TRIALS:
            raise ValueError("invalid dimensions or trial count")
        _finite(self.budget_seconds, "budget_seconds", positive=True)

    def identity(self) -> dict[str, Any]:
        return {key: getattr(self, key) for key in _IDENTITY_FIELDS}

    @property
    def manifest_hash(self) -> str:
        return content_hash(self.identity())


@dataclass(frozen=True)
class Variant:
    variant_id: str
    axes: Mapping[str, Any]
    description: str = ""

    def __post_init__(self):
        if not isinstance(self.variant_id, str) or not self.variant_id or len(self.variant_id) > 128:
            raise ValueError("variant_id must be a non-empty short string")
        object.__setattr__(self, "axes", _tree(self.axes, "axes"))
        if not isinstance(self.description, str) or len(self.description) > 512:
            raise ValueError("description must be a short string")


@dataclass(frozen=True)
class Trial:
    variant_id: str
    trial_index: int
    status: str
    wall_seconds: float | None = None
    rank_wall_seconds: tuple[float, ...] = ()
    metrics: Mapping[str, Any] = field(default_factory=dict)
    validator_passed: bool | None = None
    raw: Mapping[str, Any] = field(default_factory=dict)
    raw_sha256: str | None = None
    reason: str = ""

    def __post_init__(self):
        if not isinstance(self.variant_id, str) or not self.variant_id:
            raise ValueError("variant_id is required")
        if type(self.trial_index) is not int or self.trial_index < 0:
            raise ValueError("trial_index must be a non-negative integer")
        if self.status not in STATUSES:
            raise ValueError("unknown trial status")
        if self.status == "completed":
            if type(self.validator_passed) is not bool or self.wall_seconds is None:
                raise ValueError("completed trial requires a validator result and wall time")
            if not self.raw:
                raise ValueError("completed trial requires raw evidence")
        elif self.validator_passed is not None and type(self.validator_passed) is not bool:
            raise ValueError("validator_passed must be bool or null")
        if self.wall_seconds is not None:
            _finite(self.wall_seconds, "wall_seconds", nonnegative=True)
        if not isinstance(self.rank_wall_seconds, (tuple, list)):
            raise ValueError("rank_wall_seconds must be a sequence")
        ranks = tuple(_finite(v, "rank wall seconds", nonnegative=True) for v in self.rank_wall_seconds)
        object.__setattr__(self, "rank_wall_seconds", ranks)
        metrics = _tree(self.metrics, "metrics") if self.metrics else {}
        for key, value in metrics.items():
            if not isinstance(key, str) or type(value) not in (int, float):
                raise ValueError("metrics must be finite numeric values")
            _finite(value, f"metric {key}")
        object.__setattr__(self, "metrics", metrics)
        raw = _tree(self.raw, "raw", max_bytes=MAX_RAW_BYTES) if self.raw else {}
        object.__setattr__(self, "raw", raw)
        if raw:
            digest = hashlib.sha256(canonical(raw).encode()).hexdigest()
            if self.raw_sha256 != digest:
                raise ValueError("raw_sha256 does not match raw evidence")
        elif self.raw_sha256 is not None:
            raise ValueError("raw hash provided without raw evidence")
        if not isinstance(self.reason, str) or len(self.reason) > 1024:
            raise ValueError("reason must be a short string")

    @classmethod
    def completed(cls, variant_id: str, trial_index: int, wall_seconds: float, raw: Mapping[str, Any], *,
                  rank_wall_seconds=(), metrics=None, validator_passed=True, **kwargs):
        frozen = _tree(raw, "raw")
        measured = {"wall_seconds": wall_seconds,
                    "rank_wall_seconds": list(rank_wall_seconds),
                    "metrics": dict(metrics or {}),
                    "validator_passed": validator_passed}
        if "measurement" in frozen and frozen["measurement"] != measured:
            raise ValueError("raw measurement does not match trial fields")
        frozen = _tree({**frozen, "measurement": measured}, "raw", max_bytes=MAX_RAW_BYTES)
        return cls(variant_id, trial_index, "completed", wall_seconds=wall_seconds,
                   rank_wall_seconds=rank_wall_seconds, metrics=metrics or {},
                   validator_passed=validator_passed, raw=frozen,
                   raw_sha256=hashlib.sha256(canonical(frozen).encode()).hexdigest(), **kwargs)


def paired_order(variants: tuple[Variant, ...] | list[Variant], trial_count: int, seed: int):
    """Return a seeded, balanced alternating execution order per trial pair."""
    if not 1 <= len(variants) <= MAX_VARIANTS or not 1 <= trial_count <= MAX_TRIALS:
        raise ValueError("campaign exceeds variant/trial bounds")
    if type(seed) is not int or seed < 0:
        raise ValueError("seed must be a non-negative integer")
    rng = random.Random(seed)
    order = []
    ids = [v.variant_id for v in variants]
    shuffled = ids[:]
    rng.shuffle(shuffled)
    for trial in range(trial_count):
        shift = trial % len(shuffled)
        ordered = shuffled[shift:] + shuffled[:shift]
        order.extend((variant_id, trial) for variant_id in ordered)
    return tuple(order)


def _percentile(values: list[float], q: float) -> float:
    ordered = sorted(values)
    pos = (len(ordered) - 1) * q
    lo, hi = math.floor(pos), math.ceil(pos)
    fraction = pos - lo
    return ordered[lo] * (1 - fraction) + ordered[hi] * fraction


def _median(values: list[float]) -> float:
    ordered = sorted(values)
    middle = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[middle]
    return ordered[middle - 1] / 2 + ordered[middle] / 2


def _summary(values: list[float]) -> dict:
    median = _median(values)
    spread = max(values) - min(values)
    variation = spread / median if median != 0 and math.isfinite(spread) else None
    if variation is not None and not math.isfinite(variation):
        variation = None
    return {"count": len(values), "median": median, "p95": _percentile(values, .95),
            "min": min(values), "max": max(values), "variation": variation}


def _paired_summary(base: list[float], target: list[float], *, seed: int, bootstrap_samples: int,
                    expected_pairs: int):
    paired = [(a, b) for a, b in zip(base, target)]
    diffs = [b - a for a, b in paired]
    if any(not math.isfinite(value) for value in diffs):
        return {"matched_pairs": len(diffs), "difference": None, "ratio": None,
                "difference_ci95": None, "claim": "unconfirmed",
                "diagnostics": ["paired arithmetic exceeded finite range"]}
    ratio_values = []
    for a, b in paired:
        if a:
            ratio = b / a
            if math.isfinite(ratio):
                ratio_values.append(ratio)
    ratios = ratio_values
    if not diffs:
        return {"matched_pairs": 0, "difference": None, "ratio": None, "difference_ci95": None,
                "claim": "unconfirmed", "diagnostics": ["no matched completed pairs"]}
    rng = random.Random(seed)
    draws = [_median(rng.choices(diffs, k=len(diffs))) for _ in range(bootstrap_samples)]
    low, high = _percentile(draws, .025), _percentile(draws, .975)
    diagnostics = []
    if len(diffs) < 7:
        diagnostics.append("small sample; interval is diagnostic")
    if len(diffs) < expected_pairs:
        diagnostics.append("incomplete paired trial set")
    if low <= 0 <= high:
        diagnostics.append("interval crosses zero")
    return {"matched_pairs": len(diffs), "difference": _summary(diffs),
            "ratio": _summary(ratios) if ratios else None,
            "difference_ci95": [low, high],
            "claim": "unconfirmed" if diagnostics else "observed-direction-only",
            "diagnostics": diagnostics}


@dataclass(frozen=True)
class Campaign:
    case: CaseManifest
    variants: tuple[Variant, ...]
    trials: tuple[Trial, ...] = ()

    def __post_init__(self):
        if not isinstance(self.case, CaseManifest):
            raise TypeError("case must be CaseManifest")
        variants = tuple(self.variants)
        if not 1 <= len(variants) <= MAX_VARIANTS:
            raise ValueError("campaign must have 1..16 variants")
        ids = [v.variant_id for v in variants]
        if len(set(ids)) != len(ids):
            raise ValueError("variant ids must be unique")
        base = variants[0]
        for variant in variants[1:]:
            keys = set(base.axes) | set(variant.axes)
            changed = [key for key in keys if base.axes.get(key) != variant.axes.get(key)]
            if len(changed) != 1:
                raise ValueError("each variant must differ from baseline on exactly one policy axis")
        trials = tuple(self.trials)
        if len(trials) > MAX_TRIALS * len(variants):
            raise ValueError("trial row bound exceeded")
        report_input_bytes = 1024 + len(variants) * 512 + len(trials) * 512
        seen = set()
        for trial in trials:
            if trial.variant_id not in ids or trial.trial_index >= self.case.trials:
                raise ValueError("trial does not belong to this campaign")
            key = (trial.variant_id, trial.trial_index)
            if key in seen:
                raise ValueError("duplicate variant/trial row")
            seen.add(key)
            if trial.raw:
                report_input_bytes += len(canonical(trial.raw).encode("utf-8"))
                required = {"case_manifest_hash": self.case.manifest_hash,
                            "variant_id": trial.variant_id,
                            "trial_index": trial.trial_index}
                if any(trial.raw.get(name) != value for name, value in required.items()):
                    raise ValueError("raw evidence identity does not match campaign pair")
                if not isinstance(trial.raw.get("scope"), str) or not trial.raw["scope"]:
                    raise ValueError("raw evidence must declare measurement scope")
        if report_input_bytes > MAX_REPORT_BYTES:
            raise ValueError("comparison report input exceeds size limit")
        object.__setattr__(self, "variants", variants)
        object.__setattr__(self, "trials", trials)

    def _validation_errors(self, trial: Trial) -> list[str]:
        if trial.status != "completed":
            return []
        errors = []
        if trial.validator_passed is not True:
            errors.append("validator-failed")
        expected_measurement = {"wall_seconds": trial.wall_seconds,
                                "rank_wall_seconds": list(trial.rank_wall_seconds),
                                "metrics": dict(trial.metrics),
                                "validator_passed": trial.validator_passed}
        if not isinstance(trial.raw.get("measurement"), Mapping) or not _equivalent(
                trial.raw["measurement"], expected_measurement):
            errors.append("raw-measurement-mismatch-or-missing")
        if trial.wall_seconds is None or trial.wall_seconds > self.case.budget_seconds:
            errors.append("wall-budget-exceeded")
        if len(trial.rank_wall_seconds) != self.case.ranks:
            errors.append("rank-count-mismatch")
        elif trial.rank_wall_seconds:
            rank_max = max(trial.rank_wall_seconds)
            tolerance = max(1e-12, 1e-9 * max(abs(rank_max), abs(float(trial.wall_seconds or 0.0))))
            if abs(float(trial.wall_seconds) - rank_max) > tolerance:
                errors.append("wall-rank-max-mismatch")
        ram_limit = self.case.resources.get("ram_per_rank_bytes")
        disk_limit = self.case.resources.get("disk_per_rank_bytes")
        requirements = (("managed_peak_bytes_max", ram_limit, "managed-memory-budget-exceeded"),
                        ("scratch_bytes_max", disk_limit, "scratch-budget-exceeded"))
        for metric, per_rank, error in requirements:
            if per_rank is None:
                continue
            if type(per_rank) is not int or per_rank < 0:
                errors.append("invalid-resource-manifest")
                continue
            value = trial.metrics.get(metric)
            if type(value) is not int or value < 0:
                errors.append(f"{metric}-missing-or-invalid")
            elif value > per_rank:
                errors.append(error)
        return errors

    def compare(self, *, seed: int | None = None, bootstrap_samples: int = 2000) -> "ComparisonReport":
        if type(bootstrap_samples) is not int or not 100 <= bootstrap_samples <= 10000:
            raise ValueError("bootstrap_samples must be between 100 and 10000")
        seed = self.case.seed if seed is None else seed
        if type(seed) is not int or seed < 0:
            raise ValueError("seed must be a non-negative integer")
        rows = []
        summaries = {}
        for variant in self.variants:
            trials = [t for t in self.trials if t.variant_id == variant.variant_id]
            errors_by_index = {t.trial_index: self._validation_errors(t) for t in trials}
            usable = [t for t in trials if t.status == "completed" and not errors_by_index[t.trial_index]
                      and t.raw and t.raw_sha256 == hashlib.sha256(canonical(t.raw).encode()).hexdigest()]
            values = [float(t.wall_seconds) for t in usable]
            metrics = {"wall_seconds": _summary(values)} if values else {}
            rank_max = [max(t.rank_wall_seconds) for t in usable if t.rank_wall_seconds]
            if rank_max:
                metrics["rank_max_seconds"] = _summary(rank_max)
            for metric in sorted({k for t in usable for k in t.metrics}):
                metric_values = [float(t.metrics[metric]) for t in usable if metric in t.metrics]
                if metric_values:
                    metrics[metric] = _summary(metric_values)
            statuses = {status: sum(t.status == status for t in trials) for status in sorted(STATUSES)}
            failed_validations = sum(bool(errors_by_index[t.trial_index]) for t in trials)
            statuses["validation_failed"] = failed_validations
            statuses["unmeasured"] += self.case.trials - len(trials)
            evidence_rows = [{"trial_index": t.trial_index, "status": t.status,
                              "wall_seconds": t.wall_seconds,
                              "rank_wall_seconds": list(t.rank_wall_seconds),
                              "validator_passed": t.validator_passed, "metrics": t.metrics,
                              "raw": t.raw, "raw_sha256": t.raw_sha256, "reason": t.reason,
                              "validation_errors": errors_by_index[t.trial_index]}
                             for t in sorted(trials, key=lambda item: item.trial_index)]
            rows.append({"variant_id": variant.variant_id, "axes": variant.axes,
                         "status": "completed" if len(usable) == self.case.trials else
                         ("failed" if failed_validations else
                          (trials[0].status if trials and len({t.status for t in trials}) == 1 else "unmeasured")),
                         "statuses": statuses, "metrics": metrics,
                         "trials": evidence_rows})
            summaries[variant.variant_id] = {t.trial_index: t for t in usable}
        comparisons = []
        baseline = self.variants[0]
        for variant in self.variants[1:]:
            left, right = summaries[baseline.variant_id], summaries[variant.variant_id]
            candidate_indices = sorted(set(left) & set(right))
            matched = [i for i in candidate_indices
                       if left[i].raw.get("scope") == right[i].raw.get("scope")]
            scope_mismatches = len(candidate_indices) - len(matched)
            row = _paired_summary([float(left[i].wall_seconds) for i in matched],
                                  [float(right[i].wall_seconds) for i in matched],
                                  seed=seed, bootstrap_samples=bootstrap_samples,
                                  expected_pairs=self.case.trials)
            if scope_mismatches:
                row["claim"] = "unconfirmed"
                row["diagnostics"].append(f"{scope_mismatches} paired trial(s) have mismatched measurement scope")
            row["scope_mismatches"] = scope_mismatches
            metric_names = sorted({name for i in matched
                                   for name in set(left[i].metrics) & set(right[i].metrics)})
            paired_metrics = {}
            for metric_index, metric in enumerate(metric_names):
                metric_pairs = [i for i in matched
                                if metric in left[i].metrics and metric in right[i].metrics]
                paired_metrics[metric] = _paired_summary(
                    [float(left[i].metrics[metric]) for i in metric_pairs],
                    [float(right[i].metrics[metric]) for i in metric_pairs],
                    seed=(seed + metric_index + 1) % (1 << 64),
                    bootstrap_samples=bootstrap_samples,
                    expected_pairs=self.case.trials)
            row["paired_metrics"] = paired_metrics
            changed_axis = next(k for k in set(baseline.axes) | set(variant.axes)
                                if baseline.axes.get(k) != variant.axes.get(k))
            comparisons.append({"baseline": baseline.variant_id, "variant": variant.variant_id,
                                "axis": changed_axis, **row})
        report = ComparisonReport(self.case.case_id, self.case.manifest_hash, seed, tuple(rows),
                                  tuple(comparisons), paired_order(self.variants, self.case.trials, seed))
        if len(canonical(report.to_dict()).encode()) > MAX_REPORT_BYTES:
            raise ValueError("comparison report exceeds size limit")
        return report


@dataclass(frozen=True)
class ComparisonReport:
    case_id: str
    manifest_hash: str
    seed: int
    rows: tuple[dict, ...]
    comparisons: tuple[dict, ...]
    planned_order: tuple[tuple[str, int], ...]

    def to_dict(self) -> dict:
        return {"schema_version": 1, "kind": "comparison-report", "case_id": self.case_id,
                "manifest_hash": self.manifest_hash, "seed": self.seed,
                "rows": list(self.rows), "comparisons": list(self.comparisons),
                "planned_order": [list(item) for item in self.planned_order],
                "extrapolation": "none", "predictive_interval": False}

    def to_json(self) -> str:
        data = canonical(self.to_dict())
        if len(data.encode()) > MAX_REPORT_BYTES:
            raise ValueError("comparison report exceeds size limit")
        return data
