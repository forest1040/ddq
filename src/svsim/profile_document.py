"""Versioned, checksummed profile documents and safe legacy adapters."""

from __future__ import annotations

from copy import deepcopy
from dataclasses import asdict, is_dataclass
from datetime import datetime, timezone
import hashlib
import importlib.metadata
import json
import math
import os
from pathlib import Path
import platform
import re
import statistics
import subprocess
import sys
from typing import Any

from .calibration_conditions import canonical_condition, condition_key
from .journal import canonical


SCHEMA_VERSION = 1
DOCUMENT_KIND = "hardware-profile"
DEFAULT_READ_LIMIT = 16 * 1024 * 1024
DEFAULT_WRITE_LIMIT = 4 * 1024 * 1024


def _now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _hash(value: Any) -> str:
    return hashlib.sha256(canonical(value).encode("utf-8")).hexdigest()


def _parse_timestamp(value: str) -> datetime:
    if not isinstance(value, str):
        raise ValueError("timestamp must be an ISO-8601 string")
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("timestamp must include a timezone")
    return parsed.astimezone(timezone.utc)


def _decimal_string(value: Any, field: str) -> str:
    if isinstance(value, bool) or not isinstance(value, (int, str)):
        raise ValueError(f"{field} must be a non-negative integer")
    if isinstance(value, str):
        if len(value) > 2048 or not value.isascii() or not value.isdecimal():
            raise ValueError(f"{field} must be a decimal integer string")
        number = int(value)
    else:
        number = value
    if number < 0:
        raise ValueError(f"{field} must be non-negative")
    return str(number)


def _count_or_bytes_key(key: str) -> bool:
    name = key.lower()
    return (name.endswith("_bytes") or "_bytes_" in name or name.endswith("_count") or
            name.endswith("_calls") or name in {
        "bytes", "count", "calls", "warmup", "warmups", "repetitions",
        "sample_count", "warmup_count",
    })


_SMALL_INT_KEYS = {"qubits", "ranks", "threads", "block_bits"}


def _wire_prepare(value: Any, key: str | None = None) -> Any:
    if isinstance(value, dict):
        return {str(k): _wire_prepare(v, str(k)) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_wire_prepare(v, key) for v in value]
    if isinstance(value, bool):
        if key is not None and (_count_or_bytes_key(key) or key.lower() in _SMALL_INT_KEYS):
            raise ValueError(f"{key} cannot be boolean")
        return value
    if isinstance(value, int):
        if key is not None and _count_or_bytes_key(key):
            return _decimal_string(value, key)
        if key is not None and key.lower() in _SMALL_INT_KEYS:
            if value < 0:
                raise ValueError(f"{key} must be non-negative")
            return value
        if value < 0 and key is not None and (_count_or_bytes_key(key) or key.lower().endswith("_bytes")):
            raise ValueError(f"{key} must be non-negative")
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("wire numbers must be finite")
        if key is not None and (_count_or_bytes_key(key) or key.lower() in _SMALL_INT_KEYS):
            raise ValueError(f"{key} must be an integer count")
        return value
    if value is None or isinstance(value, str):
        if key is not None and _count_or_bytes_key(key):
            if value is None:
                return None
            return _decimal_string(value, key)
        if key is not None and key.lower() in _SMALL_INT_KEYS:
            if value is None:
                return None
            raise ValueError(f"{key} must remain a JSON integer")
        return value
    raise TypeError(f"not JSON serializable: {type(value).__name__}")


def _wire_restore(value: Any, key: str | None = None) -> Any:
    if isinstance(value, dict):
        return {k: _wire_restore(v, k) for k, v in value.items()}
    if isinstance(value, list):
        return [_wire_restore(v, key) for v in value]
    if value is None:
        return None
    if key is not None and _count_or_bytes_key(key):
        return int(_decimal_string(value, key))
    if key is not None and key.lower() in _SMALL_INT_KEYS:
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise ValueError(f"{key} must be a non-negative JSON integer")
        return value
    if isinstance(value, float) and not math.isfinite(value):
        raise ValueError("wire numbers must be finite")
    return value


def wire_encode(obj: Any) -> Any:
    """Convert an object to JSON-safe values, stringifying counts and bytes."""
    _check_tree(obj)
    return _wire_prepare(obj)


def wire_decode(obj: Any) -> Any:
    """Restore decimal-string counts and byte quantities to Python integers."""
    _check_tree(obj)
    return _wire_restore(obj)


def _pairs_no_duplicates(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def _reject_constant(value):
    raise ValueError(f"invalid JSON numeric constant: {value}")


def _parse_float(value):
    parsed = float(value)
    if not math.isfinite(parsed):
        raise ValueError("JSON numbers must be finite")
    return parsed


def read_document(path: str | os.PathLike[str], max_bytes: int = DEFAULT_READ_LIMIT) -> dict[str, Any]:
    if not isinstance(max_bytes, int) or isinstance(max_bytes, bool) or max_bytes < 1:
        raise ValueError("max_bytes must be positive")
    source = Path(path)
    if source.stat().st_size > max_bytes:
        raise ValueError("document exceeds byte limit")
    with source.open("rb") as stream:
        raw = stream.read(max_bytes + 1)
    if len(raw) > max_bytes:
        raise ValueError("document exceeds byte limit")
    try:
        value = json.loads(raw.decode("utf-8"), object_pairs_hook=_pairs_no_duplicates,
                           parse_constant=_reject_constant, parse_float=_parse_float)
    except RecursionError as exc:
        raise ValueError("document nesting exceeds parser limit") from exc
    if not isinstance(value, dict):
        raise ValueError("document root must be an object")
    return value


def write_document(path: str | os.PathLike[str], data: dict[str, Any],
                   overwrite: bool = False, max_bytes: int = DEFAULT_WRITE_LIMIT) -> None:
    if not isinstance(max_bytes, int) or isinstance(max_bytes, bool) or max_bytes < 1:
        raise ValueError("max_bytes must be positive")
    target = Path(path)
    if target.exists() and not overwrite:
        raise FileExistsError(target)
    raw = (canonical(wire_encode(data)) + "\n").encode("utf-8")
    if len(raw) > max_bytes:
        raise ValueError("document exceeds byte limit")
    target.parent.mkdir(parents=True, exist_ok=True)
    mode = "wb" if overwrite else "xb"
    with target.open(mode) as stream:
        stream.write(raw)


def _content(data: dict[str, Any]) -> dict[str, Any]:
    result = deepcopy(data)
    result.pop("profile_id", None)
    return result


def _configuration_fingerprint(hardware: dict, software: dict, allocation: dict) -> str:
    volatile = {"ram_available_bytes", "free_bytes", "used_bytes", "available_bytes",
                "timestamp", "created_at", "observed_at", "measured_at", "legacy_profile_id",
                "job_id", "pbs_job_id", "slurm_job_id", "allocation_id", "started_at", "ended_at"}

    def stable(value):
        if isinstance(value, dict):
            return {k: stable(v) for k, v in value.items() if k.lower() not in volatile}
        if isinstance(value, (list, tuple)):
            return [stable(item) for item in value]
        return value

    return _hash({"hardware": stable(hardware), "software": stable(software), "allocation": stable(allocation)})


def _check_tree(value: Any, *, max_depth: int = 64, max_nodes: int = 200_000) -> None:
    stack = [(value, 0)]
    count = 0
    while stack:
        current, depth = stack.pop()
        count += 1
        if count > max_nodes or depth > max_depth:
            raise ValueError("profile structure exceeds nesting or node limit")
        if isinstance(current, dict):
            stack.extend((item, depth + 1) for item in current.values())
        elif isinstance(current, (list, tuple)):
            stack.extend((item, depth + 1) for item in current)


def _validate_calibration(record: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(record, dict):
        raise ValueError("calibration record must be an object")
    row = deepcopy(record)
    required = {"operation", "condition", "condition_key", "evidence", "scope", "quality",
                "units", "summary", "trials", "timestamp", "warmup_count", "sample_count", "provenance"}
    missing = sorted(required - row.keys())
    if missing:
        raise ValueError(f"calibration missing fields: {', '.join(missing)}")
    if row["evidence"] != "measured" or row["scope"] not in {"same-host", "cross-host"}:
        raise ValueError("calibration must be a measured same-host or cross-host record")
    if row["quality"] != "validated":
        raise ValueError("calibration quality must be validated")
    if row["units"] != "calls":
        raise ValueError("calibration units must be calls")
    condition = canonical_condition(row["condition"])
    if row["condition_key"] != condition_key(condition):
        raise ValueError("calibration condition_key mismatch")
    _check_tree(row, max_depth=32, max_nodes=50_000)
    if not isinstance(row["summary"], dict) or not isinstance(row["trials"], list):
        raise ValueError("calibration summary/trials invalid")
    for field in ("median_seconds", "mean_seconds", "min_seconds", "max_seconds", "p95_seconds"):
        value = row["summary"].get(field)
        if not isinstance(value, (int, float)) or isinstance(value, bool) or not math.isfinite(value) or value < 0:
            raise ValueError(f"invalid summary {field}")
    row["warmup_count"] = int(_decimal_string(row["warmup_count"], "warmup_count"))
    row["sample_count"] = int(_decimal_string(row["sample_count"], "sample_count"))
    if row["sample_count"] < 1 or row["sample_count"] != len(row["trials"]):
        raise ValueError("sample_count must match non-empty trials")
    if not row["trials"]:
        raise ValueError("at least one successful trial is required")
    wall_values = []
    for trial in row["trials"]:
        if not isinstance(trial, dict):
            raise ValueError("trial must be an object")
        for field in ("wall_seconds", "cpu_seconds"):
            value = trial.get(field)
            if field == "wall_seconds" and (not isinstance(value, (int, float)) or isinstance(value, bool)):
                raise ValueError("trial wall_seconds is required")
            if value is not None and (not isinstance(value, (int, float)) or isinstance(value, bool) or not math.isfinite(value) or value < 0):
                raise ValueError(f"invalid trial {field}")
            if field == "wall_seconds":
                if value <= 0:
                    raise ValueError("successful trial wall_seconds must be positive")
                wall_values.append(float(value))
        if trial.get("success", True) is not True:
            raise ValueError("calibration contains unsuccessful trial")
        for field in ("bytes", "count"):
            trial[field] = int(_decimal_string(trial[field], field))
        if trial["count"] != 1:
            raise ValueError("call calibration trials must measure exactly one call")
        timestamp = trial.get("timestamp")
        if not isinstance(timestamp, str):
            raise ValueError("trial timestamp is required")
        try:
            _parse_timestamp(timestamp)
        except ValueError as exc:
            raise ValueError("trial timestamp must be ISO-8601") from exc
    ordered = sorted(wall_values)
    p95_index = (len(ordered) - 1) * 0.95
    low_index = math.floor(p95_index)
    high_index = math.ceil(p95_index)
    p95 = ordered[low_index] + (ordered[high_index] - ordered[low_index]) * (p95_index - low_index)
    expected = {
        "median_seconds": statistics.median(ordered),
        "mean_seconds": statistics.fmean(ordered),
        "min_seconds": ordered[0],
        "max_seconds": ordered[-1],
        "p95_seconds": p95,
    }
    for name, value in expected.items():
        if not math.isclose(float(row["summary"][name]), value, rel_tol=1e-6, abs_tol=1e-9):
            raise ValueError(f"summary {name} inconsistent with raw trials")
    try:
        _parse_timestamp(row["timestamp"])
    except ValueError as exc:
        raise ValueError("calibration timestamp must be ISO-8601") from exc
    if not isinstance(row["provenance"], dict):
        raise ValueError("provenance must be an object")
    return row


class ProfileDocument:
    def __init__(self, data: dict[str, Any]):
        if not isinstance(data, dict):
            raise TypeError("profile data must be a dict")
        _check_tree(data)
        copied = deepcopy(data)
        if (copied.get("kind") != DOCUMENT_KIND or
                isinstance(copied.get("schema_version"), bool) or
                not isinstance(copied.get("schema_version"), int) or
                copied.get("schema_version") != SCHEMA_VERSION):
            raise ValueError("unsupported profile document kind or schema_version")
        required = {"kind", "schema_version", "profile_id", "configuration_fingerprint", "created_at",
                    "hardware", "software", "allocation", "calibrations", "discovery", "provenance"}
        missing = sorted(required - copied.keys())
        if missing:
            raise ValueError(f"profile missing fields: {', '.join(missing)}")
        unknown = sorted(copied.keys() - required)
        if unknown:
            raise ValueError(f"unknown profile fields: {', '.join(unknown)}")
        for key in ("hardware", "software", "allocation", "discovery", "provenance"):
            if not isinstance(copied[key], dict):
                raise ValueError(f"{key} must be an object")
        if not isinstance(copied["calibrations"], list):
            raise ValueError("calibrations must be a list")
        if len(copied["calibrations"]) > 256:
            raise ValueError("profile exceeds 256 calibration conditions")
        expected_config = _configuration_fingerprint(copied["hardware"], copied["software"], copied["allocation"])
        if copied["configuration_fingerprint"] != expected_config:
            raise ValueError("configuration_fingerprint mismatch")
        if copied["profile_id"] != _hash(_content(copied)):
            raise ValueError("profile_id content hash mismatch")
        copied["calibrations"] = [_validate_calibration(row) for row in copied["calibrations"]]
        # Verify the immutable identity after normalizing validated records.
        if copied["profile_id"] != _hash(_content(copied)):
            raise ValueError("profile_id content hash mismatch")
        self._data = copied

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "ProfileDocument":
        _check_tree(value)
        return cls(wire_decode(value))

    @classmethod
    def load(cls, path: str | os.PathLike[str]) -> "ProfileDocument":
        return cls.from_dict(read_document(path))

    @classmethod
    def create(cls, hardware: dict[str, Any], software: dict[str, Any], allocation: dict[str, Any],
               calibrations=(), discovery=None, provenance=None) -> "ProfileDocument":
        calibration_rows = list(calibrations)
        if len(calibration_rows) > 256:
            raise ValueError("profile exceeds 256 calibration conditions")
        _check_tree({"hardware": hardware, "software": software, "allocation": allocation,
                     "calibrations": calibration_rows, "discovery": discovery or {},
                     "provenance": provenance or {}})
        data = {
            "kind": DOCUMENT_KIND,
            "schema_version": SCHEMA_VERSION,
            "created_at": _now(),
            "hardware": deepcopy(hardware),
            "software": deepcopy(software),
            "allocation": deepcopy(allocation),
            "calibrations": [_validate_calibration(row) for row in calibration_rows],
            "discovery": deepcopy(discovery or {}),
            "provenance": deepcopy(provenance or {}),
        }
        data["configuration_fingerprint"] = _configuration_fingerprint(data["hardware"], data["software"], data["allocation"])
        data["profile_id"] = _hash(_content(data))
        return cls(data)

    @classmethod
    def from_legacy(cls, profile) -> "ProfileDocument":
        raw = asdict(profile) if is_dataclass(profile) else deepcopy(profile)
        known_hardware = {key: raw.get(key) for key in ("cpu_count", "cpu_model", "ram_total_bytes", "ram_available_bytes", "devices", "network_interfaces")}
        hardware = {**known_hardware,
                    "os_name": raw.get("os_name"), "os_release": raw.get("os_release"),
                    "machine": (raw.get("metadata") or {}).get("machine")}
        software = {"python_version": raw.get("python_version"), "configuration_hash": raw.get("configuration_hash")}
        allocation = {"scope": "unknown", "ranks": None, "threads": None}
        missing = ["allocation details", "compiler/build/OpenMP fingerprint", "NUMA topology",
                   "per-rank memory limits", "MPI library/runtime version", "CPU/thread load conditions",
                   "filesystem mount and cache conditions", "per-trial raw measurements",
                   "calibration provenance", "trial timestamps", "confidence interval semantics"]
        calibrations = []
        for sample in raw.get("benchmarks", ()):
            missing.append("legacy aggregate calibration not imported as a measured trial")
        return cls.create(hardware, software, allocation, calibrations=(),
                          discovery={"legacy_version": raw.get("version"),
                                     "legacy_profile_id": raw.get("profile_id"),
                                     "legacy_created_at": raw.get("created_at")},
                          provenance={"adapter": "lifecycle.HardwareProfile", "loss_report": sorted(set(missing))})

    @property
    def data(self) -> dict[str, Any]:
        return deepcopy(self._data)

    @property
    def profile_id(self) -> str:
        return self._data["profile_id"]

    @property
    def configuration_fingerprint(self) -> str:
        return self._data["configuration_fingerprint"]

    @property
    def calibrations(self) -> tuple[dict[str, Any], ...]:
        return tuple(deepcopy(self._data["calibrations"]))

    @property
    def loss_report(self) -> tuple[str, ...]:
        return tuple(self._data["provenance"].get("loss_report", ()))

    def match(self, condition: dict[str, Any]) -> dict[str, Any] | None:
        normalized = canonical_condition(condition)
        key = condition_key(normalized)
        matches = []
        for row in self._data["calibrations"]:
            if row["condition"] != normalized or row["condition_key"] != key:
                continue
            if row["evidence"] != "measured" or row["quality"] != "validated":
                continue
            if row["scope"] not in {"same-host", "cross-host"}:
                continue
            if row["provenance"].get("legacy_aggregate") or row["provenance"].get("external_backend"):
                continue
            matches.append(row)
        if not matches:
            return None
        latest = max(matches, key=lambda row: _parse_timestamp(row["timestamp"]))
        return deepcopy(latest)

    def to_dict(self) -> dict[str, Any]:
        return wire_encode(self._data)

    def save(self, path: str | os.PathLike[str], overwrite: bool = False) -> None:
        write_document(path, self._data, overwrite=overwrite)


def probe_profile(allocation=None, storage_paths=()) -> ProfileDocument:
    """Read-only host discovery without initializing MPI or running benchmarks."""
    from .lifecycle import HardwareProfiler

    legacy = HardwareProfiler(storage_paths or ("/",)).discover()
    raw = asdict(legacy)
    hardware = {
        "host_id": hashlib.sha256(str(raw.get("hostname", "unknown")).encode()).hexdigest(),
        "machine": (raw.get("metadata") or {}).get("machine"),
        "cpu_count": raw.get("cpu_count"),
        "cpu_model": raw.get("cpu_model"),
        "ram_total_bytes": raw.get("ram_total_bytes"),
        "ram_available_bytes": raw.get("ram_available_bytes"),
        "devices": raw.get("devices", []),
        "network_interfaces": raw.get("network_interfaces", []),
        "os_name": raw.get("os_name"),
        "os_release": raw.get("os_release"),
        "performance_cores": None,
        "efficiency_cores": None,
    }
    if platform.system() == "Darwin":
        try:
            brand = subprocess.run(["sysctl", "-n", "machdep.cpu.brand_string"],
                                   check=True,capture_output=True,text=True,timeout=2).stdout.strip()
            if brand: hardware["cpu_model"] = brand
        except (OSError, subprocess.SubprocessError):
            pass
        try:
            output = subprocess.run(["sysctl", "-n", "hw.perflevel0.logicalcpu", "hw.perflevel1.logicalcpu"],
                                    check=True, capture_output=True, text=True, timeout=2).stdout.splitlines()
            if len(output) >= 2:
                hardware["performance_cores"], hardware["efficiency_cores"] = map(int, output[:2])
        except (OSError, subprocess.SubprocessError, ValueError):
            pass
    versions = {}
    for package in ("numpy", "mpi4py"):
        try:
            versions[package] = importlib.metadata.version(package)
        except importlib.metadata.PackageNotFoundError:
            versions[package] = None
    thread_env = ("PYTHONHASHSEED", "SEED", "SVSIM_SEED", "OMP_NUM_THREADS", "OMP_DYNAMIC", "OPENBLAS_NUM_THREADS",
                  "MKL_NUM_THREADS", "VECLIB_MAXIMUM_THREADS", "NUMEXPR_NUM_THREADS")
    software = {"python_version": sys.version.split()[0], "os": platform.platform(),
                "architecture": platform.machine(), "package_versions": versions,
                "mpi_initialized": False,
                "thread_environment": {name: os.environ.get(name) for name in thread_env}}
    repo_root = Path(__file__).resolve().parents[2]
    provenance = _git_snapshot(repo_root)
    build_manifest = {name: value for name, value in provenance["dirty_manifest"].items()
                      if name.startswith("src/") or name in {"pyproject.toml", "uv.lock"}}
    software["build"] = {"git_revision": provenance["git_revision"],
                         "dirty_source_hash": _hash(build_manifest),
                         "uv_lock_sha256": provenance["uv_lock_sha256"]}
    provenance["seed_thread_environment"] = {name: os.environ.get(name) for name in thread_env}
    return ProfileDocument.create(hardware, software, deepcopy(allocation or {"scope": "local"}),
                                  discovery={"method": "HardwareProfiler.discover", "legacy_profile_id": raw.get("profile_id"),
                                             "read_only": True,
                                             "unknown_reasons": [{"field":key,"reason":"not exposed by the host profiler or unavailable under current permissions"}
                                                 for key in ("ram_total_bytes","ram_available_bytes","performance_cores","efficiency_cores")
                                                 if hardware[key] is None]},
                                  provenance={"source": "discovered", **provenance})


def _git_snapshot(root: Path) -> dict[str, Any]:
    """Collect bounded source/lock provenance using read-only git commands."""
    result: dict[str, Any] = {"git_revision": None, "dirty_manifest": {},
                              "dirty_manifest_hash": None, "uv_lock_sha256": None}

    def git(*args):
        try:
            proc = subprocess.run(["git", "-C", str(root), *args], check=True,
                                  capture_output=True, text=True, timeout=3)
            return proc.stdout
        except (OSError, subprocess.SubprocessError):
            return None

    result["git_revision"] = (git("rev-parse", "HEAD") or "").strip() or None
    changed = set()
    for args in (("diff", "--name-only", "HEAD"), ("ls-files", "--others", "--exclude-standard")):
        output = git(*args)
        if output:
            changed.update(line for line in output.splitlines() if line)
    manifest = {}
    remaining = 4 * 1024 * 1024
    for relative in sorted(changed):
        candidate = (root / relative).resolve()
        try:
            candidate.relative_to(root.resolve())
            size = candidate.stat().st_size
            if not candidate.is_file() or size > min(1024 * 1024, remaining):
                manifest[relative] = {"sha256": None, "status": "omitted-size-or-type", "size_bytes": size}
                continue
            contents = candidate.read_bytes()
        except (OSError, ValueError):
            manifest[relative] = {"sha256": None, "status": "unreadable"}
            continue
        remaining -= len(contents)
        manifest[relative] = {"sha256": hashlib.sha256(contents).hexdigest(), "size_bytes": size}
    result["dirty_manifest"] = manifest
    result["dirty_manifest_hash"] = _hash(manifest)
    lock = root / "uv.lock"
    try:
        if lock.is_file() and lock.stat().st_size <= 4 * 1024 * 1024:
            result["uv_lock_sha256"] = hashlib.sha256(lock.read_bytes()).hexdigest()
    except OSError:
        pass
    return result
