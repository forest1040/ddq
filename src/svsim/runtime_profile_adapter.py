"""Strict matching from immutable profile cells to runtime conditions."""

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
import math

from .calibration_conditions import canonical_condition, condition_key
from .runtime_cost import CostSample


_VOLATILE_CONTEXT_KEYS = {"run_id", "job_id", "root", "root_id", "root_version",
                          "event_id", "group_id", "started_at", "ended_at", "timestamp",
                          "observed_at", "measured_at", "created_at"}


def _stable_context(value):
    if isinstance(value, dict):
        return {str(key): _stable_context(child) for key, child in value.items()
                if str(key).lower() not in _VOLATILE_CONTEXT_KEYS and
                str(key) != "configuration_fingerprint"}
    if isinstance(value, (list, tuple)):
        return [_stable_context(child) for child in value]
    return value


@dataclass(frozen=True)
class ProfileMatch:
    row: dict
    profile_id: str
    configuration_fingerprint: str


class RuntimeProfileAdapter:
    def __init__(self, document, *, configuration_fingerprint: str | None = None,
                 scope: str = "same-host"):
        self.document = document
        self.fingerprint = configuration_fingerprint or document.configuration_fingerprint
        self.scope = scope

    def match(self, context, condition, required_quality="validated", *, scope=None):
        if required_quality not in {"validated", "statistical", "conditional-envelope"}:
            raise ValueError("unknown required quality")
        # Full context equality is intentional: a partial context cannot certify a cell.
        context_fingerprint = context.get("configuration_fingerprint") if isinstance(context, dict) else None
        if context_fingerprint != self.fingerprint:
            return None
        if not isinstance(context, dict):
            return None
        normalized = canonical_condition(condition)
        key = condition_key(normalized)
        wanted_scope = scope or self.scope
        matches = []
        for row in self.document.calibrations:
            if row["condition"] != normalized or row["condition_key"] != key:
                continue
            if row["evidence"] != "measured" or row["quality"] != "validated":
                continue
            if row["scope"] != wanted_scope:
                continue
            provenance = row["provenance"]
            declared_context = provenance.get("execution_context")
            if (not isinstance(declared_context, dict) or
                    _stable_context(declared_context) != _stable_context(context)):
                continue
            if (provenance.get("legacy_aggregate") or provenance.get("external_backend") or
                    provenance.get("manual") or provenance.get("injected") or
                    provenance.get("evidence") in {"injected", "external", "assumed", "manual"}):
                continue
            # Statistical claims must be explicit and method-bearing.
            if required_quality == "statistical" and not row.get("statistical_summary", {}).get("method"):
                continue
            if required_quality == "conditional-envelope":
                envelope = provenance.get("certified_envelope")
                coverage = envelope.get("held_out_coverage") if isinstance(envelope, dict) else None
                if (not isinstance(envelope, dict) or not envelope.get("method") or
                        isinstance(coverage, bool) or not isinstance(coverage, (int, float)) or
                        not math.isfinite(coverage) or not 0 < coverage <= 1 or
                        envelope.get("interval_kind") != "conditional-prediction" or
                        envelope.get("validation") != "held-out" or
                        envelope.get("condition_key") != key or
                        envelope.get("configuration_fingerprint") != self.fingerprint or
                        envelope.get("scope") != wanted_scope or
                        envelope.get("units") != "seconds/call" or
                        isinstance(envelope.get("held_out_count"), bool) or
                        not isinstance(envelope.get("held_out_count"), int) or
                        envelope.get("held_out_count", 0) < 5 or
                        isinstance(envelope.get("held_out_success_count"), bool) or
                        not isinstance(envelope.get("held_out_success_count"), int) or
                        not 0 <= envelope.get("held_out_success_count", -1) <= envelope.get("held_out_count", 0) or
                        abs(coverage - envelope.get("held_out_success_count", 0) /
                            envelope.get("held_out_count", 1)) > 1e-12):
                    continue
            matches.append(row)
        if not matches:
            return None
        row = matches[-1]
        return ProfileMatch(deepcopy(row), self.document.profile_id,
                            self.document.configuration_fingerprint)

    def cost_sample(self, context, condition, *, scope=None):
        """Return only explicit certified envelopes; extrema are diagnostics, never bounds."""
        matched = self.match(context, condition, "conditional-envelope", scope=scope)
        if matched is None:
            return None
        envelope = matched.row["provenance"]["certified_envelope"]
        values = [envelope.get(name) for name in ("lower_seconds", "expected_seconds", "upper_seconds")]
        if any(isinstance(value, bool) or not isinstance(value, (int, float)) or
               not math.isfinite(value) or value < 0 for value in values):
            return None
        lower, expected, upper = values
        if not lower <= expected <= upper:
            return None
        return CostSample(condition=condition_key(canonical_condition(condition)),
                          lower_seconds=float(lower), mean_seconds=float(expected),
                          upper_seconds=float(upper), measured=True)
