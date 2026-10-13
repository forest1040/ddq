"""Convert exact-context, held-out certified gradient evidence to MethodCost."""
from __future__ import annotations

import hashlib
import json
from typing import Any, Mapping

from .gradient_calibration import (REPORT_PRODUCER, REPORT_SCHEMA_VERSION,
                                   GradientCalibrationReport, context_fingerprint)
from .gradient_scheduler import MethodCost


class GradientCostAdapter:
    """Strict adapter; raw extrema and caller supplied costs never certify a cost."""

    def __init__(self):
        self.last_reason = "NO_REPORT"
        self.rejected_methods: dict[str, str] = {}

    def adapt(self, report: GradientCalibrationReport | Mapping[str, Any] | None,
              context: Mapping[str, Any]) -> dict[str, MethodCost]:
        """Return only explicitly certified, held-out envelopes for exact context."""
        self.last_reason = "NO_REPORT"
        self.rejected_methods = {}
        if report is None:
            return {}
        try:
            if isinstance(report, GradientCalibrationReport):
                data = report.to_dict()
            elif isinstance(report, Mapping):
                data = dict(report)
            else:
                return {}
            if (data.get("schema_version") != REPORT_SCHEMA_VERSION or
                    data.get("producer") != REPORT_PRODUCER or data.get("evidence") != "measured" or
                    data.get("calibration_id") != data.get("context_hash")):
                self.last_reason = "INVALID_PROVENANCE"
                return {}
            claimed_report_hash = data.get("report_hash")
            hash_payload = dict(data)
            hash_payload.pop("report_hash", None)
            actual_report_hash = hashlib.sha256(json.dumps(hash_payload, sort_keys=True,
                                                            separators=(",", ":"),
                                                            allow_nan=False).encode()).hexdigest()
            if not isinstance(claimed_report_hash, str) or claimed_report_hash != actual_report_hash:
                self.last_reason = "REPORT_INTEGRITY_MISMATCH"
                return {}
            if len(json.dumps(data, sort_keys=True, separators=(",", ":"),
                              allow_nan=False).encode()) > 4 * 1024 * 1024:
                self.last_reason = "REPORT_TOO_LARGE"
                return {}
            wanted_hash = context_fingerprint(context)
            if (data.get("context_hash") != wanted_hash or
                    context_fingerprint(data.get("context", {})) != wanted_hash):
                self.last_reason = "CONTEXT_MISMATCH"
                return {}
            observations = data.get("observations")
            envelopes = data.get("certified_envelopes")
            diagnostic = data.get("diagnostic", {})
            if not isinstance(observations, (list, tuple)) or not isinstance(envelopes, Mapping):
                self.last_reason = "INVALID_REPORT"
                return {}
            by_ref = {row.get("raw_ref"): row for row in observations if isinstance(row, Mapping)}
            result: dict[str, MethodCost] = {}
            for method, envelope in envelopes.items():
                if method not in {"adjoint", "recompute", "parameter-shift"} or not isinstance(envelope, Mapping):
                    continue
                refs = envelope.get("held_out_raw_refs")
                training_refs = envelope.get("training_raw_refs")
                if (envelope.get("condition_key") != wanted_hash or envelope.get("validation") != "held-out" or
                        not isinstance(envelope.get("method"), str) or not envelope["method"] or
                        not isinstance(envelope.get("model_id"), str) or not envelope["model_id"] or
                        envelope.get("evidence") != "measured" or
                        envelope.get("scope") not in {"same-host", "cross-host"} or
                        envelope.get("units") != "seconds/gradient" or
                        envelope.get("measurement") not in {"whole-trial", "runner"} or
                        not isinstance(training_refs, (list, tuple)) or not training_refs or
                        not isinstance(refs, (list, tuple)) or len(refs) < 5 or len(set(refs)) != len(refs)):
                    continue
                if set(refs).intersection(training_refs):
                    continue
                rows = [by_ref.get(ref) for ref in refs]
                if any(row is None or row.get("method") != method or row.get("warmup") is not False or
                       row.get("success") is not True or row.get("validated") is not True for row in rows):
                    continue
                if any(row.get("context_before_hash") != wanted_hash or
                       row.get("context_after_hash") != wanted_hash for row in rows):
                    continue
                raw_valid = True
                for row in rows:
                    content = dict(row)
                    claimed_hash = content.pop("raw_hash", None)
                    actual_hash = hashlib.sha256(json.dumps(content, sort_keys=True, separators=(",", ":"),
                                                  allow_nan=False).encode()).hexdigest()
                    raw_valid &= isinstance(claimed_hash, str) and claimed_hash == actual_hash
                if not raw_valid:
                    continue
                lower = envelope.get("lower_seconds")
                mean = envelope.get("expected_seconds")
                upper = envelope.get("upper_seconds")
                confidence = envelope.get("confidence")
                if (any(isinstance(v, bool) or not isinstance(v, (float, int)) for v in
                        (lower, mean, upper, confidence)) or not 0 <= lower <= mean <= upper or
                        not 0.95 <= confidence <= 1):
                    continue
                measurement = envelope["measurement"]
                observed = [row.get("wall_seconds") if measurement == "whole-trial" else row.get("runner_seconds")
                            for row in rows]
                if any(isinstance(value, bool) or not isinstance(value, (float, int)) for value in observed):
                    continue
                coverage = sum(lower <= value <= upper for value in observed) / len(observed)
                if (envelope.get("held_out_count") != len(refs) or
                        envelope.get("held_out_success_count") != len(rows) or
                        envelope.get("held_out_coverage") != coverage or coverage < confidence):
                    continue
                if measurement != "runner":
                    self.rejected_methods[method] = "MEASUREMENT_SCOPE_MISMATCH"
                    continue
                ram, disk = envelope.get("ram_bytes"), envelope.get("disk_bytes")
                if (any(isinstance(v, bool) or not isinstance(v, int) or v < 0 for v in (ram, disk))):
                    continue
                result[method] = MethodCost(float(mean), float(lower), float(upper), ram, disk,
                                            confidence=float(confidence), measured=True)
            self.last_reason = ("CERTIFIED_HELD_OUT" if result else
                                "MEASUREMENT_SCOPE_MISMATCH" if self.rejected_methods else
                                "NO_QUALIFIED_HELD_OUT_ENVELOPE")
            return result
        except (TypeError, ValueError, KeyError):
            self.last_reason = "INVALID_REPORT"
            return {}


def gradient_method_costs(report, context):
    """Convenience function for callers that only need an eligible cost map."""
    return GradientCostAdapter().adapt(report, context)
