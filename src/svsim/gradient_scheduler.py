"""Pure gradient-method planning and certified parameter-shift rules."""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import math
from typing import Mapping, Sequence

from .ir import GateOp
from .parameters import ParameterBinding, ParameterSymbol


_METHODS = ("adjoint", "recompute", "parameter-shift")
_AUTO_CONFIDENCE = 0.95


def shift_rule(gate: GateOp) -> tuple[tuple[float, float], ...]:
    """Return signed occurrence-local shift/weight pairs for a certified gate.

    The tuple encodes ``sum(weight * f(theta + shift))``. Frequencies are 1
    for an uncontrolled Pauli rotation and {1/2, 1} for controlled rotations.
    """
    if gate.angle is None:
        raise ValueError("gate has no parameterized angle")
    if gate.kind not in {"RX", "RY", "RZ", "PAULI_ROT"}:
        raise ValueError(f"no certified shift rule for {gate.kind}")
    if gate.kind == "PAULI_ROT" and gate.controls:
        raise ValueError("controlled PAULI_ROT shift rule is not certified")
    if gate.controls:
        c = (0.5 - math.sqrt(0.5)) / 2.0
        return ((math.pi / 2, 0.5), (-math.pi / 2, -0.5),
                (math.pi, c), (-math.pi, -c))
    return ((math.pi / 2, 0.5), (-math.pi / 2, -0.5))


@dataclass(frozen=True)
class MethodCost:
    """Measured method cost with a confidence interval and resource peak."""
    mean_seconds: float
    lower_seconds: float
    upper_seconds: float
    ram_bytes: int
    disk_bytes: int
    confidence: float = 1.0
    measured: bool = True

    def __post_init__(self) -> None:
        vals = (self.mean_seconds, self.lower_seconds, self.upper_seconds, self.confidence)
        if any(not math.isfinite(float(v)) for v in vals):
            raise ValueError("cost interval and confidence must be finite")
        if self.lower_seconds < 0 or self.lower_seconds > self.mean_seconds or self.mean_seconds > self.upper_seconds:
            raise ValueError("invalid cost confidence interval")
        if not 0 <= self.confidence <= 1:
            raise ValueError("confidence must be between zero and one")
        if any(isinstance(v, bool) or not isinstance(v, int) for v in (self.ram_bytes, self.disk_bytes)):
            raise TypeError("resource estimates must be integer bytes")
        if self.ram_bytes < 0 or self.disk_bytes < 0:
            raise ValueError("resource estimates cannot be negative")
        if not isinstance(self.measured, bool):
            raise TypeError("measured must be bool")


@dataclass(frozen=True)
class GradientOccurrence:
    source_id: int
    parameter_id: str
    chain_factor: float
    shifts: tuple[tuple[float, float], ...] | None


@dataclass(frozen=True)
class GradientSchedule:
    method: str
    occurrences: tuple[GradientOccurrence, ...]
    reason: str
    binding_id: str
    estimated_seconds: float | None = None
    cost_interval: tuple[float, float] | None = None
    ram_bytes: int | None = None
    disk_bytes: int | None = None
    confidence: float | None = None


class _BindingView:
    def __init__(self, values: Mapping[str, float]):
        self._values = {str(k): float(v) for k, v in values.items()}
        if any(not math.isfinite(v) for v in self._values.values()):
            raise ValueError("binding values must be finite")

    def value(self, parameter_id: str) -> float:
        try:
            return self._values[parameter_id]
        except KeyError as exc:
            raise ValueError(f"binding is missing parameter {parameter_id}") from exc

    @property
    def binding_id(self) -> str:
        data = json.dumps(sorted(self._values.items()), separators=(",", ":"))
        return hashlib.sha256(data.encode()).hexdigest()


class GradientScheduler:
    """Choose a gradient method from exact applicability and supplied costs."""

    def choose(self, gates: Sequence[GateOp], requested: Sequence[str | ParameterSymbol],
               binding: ParameterBinding | Mapping[str, float], method: str = "auto",
               costs: Mapping[str, MethodCost] | None = None, *,
               available_ram_bytes: int | None = None,
               available_disk_bytes: int | None = None) -> GradientSchedule:
        if method not in {*_METHODS, "auto"}:
            raise ValueError(f"unsupported gradient method {method!r}")
        for name, amount in (("available_ram_bytes", available_ram_bytes),
                             ("available_disk_bytes", available_disk_bytes)):
            if amount is not None and (isinstance(amount, bool) or not isinstance(amount, int)):
                raise TypeError(f"{name} must be an integer")
            if amount is not None and amount < 0:
                raise ValueError(f"{name} must be non-negative")

        if isinstance(binding, Mapping):
            binding_view = _BindingView(binding)
        elif isinstance(binding, ParameterBinding):
            binding_view = binding
        else:
            raise TypeError("binding must be ParameterBinding or a parameter-id mapping")
        requested_ids: list[str] = []
        for item in requested:
            parameter_id = item.parameter_id if isinstance(item, ParameterSymbol) else str(item)
            if not parameter_id:
                raise ValueError("requested parameter IDs cannot be empty")
            if parameter_id in requested_ids:
                raise ValueError("requested parameter IDs must be unique")
            requested_ids.append(parameter_id)

        occurrences: list[GradientOccurrence] = []
        shift_supported = True
        gate_sequence = tuple(gates)
        source_ids = [gate.op_id if gate.op_id >= 0 else ordinal for ordinal, gate in enumerate(gate_sequence)]
        if len(set(source_ids)) != len(source_ids):
            raise ValueError("gate source occurrence IDs must be unique")
        for ordinal, gate in enumerate(gate_sequence):
            if gate.angle is None:
                continue
            gate_source = gate.op_id if gate.op_id >= 0 else ordinal
            active = [pid for pid in requested_ids if pid in gate.dependencies]
            if not active:
                continue
            try:
                rule = shift_rule(gate)
            except ValueError:
                rule = None
                shift_supported = False
            for parameter_id in active:
                factor = gate.angle.derivative(parameter_id).evaluate(binding_view)
                if not math.isfinite(factor):
                    raise ValueError("chain-rule factor must be finite")
                if factor != 0:
                    occurrences.append(GradientOccurrence(gate_source, parameter_id, float(factor), rule))

        supplied_costs = dict(costs or {})
        unknown_costs = set(supplied_costs) - set(_METHODS)
        if unknown_costs:
            raise ValueError(f"unknown method cost keys: {sorted(unknown_costs)}")
        if any(not isinstance(cost, MethodCost) for cost in supplied_costs.values()):
            raise TypeError("costs must map methods to MethodCost instances")

        def feasible(name: str) -> bool:
            estimate = supplied_costs.get(name)
            if estimate is None:
                return True
            return ((available_ram_bytes is None or estimate.ram_bytes <= available_ram_bytes) and
                    (available_disk_bytes is None or estimate.disk_bytes <= available_disk_bytes))

        if method == "parameter-shift" and not shift_supported:
            raise ValueError("parameter-shift requested for an unsupported generator")

        chosen = method
        reason: str
        if method == "auto":
            applicable = ["adjoint", "recompute"]
            if shift_supported:
                applicable.append("parameter-shift")
            complete = all(name in supplied_costs for name in applicable)
            confident = complete and all(
                supplied_costs[name].measured and supplied_costs[name].confidence >= _AUTO_CONFIDENCE
                for name in applicable
            )
            if confident:
                candidates = [name for name in applicable if feasible(name)]
                if not candidates:
                    raise ValueError("no gradient method fits the supplied RAM/disk budget")
                chosen = min(candidates, key=lambda name: (supplied_costs[name].upper_seconds,
                                                           _METHODS.index(name)))
                reason = "Lowest measured confidence-interval upper bound among feasible methods"
            else:
                chosen = "adjoint"
                if not feasible(chosen):
                    raise ValueError("adjoint fallback does not fit the supplied RAM/disk budget")
                reason = "Incomplete or low-confidence costs; conservative adjoint fallback"
        else:
            if not feasible(chosen):
                raise ValueError(f"requested {chosen} method exceeds the supplied RAM/disk budget")
            reason = f"Explicit {chosen} method request"

        estimate = supplied_costs.get(chosen)
        if chosen == "parameter-shift" and not shift_supported:
            raise ValueError("parameter-shift requested for an unsupported generator")
        return GradientSchedule(
            method=chosen,
            occurrences=tuple(occurrences),
            reason=reason,
            binding_id=binding_view.binding_id,
            estimated_seconds=estimate.mean_seconds if estimate else None,
            cost_interval=(estimate.lower_seconds, estimate.upper_seconds) if estimate else None,
            ram_bytes=estimate.ram_bytes if estimate else None,
            disk_bytes=estimate.disk_bytes if estimate else None,
            confidence=estimate.confidence if estimate else None,
        )
