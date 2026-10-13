import math

import pytest

from svsim.gradient_scheduler import GradientScheduler, MethodCost, shift_rule
from svsim.ir import GateNormalizer, GateOp, GlobalSequencer
from svsim.parameters import ParameterBinding, ParameterSymbol, expression


def circuit(theta):
    norm = GateNormalizer(4)
    seq = GlobalSequencer()
    return (
        seq.append(norm.gate("RX", (0,), angle=2 * theta + theta)),
        seq.append(norm.gate("RZ", (1,), angle=theta * theta)),
    )


def costs(*, adjoint=(10, 8, 12, 8, 16), recompute=(6, 6, 9, 4, 20), shift=(4, 3, 5, 8, 2), confidence=0.99):
    def make(spec):
        mean, low, high, ram, disk = spec
        return MethodCost(mean, low, high, ram, disk, confidence=confidence)
    return {"adjoint": make(adjoint), "recompute": make(recompute), "parameter-shift": make(shift)}


def test_occurrences_are_per_gate_and_chain_factors_use_binding():
    theta = ParameterSymbol("theta", "theta-id")
    binding = ParameterBinding("circuit", ((theta, 0.25),))
    plan = GradientScheduler().choose(circuit(theta), (theta,), binding, method="adjoint")
    assert [item.source_id for item in plan.occurrences] == [0, 1]
    assert [item.chain_factor for item in plan.occurrences] == [3.0, 0.5]
    assert all(item.parameter_id == "theta-id" for item in plan.occurrences)
    assert all(item.shifts is not None for item in plan.occurrences)


def test_certified_uncontrolled_and_controlled_shift_rules():
    theta = ParameterSymbol("theta", "t")
    norm = GateNormalizer(3)
    plain = norm.gate("PAULI_ROT", (0, 1), angle=theta, paulis=("X", "Z"))
    expected_plain = ((math.pi / 2, 0.5), (-math.pi / 2, -0.5))
    assert all(actual == pytest.approx(expected) for actual, expected in zip(shift_rule(plain), expected_plain))
    controlled = norm.gate("RY", (0,), controls=(1,), angle=theta)
    rule = shift_rule(controlled)
    coeff = (0.5 - math.sqrt(0.5)) / 2
    expected_controlled = ((math.pi / 2, .5), (-math.pi / 2, -.5), (math.pi, coeff), (-math.pi, -coeff))
    assert all(actual == pytest.approx(expected) for actual, expected in zip(rule, expected_controlled))


def test_auto_uses_lowest_confident_feasible_upper_bound():
    theta = ParameterSymbol("theta", "t")
    binding = ParameterBinding("c", ((theta, 0.1),))
    costs_by_method = costs()
    plan = GradientScheduler().choose(circuit(theta), (theta,), binding, costs=costs_by_method,
                                       available_ram_bytes=8, available_disk_bytes=4)
    assert plan.method == "parameter-shift"
    assert plan.cost_interval == (3, 5)
    assert plan.reason.startswith("Lowest measured")


def test_auto_defaults_to_adjoint_when_estimates_are_incomplete_or_uncertain():
    theta = ParameterSymbol("theta", "t")
    binding = ParameterBinding("c", ((theta, 0.1),))
    scheduler = GradientScheduler()
    incomplete = {"adjoint": costs()["adjoint"]}
    assert scheduler.choose(circuit(theta), (theta,), binding, costs=incomplete).method == "adjoint"
    uncertain = costs(confidence=0.8)
    plan = scheduler.choose(circuit(theta), (theta,), binding, costs=uncertain)
    assert plan.method == "adjoint"
    assert "fallback" in plan.reason


def test_resource_admission_and_explicit_unsupported_generator():
    theta = ParameterSymbol("theta", "t")
    binding = ParameterBinding("c", ((theta, 0.1),))
    with pytest.raises(ValueError, match="budget"):
        GradientScheduler().choose(circuit(theta), (theta,), binding, method="adjoint",
                                   costs={"adjoint": MethodCost(1, 1, 1, 10, 0)},
                                   available_ram_bytes=9)
    unsupported = GateOp("DENSE_UNITARY", (0,), angle=expression(theta), op_id=8)
    assert GradientScheduler().choose((unsupported,), (theta,), binding).method == "adjoint"
    with pytest.raises(ValueError, match="unsupported generator"):
        GradientScheduler().choose((unsupported,), (theta,), binding, method="parameter-shift")


def test_rejects_finite_difference_and_bad_resource_or_cost_values():
    theta = ParameterSymbol("theta", "t")
    binding = ParameterBinding("c", ((theta, 0.1),))
    with pytest.raises(ValueError, match="unsupported gradient method"):
        GradientScheduler().choose(circuit(theta), (theta,), binding, method="finite-difference")
    with pytest.raises(ValueError):
        GradientScheduler().choose(circuit(theta), (theta,), binding, available_disk_bytes=-1)
    with pytest.raises(ValueError):
        MethodCost(1, 2, 1, 0, 0)
    with pytest.raises(ValueError):
        MethodCost(1, 1, 1, -1, 0)


def test_mapping_binding_and_multiple_shared_parameter_occurrences():
    theta = ParameterSymbol("theta", "t")
    plan = GradientScheduler().choose(circuit(theta), ("t",), {"t": .25}, method="parameter-shift")
    assert len(plan.occurrences) == 2
    assert plan.binding_id
    assert plan.occurrences[0].shifts == shift_rule(circuit(theta)[0])
