from fractions import Fraction
import math

import pytest

from svsim.parameters import ExactPi, Expr, ParameterBinding, ParameterSymbol, exact_pi_coefficient, expression


def test_exact_pi_is_normalized_expression_and_derivative_is_zero():
    p = ExactPi(2, -4)
    assert p == ExactPi(-1, 2)
    assert expression(p).evaluate(ParameterBinding('c')) == -math.pi / 2
    assert expression(p).dependencies == frozenset()
    assert exact_pi_coefficient(expression(p), ParameterBinding('c')) == Fraction(-1, 2)
    assert expression(p).derivative('unused').evaluate(ParameterBinding('c')) == 0


def test_binding_keeps_legacy_hash_without_witness_and_versions_witness_hash():
    legacy = ParameterBinding('c', (('theta', math.pi / 2),))
    witnessed = ParameterBinding('c', (('theta', ExactPi(1, 2)),))
    assert legacy.binding_id == ParameterBinding('c', (('theta', math.pi / 2),)).binding_id
    assert legacy.exact_witnesses == ()
    assert witnessed.exact_witnesses == (('theta', ExactPi(1, 2)),)
    assert witnessed.binding_id != legacy.binding_id
    assert witnessed.witness_hash != ParameterBinding('c').witness_hash
    with pytest.raises(ValueError, match='projection mismatch'):
        ParameterBinding('c', (('theta', 0.0),), (('theta', ExactPi(1, 2)),))


def test_shift_preserves_only_provable_exact_witness():
    theta = ParameterSymbol('theta', 'theta')
    binding = ParameterBinding('c', ((theta, ExactPi(1, 2)),))
    moved = binding.shifted('theta', ExactPi(1, 2))
    assert moved.exact_witnesses == (('theta', ExactPi(1)),)
    assert exact_pi_coefficient(expression(theta), moved) == Fraction(1)
    approx = binding.shifted('theta', 0.25)
    assert approx.exact_witnesses == ()


def test_exact_analysis_uses_exact_binary_rationals_not_float_results():
    theta = ParameterSymbol('theta', 'theta')
    binding = ParameterBinding('c', (('theta', 0.5),))
    assert exact_pi_coefficient((theta * ExactPi(3)) / 2, binding) == Fraction(3, 4)
    assert exact_pi_coefficient(expression(ExactPi(1, 3)) * 2, binding) == Fraction(2, 3)
    assert exact_pi_coefficient(expression(1e-300) * ExactPi(1), binding) == Fraction.from_float(1e-300)
    assert exact_pi_coefficient(expression(1.0) / expression(0.0) * ExactPi(1), binding) is None


def test_exact_analysis_proves_rational_identities_and_special_angles():
    binding = ParameterBinding('c')
    pi = expression(ExactPi(1))
    assert exact_pi_coefficient((pi / pi) - 1, binding) == 0
    assert exact_pi_coefficient(expression(2) / expression(2) - 1, binding) == 0
    assert exact_pi_coefficient(expression(0) + pi, binding) == 1
    assert exact_pi_coefficient(Expr('sin', (expression(ExactPi(1, 2)),)) * pi, binding) == 1
    assert exact_pi_coefficient(Expr('cos', (expression(ExactPi(1)),)) * pi, binding) == -1
    assert exact_pi_coefficient(Expr('sin', (expression(ExactPi(1)),)), binding) == 0
    assert exact_pi_coefficient(Expr('cos', (expression(ExactPi(1, 2)),)), binding) == 0


def test_exact_analysis_does_not_turn_float_underflow_into_zero():
    binding = ParameterBinding('c')
    tiny = expression(1e-300)
    proven = exact_pi_coefficient((tiny * tiny) * ExactPi(1), binding)
    assert proven == Fraction.from_float(1e-300) ** 2
    assert proven != 0


def test_duplicate_exact_witnesses_are_rejected():
    with pytest.raises(ValueError, match='Duplicate exact witnesses'):
        ParameterBinding('c', (('theta', ExactPi(1)),),
                         (('theta', ExactPi(1)), ('theta', ExactPi(1))))
