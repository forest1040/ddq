import numpy as np
import pytest

from svsim.distributed_kernels import (
    active_offsets, pack_active, partial_mcx_pair, pauli_inner,
    apply_pauli_action, inverse_gate,
)
from svsim.ir import GateNormalizer
from svsim.unitary import MatrixAssetRegistry


def test_active_offsets_are_lazy_ordered_and_validate_lower_bits():
    it = active_offsets(5, (0, 3), (1, 0))
    assert iter(it) is it
    assert list(it) == [1, 3, 5, 7, 17, 19, 21, 23]
    assert list(active_offsets(3)) == list(range(8))
    with pytest.raises(ValueError):
        list(active_offsets(2, (2,), (1,)))


def test_pack_active_writes_prefix_only():
    src = np.arange(16)
    out = np.full(8, -1)
    count = pack_active(src, out, active_offsets(4, (1, 3), (0, 1)))
    assert count == 4
    np.testing.assert_array_equal(out, [8, 9, 12, 13, -1, -1, -1, -1])


@pytest.mark.parametrize("controls,values", [((), ()), ((0, 2), (1, 0)), ((1,), (0,))])
def test_partial_mcx_pair_exchanges_only_matching_offsets(controls, values):
    a = np.arange(8, dtype=complex) + 1j
    b = 10 + np.arange(8, dtype=complex) - 2j
    a0, b0 = a.copy(), b.copy()
    oa, ob = np.empty_like(a), np.empty_like(b)
    partial_mcx_pair(a, b, oa, ob, controls, values)
    expected_a, expected_b = a.copy(), b.copy()
    for i in active_offsets(3, controls, values):
        expected_a[i], expected_b[i] = b[i], a[i]
    np.testing.assert_array_equal(oa, expected_a)
    np.testing.assert_array_equal(ob, expected_b)
    np.testing.assert_array_equal(a, a0); np.testing.assert_array_equal(b, b0)


def _dense_pauli(letters):
    mats = {
        "I": np.eye(2, dtype=complex),
        "X": np.array([[0, 1], [1, 0]], complex),
        "Y": np.array([[0, -1j], [1j, 0]], complex),
        "Z": np.diag([1, -1]).astype(complex),
    }
    # vectors use bit zero as LSB; Kronecker factors therefore run high to low.
    out = np.array([[1]], complex)
    for letter in reversed(letters):
        out = np.kron(out, mats[letter])
    return out


def _masks(letters):
    x = sum(1 << q for q, p in enumerate(letters) if p in "XY")
    z = sum(1 << q for q, p in enumerate(letters) if p in "YZ")
    return x, z


@pytest.mark.parametrize("letters", [("Y",), ("X", "Y"), ("Z", "I", "Y"), ("I", "I")])
def test_pauli_inner_matches_dense_random_complex_state(letters):
    rng = np.random.default_rng(501 + len(letters))
    b = len(letters) - 1
    # exercise one upper X/Y bit when the string is wide enough
    block_bits = max(1, len(letters) - 1)
    dim = 1 << len(letters)
    psi = rng.normal(size=dim) + 1j * rng.normal(size=dim)
    bra = rng.normal(size=dim) + 1j * rng.normal(size=dim)
    x, z = _masks(letters)
    # Sum per source block with its destination partner.
    got = 0j
    nblocks = 1 << (len(letters) - block_bits)
    for h in range(nblocks):
        partner = h ^ (x >> block_bits)
        got += pauli_inner(bra[partner << block_bits:(partner + 1) << block_bits],
                           psi[h << block_bits:(h + 1) << block_bits],
                           x, z, h, block_bits)
    expected = np.vdot(bra, _dense_pauli(letters) @ psi)
    assert got == pytest.approx(expected, abs=1e-12)


def test_pauli_inner_control_projector_uses_source_index():
    rng = np.random.default_rng(8)
    ket = rng.normal(size=8) + 1j * rng.normal(size=8)
    bra = rng.normal(size=8) + 1j * rng.normal(size=8)
    x, z = 1, 1  # Y on local bit zero
    got = pauli_inner(bra, ket, x, z, 0, 3, controls=(2, 1), control_values=(0, 1))
    dense = _dense_pauli(("Y", "I", "I"))
    projected = np.array([ket[i] if ((i >> 2) & 1) == 0 and ((i >> 1) & 1) == 1 else 0 for i in range(8)])
    assert got == pytest.approx(np.vdot(bra, dense @ projected), abs=1e-12)


@pytest.mark.parametrize("letters", [("Y",), ("X", "Y"), ("Z", "I", "Y")])
def test_apply_pauli_action_matches_global_dense_partner_block(letters):
    rng = np.random.default_rng(91)
    n = len(letters); b = max(1, n - 1)
    x, z = _masks(letters)
    nblocks = 1 << (n - b)
    state = rng.normal(size=1 << n) + 1j * rng.normal(size=1 << n)
    full = _dense_pauli(letters) @ state
    for dest in range(nblocks):
        srcblock = dest ^ (x >> b)
        src = state[srcblock << b:(srcblock + 1) << b].copy()
        before = src.copy()
        out = np.empty(1 << b, complex)
        apply_pauli_action(src, out, x, z, dest, b, coefficient=0.37 - 0.2j)
        np.testing.assert_allclose(src, before)
        np.testing.assert_allclose(out, (0.37 - 0.2j) * full[dest << b:(dest + 1) << b])


def test_apply_pauli_action_accumulates_and_rejects_alias():
    src = np.array([1 + 2j, 3 - 1j])
    out = np.array([2, 4], complex)
    apply_pauli_action(src, out, 1, 1, 0, 1, coefficient=2, accumulate=True)
    expected = np.array([2, 4], complex) + 2 * (_dense_pauli(("Y",)) @ src)
    np.testing.assert_allclose(out, expected)
    with pytest.raises(ValueError):
        apply_pauli_action(src, src, 0, 0, 0, 1)


def test_inverse_gate_angles_fixed_gates_and_dense_asset():
    normalizer = GateNormalizer(3)
    gate = normalizer.gate("RY", (1,), angle=0.25)
    inv = inverse_gate(gate, normalizer.assets)
    assert inv.angle.evaluate(None) == pytest.approx(-0.25)
    assert inv.dependencies == gate.dependencies and inv.targets == gate.targets
    assert inverse_gate(normalizer.gate("T", (0,)), normalizer.assets).kind == "TDG"
    assert inverse_gate(normalizer.gate("MCX", (2,), controls=(0,), control_values=(0,)), normalizer.assets) == normalizer.gate("MCX", (2,), controls=(0,), control_values=(0,))
    assets = MatrixAssetRegistry()
    matrix = np.array([[1, 1j], [1j, 1]], complex) / np.sqrt(2)
    dense_gate = GateNormalizer(1, assets).gate("DENSE_UNITARY", (0,), matrix=matrix)
    inverse = inverse_gate(dense_gate, assets)
    np.testing.assert_allclose(assets.get(inverse.matrix_asset_ref).matrix, matrix.conj().T)
    assert dense_gate.matrix_asset_ref != inverse.matrix_asset_ref
