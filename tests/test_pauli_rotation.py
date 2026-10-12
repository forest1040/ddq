import numpy as np
import pytest

from svsim.pauli_rotation import SignedPauli, apply_pauli_pair, apply_pauli_rotation


def phase_for(pauli, index):
    x, z, r = pauli.x_mask, pauli.z_mask, pauli.sign_bit
    return (-1) ** (r + (z & index).bit_count()) * (1j ** (x & z).bit_count())


def dense_rotation(state, pauli, angle):
    out = np.empty_like(state, dtype=complex)
    c, s = np.cos(angle / 2), np.sin(angle / 2)
    for i in range(len(state)):
        src = i ^ pauli.x_mask
        out[i] = c * state[i] - 1j * s * phase_for(pauli, src) * state[src]
    return out


@pytest.mark.parametrize("x,z,r", [
    (0, 0, 0), (0, 5, 1), (3, 3, 0), (5, 3, 1), (7, 7, 1),
    (1, 6, 0), (6, 5, 1),
])
def test_local_rotation_matches_dense_reference(x, z, r):
    rng = np.random.default_rng(100 + x * 11 + z * 3 + r)
    bits, upper = 3, 0b101
    p = SignedPauli(x, z | (0b1000 if upper else 0), r)
    initial = rng.normal(size=1 << bits) + 1j * rng.normal(size=1 << bits)
    expected = np.empty_like(initial)
    c, s = np.cos(0.731 / 2), np.sin(0.731 / 2)
    for i in range(len(initial)):
        src = i ^ x
        expected[i] = c * initial[i] - 1j * s * phase_for(p, (upper << bits) | src) * initial[src]
    actual = initial.copy()
    apply_pauli_rotation(actual, p, 0.731, block_id=upper, block_bits=bits)
    np.testing.assert_allclose(actual, expected, atol=1e-12)


@pytest.mark.parametrize("x,z,r", [(8, 8, 0), (9, 11, 1), (12, 10, 0), (15, 15, 1)])
def test_pair_rotation_matches_dense_reference(x, z, r):
    rng = np.random.default_rng(400 + x + z + r)
    bits, block_a = 2, 1
    p = SignedPauli(x, z, r)
    block_b = block_a ^ (x >> bits)
    a = rng.normal(size=1 << bits) + 1j * rng.normal(size=1 << bits)
    b = rng.normal(size=1 << bits) + 1j * rng.normal(size=1 << bits)
    original_a, original_b = a.copy(), b.copy()
    oa, ob = np.empty_like(a), np.empty_like(b)
    apply_pauli_pair(a, b, oa, ob, p, -0.417, block_a, block_b, bits)

    # Assemble only the two partner blocks into their full sparse-state slots.
    upper_x = x >> bits
    full = np.zeros(1 << (bits + upper_x.bit_length()), dtype=complex)
    for i, v in enumerate(a):
        full[(block_a << bits) | i] = v
        full[(block_b << bits) | i] = b[i]
    dense = dense_rotation(full, p, -0.417)
    for i in range(1 << bits):
        assert oa[i] == pytest.approx(dense[(block_a << bits) | i])
        assert ob[i] == pytest.approx(dense[(block_b << bits) | i])
    np.testing.assert_array_equal(a, original_a)
    np.testing.assert_array_equal(b, original_b)


@pytest.mark.parametrize("args", [(-1, 0, 0), (0, -1, 0), (0, 0, 2), (1.5, 0, 0)])
def test_signed_pauli_rejects_invalid_fields(args):
    with pytest.raises(ValueError):
        SignedPauli(*args)


def test_local_rejects_upper_x_and_pair_rejects_wrong_partner():
    p = SignedPauli(4, 0)
    with pytest.raises(ValueError):
        apply_pauli_rotation(np.ones(4, complex), p, 0.2, block_bits=2)
    buffers = [np.ones(4, complex) for _ in range(4)]
    with pytest.raises(ValueError):
        apply_pauli_pair(*buffers, p, 0.2, 0, 0, 2)
