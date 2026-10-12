import numpy as np

from svsim.ir import GateNormalizer
from svsim.kernels import eager_reference
from svsim.parameters import ParameterBinding
from svsim.streaming_kernels import apply_group, apply_local
from svsim.unitary import MatrixAssetRegistry


def test_group_dense_reversed_target_order_and_open_outer_control():
    assets = MatrixAssetRegistry()
    rng = np.random.default_rng(12)
    raw = rng.normal(size=(4, 4)) + 1j*rng.normal(size=(4, 4))
    q, r = np.linalg.qr(raw)
    q = q @ np.diag(np.diag(r)/np.abs(np.diag(r))).conj()
    norm = GateNormalizer(4, assets)
    gate = norm.gate('DENSE_UNITARY', (2, 0), controls=(3,), control_values=(0,), matrix=q)
    state = rng.normal(size=16) + 1j*rng.normal(size=16)
    inputs = [state[g*4:(g+1)*4].copy() for g in range(4)]
    outputs = [np.empty(4, complex) for _ in inputs]
    # Compact group bits are physical positions 2 then 3; control bit 3 is
    # filtered, so process each control partition as the runtime would.
    apply_group(inputs, outputs, gate, 2, (2, 3), assets)
    actual = np.concatenate(outputs)
    # Direct oracle with the same open control.
    expected = eager_reference([gate], 4, assets, initial_state=state)
    np.testing.assert_allclose(actual, expected, atol=1e-12)
    np.testing.assert_array_equal(np.concatenate(inputs), state)


def test_local_mcx_and_upper_diagonal_phase():
    assets = MatrixAssetRegistry()
    norm = GateNormalizer(3, assets)
    local = norm.gate('MCX', (0,), controls=(1, 2), control_values=(0, 1))
    block = np.arange(4, dtype=complex)
    original = block.copy()
    apply_local(block, local, assets, block_bits=2, block_id=1)
    expected = original.copy()
    expected[0], expected[1] = original[1], original[0]
    np.testing.assert_array_equal(block, expected)
    phase = norm.gate('RZ', (2,), angle=np.pi/2)
    block = np.ones(4, complex)
    apply_local(block, phase, assets, block_bits=2, block_id=1)
    np.testing.assert_allclose(block, np.exp(1j*np.pi/4), atol=1e-12)


def test_group_mcx_swaps_only_predicated_pairs():
    assets = MatrixAssetRegistry()
    gate = GateNormalizer(3, assets).gate('MCX', (2,), controls=(0,), control_values=(0,))
    inputs = [np.array([complex(10*g+i) for i in range(4)]) for g in range(2)]
    outputs = [np.empty(4, complex) for _ in inputs]
    apply_group(inputs, outputs, gate, 2, (2,), assets)
    expected = np.concatenate(inputs).copy()
    expected[[0, 4]] = expected[[4, 0]]
    expected[[2, 6]] = expected[[6, 2]]
    np.testing.assert_array_equal(np.concatenate(outputs), expected)
