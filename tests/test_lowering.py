import numpy as np
import pytest

from svsim.ir import GateNormalizer
from svsim.lowering import StructuredExactDecomposer
from svsim.unitary import MatrixAssetRegistry


def random_unitary(n, rng):
    a = rng.normal(size=(n, n)) + 1j*rng.normal(size=(n, n))
    q, r = np.linalg.qr(a)
    return q @ np.diag(np.diag(r)/np.abs(np.diag(r))).conj()


@pytest.mark.parametrize('width', [1, 2, 3, 4])
def test_random_unitary_structured_exact_reconstructs(width):
    assets = MatrixAssetRegistry()
    matrix = random_unitary(1 << width, np.random.default_rng(100+width))
    gate = GateNormalizer(width, assets).gate('DENSE_UNITARY', tuple(reversed(range(width))), matrix=matrix)
    derived = StructuredExactDecomposer(assets).decompose(gate)
    assert derived
    assert all(op.source_op_id == gate.op_id for op in derived)
    assert all(op.kind in {'MCX', 'DENSE_UNITARY'} for op in derived)
    for op in derived:
        if op.kind == 'DENSE_UNITARY':
            matrix = assets.get(op.matrix_asset_ref).matrix
            assert op.diagonal == np.array_equal(matrix, np.diag(np.diag(matrix)))
        else:
            assert op.permutation
    # Independent state action also checks reversed little-endian target order.
    from svsim.kernels import apply_gate
    rng = np.random.default_rng(width)
    state = rng.normal(size=1 << width) + 1j*rng.normal(size=1 << width)
    expected = apply_gate(state, gate, assets)
    actual = state.copy()
    for op in derived: actual = apply_gate(actual, op, assets)
    np.testing.assert_allclose(actual, expected, atol=2e-9, rtol=2e-9)


def test_decomposition_preserves_open_outer_control_and_source_error():
    assets = MatrixAssetRegistry()
    matrix = random_unitary(8, np.random.default_rng(8))
    gate = GateNormalizer(4, assets).gate('DENSE_UNITARY', (3, 1, 0), controls=(2,),
                                          control_values=(0,), matrix=matrix,
                                          approximation_error=2e-8)
    derived = StructuredExactDecomposer(assets).decompose(gate)
    assert derived[0].approximation_error >= gate.approximation_error
    assert all(op.source_op_id == gate.op_id for op in derived)
    assert all(2 in op.controls and op.control_values[op.controls.index(2)] == 0
               for op in derived if op.controls)


def test_lowering_enforces_operation_bound():
    assets = MatrixAssetRegistry()
    matrix = random_unitary(16, np.random.default_rng(44))
    gate = GateNormalizer(4, assets).gate('DENSE_UNITARY', (0, 1, 2, 3), matrix=matrix)
    with pytest.raises(ValueError, match='operation limit'):
        StructuredExactDecomposer(assets, max_operations=1).decompose(gate)
