"""Stored runtime numerical checks against the small eager oracle."""
import numpy as np
import pytest

from svsim.kernels import apply_gate
from svsim.parameters import ParameterSymbol
from svsim.storage import StorageConfig
from svsim.stored_runtime import StoredSimulator


def config(path, *, block_bits=3, ram=512*1024, metadata=128*1024):
    return StorageConfig(str(path), block_bits, ram_budget_bytes=ram,
                         backend='buffered', alignment=64,
                         metadata_reserve_bytes=metadata)


def random_unitary(width, seed):
    rng = np.random.default_rng(seed)
    raw = rng.normal(size=(1 << width, 1 << width)) + 1j*rng.normal(size=(1 << width, 1 << width))
    q, r = np.linalg.qr(raw)
    return q @ np.diag(np.diag(r)/np.abs(np.diag(r))).conj()


def append_mixed_circuit(sim, num_qubits, theta, *, dense=True):
    sim.gate('H', (0,))
    sim.gate('S', (2,))
    sim.gate('RY', (1,), angle=theta)
    sim.gate('RZ', (num_qubits-1,), angle=0.31)
    sim.gate('MCX', (3,), controls=(0, num_qubits-1), control_values=(1, 0))
    sim.gate('CZ', (2, num_qubits-1))
    sim.gate('CX', (1, num_qubits-1))
    sim.gate('MCX', (0,), controls=(1, 2), control_values=(0, 1))
    if dense:
        sim.gate('DENSE_UNITARY', (num_qubits-1, 0),
                 controls=(num_qubits-2,), control_values=(0,),
                 matrix=random_unitary(2, 17))


def oracle(initial, gates, assets, binding):
    state = np.array(initial, dtype=complex, copy=True)
    for gate in gates:
        state = apply_gate(state, gate, assets, binding)
    return state


@pytest.mark.parametrize('num_qubits', [8, 10, 12])
def test_stored_streaming_mixed_local_joint_and_controlled_dense_matches_oracle(tmp_path, num_qubits):
    rng = np.random.default_rng(1000+num_qubits)
    initial = rng.normal(size=1 << num_qubits) + 1j*rng.normal(size=1 << num_qubits)
    initial /= np.linalg.norm(initial)
    theta = ParameterSymbol('theta')
    with StoredSimulator(num_qubits, config(tmp_path/'stored'), initial_state=initial) as sim:
        append_mixed_circuit(sim, num_qubits, theta)
        binding = sim.bind({theta: 0.271})
        expected = oracle(initial, sim.sequencer.operations, sim.assets, binding)
        actual = sim.state(binding)
        np.testing.assert_allclose(actual, expected, atol=2e-10, rtol=2e-10)


@pytest.mark.parametrize(('a', 'b'), [(0, 2), (3, 5), (1, 4)])
def test_physical_layout_swaps_preserve_logical_state(tmp_path, a, b):
    num_qubits = 6
    initial = np.arange(1 << num_qubits, dtype=complex) + 1j
    initial /= np.linalg.norm(initial)
    with StoredSimulator(num_qubits, config(tmp_path/'stored', block_bits=3), initial_state=initial) as sim:
        sim.gate('H', (1,))
        sim.gate('CX', (1, 4))
        expected = sim.state().copy()
        sim.physical_index_bit_swap(a, b)
        np.testing.assert_allclose(sim.state(), expected, atol=1e-12, rtol=1e-12)


@pytest.mark.parametrize('mode', ['eager', 'batching', 'deferred-no-frame', 'per-block-frame'])
def test_materialization_modes_match_eager_oracle(tmp_path, mode):
    num_qubits = 8
    initial = np.random.default_rng(42).normal(size=1 << num_qubits).astype(complex)
    initial += 1j*np.random.default_rng(43).normal(size=1 << num_qubits)
    initial /= np.linalg.norm(initial)
    theta = ParameterSymbol('theta')
    with StoredSimulator(num_qubits, config(tmp_path/'stored'), initial_state=initial, mode=mode) as sim:
        append_mixed_circuit(sim, num_qubits, theta)
        binding = sim.bind({theta: -0.33})
        expected = oracle(initial, sim.sequencer.operations, sim.assets, binding)
        np.testing.assert_allclose(sim.state(binding), expected, atol=3e-10, rtol=3e-10)


@pytest.mark.parametrize('width', [3, 4])
def test_structured_exact_stored_execution_matches_native(width, tmp_path):
    num_qubits = max(6, width+2)
    initial = np.random.default_rng(500+width).normal(size=1 << num_qubits)
    initial = initial + 1j*np.random.default_rng(600+width).normal(size=1 << num_qubits)
    initial = np.asarray(initial / np.linalg.norm(initial), dtype=complex)
    matrix = random_unitary(width, 700+width)
    targets = tuple(reversed(range(width)))
    native_config = config(tmp_path/'native', block_bits=3, ram=2*1024*1024, metadata=512*1024)
    lowered_config = config(tmp_path/'lowered', block_bits=3, ram=2*1024*1024, metadata=512*1024)
    with StoredSimulator(num_qubits, native_config, initial_state=initial, execution='auto') as native:
        native.gate('DENSE_UNITARY', targets, matrix=matrix)
        expected = native.state()
    with StoredSimulator(num_qubits, lowered_config, initial_state=initial, execution='structured-exact') as lowered:
        lowered.gate('DENSE_UNITARY', targets, matrix=matrix)
        actual = lowered.state()
    np.testing.assert_allclose(actual, expected, atol=2e-9, rtol=2e-9)


def test_named_fallback_mode_matches_native(tmp_path):
    initial = np.random.default_rng(8).normal(size=64) + 1j*np.random.default_rng(9).normal(size=64)
    initial = initial / np.linalg.norm(initial)
    def run(path, execution):
        sim = StoredSimulator(6, config(path, block_bits=3), initial_state=initial, execution=execution)
        sim.gate('RY', (0,), angle=0.231)
        sim.gate('CZ', (0, 5))
        try: return sim.state()
        finally: sim.close()
    np.testing.assert_allclose(run(tmp_path/'native', 'auto'), run(tmp_path/'fallback', 'fallback'), atol=1e-11, rtol=1e-11)


def test_insufficient_task_budget_fails_before_root_or_frontier_mutation(tmp_path):
    reserve = 64*1024
    sim = StoredSimulator(6, config(tmp_path/'too-small', block_bits=2,
                                    ram=reserve+128, metadata=reserve),frame_policy='materialize')
    try:
        sim.gate('H', (0,))
        original_root = sim.root
        original_view = sim.block_view(0)
        with pytest.raises(MemoryError, match='requires at least'):
            sim.materialize()
        assert sim.root == original_root
        assert sim.frontier == -1
        assert sim.block_view(0) == original_view
        assert sim.cache.entries == {}
    finally:
        sim.close()


@pytest.mark.parametrize('target',[0,3,7])
@pytest.mark.parametrize('count',range(7))
def test_stored_mcx_zero_to_six_open_controls(tmp_path,target,count):
    initial = np.random.default_rng(71).normal(size=256)+1j*np.random.default_rng(72).normal(size=256)
    initial /= np.linalg.norm(initial)
    with StoredSimulator(8,config(tmp_path/'stored'),initial_state=initial) as sim:
        controls = tuple(q for q in range(8) if q != target)[:count]
        values = tuple(q % 2 for q in controls)
        gate = sim.gate('MCX',(target,),controls=controls,control_values=values)
        assert gate.matrix_asset_ref is None
        np.testing.assert_allclose(sim.state(),apply_gate(initial,gate,sim.assets),atol=1e-14)


def test_append_and_execute_after_layout_barrier(tmp_path):
    with StoredSimulator(8,config(tmp_path/'stored')) as sim:
        sim.gate('H',(0,)); sim.gate('H',(7,))
        sim.physical_index_bit_swap(0,7)
        sim.gate('RY',(7,),angle=.37); sim.gate('CX',(7,0))
        from svsim.kernels import eager_reference
        np.testing.assert_allclose(sim.state(),eager_reference(sim.sequencer.operations,8,sim.assets),atol=1e-14)


def test_upper_diagonal_and_clifford_metadata_only_no_amplitude_io(tmp_path):
    with StoredSimulator(8,config(tmp_path/'stored')) as sim:
        sim.gate('H',(0,)); sim.gate('CX',(7,1)); sim.gate('RZ',(7,),angle=.7); sim.gate('CZ',(6,7))
        sim.materialize()
        assert sim.trace.read_bytes == sim.trace.write_bytes == 0
        assert not sim.cache.entries
        assert sim.block_view(0).frame_asset_id != sim.block_view(16).frame_asset_id
        assert sim.block_view(0).phase != sim.block_view(16).phase
