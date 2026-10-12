"""Phase 2 runtime regressions against the eager scalar oracle."""
import numpy as np
import pytest

from svsim.kernels import eager_reference
from svsim.observables import BlockPauliInner, ObservableIR, PauliTerm, StateHandle
from svsim.parameters import ExactPi, ParameterSymbol
from svsim.storage import StorageConfig
from svsim.stored_runtime import StoredSimulator


def random_state(n, seed):
    rng = np.random.default_rng(seed)
    state = rng.normal(size=1 << n) + 1j * rng.normal(size=1 << n)
    return state / np.linalg.norm(state)


def make_sim(path, n=6, block_bits=2, *, frame_policy="auto", alignment_policy="auto",
             ram=1 << 20, metadata=1 << 17, initial_state=None):
    config = StorageConfig(str(path), block_bits, ram_budget_bytes=ram,
                           metadata_reserve_bytes=metadata, alignment=64)
    return StoredSimulator(n, config, initial_state=initial_state,
                           frame_policy=frame_policy, alignment_policy=alignment_policy)


@pytest.mark.parametrize("frame_policy", ["auto", "keep", "materialize"])
@pytest.mark.parametrize("alignment_policy", ["auto", "input-a", "input-b", "identity"])
def test_random_clifford_nonclifford_and_pauli_rotations_match_eager(
        tmp_path, frame_policy, alignment_policy):
    initial = random_state(6, 120)
    with make_sim(tmp_path / "state", frame_policy=frame_policy,
                  alignment_policy=alignment_policy, initial_state=initial) as sim:
        sim.gate("H", (0,))
        sim.gate("T", (1,))
        sim.gate("S", (2,))
        sim.gate("RZ", (5,), angle=-0.392)
        sim.gate("CX", (4, 1))
        sim.gate("PAULI_ROT", (0, 3, 4, 5), paulis=("Y", "Z", "X", "Y"), angle=0.713)
        sim.gate("PAULI_ROT", (0, 4, 5), paulis=("X", "Y", "X"), angle=-0.281)
        expected = eager_reference(sim.sequencer.operations, 6, sim.assets,
                                   initial_state=initial)
        np.testing.assert_allclose(sim.state(), expected, atol=3e-13, rtol=3e-13)


@pytest.mark.parametrize("kind,targets,kwargs", [
    ("X", (5,), {}),
    ("CX", (5, 4), {}),
    ("MCX", (6,), {"controls": (4, 5), "control_values": (0, 1)}),
    ("SWAP", (4, 6), {}),
])
def test_upper_permutations_materialize_without_amplitude_io(tmp_path, kind, targets, kwargs):
    with make_sim(tmp_path / "state", n=8, block_bits=3) as sim:
        sim.cache.flush()
        sim.cache.clear()
        before = (sim.trace.read_bytes, sim.trace.write_bytes)
        sim.gate(kind, targets, **kwargs)
        sim.materialize()
        assert (sim.trace.read_bytes, sim.trace.write_bytes) == before
        assert sim.budget.peak <= sim.config.ram_budget_bytes


@pytest.mark.parametrize('num_qubits',[40,50])
def test_large_deferred_metadata_does_not_expand_amplitudes_or_block_views(tmp_path,num_qubits):
    # 2**40 amplitudes with 2**24 amplitudes per block; this exercises metadata
    # admission and deferred registration without allocating either block.
    config = StorageConfig(str(tmp_path / "large"), 24, ram_budget_bytes=1 << 30,
                           metadata_reserve_bytes=128 * 1024, alignment=64)
    with StoredSimulator(num_qubits, config, frame_policy="keep") as sim:
        sim.gate("H", (1,))
        sim.gate("T", (0,), controls=(39,))
        sim.materialize()
        assert sim.budget.peak == config.metadata_reserve_bytes
        assert sim.store.db.execute("SELECT count(*) FROM views").fetchone()[0] == 0
        assert sim.store.db.execute("SELECT count(*) FROM views_v2").fetchone()[0] == 0
        assert sim.store.db.execute("SELECT count(*) FROM root_edges").fetchone()[0] == 1


def test_exact_pi_conditional_phase_and_float_angle_log_survive_alignment(tmp_path):
    initial = random_state(6, 127)
    with make_sim(tmp_path / "state", initial_state=initial,
                  frame_policy="keep", alignment_policy="input-b") as sim:
        sim.gate("RZ", (0,), controls=(4,), angle=ExactPi(2))
        sim.gate("H", (4,))
        sim.materialize(frontier=0)
        for h in range(sim.store.num_blocks):
            view = sim.block_view(h)
            expected_phase = -1 if (h >> (4 - sim.block_bits)) & 1 else 1
            assert view.phase_value.evaluate() == pytest.approx(expected_phase)
        sim.gate("PAULI_ROT", (0, 1), paulis=("X", "Y"), angle=float(np.pi / 2))
        expected = eager_reference(sim.sequencer.operations, 6, sim.assets,
                                   initial_state=initial)
        sim.materialize()
        np.testing.assert_allclose(sim.state(), expected, atol=3e-13, rtol=3e-13)
        # Numeric-angle projections remain available for observability; exact
        # phases are carried separately in PhaseValue on v2 views.
        views = [sim.block_view(h) for h in range(sim.store.num_blocks)]
        assert all(view.phase_value is not None for view in views)
        assert all(view.base_coordinate_id for view in views)
        logged_angles = [entry.get("angle") for view in views for entry in view.base_log]
        assert any(isinstance(angle, (int, float)) and angle == pytest.approx(np.pi / 2)
                   for angle in logged_angles)


@pytest.mark.parametrize("alignment_policy", ["auto", "input-a", "input-b", "identity"])
def test_alignment_preserves_different_input_frames_and_relative_phases(tmp_path, alignment_policy):
    initial = random_state(6, 128)
    with make_sim(tmp_path / "state", initial_state=initial,
                  frame_policy="keep", alignment_policy=alignment_policy) as sim:
        sim.gate("RZ", (4,), angle=ExactPi(1))
        sim.gate("H", (0,), controls=(4,))
        sim.gate("RZ", (5,), angle=ExactPi(1, 2))
        sim.gate("H", (5,))
        expected = eager_reference(sim.sequencer.operations, 6, sim.assets,
                                   initial_state=initial)
        np.testing.assert_allclose(sim.state(), expected, atol=3e-13, rtol=3e-13)


def test_physical_swap_snapshot_and_streaming_observable_match_reference(tmp_path):
    initial = random_state(6, 129)
    config = StorageConfig(str(tmp_path / "state"), 2, ram_budget_bytes=1 << 20,
                           metadata_reserve_bytes=1 << 17, alignment=64)
    with StoredSimulator(6, config, initial_state=initial) as sim:
        sim.gate("H", (0,))
        sim.gate("PAULI_ROT", (1, 4, 5), paulis=("Y", "X", "Y"), angle=0.39)
        expected = eager_reference(sim.sequencer.operations, 6, sim.assets,
                                   initial_state=initial)
        sim.physical_index_bit_swap(0, 5)
        np.testing.assert_allclose(sim.state(), expected, atol=3e-13, rtol=3e-13)
        observables = [ObservableIR((PauliTerm.from_string(s),))
                       for s in ("YIIIZI", "IXIIIX", "ZIZIII")]
        oracle_state = StateHandle.from_vector(expected, 2)
        oracle = [BlockPauliInner().expectation(oracle_state, observable)
                  for observable in observables]
        np.testing.assert_allclose(sim.expectation(observables), oracle, atol=3e-13)
        snapshot = sim.commit_snapshot()
        with StoredSimulator.resume(config, snapshot) as resumed:
            np.testing.assert_allclose(resumed.state(), expected, atol=3e-13, rtol=3e-13)
            np.testing.assert_allclose(resumed.expectation(observables), oracle, atol=3e-13)


def test_deferred_frame_log_prefix_snapshot_fork_and_resume(tmp_path):
    initial = random_state(6, 130)
    theta = ParameterSymbol("theta", parameter_id="theta-phase2")
    config = StorageConfig(str(tmp_path / "state"), 2, ram_budget_bytes=1 << 20,
                           metadata_reserve_bytes=1 << 17, alignment=64)
    with StoredSimulator(6, config, initial_state=initial, frame_policy="keep") as sim:
        sim.gate("H", (0,))
        sim.gate("PAULI_ROT", (1, 4), paulis=("Y", "X"), angle=theta)
        binding = sim.bind({theta.parameter_id: 0.37})
        sim.materialize(frontier=1, binding=binding)
        snapshot = sim.commit_snapshot()
        fork = sim.fork(snapshot, binding=binding)
        try:
            fork.materialize(binding=binding)
            expected = eager_reference(fork.sequencer.operations, 6, fork.assets,
                                       binding, initial_state=initial)
            np.testing.assert_allclose(fork.state(binding=binding), expected,
                                       atol=3e-13, rtol=3e-13)
        finally:
            fork.close()


def test_joint_failure_does_not_publish_partial_root(tmp_path, monkeypatch):
    initial = random_state(6, 131)
    with make_sim(tmp_path / "state", initial_state=initial, frame_policy="keep") as sim:
        sim.gate("H", (0,))
        sim.gate("PAULI_ROT", (4, 5), paulis=("X", "Y"), angle=0.29)
        root, frontier = sim.root, sim.frontier
        original = sim._joint_phase2

        def fail(*args, **kwargs):
            raise OSError("injected joint failure")

        monkeypatch.setattr(sim, "_joint_phase2", fail)
        with pytest.raises(OSError, match="injected joint failure"):
            sim.materialize()
        assert (sim.root, sim.frontier) == (root, frontier)
        monkeypatch.setattr(sim, "_joint_phase2", original)
        expected = eager_reference(sim.sequencer.operations, 6, sim.assets,
                                   initial_state=initial)
        np.testing.assert_allclose(sim.state(), expected, atol=3e-13, rtol=3e-13)


@pytest.mark.parametrize('seed',[31,32,33])
def test_random_8q_words_include_unaligned_controlled_and_dense_fallback(tmp_path,seed):
    initial = random_state(8,seed)
    rng = np.random.default_rng(seed)
    config = StorageConfig(str(tmp_path/'random'),3,ram_budget_bytes=2*1024**2,metadata_reserve_bytes=1024**2,alignment=64)
    with StoredSimulator(8,config,initial_state=initial,frame_policy='keep') as sim:
        for step in range(50):
            q = int(rng.integers(8))
            kind = ('H','S','T','RX','RY','RZ','CX','CZ','SWAP','PAULI_ROT')[int(rng.integers(10))]
            if kind in {'CX','CZ','SWAP'}:
                pair = tuple(int(v) for v in rng.choice(8,2,replace=False)); sim.gate(kind,pair)
            elif kind == 'PAULI_ROT':
                targets = tuple(int(v) for v in rng.choice(8,3,replace=False))
                sim.gate(kind,targets,paulis=tuple(rng.choice(['I','X','Y','Z'],3)),angle=float(rng.normal()))
            elif kind in {'RX','RY','RZ'}:
                angle = ExactPi(int(rng.integers(-4,5)),2) if step%3 == 0 else float(rng.normal())
                sim.gate(kind,(q,),angle=angle)
            else: sim.gate(kind,(q,))
        sim.gate('H',(7,),controls=(0,),control_values=(0,))
        raw = rng.normal(size=(4,4))+1j*rng.normal(size=(4,4)); matrix,_ = np.linalg.qr(raw)
        sim.gate('DENSE_UNITARY',(1,7),controls=(2,),matrix=matrix)
        expected = eager_reference(sim.sequencer.operations,8,sim.assets,initial_state=initial)
        np.testing.assert_allclose(sim.state(),expected,atol=1e-11,rtol=1e-11)


@pytest.mark.parametrize('alignment',['input-a','input-b','auto'])
def test_identity_tableau_retains_conditional_hs_cycle_scalar(tmp_path,alignment):
    initial = random_state(3,71)
    with make_sim(tmp_path/'cycle',n=3,block_bits=2,initial_state=initial,
                  frame_policy='keep',alignment_policy=alignment) as sim:
        for _ in range(3):
            sim.gate('H',(0,),controls=(2,)); sim.gate('S',(0,),controls=(2,))
        sim.materialize()
        assert sim.block_view(0).frame_asset_id == sim.block_view(1).frame_asset_id
        assert sim.block_view(0).phase_value.z8 == 0
        assert sim.block_view(1).phase_value.z8 == 1
        sim.gate('H',(2,))
        expected = eager_reference(sim.sequencer.operations,3,sim.assets,initial_state=initial)
        np.testing.assert_allclose(sim.state(),expected,atol=1e-12)


@pytest.mark.parametrize('letters',[('X','Y','Z'),('I','I','Z')])
def test_exact_quarter_pauli_with_upper_z_is_frame_only(tmp_path,letters):
    initial = random_state(4,72)
    with make_sim(tmp_path/'quarter',n=4,block_bits=2,initial_state=initial,frame_policy='keep') as sim:
        sim.cache.flush(); sim.cache.clear()
        before = sim.trace.read_bytes,sim.trace.write_bytes
        sim.gate('H',(0,))
        sim.gate('PAULI_ROT',(0,1,3),paulis=letters,angle=ExactPi(-3,2))
        sim.materialize()
        assert (sim.trace.read_bytes,sim.trace.write_bytes) == before
        assert all(not sim.block_view(h).base_log for h in range(sim.store.num_blocks))
        expected = eager_reference(sim.sequencer.operations,4,sim.assets,initial_state=initial)
        np.testing.assert_allclose(sim.state(),expected,atol=1e-12)
