"""Serial COMM_SELF checks for streamed distributed adjoint kernels."""
import numpy as np
import pytest

MPI = pytest.importorskip("mpi4py.MPI")

from svsim.distributed import DistributedConfig, DistributedSimulator
from svsim.kernels import eager_reference
from svsim.observables import ObservableIR, PauliTerm
from svsim.parameters import ExactPi, ParameterSymbol


def test_comm_self_adjoint_matches_finite_difference(tmp_path):
    rng = np.random.default_rng(20261013)
    n, block_bits = 5, 2
    initial = rng.normal(size=1 << n) + 1j * rng.normal(size=1 << n)
    initial /= np.linalg.norm(initial)
    theta = ParameterSymbol("shared-angle", parameter_id="serial-shared-angle")
    config = DistributedConfig(str(tmp_path / "serial-adjoint"), block_bits,
                               ram_budget_bytes_per_rank=8 * 1024**2,
                               metadata_reserve_bytes=256 * 1024,
                               amplitude_store="ram")
    observable = ObservableIR((PauliTerm.from_string("YIZXI", 0.7),
                               PauliTerm.from_string("XXIII", -0.23),
                               PauliTerm.from_string("IIIII", 0.11)))

    with DistributedSimulator(n, config, comm=MPI.COMM_SELF,
                              initial_state=initial) as sim:
        sim.gate("H", (4,))
        sim.gate("RX", (0,), angle=2 * theta + 0.07)
        # A mixed open/closed projector exercises the inactive identity branch.
        sim.gate("RY", (3,), controls=(0, 2), control_values=(0, 1), angle=-theta)
        sim.gate("PAULI_ROT", (1, 4), paulis=("Y", "X"), angle=theta)
        binding = sim.bind({theta.parameter_id: ExactPi(1, 2)})

        # The adjoint path must use block fetches, never the explicit debug gather.
        sim.state = lambda **kwargs: (_ for _ in ()).throw(AssertionError("gradient gathered state"))
        actual = sim.gradient(observable, parameters=(theta,), binding=binding)
        assert set(actual) == {theta.parameter_id}

        def value(at):
            shifted = binding.shifted(theta.parameter_id, at)
            state = eager_reference(sim.sequencer.operations, n, sim.assets, shifted,
                                    initial_state=initial)
            total = 0j
            for term in observable.terms:
                indices = np.arange(1 << n)
                partner = indices ^ term.x_mask
                phase = (1j ** (term.x_mask & term.z_mask).bit_count()) * np.where(
                    np.array([((int(i) & term.z_mask).bit_count() & 1) for i in indices]), -1, 1)
                total += term.coefficient * np.vdot(state[partner], phase * state)
            return float(total.real)

        epsilon = 2e-6
        finite_difference = (value(epsilon) - value(-epsilon)) / (2 * epsilon)
        assert actual[theta.parameter_id] == pytest.approx(finite_difference, abs=2e-7, rel=2e-7)


def test_comm_self_gradient_restores_original_cut(tmp_path):
    theta = ParameterSymbol("cut-angle", parameter_id="serial-cut-angle")
    config = DistributedConfig(str(tmp_path / "restore-cut"), 1,
                               ram_budget_bytes_per_rank=4 * 1024**2,
                               metadata_reserve_bytes=256 * 1024,
                               amplitude_store="ram")
    with DistributedSimulator(3, config, comm=MPI.COMM_SELF) as sim:
        sim.gate("H", (0,))
        sim.gate("RZ", (2,), angle=theta)
        binding = sim.bind({theta.parameter_id: 0.31})
        sim.materialize(binding=binding)
        before = (sim.root, sim.frontier, sim.layout, sim.binding)
        sim.gradient(ObservableIR((PauliTerm.from_string("ZII"),)), binding=binding)
        assert (sim.root, sim.frontier, sim.layout, sim.binding) == before
