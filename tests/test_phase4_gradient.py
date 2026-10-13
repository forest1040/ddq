import numpy as np
import pytest

MPI = pytest.importorskip("mpi4py.MPI")

from svsim.async_scheduler import AsyncDistributedSimulator, AsyncPolicy
from svsim.distributed import DistributedConfig
from svsim.gradient_scheduler import GradientScheduler
from svsim.kernels import eager_reference
from svsim.layout import QubitLayout
from svsim.observables import BlockPauliInner, ObservableIR, PauliTerm, StateHandle
from svsim.parameters import ParameterSymbol
from svsim.phase4_gradient import execute_gradient


def make_sim(path, *, swap_policy="S0"):
    config = DistributedConfig(str(path), 2, ram_budget_bytes_per_rank=16 * 1024**2,
                               metadata_reserve_bytes=256 * 1024, amplitude_store="file")
    policy = AsyncPolicy(io_engine="cooperative", swap_policy=swap_policy,
                         lookahead_gates=8, prefetch_blocks=0)
    return AsyncDistributedSimulator(4, config, comm=MPI.COMM_SELF, policy=policy)


def append_parameterized(sim, theta):
    sim.gate("H", (0,))
    sim.gate("RX", (0,), angle=2 * theta + 0.07)
    sim.gate("H", (3,))
    sim.gate("RY", (1,), controls=(0,), control_values=(1,), angle=-theta)
    sim.gate("PAULI_ROT", (2, 3), paulis=("Y", "X"), angle=theta)


def make_schedules(sim, theta, binding):
    scheduler = GradientScheduler()
    requested = (theta.parameter_id,)
    adjoint = scheduler.choose(sim.sequencer.operations, requested, binding, method="adjoint")
    recompute = scheduler.choose(sim.sequencer.operations, requested, binding, method="recompute")
    shift = scheduler.choose(sim.sequencer.operations, requested, binding, method="parameter-shift")
    return requested, adjoint, recompute, shift


def eager_value(sim, observable, binding):
    state = eager_reference(sim.sequencer.operations, sim.num_qubits, sim.assets, binding)
    handle = StateHandle.from_vector(state, sim.block_bits, layout=QubitLayout.identity(sim.num_qubits))
    return BlockPauliInner().expectation(handle, observable)


def finite_difference(sim, observable, binding, parameter_id):
    epsilon = 1e-6
    return (eager_value(sim, observable, binding.shifted(parameter_id, epsilon)) -
            eager_value(sim, observable, binding.shifted(parameter_id, -epsilon))) / (2 * epsilon)


def test_adjoint_recompute_and_occurrence_shift_match_eager_finite_difference(tmp_path):
    theta = ParameterSymbol("shared", "phase4-shared")
    observable = ObservableIR((PauliTerm.from_string("YIZX", 0.7),
                               PauliTerm.from_string("XXII", -0.23),
                               PauliTerm.from_string("IIII", 0.11)))
    with make_sim(tmp_path / "methods") as sim:
        append_parameterized(sim, theta)
        binding = sim.bind({theta.parameter_id: 0.31})
        sim.materialize(binding=binding)
        requested, adjoint_schedule, recompute_schedule, shift_schedule = make_schedules(sim, theta, binding)
        cut = (sim.root, sim.frontier, sim.layout, sim.binding)

        adjoint = execute_gradient(sim, observable, requested, adjoint_schedule)
        recompute = execute_gradient(sim, observable, requested, recompute_schedule)
        budget_before_shift = sim.budget.used
        shifted = execute_gradient(sim, observable, requested, shift_schedule)
        expected = finite_difference(sim, observable, binding, theta.parameter_id)
        assert adjoint[theta.parameter_id] == pytest.approx(expected, abs=3e-7, rel=3e-7)
        assert recompute[theta.parameter_id] == pytest.approx(expected, abs=3e-7, rel=3e-7)
        assert shifted[theta.parameter_id] == pytest.approx(expected, abs=3e-7, rel=3e-7)
        assert (sim.root, sim.frontier, sim.layout, sim.binding) == cut
        assert sim.budget.used == budget_before_shift


def test_recompute_uses_s2_actual_order_and_layout_tape(tmp_path):
    theta = ParameterSymbol("layout-shared", "phase4-layout-shared")
    observable = ObservableIR((PauliTerm.from_string("ZIXI"), PauliTerm.from_string("IIIZ", 0.2)))
    with make_sim(tmp_path / "tape", swap_policy="S2") as sim:
        sim.gate("RX", (0,), angle=theta)
        sim.gate("H", (3,))
        sim.gate("RY", (1,), angle=2 * theta)
        sim.gate("RZ", (2,), angle=-theta)
        binding = sim.bind({theta.parameter_id: 0.27})
        sim.materialize(frontier=0, binding=binding)
        sim.physical_index_bit_swap(0, 3)
        sim.materialize(binding=binding)
        assert any(entry["kind"] == "layout" for entry in sim.history)
        assert [entry["source"] for entry in sim.history if entry["kind"] == "gate"][:3] != [0, 1, 2]
        requested, adjoint_schedule, recompute_schedule, shift_schedule = make_schedules(sim, theta, binding)
        expected = execute_gradient(sim, observable, requested, adjoint_schedule)
        cut = (sim.root, sim.frontier, sim.layout, sim.binding)
        actual = execute_gradient(sim, observable, requested, recompute_schedule)
        assert actual[theta.parameter_id] == pytest.approx(expected[theta.parameter_id], abs=1e-9)
        shifted = execute_gradient(sim, observable, requested, shift_schedule)
        assert shifted[theta.parameter_id] == pytest.approx(expected[theta.parameter_id], abs=3e-7)
        assert (sim.root, sim.frontier, sim.layout, sim.binding) == cut


def test_recompute_reclaims_old_adjoint_roots_during_long_chain(tmp_path):
    theta = ParameterSymbol("long-chain", "phase4-long-chain")
    config = DistributedConfig(str(tmp_path / "ram"), 2,
                               ram_budget_bytes_per_rank=16 * 1024**2,
                               metadata_reserve_bytes=256 * 1024,
                               amplitude_store="ram")
    with AsyncDistributedSimulator(4, config, comm=MPI.COMM_SELF,
                                   policy=AsyncPolicy(io_engine="cooperative")) as sim:
        for index in range(24):
            sim.gate("RX" if index % 2 == 0 else "RY", (index % 3,),
                     angle=(index % 4 + 1) * theta + 0.013 * index)
        binding = sim.bind({theta.parameter_id: 0.19})
        sim.materialize(binding=binding)
        requested = (theta.parameter_id,)
        schedule = GradientScheduler().choose(sim.sequencer.operations, requested,
                                              binding, method="recompute")
        peaks = []
        reclaim = sim._memory_reclaim

        def monitored(roots):
            reclaim(roots)
            peaks.append(len(sim.store.resident))

        sim._memory_reclaim = monitored
        execute_gradient(sim, ObservableIR((PauliTerm.from_string("ZIII"),)),
                         requested, schedule)
        assert peaks
        # Four blocks for this state; a broad version history grows with the
        # 24 reverse steps, while the intended workspace remains a few cuts.
        assert max(peaks) <= 20
