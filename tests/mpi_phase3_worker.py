"""Worker invoked under mpiexec by test_phase3_mpi.py."""
import argparse
import os
from pathlib import Path
import sys

import numpy as np
from mpi4py import MPI

from svsim.distributed import DistributedConfig, DistributedSimulator
from svsim.kernels import eager_reference
from svsim.mpi_transport import CollectiveError
from svsim.observables import BlockPauliInner, ObservableIR, PauliTerm, StateHandle
from svsim.parameters import ParameterSymbol


def progress(message):
    if os.environ.get("PHASE3_MPI_DEBUG"):
        print(f"rank {MPI.COMM_WORLD.Get_rank()}: {message}", file=sys.stderr, flush=True)


def dense_unitary(width, seed):
    rng = np.random.default_rng(seed)
    matrix = rng.normal(size=(1 << width, 1 << width)) + 1j * rng.normal(size=(1 << width, 1 << width))
    q, r = np.linalg.qr(matrix)
    q = q @ np.diag(np.diag(r) / np.abs(np.diag(r))).conj()
    return q


def run_state_case(directory: Path, exchange: str):
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    n, block_bits = 8, 2
    rng = np.random.default_rng(803)
    initial = rng.normal(size=1 << n) + 1j * rng.normal(size=1 << n)
    initial = initial / np.linalg.norm(initial)
    theta = ParameterSymbol("theta", parameter_id="phase3-mpi-theta")
    binding_value = 0.231
    config = DistributedConfig(str(directory), block_bits,
                               ram_budget_bytes_per_rank=2 * 1024**2,
                               metadata_reserve_bytes=256 * 1024,
                               tile_bytes=256 * 1024,
                               mcx_exchange=exchange)
    with DistributedSimulator(n, config, comm=comm, initial_state=initial) as sim:
        progress(f"{exchange}: initialized")
        sim.gate("H", (0,))
        sim.gate("T", (1,))
        sim.gate("RX", (3,), angle=2 * theta + 0.1)
        sim.gate("RZ", (6,), angle=-0.19)
        # Exercise no-control and zero through six mixed open/closed controls.
        for k in range(7):
            controls = tuple(range(k))
            values = tuple((i + k) % 2 for i in range(k))
            sim.gate("MCX", (7,), controls=controls, control_values=values)
            progress(f"{exchange}: registered MCX {k}")
        # Multiple upper X bits force a partner exchange in the Pauli path.
        sim.gate("PAULI_ROT", (1, 4, 5, 6), paulis=("Y", "X", "I", "X"), angle=0.37)
        sim.gate("H", (6,))  # global H
        sim.gate("SWAP", (5, 7))  # virtual permutation
        sim.gate("DENSE_UNITARY", (0, 4, 5, 7), matrix=dense_unitary(4, 91))
        binding = sim.bind({theta.parameter_id: binding_value})
        expected = eager_reference(sim.sequencer.operations, n, sim.assets, binding,
                                   initial_state=initial)

        # Exercise S0/S1 layout mapping across block, shard, and global bits.
        sim.materialize(binding=binding)
        progress(f"{exchange}: materialized")
        sim.physical_index_bit_swap(0, 7)
        sim.physical_index_bit_swap(2, 5)
        got = sim.state(binding=binding, allow_gather=True)
        progress(f"{exchange}: gathered state")
        error = None
        if rank == 0:
            try: np.testing.assert_allclose(got, expected, atol=5e-11, rtol=5e-11)
            except Exception as exc: error = str(exc)
        sim.transport.vote(error)

        observables = [ObservableIR((PauliTerm.from_string(s),))
                       for s in ("YIIXIZXI", "ZIZIIXIZ", "IIIIIIII")]
        sim.state = lambda **kwargs: (_ for _ in ()).throw(
            AssertionError("expectation gathered the full state"))
        actual = sim.expectation(observables, binding=binding)
        progress(f"{exchange}: expectations")
        handle = StateHandle.from_vector(expected, block_bits)
        oracle = [BlockPauliInner().expectation(handle, observable) for observable in observables]
        np.testing.assert_allclose(actual, oracle, atol=5e-10, rtol=5e-10)

        snapshot = sim.commit_snapshot()
        progress(f"{exchange}: committed snapshot")
        root = sim.root
    # Resume checks rank manifests and payloads after closing every writer.
    with DistributedSimulator.resume(config, snapshot, comm=comm) as resumed:
        progress(f"{exchange}: resumed")
        got = resumed.state(binding=binding, allow_gather=True)
        error = None
        if rank == 0:
            try: np.testing.assert_allclose(got, expected, atol=5e-11, rtol=5e-11)
            except Exception as exc: error = str(exc)
        resumed.transport.vote(error)
        assert resumed.root == root


def run_prefix_case(directory: Path):
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    n, block_bits = 8, 2
    rng = np.random.default_rng(804)
    initial = rng.normal(size=1 << n) + 1j * rng.normal(size=1 << n)
    initial = initial / np.linalg.norm(initial)
    theta = ParameterSymbol("prefix-theta", parameter_id="phase3-prefix-theta")
    config = DistributedConfig(str(directory), block_bits,
                               ram_budget_bytes_per_rank=2 * 1024**2,
                               metadata_reserve_bytes=256 * 1024,
                               tile_bytes=256 * 1024)
    with DistributedSimulator(n, config, comm=comm, initial_state=initial) as sim:
        progress("prefix: initialized")
        sim.gate("H", (0,))
        sim.gate("T", (1,))
        sim.gate("RX", (3,), angle=theta)
        sim.gate("RZ", (5,), angle=0.17)
        sim.gate("H", (7,))
        binding = sim.bind({theta.parameter_id: 0.41})
        prefix = sim.commit_prefix((theta,), binding=binding)
        progress("prefix: committed prefix")
        assert prefix["frontier"] == 1
        with sim.fork(prefix, binding=binding) as forked:
            progress("prefix: forked")
            expected = eager_reference(forked.sequencer.operations, n, forked.assets,
                                       binding, initial_state=initial)
            got = forked.state(binding=binding, allow_gather=True)
            error = None
            if rank == 0:
                try: np.testing.assert_allclose(got, expected, atol=5e-11, rtol=5e-11)
                except Exception as exc: error = str(exc)
            forked.transport.vote(error)
            events = list(forked.trace.iter_events())
            event = next(row["metadata"] for row in events
                         if row["metadata"].get("path") == "DISTRIBUTED_PREFIX_FORK")
            assert event["path"] == "DISTRIBUTED_PREFIX_FORK"
            assert event["amplitude_copy_bytes"] == 0


def run_distributed_gradient_case(directory: Path):
    comm = MPI.COMM_WORLD
    n, block_bits = 6, 2
    rng = np.random.default_rng(805)
    initial = rng.normal(size=1 << n) + 1j * rng.normal(size=1 << n)
    initial = initial / np.linalg.norm(initial)
    theta = ParameterSymbol("gradient-theta", parameter_id="phase3-gradient-theta")
    config = DistributedConfig(str(directory), block_bits,
                               ram_budget_bytes_per_rank=4 * 1024**2,
                               metadata_reserve_bytes=256 * 1024,
                               tile_bytes=256 * 1024,
                               swap_policy="S1")
    observable = ObservableIR((PauliTerm.from_string("ZIXYZI", 0.7),
                               PauliTerm.from_string("XXIIIZ", -0.2)))
    with DistributedSimulator(n, config, comm=comm, initial_state=initial) as sim:
        sim.gate("H", (5,))
        sim.gate("RX", (4,), angle=2 * theta + 0.1)
        sim.gate("PAULI_ROT", (0, 2, 5), paulis=("Y", "X", "X"), angle=0.3 * theta)
        binding = sim.bind({theta.parameter_id: 0.31})
        sim.materialize(binding=binding)
        sim.physical_index_bit_swap(1, 5)
        before = (sim.root, sim.frontier, sim.layout, sim.binding)

        # If the reverse pass gathers a complete state, this will fail.
        sim.state = lambda **kwargs: (_ for _ in ()).throw(AssertionError("gradient gathered state"))
        actual = sim.gradient(observable, parameters=(theta,), binding=binding)
        error = None
        try:
            assert set(actual) == {theta.parameter_id}
            assert (sim.root, sim.frontier, sim.layout, sim.binding) == before

            def value(at):
                shifted = binding.shifted(theta.parameter_id, at)
                state = eager_reference(sim.sequencer.operations, n, sim.assets, shifted,
                                        initial_state=initial)
                handle = StateHandle.from_vector(state, block_bits)
                return BlockPauliInner().expectation(handle, observable)

            epsilon = 2e-6
            finite_difference = (value(epsilon) - value(-epsilon)) / (2 * epsilon)
            assert abs(actual[theta.parameter_id] - finite_difference) <= 5e-7 * max(
                1.0, abs(finite_difference))
        except Exception as exc:
            error = str(exc)
        sim.transport.vote(error)


def run_collective_admission_failure_case(directory: Path):
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    n, block_bits = 5, 1
    config = DistributedConfig(str(directory), block_bits,
                               ram_budget_bytes_per_rank=2 * 1024**2,
                               metadata_reserve_bytes=256 * 1024,
                               tile_bytes=256 * 1024)
    with DistributedSimulator(n, config, comm=comm) as sim:
        sim.gate("H", (4,))
        before = (sim.root, sim.frontier)
        ensure = sim.cache.ensure
        if rank == 1:
            def fail_once(_amount):
                raise MemoryError("injected rank-local admission failure")
            sim.cache.ensure = fail_once
        caught = False
        try:
            sim.materialize(binding=sim.bind())
        except CollectiveError:
            caught = True
        finally:
            sim.cache.ensure = ensure
        assert caught
        assert (sim.root, sim.frontier) == before
        sim.transport.agree({"root": sim.root, "frontier": sim.frontier})

        # The failed admission must leave the old cut usable for a retry.
        sim.materialize(binding=sim.bind())
        got = sim.state(allow_gather=True)
        expected = eager_reference(sim.sequencer.operations, n, sim.assets)
        error = None
        if rank == 0:
            try: np.testing.assert_allclose(got, expected, atol=2e-12, rtol=2e-12)
            except Exception as exc: error = str(exc)
        sim.transport.vote(error)


def run_ram_backend_case(directory: Path):
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    n, block_bits = 6, 2
    rng = np.random.default_rng(806)
    initial = rng.normal(size=1 << n) + 1j * rng.normal(size=1 << n)
    initial = initial / np.linalg.norm(initial)
    theta = ParameterSymbol("ram-theta", parameter_id="phase3-ram-theta")
    config = DistributedConfig(str(directory), block_bits,
                               ram_budget_bytes_per_rank=4 * 1024**2,
                               metadata_reserve_bytes=256 * 1024,
                               tile_bytes=256 * 1024,
                               amplitude_store="ram")
    observable = ObservableIR((PauliTerm.from_string("YIZXIZ", 0.6),
                               PauliTerm.from_string("XXIIII", -0.15)))
    with DistributedSimulator(n, config, comm=comm, initial_state=initial) as sim:
        sim.gate("H", (5,))
        sim.gate("RX", (0,), angle=2 * theta + 0.07)
        sim.gate("PAULI_ROT", (1, 4, 5), paulis=("Y", "X", "Z"), angle=0.2 * theta)
        binding = sim.bind({theta.parameter_id: 0.28})
        expected = eager_reference(sim.sequencer.operations, n, sim.assets, binding,
                                   initial_state=initial)

        # The reverse pass must use live RAM extents without gathering state.
        sim.state = lambda **kwargs: (_ for _ in ()).throw(
            AssertionError("RAM gradient gathered state"))
        actual = sim.gradient(observable, parameters=(theta,), binding=binding)
        error = None
        try:
            def value(at):
                shifted = binding.shifted(theta.parameter_id, at)
                vector = eager_reference(sim.sequencer.operations, n, sim.assets, shifted,
                                         initial_state=initial)
                handle = StateHandle.from_vector(vector, block_bits)
                return BlockPauliInner().expectation(handle, observable)
            epsilon = 2e-6
            finite_difference = (value(epsilon) - value(-epsilon)) / (2 * epsilon)
            assert abs(actual[theta.parameter_id] - finite_difference) <= 5e-7 * max(
                1.0, abs(finite_difference))
        except Exception as exc:
            error = str(exc)
        sim.transport.vote(error)

        snapshot = sim.commit_snapshot()
        assert sim.store.durable and sim.store.resident_bytes == 0
        removed = sim.collect_garbage()
        assert isinstance(removed, int) and removed >= 0
        expected_value = BlockPauliInner().expectation(
            StateHandle.from_vector(expected, block_bits), observable)
        actual_value = sim.expectation(observable, binding=binding)
        np.testing.assert_allclose(actual_value, expected_value, atol=5e-10, rtol=5e-10)

    with DistributedSimulator.resume(config, snapshot, comm=comm) as resumed:
        got = resumed.state(binding=binding, allow_gather=True)
        error = None
        if rank == 0:
            try: np.testing.assert_allclose(got, expected, atol=5e-11, rtol=5e-11)
            except Exception as exc: error = str(exc)
        resumed.transport.vote(error)
        assert resumed.store.durable
        removed = resumed.collect_garbage()
        assert isinstance(removed, int) and removed >= 0


def run_packed_volume_case(directory: Path):
    comm = MPI.COMM_WORLD
    rank, size = comm.Get_rank(), comm.Get_size()
    n, block_bits = 8, 6
    rng = np.random.default_rng(807)
    initial = rng.normal(size=1 << n) + 1j * rng.normal(size=1 << n)
    initial = initial / np.linalg.norm(initial)
    config = DistributedConfig(str(directory), block_bits,
                               ram_budget_bytes_per_rank=4 * 1024**2,
                               metadata_reserve_bytes=256 * 1024,
                               tile_bytes=256 * 1024,
                               mcx_exchange="packed")
    for k in range(7):
        # A fixed upper control selects one remote block pair. The loop adds
        # k lower controls, so the selected payload shrinks by exactly 2**k.
        lower_controls = tuple(range(k))
        lower_values = tuple((i + k) % 2 for i in range(k))
        controls = lower_controls + (6,)
        values = lower_values + (1,)
        case_dir = directory / f"controls-{k}"
        with DistributedSimulator(n, config.__class__(**{**config.__dict__,
                                                          "directory": str(case_dir)}),
                                  comm=comm, initial_state=initial) as sim:
            sim.gate("MCX", (7,), controls=controls, control_values=values)
            sim.materialize()
            local_sent = sim.transport.send_bytes
            global_sent = int(sim.transport.sum_array(np.asarray([local_sent], dtype=np.int64))[0])
            active = 1 << (block_bits - k)
            expected_sent = 0 if k == 0 or size == 1 else 2 * active * 16
            assert global_sent == expected_sent, (
                f"k={k}: sent {global_sent} bytes, expected {expected_sent}")

            expected = eager_reference(sim.sequencer.operations, n, sim.assets,
                                       initial_state=initial)
            got = sim.state(allow_gather=True)
            error = None
            if rank == 0:
                try: np.testing.assert_allclose(got, expected, atol=3e-12, rtol=3e-12)
                except Exception as exc: error = str(exc)
            sim.transport.vote(error)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--directory", required=True)
    args = parser.parse_args()
    base = Path(args.directory)
    for exchange in ("packed", "full"):
        run_state_case(base / exchange, exchange)
    run_prefix_case(base / "prefix")
    run_ram_backend_case(base / "ram")
    run_packed_volume_case(base / "packed-volume")
    if MPI.COMM_WORLD.Get_size() >= 2:
        run_collective_admission_failure_case(base / "failure")
        run_distributed_gradient_case(base / "gradient")
    MPI.COMM_WORLD.Barrier()


if __name__ == "__main__":
    main()
