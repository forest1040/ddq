"""Two-rank policy regression worker; synthetic rates are test inputs only."""
import argparse
from pathlib import Path

import numpy as np
from mpi4py import MPI

from svsim.distributed import DistributedConfig, DistributedSimulator
from svsim.distributed_plan import MPIProfile
from svsim.kernels import eager_reference
from svsim.mpi_health import mpi_condition
from svsim.observables import ObservableIR, PauliTerm
from svsim.parameters import ParameterSymbol


def synthetic_profile(sim):
    # This deliberately synthetic profile exercises policy selection; it is
    # not a calibration result or a claim about actual network performance.
    condition = mpi_condition(sim, 0, 1, 16)
    peer = {"source": 0, "destination": 1, "bytes": 16,
            "seconds": 1e-8, **condition.details}
    return MPIProfile(
        profile_id="SYNTHETIC-test-input-no-performance-claim",
        block_bytes=sim.store.payload_bytes,
        tile_bytes=sim.config.tile_bytes,
        backend=sim.config.backend,
        bandwidth_bytes_per_second=1e9,
        latency_seconds=1e-8,
        packing_seconds_per_byte=0.0,
        host_scope="same-host",
        peer_samples=(peer,),
    )


def main(directory: Path):
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    n, block_bits = 6, 2
    rng = np.random.default_rng(9917)
    initial = rng.normal(size=1 << n) + 1j * rng.normal(size=1 << n)
    initial /= np.linalg.norm(initial)
    theta = ParameterSymbol("policy-theta", parameter_id="policy-test-theta")
    config = DistributedConfig(str(directory), block_bits,
                               ram_budget_bytes_per_rank=8 * 1024**2,
                               metadata_reserve_bytes=256 * 1024,
                               swap_policy="S1")
    observable = ObservableIR((PauliTerm.from_string("IIIIIZ"),
                               PauliTerm.from_string("XIIIII", 0.3)))

    with DistributedSimulator(n, config, comm=comm, initial_state=initial,
                              storage_bandwidth=1e12) as sim:
        # Exercise the public calibration path on both peers, then replace its
        # measured profile with a labelled synthetic fixture for S1 selection.
        calibrated = sim.calibrate_network(repetitions=1)
        assert calibrated is not None
        assert calibrated.matches(sim.store.payload_bytes, config.tile_bytes, config.backend)
        assert {(p["source"], p["destination"]) for p in calibrated.peer_samples} == {(0, 1), (1, 0)}
        assert sim.network_profile == calibrated and sim.transport.observer is not None

        profile = synthetic_profile(sim)
        sim.set_network_profile(profile)
        assert sim.network_profile is profile
        assert sim.transport.observer is not None

        for _ in range(20):
            sim.gate("H", (5,))
        sim.gate("RX", (5,), angle=theta)
        binding = sim.bind({theta.parameter_id: 0.37})
        sim.local._register()
        plan_before = sim.local.plan
        sim.materialize(binding=binding)

        decisions = [event["metadata"] for event in sim.trace.iter_events()
                     if event["metadata"].get("path") == "S1_DECISION"]
        assert any(decision["bits"] is not None and
                   decision["reason"] == "measured-lower-bound-profitable-S1" and
                   decision["profile_id"] == profile.profile_id for decision in decisions)
        assert any(event["kind"] == "layout" for event in sim.history)

        expected = eager_reference(sim.sequencer.operations, n, sim.assets, binding,
                                   initial_state=initial)
        actual_state = sim.state(binding=binding, allow_gather=True)
        error = None
        if rank == 0:
            try:
                np.testing.assert_allclose(actual_state, expected, atol=5e-11, rtol=5e-11)
            except Exception as exc:
                error = str(exc)
        sim.transport.vote(error)

        cut = (sim.root, sim.frontier, sim.layout, sim.binding)
        actual_gradient = sim.gradient(observable, parameters=(theta,), binding=binding)
        assert (sim.root, sim.frontier, sim.layout, sim.binding) == cut

        def value(delta):
            shifted = binding.shifted(theta.parameter_id, delta)
            vector = eager_reference(sim.sequencer.operations, n, sim.assets, shifted,
                                     initial_state=initial)
            from svsim.observables import BlockPauliInner, StateHandle
            state = StateHandle.from_vector(vector, block_bits)
            return BlockPauliInner().expectation(state, observable)

        eps = 2e-6
        fd = (value(eps) - value(-eps)) / (2 * eps)
        error = None
        try:
            assert abs(actual_gradient[theta.parameter_id] - fd) <= 5e-7 * max(1.0, abs(fd))
            assert sim.local.plan is plan_before
        except Exception as exc:
            error = str(exc)
        sim.transport.vote(error)

        # Delayed rank 1 provides a real, bounded slow-peer observation. The
        # observer must mark degradation while leaving execution policy/plan
        # untouched; the delay is reset before the worker exits.
        sim.set_network_profile(profile)
        plan_before = sim.local.plan
        root_before = sim.root
        sim.transport.set_delay(1, 0.005)
        send = bytearray(b"0123456789abcdef") if rank == 0 else None
        recv = bytearray(16) if rank == 1 else None
        for sequence in range(24):
            sim.transport.transfer(send, recv, 0, 1,
                                   metadata={"path": "slow-rank-test", "sequence": sequence})
        sim.transport.set_delay(None, 0)
        assert sim.local.plan is plan_before
        assert sim.root == root_before
        assert sim.config.swap_policy == "S1"
        error = None
        if rank == 0:
            try:
                health = [event for event in sim.trace.iter_events()
                          if event["metadata"].get("path") == "MPI_HEALTH"]
                assert health and health[-1]["health_state"] == "DEGRADED"
                assert health[-1]["metadata"]["mode"] == "observe"
                assert health[-1]["metadata"]["plan_unchanged"] is True
            except Exception as exc:
                error = str(exc)
        sim.transport.vote(error)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--directory", type=Path, required=True)
    main(parser.parse_args().directory)
