"""2-rank S1: the remaining-work capture replays every S1 layout decision exactly."""
import collections
import sys
from pathlib import Path

from mpi4py import MPI

from svsim import AsyncDistributedSimulator, AsyncPolicy, DistributedConfig
from svsim.distributed_plan import MPIProfile
from svsim.remaining_work import CaptureLimits, RemainingWorkExtractor, s1_moves, strip_ids
from svsim.runtime_observed_costs import EXECUTOR_STAGES, _class

comm = MPI.COMM_WORLD
rank = comm.Get_rank()
config = DistributedConfig(str(Path(sys.argv[1]) / f'owner-{rank}'), 2, ram_budget_bytes_per_rank=64 * 1024**2,
                           metadata_reserve_bytes=8 * 1024**2, frame_policy='materialize')
policy = AsyncPolicy(io_engine='cooperative', swap_policy='S1', lookahead_gates=16, prefetch_blocks=0)
with AsyncDistributedSimulator(5, config, comm=comm, policy=policy, storage_bandwidth=1e12) as sim:
    # Deliberately synthetic profile: exercises S1 decisions, no performance claim.
    sim.network_profile = MPIProfile('SYNTHETIC-s1-eta-no-performance-claim', sim.store.payload_bytes,
                                     sim.config.tile_bytes, sim.config.backend, 1e9, 1e-8, 0.0)
    sim.runtime.configure_measurements(max_bytes=2 * 1024**2)
    assert s1_moves(sim)
    for layer in range(6):  # runs of rotations on a rank-bit qubit make S1 move it local
        for _ in range(12): sim.gate('RX', (4 - layer % 2,), angle=.1)
        sim.gate('H', (layer % 2,))
    sim.materialize(frontier=3, binding=sim.bind())
    end = len(sim.sequencer.operations) - 1
    extractor = RemainingWorkExtractor(CaptureLimits(scan_items=4096, scan_seconds=1))
    snap = extractor.capture(sim, endpoint_frontier=end)
    assert snap.future_complete and not snap.future_reasons, snap.future_reasons
    predicted = collections.Counter()
    for task in snap.window + snap.future:
        if task.kind in {'executor', 'executor-count'}: predicted[_class(strip_ids(task.condition))] += task.count
    def executed():
        return collections.Counter(_class(r.condition) for r in sim.runtime.measurements.cost_observations()
                                   if r.stage in EXECUTOR_STAGES)
    layouts_before, before = sim.layout.logical_to_physical, executed()
    # One whole epoch later the incremental scan (replayed layouts) equals a fresh one.
    sim.materialize(frontier=3 + 16)
    shape = lambda s: collections.Counter({_class(t.condition): t.count for t in s.future})
    incremental = extractor.capture(sim, endpoint_frontier=end)
    fresh = RemainingWorkExtractor(CaptureLimits(scan_items=4096, scan_seconds=1)).capture(sim, endpoint_frontier=end)
    assert incremental.future_complete and shape(incremental) == shape(fresh)
    sim.materialize()
    actual = executed() - before
    assert sim.layout.logical_to_physical != layouts_before, 'S1 did not move the layout; the test needs a decision'
    # Every call, with the layout S1 chose for it, was predicted exactly.
    assert predicted == actual, (sorted(predicted.values()), sorted(actual.values()))
    assert sum(predicted.values()) == end - 3
if rank == 0: print('s1-eta-replay ok', flush=True)
