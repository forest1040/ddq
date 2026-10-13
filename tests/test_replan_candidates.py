from types import SimpleNamespace

import pytest

from svsim.async_scheduler import AsyncPolicy
from svsim.replan_candidates import RuntimeCapabilityRegistry, generate


class _TransportData:
    def Get_attr(self, key): return 31


def _sim(*, policy=None, private=False, io_capacity=8, layout=(0, 1), placement=1):
    return SimpleNamespace(
        root='root-1', frontier=4, num_qubits=2, block_bits=1, size=1, rank=0,
        layout=SimpleNamespace(logical_to_physical=layout), placement_epoch=placement,
        sequencer=SimpleNamespace(operations=[object()] * 8), binding=None,
        runtime=SimpleNamespace(plan_version=2, policy_epoch=3, profile_version=4,
                                profile_adapter=SimpleNamespace(fingerprint='build-a')),
        policy=policy or AsyncPolicy(prefetch_blocks=2, max_io_requests=4,
                                     max_mpi_requests=4, lookahead_gates=8,
                                     beam_width=4, max_active_jobs=3),
        config=SimpleNamespace(backend='buffered'),
        io_engine=SimpleNamespace(capacity=io_capacity),
        transport=SimpleNamespace(data=_TransportData(), _MPI=SimpleNamespace(TAG_UB=0)),
        budget=SimpleNamespace(limit=100_000, used=10_000), _private_epoch=private,
    )


def _snapshot(snapshot_id='snap-a'):
    return SimpleNamespace(snapshot_id=snapshot_id)


def test_generation_is_finite_bounded_and_candidate_ids_are_contextual():
    sim = _sim()
    first = generate(sim, _snapshot(), max_candidates=6)
    repeated = generate(sim, _snapshot(), max_candidates=6)
    assert 1 <= len(first) <= 6
    assert [p.proposal_id for p in first] == [p.proposal_id for p in repeated]
    assert len({item.proposal_id for item in first}) == len(first)
    assert first[0].classification == 'reference'
    assert all(not item.eligible for item in first)
    assert all(item.source_snapshot_id == 'snap-a' for item in first)
    assert generate(sim, _snapshot('snap-b'), max_candidates=6)[0].proposal_id != first[0].proposal_id


@pytest.mark.parametrize('limit', [0, 17, True, 1.5])
def test_candidate_limit_rejects_unbounded_or_noninteger_values(limit):
    with pytest.raises(ValueError, match='Candidate limit'):
        generate(_sim(), _snapshot(), max_candidates=limit)


def test_wall_deadline_stops_candidate_enumeration():
    ticks = iter((0.0, 0.2))
    proposals = generate(_sim(), _snapshot(), max_candidates=16,
                         wall_seconds=0.05, clock=lambda: next(ticks))
    assert len(proposals) == 1


def test_private_cut_and_unsupported_or_overcapacity_changes_are_rejected():
    policy = AsyncPolicy(max_io_requests=4, max_mpi_requests=4)
    capability = RuntimeCapabilityRegistry(io_capacity=2, mpi_capacity=8,
                                            available_ram_bytes=0, private=False)
    assert capability.check(policy, {'max_io_requests': 1}) is None
    assert capability.check(policy, {'max_io_requests': 3}) == 'NATIVE_QUEUE_CAPACITY'
    assert capability.check(policy, {'invented_switch': 1}) == 'RESTART_REQUIRED_OR_UNSUPPORTED'
    small_policy = AsyncPolicy(max_io_requests=1, max_mpi_requests=1)
    assert RuntimeCapabilityRegistry(8, 8, 1000).check(
        small_policy, {'prefetch_blocks': 65}) == 'PREFETCH_METADATA_LIMIT'
    assert RuntimeCapabilityRegistry(8, 8, 100).check(
        small_policy, {'max_io_requests': 2}) == 'QUEUE_RAM_ADMISSION'
    assert RuntimeCapabilityRegistry(8, 8, 1000, private=True).check(
        policy, {'max_io_requests': 1}) == 'PRIVATE_CUT'
    assert all(item.capability_reason == 'PRIVATE_CUT'
               for item in generate(_sim(private=True), _snapshot(), max_candidates=3))


def test_generated_candidates_explain_native_capacity_and_ram_rejections():
    proposals = generate(_sim(io_capacity=0), _snapshot(), max_candidates=4)
    by_updates = {dict(item.updates).get('max_io_requests'): item for item in proposals
                  if 'max_io_requests' in dict(item.updates)}
    assert by_updates[1].capability_reason == 'NATIVE_QUEUE_CAPACITY'
    assert by_updates[1].reason == 'NATIVE_QUEUE_CAPACITY'


def test_proposal_context_ignores_rank_but_tracks_layout_and_placement():
    from svsim.replan_candidates import proposal_context
    a = _sim()
    b = _sim(); b.rank = 9
    assert proposal_context(a) == proposal_context(b)
    assert proposal_context(a) != proposal_context(_sim(layout=(1, 0)))
    assert proposal_context(a) != proposal_context(_sim(placement=2))
