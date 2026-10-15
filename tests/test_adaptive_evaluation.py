import pytest

from svsim.adaptive_evaluation import AdaptiveEvaluation
from svsim.lifecycle import HardwareProfile


PROFILE=HardwareProfile('evaluation',1,'fixed','fake-host','fake','0','3',1,None,None,None)


def observe(session,delay=0,success=True):
    return session.observe({'actual_service_seconds':.001,'injected_delay_seconds':delay,
        'success':success,'source_ids':(len(session.events),)},profile=PROFILE,
        circuit_hash='semantic-source',num_qubits=4,block_bits=2,ranks=1)


def test_persistence_transient_recovery_and_missing_baseline():
    session=AdaptiveEvaluation()
    for _ in range(4):observe(session)
    assert session.events[-1]['health']=='BASELINED'
    for _ in range(4):observe(session,.01)
    assert session.events[-1]['health']=='DEGRADED'
    for _ in range(4):observe(session)
    assert session.events[-1]['health']=='HEALTHY'
    report=session.report(candidates=({'eligible':False},))
    assert report['degraded_observed'] and not report['positive_gain_verified']
    assert report['eligible_candidate_count']==0
    assert report['scope']=='injected'
    assert not any(row['production_profile'] for row in report['events'])
    transient=AdaptiveEvaluation()
    for _ in range(4):observe(transient)
    observe(transient,.01)
    for _ in range(5):observe(transient)
    assert not transient.report()['degraded_observed']
    missing=AdaptiveEvaluation(baseline_groups=0)
    assert observe(missing,.01)['health']=='INSUFFICIENT_BASELINE'


def test_failed_events_bounds_and_invalid_values():
    session=AdaptiveEvaluation(max_events=1)
    assert observe(session,success=False)['health']=='INSUFFICIENT_METRICS'
    assert observe(session)['reason']=='EVALUATION_EVENT_LIMIT'
    assert session.report()['dropped_events']==1
    with pytest.raises(ValueError):observe(AdaptiveEvaluation(),float('nan'))
    with pytest.raises(ValueError):AdaptiveEvaluation(max_events=True)


def test_estimate_reports_excess_paid_since_the_baseline():
    session=AdaptiveEvaluation()
    for _ in range(4):observe(session)
    for _ in range(4):row=observe(session,.01)
    assert row['health']=='DEGRADED'
    # Four degraded calls of 10 ms each above the 1 ms baseline; the resource is unattributed here.
    assert row['estimate']['paid_excess_seconds']==pytest.approx(.04)
    assert row['estimate']['resource'] is None
