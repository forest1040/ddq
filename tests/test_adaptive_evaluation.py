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


@pytest.mark.parametrize('values,reason', [
    ([.001,.001,.004,.004],'DRIFTING_BASELINE'),
    ([.001,.01,.001,.01],'DRIFTING_BASELINE'),
    ([.001,.001,.0012,.0012],'LOW_BASELINE_HELD_OUT_COVERAGE'),
])
def test_unstable_prefix_cannot_produce_replan_evidence(values,reason):
    session=AdaptiveEvaluation()
    for value in values:
        row=session.observe({'actual_service_seconds':value,'injected_delay_seconds':0,
            'success':True,'source_ids':(len(session.events),),'work_class':'same'},profile=PROFILE,
            circuit_hash='source',num_qubits=4,block_bits=2,ranks=1)
    assert row['health']=='INSUFFICIENT_BASELINE' and row['reason']==reason
    assert not row['baseline_quality']['certified']
    for _ in range(8):observe(session,.1)
    assert session.estimate() is None
    assert not session.report()['positive_gain_verified']


def test_short_prefix_is_unknown_and_certified_prefix_stays_evaluation_only():
    session=AdaptiveEvaluation(baseline_groups=3)
    for _ in range(3):row=observe(session)
    assert row['reason']=='INSUFFICIENT_BASELINE_SAMPLES'
    assert session.estimate() is None
    session=AdaptiveEvaluation()
    for _ in range(4):observe(session)
    for _ in range(4):row=observe(session,.01)
    quality=row['estimate']['baseline_quality']
    assert quality['certified'] and quality['held_out_coverage']==1
    assert quality['scope']=='injected' and quality['production_profile'] is False


def test_excess_interval_and_paid_excess_use_baseline_envelope():
    session=AdaptiveEvaluation()
    for service in (.001,.0011,.001,.0011):
        session.observe({'actual_service_seconds':service,'injected_delay_seconds':0,
            'success':True,'source_ids':(len(session.events),)},profile=PROFILE,
            circuit_hash='source',num_qubits=4,block_bits=2,ranks=1)
    for _ in range(4):row=observe(session,.01)
    estimate=row['estimate']
    assert estimate['extra_lower_per_call']==pytest.approx(.0099)
    assert estimate['extra_upper_per_call']==pytest.approx(.01)
    assert estimate['paid_excess_seconds']==pytest.approx(4*.0099)


def test_resource_injected_service_cannot_enter_noninjected_prefix():
    session=AdaptiveEvaluation()
    for _ in range(4):
        row=session.observe({'actual_service_seconds':.1,'injected_delay_seconds':0,
            'resource_injected_seconds':.09,'success':True,'source_ids':(len(session.events),)},
            profile=PROFILE,circuit_hash='source',num_qubits=4,block_bits=2,ranks=1)
    assert not session.prefix and row['health']=='INSUFFICIENT_BASELINE'
    assert session.estimate() is None
