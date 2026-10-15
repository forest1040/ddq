import pytest

from svsim.replan_candidates import ReplanProposal
from svsim.replan_cost import ReplanCosts, evaluate


def _proposal(*, capability_reason=None, context='ctx'):
    return ReplanProposal('proposal', context, 'snapshot', (), 'LIVE_TUNE',
                          capability_reason, 100.0)


def _costs(**overrides):
    values = dict(continue_lower_seconds=100.0, execution_upper_seconds=20.0,
                  planning_seconds=2.0, drain_seconds=3.0, migration_seconds=4.0,
                  calibration_seconds=5.0, validation_seconds=6.0,
                  checkpoint_seconds=7.0, cleanup_seconds=8.0,
                  context_hash='ctx', qualified=True)
    values.update(overrides)
    return ReplanCosts(**values)


def test_gain_counts_every_transition_cost_once():
    result = evaluate(_proposal(), _costs(), minimum_gain_fraction=.1,
                      absolute_margin_seconds=.1)
    assert result.total_candidate_upper_seconds == 55.0
    assert result.gain_lower_seconds == 45.0
    assert result.eligible
    assert result.reason == 'MEASURED_POSITIVE_GAIN'


@pytest.mark.parametrize('component', [
    'continue_lower_seconds', 'execution_upper_seconds', 'planning_seconds',
    'drain_seconds', 'migration_seconds', 'calibration_seconds',
    'validation_seconds', 'checkpoint_seconds', 'cleanup_seconds',
])
def test_unknown_cost_component_cannot_claim_gain(component):
    result = evaluate(_proposal(), _costs(**{component: None}))
    assert not result.eligible
    assert result.gain_lower_seconds is None
    assert result.total_candidate_upper_seconds is None
    assert result.reason == 'UNKNOWN_COST_COMPONENT'


def test_unqualified_or_context_mismatched_cost_cannot_be_eligible():
    assert evaluate(_proposal(), _costs(qualified=False)).reason == 'UNQUALIFIED_OR_MISMATCHED_COST'
    result = evaluate(_proposal(context='new-context'), _costs())
    assert not result.eligible
    assert result.gain_lower_seconds == 45.0
    assert result.reason == 'UNQUALIFIED_OR_MISMATCHED_COST'


def test_capability_rejection_precedes_numeric_gain():
    result = evaluate(_proposal(capability_reason='MPI_TAG_CAPACITY'), _costs())
    assert result.reason == 'MPI_TAG_CAPACITY'
    assert not result.eligible
    assert result.gain_lower_seconds is None


def test_strict_gain_margin_uses_larger_absolute_or_fractional_threshold():
    # Candidate total 90 gives exactly ten seconds of gain, equal to the fractional threshold.
    boundary = evaluate(_proposal(), _costs(continue_lower_seconds=100,
                           execution_upper_seconds=90, planning_seconds=0, drain_seconds=0,
                           migration_seconds=0, calibration_seconds=0, validation_seconds=0,
                           checkpoint_seconds=0, cleanup_seconds=0),
                        minimum_gain_fraction=.1, absolute_margin_seconds=2)
    assert not boundary.eligible
    assert boundary.reason == 'INSUFFICIENT_GAIN'
    # With a small baseline, the absolute threshold dominates.
    absolute = evaluate(_proposal(), _costs(continue_lower_seconds=10,
                           execution_upper_seconds=8, planning_seconds=0, drain_seconds=0,
                           migration_seconds=0, calibration_seconds=0, validation_seconds=0,
                           checkpoint_seconds=0, cleanup_seconds=0),
                        minimum_gain_fraction=.1, absolute_margin_seconds=2)
    assert not absolute.eligible
    assert absolute.gain_lower_seconds == 2


def test_overflow_is_unknown_gain_and_invalid_numbers_are_rejected():
    result = evaluate(_proposal(), _costs(continue_lower_seconds=1e308,
                          execution_upper_seconds=1e308, planning_seconds=1e308,
                          drain_seconds=1e308, migration_seconds=1e308,
                          calibration_seconds=1e308, validation_seconds=1e308,
                          checkpoint_seconds=1e308, cleanup_seconds=1e308))
    assert not result.eligible
    assert result.reason == 'COST_OVERFLOW'
    with pytest.raises(ValueError, match='Invalid replan cost'):
        _costs(execution_upper_seconds=float('inf'))
    with pytest.raises(ValueError, match='Invalid gain margin'):
        evaluate(_proposal(), _costs(), minimum_gain_fraction=1)


def test_break_even_waits_until_paid_excess_covers_one_off_costs():
    from svsim.replan_cost import one_off_seconds
    costs = _costs()
    assert one_off_seconds(costs) == 35.0  # everything except candidate execution
    early = evaluate(_proposal(), costs, paid_excess_seconds=34.9)
    assert not early.eligible and early.reason == 'BREAK_EVEN_NOT_REACHED' and early.gain_lower_seconds == 45.0
    assert evaluate(_proposal(), costs, paid_excess_seconds=35.0).eligible
    # Without a gain the reason stays INSUFFICIENT_GAIN; without the argument the rule is off.
    assert evaluate(_proposal(), _costs(continue_lower_seconds=50.0), paid_excess_seconds=1e9).reason == 'INSUFFICIENT_GAIN'
    assert evaluate(_proposal(), costs).eligible
    with pytest.raises(ValueError):
        evaluate(_proposal(), costs, paid_excess_seconds=-1.0)
