"""Cost-component oracle; it never manufactures unknown transition costs."""
from dataclasses import dataclass
import math


@dataclass(frozen=True)
class ReplanCosts:
    continue_lower_seconds: float | None = None
    execution_upper_seconds: float | None = None
    planning_seconds: float | None = None
    drain_seconds: float | None = None
    migration_seconds: float | None = None
    calibration_seconds: float | None = None
    validation_seconds: float | None = None
    checkpoint_seconds: float | None = None
    cleanup_seconds: float | None = None
    context_hash: str = ''
    qualified: bool = False

    def __post_init__(self):
        from dataclasses import fields
        for field in fields(self):
            if not field.name.endswith('seconds'):continue
            value=getattr(self,field.name)
            if value is not None and (isinstance(value,bool) or not math.isfinite(value) or value<0):raise ValueError('Invalid replan cost')
        if type(self.qualified) is not bool:raise ValueError('Invalid quality flag')


@dataclass(frozen=True)
class ReplanEvaluation:
    proposal_id: str
    gain_lower_seconds: float | None
    total_candidate_upper_seconds: float | None
    eligible: bool
    reason: str


def one_off_seconds(costs):
    """Transition costs paid once whether or not the degradation persists (everything but execution)."""
    parts=(costs.planning_seconds,costs.drain_seconds,costs.migration_seconds,costs.calibration_seconds,
           costs.validation_seconds,costs.checkpoint_seconds,costs.cleanup_seconds)
    return None if any(p is None for p in parts) else sum(parts)


def evaluate(proposal,costs,*,minimum_gain_fraction=.1,absolute_margin_seconds=.1,paid_excess_seconds=None):
    """Price one proposal. ``paid_excess_seconds`` enables the break-even rule.

    The gain lower bound assumes the observed excess persists to the endpoint,
    which a transient degradation violates. With ``paid_excess_seconds`` (the
    excess already paid since the baseline) a switch is allowed only once that
    paid excess reaches its one-off cost, the ski-rental rule: whatever the
    degradation does next, the total cost stays within about twice the
    hindsight-optimal choice of switching or not.
    """
    if isinstance(minimum_gain_fraction,bool) or isinstance(absolute_margin_seconds,bool) or not 0<=minimum_gain_fraction<1 or not math.isfinite(absolute_margin_seconds) or absolute_margin_seconds<0:
        raise ValueError('Invalid gain margin')
    if proposal.capability_reason:return ReplanEvaluation(proposal.proposal_id,None,None,False,proposal.capability_reason)
    components=[costs.execution_upper_seconds,costs.planning_seconds,costs.drain_seconds,
        costs.migration_seconds,costs.calibration_seconds,costs.validation_seconds,
        costs.checkpoint_seconds,costs.cleanup_seconds]
    if costs.continue_lower_seconds is None or any(value is None for value in components):
        return ReplanEvaluation(proposal.proposal_id,None,None,False,'UNKNOWN_COST_COMPONENT')
    total=sum(components);gain=costs.continue_lower_seconds-total
    if not all(math.isfinite(value) for value in (total,gain)):
        return ReplanEvaluation(proposal.proposal_id,None,None,False,'COST_OVERFLOW')
    if not costs.qualified or costs.context_hash!=proposal.source_context_hash:
        return ReplanEvaluation(proposal.proposal_id,gain,total,False,'UNQUALIFIED_OR_MISMATCHED_COST')
    eligible=gain>max(absolute_margin_seconds,minimum_gain_fraction*costs.continue_lower_seconds)
    if not eligible:return ReplanEvaluation(proposal.proposal_id,gain,total,False,'INSUFFICIENT_GAIN')
    if paid_excess_seconds is not None:
        if isinstance(paid_excess_seconds,bool) or not math.isfinite(paid_excess_seconds) or paid_excess_seconds<0:
            raise ValueError('Invalid paid excess')
        if paid_excess_seconds<one_off_seconds(costs):
            return ReplanEvaluation(proposal.proposal_id,gain,total,False,'BREAK_EVEN_NOT_REACHED')
    return ReplanEvaluation(proposal.proposal_id,gain,total,True,'MEASURED_POSITIVE_GAIN')
