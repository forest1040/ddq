"""Finite policy proposals and explicit runtime capability rejection."""
from dataclasses import asdict,dataclass,replace
from time import monotonic
import math

from .journal import content_hash
from .remaining_work import runtime_context


def proposal_context(sim):
    context=runtime_context(sim)
    context.pop('rank',None)
    return content_hash(context)


@dataclass(frozen=True)
class RuntimeCapabilityRegistry:
    io_capacity: int
    mpi_capacity: int
    available_ram_bytes: int
    private: bool = False

    @classmethod
    def capture(cls,sim):
        io=min(64,getattr(sim.io_engine,'capacity',64))
        tag=sim.transport.data.Get_attr(sim.transport._MPI.TAG_UB)
        mpi=min(64,tag+1 if tag is not None else 64)
        return cls(io,mpi,max(0,sim.budget.limit-sim.budget.used),sim._private_epoch)

    def check(self,policy,updates):
        if self.private:return 'PRIVATE_CUT'
        allowed={'prefetch_blocks','max_io_requests','max_mpi_requests','lookahead_gates','beam_width','swap_policy','max_active_jobs'}
        if set(updates)-allowed:return 'RESTART_REQUIRED_OR_UNSUPPORTED'
        try:target=replace(policy,**updates)
        except (TypeError,ValueError):return 'INVALID_POLICY'
        if target.max_io_requests>self.io_capacity:return 'NATIVE_QUEUE_CAPACITY'
        if target.max_mpi_requests>self.mpi_capacity:return 'MPI_TAG_CAPACITY'
        if target.prefetch_blocks>64:return 'PREFETCH_METADATA_LIMIT'
        if max(0,target.max_io_requests-policy.max_io_requests)*512>self.available_ram_bytes:return 'QUEUE_RAM_ADMISSION'
        return None


@dataclass(frozen=True)
class ReplanProposal:
    proposal_id: str
    source_context_hash: str
    source_snapshot_id: str
    updates: tuple
    classification: str
    capability_reason: str | None
    expires_at: float
    eligible: bool = False
    reason: str = 'INSUFFICIENT_CALIBRATED_COST'


def generate(sim,snapshot,*,max_candidates=16,wall_seconds=.1,clock=monotonic):
    if type(max_candidates) is not int or not 1<=max_candidates<=16:raise ValueError('Candidate limit must be 1..16')
    if isinstance(wall_seconds,bool) or not math.isfinite(wall_seconds) or not 0<wall_seconds<=1:raise ValueError('Invalid planning deadline')
    start=clock();capability=RuntimeCapabilityRegistry.capture(sim)
    context=proposal_context(sim);policy=sim.policy
    options=[{}]
    if policy.prefetch_blocks:options.append({'prefetch_blocks':0})
    if policy.max_io_requests>1:options.append({'max_io_requests':1})
    if policy.max_mpi_requests>1:options.append({'max_mpi_requests':max(1,policy.max_mpi_requests//2)})
    if policy.max_active_jobs>1:options.append({'max_active_jobs':1})
    for swap in ('S0','S2','S1'):
        if swap!=policy.swap_policy:options.append({'swap_policy':swap})
    if policy.lookahead_gates>1:options.append({'lookahead_gates':max(1,policy.lookahead_gates//2)})
    if policy.beam_width>1:options.append({'beam_width':1})
    result=[]
    expires=start+policy.candidate_ttl_seconds
    for updates in options:
        if len(result)>=max_candidates or result and clock()-start>=wall_seconds:break
        kind='reference' if not updates else ('FRONTIER_REPLAN' if set(updates)&{'swap_policy','lookahead_gates','beam_width'} else 'LIVE_TUNE')
        reason=capability.check(policy,updates)
        payload={'source_context':context,'snapshot':snapshot.snapshot_id,'updates':updates,'classification':kind}
        result.append(ReplanProposal(content_hash(payload),context,snapshot.snapshot_id,
            tuple(sorted(updates.items())),kind,reason,expires,False,reason or ('CONTINUE_REFERENCE' if not updates else 'INSUFFICIENT_CALIBRATED_COST')))
    return tuple(result)
