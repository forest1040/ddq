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


MAX_LAYOUT_SCAN=4096


def _rank_bits(sim):
    ownership=getattr(sim,'ownership',None)
    return ownership.rank_bits if ownership is not None else 0


@dataclass(frozen=True)
class RuntimeCapabilityRegistry:
    io_capacity: int
    mpi_capacity: int
    available_ram_bytes: int
    private: bool = False
    num_qubits: int = 0
    rank_bits: int = 0

    @classmethod
    def capture(cls,sim):
        io=min(64,getattr(sim.io_engine,'capacity',64))
        tag=sim.transport.data.Get_attr(sim.transport._MPI.TAG_UB)
        mpi=min(64,tag+1 if tag is not None else 64)
        return cls(io,mpi,max(0,sim.budget.limit-sim.budget.used),sim._private_epoch,getattr(sim,'num_qubits',0),_rank_bits(sim))

    def check(self,policy,updates):
        if self.private:return 'PRIVATE_CUT'
        allowed={'prefetch_blocks','max_io_requests','max_mpi_requests','lookahead_gates','beam_width','swap_policy',
                 'max_active_jobs','layout_swap'}
        if set(updates)-allowed:return 'RESTART_REQUIRED_OR_UNSUPPORTED'
        if 'layout_swap' in updates:
            bits=tuple(updates['layout_swap']) if isinstance(updates['layout_swap'],(list,tuple)) else ()
            first_rank=self.num_qubits-self.rank_bits
            if (len(updates)!=1 or len(bits)!=2 or not all(type(b) is int and 0<=b<self.num_qubits for b in bits)
                    or not self.rank_bits or not (bits[0]>=first_rank)!=(bits[1]>=first_rank)):
                return 'INVALID_LAYOUT_MIGRATION'
            return None
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
    options.extend({'layout_swap':bits} for bits in layout_options(sim,getattr(snapshot,'endpoint_frontier',None)))
    result=[]
    expires=start+policy.candidate_ttl_seconds
    for updates in options:
        if len(result)>=max_candidates or result and clock()-start>=wall_seconds:break
        kind='reference' if not updates else ('FRONTIER_REPLAN' if set(updates)&{'swap_policy','lookahead_gates','beam_width','layout_swap'} else 'LIVE_TUNE')
        reason=capability.check(policy,updates)
        payload={'source_context':context,'snapshot':snapshot.snapshot_id,'updates':updates,'classification':kind}
        result.append(ReplanProposal(content_hash(payload),context,snapshot.snapshot_id,
            tuple(sorted(updates.items())),kind,reason,expires,False,reason or ('CONTINUE_REFERENCE' if not updates else 'INSUFFICIENT_CALIBRATED_COST')))
    return tuple(result)


def layout_options(sim,end,*,limit=2):
    """Physical (rank bit, local bit) swaps that localize the rank-bit qubits the remaining gates use most.

    For each rank bit holding a used logical qubit (most used first), pair it
    with the least-used local physical bit (lowest bit on ties). Nothing for one
    rank or a remainder longer than MAX_LAYOUT_SCAN sources.
    """
    rank_bits=_rank_bits(sim)
    start=sim.frontier+1
    if not rank_bits or end is None or end<start or end-start+1>MAX_LAYOUT_SCAN:return []
    uses=[0]*sim.num_qubits
    for gate in sim.sequencer.operations[start:end+1]:
        for q in gate.qubits:uses[q]+=1
    to_logical={p:q for q,p in enumerate(sim.layout.logical_to_physical)}
    first_rank=sim.num_qubits-rank_bits
    ranked=sorted((p for p in range(first_rank,sim.num_qubits) if uses[to_logical[p]]),key=lambda p:(-uses[to_logical[p]],p))
    local=min(range(first_rank),key=lambda p:(uses[to_logical[p]],p))
    return [(p,local) for p in ranked[:limit] if uses[to_logical[p]]>uses[to_logical[local]]]
