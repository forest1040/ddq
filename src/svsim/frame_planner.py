"""Legality precedes conservative, versioned frame representation costs."""
from dataclasses import asdict, dataclass


@dataclass(frozen=True)
class FramePlanDecision:
    chosen: str
    legal_candidates: tuple[str,...]
    costs: tuple[tuple[str,float],...]
    reason: str
    working_set_bytes: int
    profile_id: str
    model_version: int = 1

    def data(self): return asdict(self)


def choose_alignment(assets,policy,working_set,profile_id,calibration=None):
    legal = ('identity','input-a','input-b')
    if policy not in {'auto',*legal}: raise ValueError('Unknown alignment policy')
    # Synthesis gate counts are a deterministic conservative CPU proxy, not
    # measured seconds. Identical tableau needs no differential Clifford.
    costs = [('identity',float(sum(len(a.word) for a in assets)))]
    for i,label in enumerate(('input-a','input-b')):
        reference = assets[min(i,len(assets)-1)]
        costs.append((label,float(sum(0 if a.tableau == reference.tableau else len(a.word)+len(reference.word) for a in assets))))
    if calibration is not None:
        costs = [(label,value*calibration.frame_gate_seconds) for label,value in costs]
    if policy == 'auto':
        if calibration is None:
            # Zero differential work is provable. Other cost differences have
            # no calibrated rate, so retain the safe identity path.
            zero = next((label for label,value in costs[1:] if value == 0),None)
            chosen = zero or 'identity'
        else: chosen = min(costs,key=lambda x:x[1])[0]
    else: chosen = policy
    reason = ('Calibrated synthesis-work estimate' if calibration is not None else 'Uncalibrated: retain only provably zero-cost common frame; otherwise identity') if policy == 'auto' else 'Explicit legal alignment policy'
    return FramePlanDecision(chosen,legal,tuple(costs),reason,working_set,calibration.profile_id if calibration is not None else profile_id)


def choose_local(policy,working_set,profile_id,*,calibration=None,frame_gate_estimate=0,block_bits=1):
    legal = ('keep','materialize')
    if policy not in {'auto',*legal}: raise ValueError('Unknown frame policy')
    if calibration is None:
        costs = (('keep',float(1+block_bits)),('materialize',float(1+frame_gate_estimate)))
        chosen = 'materialize' if policy == 'auto' else policy
        reason = 'Uncalibrated local rotation: safe native materialization' if policy == 'auto' else 'Explicit legal local policy'
    else:
        # Conjugation can grow support to all b local bits. Scalar-loop Pauli
        # kernels do not require 2**support scratch; a conservative support
        # multiplier includes parity and stride variation.
        keep = calibration.conjugation_seconds+calibration.pauli_rotation_seconds*(1+block_bits/8)
        materialize = calibration.frame_gate_seconds*(frame_gate_estimate+1)
        costs = (('keep',keep),('materialize',materialize))
        chosen = min(costs,key=lambda item:item[1])[0] if policy == 'auto' else policy
        reason = 'Calibrated local CPU estimate; synthesis-work history is an upper-bound proxy' if policy == 'auto' else 'Explicit legal local policy'
    return FramePlanDecision(chosen,legal,costs,reason,working_set,calibration.profile_id if calibration else profile_id)
