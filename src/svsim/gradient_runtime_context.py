"""Exact semantic and execution context for a prepared gradient call."""
from dataclasses import asdict
from hashlib import sha256

from .journal import canonical,content_hash
from .observables import ObservableIR


def gradient_context(sim,observable,requested):
    if not isinstance(observable,ObservableIR):observable=ObservableIR(tuple(observable))
    ids=tuple(p.parameter_id if hasattr(p,'parameter_id') else str(p) for p in requested)
    if len(set(ids))!=len(ids):raise ValueError('Duplicate gradient parameters')
    tape=sha256()
    for entry in sim.history:tape.update(canonical(entry).encode());tape.update(b'\n')
    obs=tuple((t.x_mask,t.z_mask,complex(t.coefficient).real,complex(t.coefficient).imag) for t in observable.terms)
    return {'source':sim.circuit_hash,'initial':sim.initial_state_hash,
        'binding':sim.binding.binding_id if sim.binding else None,
        'observable':content_hash(obs),'requested':ids,'n':sim.num_qubits,'b':sim.block_bits,
        'P':sim.size,'layout':sim.layout.logical_to_physical,'history':tape.hexdigest(),
        'frontier':sim.frontier,'placement_epoch':sim.placement_epoch,
        'policy':asdict(sim.policy),'backend':sim.config.backend,'store':sim.config.amplitude_store,
        'build':getattr(sim.runtime.profile_adapter,'fingerprint',None),
        'origin_present':bool(sim._gradient_origin),'frame_policy':sim.config.frame_policy,
        'precision':'complex128','endpoint':'gradient-call-after-forward',
        'injected':bool(getattr(sim,'_measurement_injected',False))}
