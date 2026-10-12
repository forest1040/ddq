"""Read each Pauli partner pair once per upper-mask group, without gather."""
from dataclasses import dataclass
from time import perf_counter
from .observables import ObservableIR


@dataclass(frozen=True)
class ObservablePlan:
    # (upper physical X mask, ((observable index, PauliTerm), ...))
    groups: tuple
    observable_count: int
    layout_id: int

    @classmethod
    def build(cls, observables, layout, block_bits):
        grouped = {}
        for index, observable in enumerate(observables):
            for term in observable.terms:
                n = len(layout.logical_to_physical)
                if (term.x_mask|term.z_mask) >> n: raise ValueError('Observable qubit out of range')
                x = sum(((term.x_mask >> q)&1) << p for q,p in enumerate(layout.logical_to_physical))
                grouped.setdefault(x >> block_bits,[]).append((index,term))
        return cls(tuple((mask,tuple(terms)) for mask,terms in sorted(grouped.items())),len(observables),layout.layout_id)


class CompensatedSum:
    def __init__(self): self.value = 0j; self.error = 0j
    def add(self, value):
        y = value-self.error
        total = self.value+y
        self.error = (total-self.value)-y
        self.value = total


def streaming_expectation(sim, observables, handle):
    sim._open()
    single = isinstance(observables,ObservableIR)
    observables = (observables,) if single else tuple(observables)
    if handle.circuit_hash != sim.circuit_hash: raise ValueError('State handle belongs to another circuit')
    # A committed handle's cut may precede this run's frontier, but its symbolic
    # phase/frame values must use the fixed binding represented by that handle.
    for _ in sim.store.root_chain(handle.root): pass
    plan = ObservablePlan.build(observables,handle.layout,sim.block_bits)
    reserved = 256*sum(len(o.terms) for o in observables)+128*len(observables)
    group_count = 2 if any(mask for mask,_ in plan.groups) else 1
    sim.cache.ensure(reserved+2*group_count*sim.cache.buffer_cost)
    sim.budget.reserve(reserved)
    accumulators = [CompensatedSum() for _ in observables]
    size = 1 << sim.block_bits
    start = perf_counter()
    groups_read = 0
    try:
        for upper_x, terms in plan.groups:
            physical_terms = []
            for observable_index, term in terms:
                x = sum(((term.x_mask >> q)&1) << p for q,p in enumerate(handle.layout.logical_to_physical))
                z = sum(((term.z_mask >> q)&1) << p for q,p in enumerate(handle.layout.logical_to_physical))
                physical_terms.append((observable_index,term.coefficient,x & (size-1),z,1j**(x&z).bit_count()))
            for h in range(sim.store.num_blocks):
                partner = h ^ upper_x
                if h > partner: continue
                partners = [h] if h == partner else [h,partner]
                with sim._logical_buffers(handle.root,partners) as (arrays,_):
                    groups_read += 1
                    for observable_index,coefficient,lower_x,z,y_phase in physical_terms:
                        subtotal = CompensatedSum()
                        for direction,block in enumerate(partners):
                            ket = arrays[direction]
                            bra = arrays[0] if len(partners) == 1 else arrays[1-direction]
                            for offset in range(size):
                                physical = (block << sim.block_bits)|offset
                                sign = -1 if (physical&z).bit_count()&1 else 1
                                subtotal.add(bra[offset^lower_x].conjugate()*y_phase*sign*ket[offset])
                        accumulators[observable_index].add(coefficient*subtotal.value)
                    if physical_terms: del ket,bra
        values = []
        for accumulator in accumulators:
            if abs(accumulator.value.imag) > 1e-10: raise ArithmeticError('Non-real Hermitian expectation residual')
            values.append(float(accumulator.value.real))
        sim.trace.record('observable-reduction',performance_class='observable',transfer_ms=(perf_counter()-start)*1000,
            metadata={'path':'STREAMING_EXPECTATION','partner_groups':groups_read,'mask_groups':len(plan.groups),
                      'observable_count':len(observables),'frontier':handle.frontier,'layout_id':handle.layout.layout_id})
        return values[0] if single else tuple(values)
    finally: sim.budget.release(reserved)
