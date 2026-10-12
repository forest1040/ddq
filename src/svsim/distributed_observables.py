"""Streaming block-pair Pauli products and distributed expectations."""
from __future__ import annotations

import operator
import numpy as np

from .distributed_kernels import pauli_inner
from .observables import ObservableIR


def _physical_mask(mask, layout, num_qubits):
    if mask < 0 or mask >> num_qubits:
        raise ValueError("Pauli mask references a qubit outside the simulator")
    return sum(((mask >> logical) & 1) << physical
               for logical, physical in enumerate(layout.logical_to_physical))


def _reduce(sim, value):
    local = np.asarray([value], dtype=np.complex128)
    return complex(sim.transport.sum_array(local)[0])


def inner(sim, bra_root, ket_root, x_mask, z_mask, controls=(),
          control_values=(), reduce=True):
    """Compute ``<bra|P|ket>`` using source-owned block pairs.

    Masks and controls use physical qubit indices. ``bra_root`` and
    ``ket_root`` may be separate roots in the simulator's block store.
    """
    n, b = sim.num_qubits, sim.block_bits
    error = None
    try:
        x_mask, z_mask = operator.index(x_mask), operator.index(z_mask)
        controls = tuple(operator.index(q) for q in controls)
        control_values = tuple(operator.index(v) for v in control_values)
        if (x_mask | z_mask) < 0 or (x_mask | z_mask) >> n:
            raise ValueError("Pauli mask references a qubit outside the simulator")
        if len(controls) != len(control_values) or any(v not in (0, 1) for v in control_values):
            raise ValueError("invalid computational projector")
        if len(set(controls)) != len(controls) or any(q < 0 or q >= n for q in controls):
            raise ValueError("invalid projector controls")
    except Exception as exc:
        error = str(exc)
    sim.transport.vote(error)
    sim.transport.agree({"bra_root": bra_root, "ket_root": ket_root,
                         "x": int(x_mask), "z": int(z_mask),
                         "controls": list(controls), "values": list(control_values),
                         "reduce": bool(reduce)})
    x_upper = x_mask >> b
    local = 0j
    correction = 0j
    for h in range(sim.ownership.num_blocks):
        owner = sim.ownership.shard_owner(h)
        partner = h ^ x_upper
        with sim.fetch(h, owner, root=ket_root, stream="ket") as ket_buf:
            with sim.fetch(partner, owner, root=bra_root, stream="bra") as bra_buf:
                if ket_buf is not None:
                    ket = ket_buf.array(sim.store.payload_bytes)
                    bra = bra_buf.array(sim.store.payload_bytes)
                    try:
                        value = pauli_inner(bra, ket, x_mask, z_mask, h, b,
                                            controls, control_values)
                        adjusted = value - correction
                        updated = local + adjusted
                        correction = (updated - local) - adjusted
                        local = updated
                    finally:
                        del ket, bra
    return _reduce(sim, local) if reduce else local


def _observables_value(sim, observables):
    prepared = []
    error = None
    try:
        for observable in observables:
            if not isinstance(observable, ObservableIR):
                observable = ObservableIR(tuple(observable))
            terms = []
            for term in observable.terms:
                x = _physical_mask(term.x_mask, sim.layout, sim.num_qubits)
                z = _physical_mask(term.z_mask, sim.layout, sim.num_qubits)
                terms.append((term, x, z))
            prepared.append(tuple(terms))
    except Exception as exc:
        error = str(exc)
    sim.transport.vote(error)
    sim.transport.agree({"root": sim.root, "layout": list(sim.layout.logical_to_physical),
                         "observables": [[(t.x_mask, t.z_mask,
                                           complex(t.coefficient).real)
                                          for t, _, _ in terms]
                                         for terms in prepared]})

    # Group all requested observables together so a source/partner pair is
    # fetched once for every distinct upper-X mask.
    groups = {}
    for oi, terms in enumerate(prepared):
        for term, x, z in terms:
            groups.setdefault(x >> sim.block_bits, []).append((oi, term, x, z))
    totals = [0j for _ in prepared]
    corrections = [0j for _ in prepared]
    for x_upper, terms in groups.items():
        for h in range(sim.ownership.num_blocks):
            owner = sim.ownership.shard_owner(h)
            partner = h ^ x_upper
            with sim.fetch(h, owner, root=sim.root, stream="ket") as ket_buf:
                with sim.fetch(partner, owner, root=sim.root, stream="bra") as bra_buf:
                    if ket_buf is None:
                        continue
                    ket = ket_buf.array(sim.store.payload_bytes)
                    bra = bra_buf.array(sim.store.payload_bytes)
                    try:
                        for oi, term, x, z in terms:
                            value = term.coefficient * pauli_inner(
                                bra, ket, x, z, h, sim.block_bits)
                            adjusted = value - corrections[oi]
                            updated = totals[oi] + adjusted
                            corrections[oi] = (updated - totals[oi]) - adjusted
                            totals[oi] = updated
                    finally:
                        del ket, bra
    reduced = sim.transport.sum_array(np.asarray(totals, dtype=np.complex128))
    values = []
    for value in reduced:
        value = complex(value)
        if abs(value.imag) > 1e-10:
            raise ArithmeticError("Hermitian expectation has an imaginary residual")
        values.append(float(value.real))
    return tuple(values)


def expectation(sim, observables):
    """Evaluate one ObservableIR or a sequence of them without gathering state."""
    if isinstance(observables, ObservableIR):
        return _observables_value(sim, (observables,))[0]
    return _observables_value(sim, tuple(observables))
