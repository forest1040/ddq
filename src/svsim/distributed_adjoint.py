"""Two-root, streaming adjoint gradient for the synchronous block runtime."""
from __future__ import annotations

from dataclasses import replace

import numpy as np

from .distributed_kernels import apply_pauli_action, inverse_gate
from .distributed_observables import _physical_mask, inner
from .ir import GateOp
from .journal import decode_gate
from .layout import QubitLayout
from .observables import ObservableIR
from .storage import StoredBlockView


def _set_cut(sim, root, frontier):
    sim.root = sim.local.root = root
    sim.frontier = sim.local.frontier = frontier


def _observable_root(sim, observable, ket_root):
    if not isinstance(observable, ObservableIR):
        observable = ObservableIR(tuple(observable))
    terms = []
    for term in observable.terms:
        x = _physical_mask(term.x_mask, sim.layout, sim.num_qubits)
        z = _physical_mask(term.z_mask, sim.layout, sim.num_qubits)
        terms.append((term, x, z))

    root = sim._new_root()
    sim._admit(5, output_blocks=sim.ownership.blocks_per_rank)
    for h in range(sim.ownership.num_blocks):
        owner = sim.ownership.shard_owner(h)
        error = None
        try:
            with sim._buffers(1, owner) as outputs:
                if outputs:
                    outbuf = outputs[0]
                    out = outbuf.array(sim.store.payload_bytes)
                    out[:] = 0
                else:
                    out = None
                for term, x, z in terms:
                    source = h ^ (x >> sim.block_bits)
                    compute_error=None
                    with sim.fetch(source, owner, root=ket_root, stream="ket") as fetched:
                        if out is not None:
                            ket = fetched.array(sim.store.payload_bytes)
                            try:
                                apply_pauli_action(ket, out, x, z, h, sim.block_bits,
                                                  coefficient=term.coefficient, accumulate=True)
                            except Exception as exc: compute_error=str(exc)
                            finally:
                                del ket
                    sim.transport.vote(compute_error)
                if outputs:
                    del out
                    out = None
                    version = sim.cache.adopt(outbuf)
                    sim._publish(root, h, StoredBlockView(version, sim.frontier, sim.frontier))
        except Exception as exc:
            error = str(exc)
        sim.transport.vote(error)
    return root


def _generator(gate, sim):
    if gate.kind in {"RX", "RY", "RZ"}:
        q = gate.targets[0]
        x = (1 << q) if gate.kind in {"RX", "RY"} else 0
        z = (1 << q) if gate.kind in {"RZ", "RY"} else 0
    elif gate.kind == "PAULI_ROT":
        x = sum(1 << q for q, p in zip(gate.targets, gate.paulis) if p in {"X", "Y"})
        z = sum(1 << q for q, p in zip(gate.targets, gate.paulis) if p in {"Z", "Y"})
    else:
        raise ValueError(f"Unsupported parameterized generator {gate.kind}")
    return x, z


def _apply_inverse_to_cut(sim, inverse, end):
    sim._execute(inverse, end)
    return sim.root


def _undo_layout(sim, entry):
    a, b = entry["bits"]
    sim._execute(GateOp("SWAP", (a, b), op_id=sim.frontier), sim.frontier,
                 physical_swap=True)
    return sim.root


def gradient(sim, observable, parameters=None):
    """Compute requested symbolic parameter derivatives using two block roots.

    Forward amplitudes are reversed in place through immutable root versions;
    the adjoint amplitudes occupy a separate root lineage. Only block-sized
    buffers are fetched and both simulator cuts are restored on return.
    """
    if not isinstance(observable, ObservableIR):
        observable = ObservableIR(tuple(observable))
    gates = sim.sequencer.operations
    dependencies = set().union(*(gate.dependencies for gate in gates)) if gates else set()
    validation_error = None
    try:
        if parameters is None:
            requested = sorted(dependencies)
        else:
            requested = sorted(p.parameter_id if hasattr(p, "parameter_id") else str(p)
                               for p in parameters)
            if len(set(requested)) != len(requested):
                raise ValueError("Duplicate requested parameter")
            missing = set(requested) - dependencies
            if missing:
                raise ValueError(f"Requested parameter has no circuit occurrence: {sorted(missing)}")
        if sim.binding is None and dependencies:
            raise ValueError("Gradient requires a parameter binding")
        binding = sim.binding
        for parameter_id in requested:
            binding.value(parameter_id)
    except Exception as exc:
        validation_error = str(exc)
        requested = []
        binding = sim.binding
    sim.transport.vote(validation_error)
    sim.transport.agree({"gradient_parameters": requested,
                         "observable": [(t.x_mask, t.z_mask, complex(t.coefficient).real)
                                        for t in observable.terms]})
    if not requested:
        return {}

    saved = (sim.root, sim.frontier, sim.local.root, sim.local.frontier,
             sim.layout, sim.local.layout, sim.binding, sim.local.binding,
             sim.placement_epoch, len(sim.history))
    final_forward = sim.root
    final_frontier = sim.frontier
    history = list(sim.history)
    old_workspace = getattr(sim,'_workspace_roots',set())
    sim._workspace_roots = {final_forward}
    try:
        adjoint_root = _observable_root(sim, observable, final_forward)
        adjoint_frontier = final_frontier
        forward_root = final_forward
        forward_frontier = final_frontier
        sim._workspace_roots = {final_forward,forward_root,adjoint_root}
        local_gradient = dict.fromkeys(requested, 0.0)

        for entry in reversed(history):
            if entry["kind"] == "layout":
                prior_layout = sim.layout
                prior_placement = sim.placement_epoch
                _set_cut(sim, forward_root, forward_frontier)
                _undo_layout(sim, entry)
                forward_root = sim.root
                forward_frontier = sim.frontier
                sim._workspace_roots = {final_forward,forward_root,adjoint_root}
                _set_cut(sim, adjoint_root, adjoint_frontier)
                _undo_layout(sim, entry)
                adjoint_root = sim.root
                adjoint_frontier = sim.frontier
                sim._workspace_roots = {final_forward,forward_root,adjoint_root}
                before = tuple(entry["layout_before"])
                sim.layout = sim.local.layout = QubitLayout(before, max(0, prior_layout.layout_id - 1))
                sim.placement_epoch = max(0, prior_placement - 1)
                continue

            source = int(entry["source"])
            physical_gate = decode_gate(entry["physical"])
            inverse = inverse_gate(physical_gate, sim.assets)

            semantic_gate = gates[source]
            if semantic_gate.dependencies and any(p in local_gradient for p in semantic_gate.dependencies):
                x, z = _generator(physical_gate, sim)
                overlap = inner(sim, adjoint_root, forward_root, x, z,
                                physical_gate.controls, physical_gate.control_values,
                                reduce=False)
                local_derivative = float(overlap.imag)
                for parameter_id in semantic_gate.dependencies:
                    if parameter_id in local_gradient:
                        factor = semantic_gate.angle.derivative(parameter_id).evaluate(binding)
                        local_gradient[parameter_id] += local_derivative * factor

            # Move both streams from cut j+1 to cut j after the overlap.
            _set_cut(sim, forward_root, forward_frontier)
            forward_root = _apply_inverse_to_cut(sim, inverse, source - 1)
            forward_frontier = sim.frontier
            sim._workspace_roots = {final_forward,forward_root,adjoint_root}
            _set_cut(sim, adjoint_root, adjoint_frontier)
            adjoint_root = _apply_inverse_to_cut(sim, inverse, source - 1)
            adjoint_frontier = sim.frontier
            sim._workspace_roots = {final_forward,forward_root,adjoint_root}

        vector = np.asarray([local_gradient[p] for p in requested], dtype=np.float64)
        reduced = sim.transport.sum_array(vector)
        return {parameter_id: float(value) for parameter_id, value in zip(requested, reduced)}
    finally:
        (sim.root, sim.frontier, sim.local.root, sim.local.frontier,
         sim.layout, sim.local.layout, sim.binding, sim.local.binding,
         sim.placement_epoch, _) = saved
        sim._workspace_roots = old_workspace
        if hasattr(sim.store,'reclaim_versions'):
            sim._memory_reclaim({sim.root,getattr(sim,'_rollback_root',sim.root)}|sim.committed_roots|old_workspace)
