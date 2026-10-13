"""Execution adapters for the Phase 4 gradient method planner."""
from __future__ import annotations

from collections import OrderedDict

import numpy as np

from .distributed_adjoint import (
    _apply_inverse_to_cut,
    _generator,
    _observable_root,
    _set_cut,
    _undo_layout,
    gradient as adjoint_gradient,
)
from .distributed_kernels import inverse_gate
from .distributed_observables import inner
from .gradient_scheduler import GradientSchedule, shift_rule
from .ir import GateOp
from .journal import decode_gate
from .layout import QubitLayout


def _requested_ids(requested):
    ids = [p.parameter_id if hasattr(p, "parameter_id") else str(p) for p in requested]
    if len(set(ids)) != len(ids):
        raise ValueError("Requested parameter IDs must be unique")
    return ids


def _load_origin(sim):
    snapshot_id = getattr(sim, "_gradient_origin", None)
    data = None
    error = None
    try:
        if not snapshot_id:
            raise ValueError("Gradient origin snapshot was not captured before forward execution")
        if sim.rank == 0:
            from .distributed_checkpoint import load_owner_global
            data = load_owner_global(sim, snapshot_id)
            if data["frontier"] != -1:
                raise ValueError("Gradient origin must be the initial circuit cut")
    except Exception as exc:
        error = str(exc)
    sim.transport.vote(error)
    header = sim.transport.broadcast_json(
        {"root": data["root"], "frontier": data["frontier"],
         "layout": data["layout"], "layout_id": data.get("layout_id", 0),
         "placement_epoch": data["placement_epoch"]} if sim.rank == 0 else None
    )
    return header


def _check_schedule(sim, schedule):
    if not isinstance(schedule, GradientSchedule):
        raise TypeError("schedule must be a GradientSchedule")
    if sim.binding is None:
        raise ValueError("Gradient execution requires a binding")
    if schedule.binding_id != sim.binding.binding_id:
        raise ValueError("Gradient schedule binding does not match the simulator binding")
    if schedule.method not in {"adjoint", "recompute", "parameter-shift"}:
        raise ValueError(f"Unsupported planned gradient method {schedule.method!r}")


def _snapshot_state(sim):
    return (sim.root, sim.frontier, sim.local.root, sim.local.frontier,
            sim.layout, sim.local.layout, sim.binding, sim.local.binding,
            sim.placement_epoch, len(sim.history), getattr(sim, "_rollback_root", sim.root),
            set(getattr(sim, "_workspace_roots", set())))


def _restore_state(sim, saved):
    (sim.root, sim.frontier, sim.local.root, sim.local.frontier,
     sim.layout, sim.local.layout, sim.binding, sim.local.binding,
     sim.placement_epoch, history_length, sim._rollback_root, workspace) = saved
    del sim.history[history_length:]
    sim._workspace_roots = workspace


def _replay_forward_to(sim, history, target_index, origin, final_root, final_frontier,
                       adjoint_root, fixed_workspace):
    """Replay the actual tape from its origin through one occurrence."""
    saved = _snapshot_state(sim)
    sim._workspace_roots = set(fixed_workspace) | {origin["root"], final_root, adjoint_root}
    sim.root = sim.local.root = origin["root"]
    sim.frontier = sim.local.frontier = origin["frontier"]
    layout = QubitLayout(tuple(origin["layout"]), origin["layout_id"])
    sim.layout = sim.local.layout = layout
    sim.placement_epoch = origin["placement_epoch"]
    sim.binding = sim.local.binding = saved[6]
    replay_root = sim.root
    target_layout = sim.layout
    try:
        for index, entry in enumerate(history[:target_index + 1]):
            sim._rollback_root = sim.root
            if entry["kind"] == "layout":
                a, b = entry["bits"]
                sim._physical_swap(a, b)
            elif entry["kind"] == "gate":
                physical = decode_gate(entry["physical"])
                sim._execute(physical, final_frontier)
                expected = tuple(entry["layout"])
                if sim.layout.logical_to_physical != expected:
                    raise ValueError("Execution tape layout does not match replay layout")
            else:
                raise ValueError(f"Unknown execution tape entry {entry['kind']!r}")
            sim._workspace_roots = set(fixed_workspace) | {
                origin["root"], final_root, adjoint_root, sim.root,
            }
            if index == target_index:
                replay_root, target_layout = sim.root, sim.layout
        return replay_root, target_layout
    finally:
        _restore_state(sim, saved)


def _recompute_adjoint(sim, observable, requested, schedule):
    """Adjoint reverse sweep with source states recomputed from the origin."""
    ids = _requested_ids(requested)
    binding = sim.binding
    gates = sim.sequencer.operations
    factors: dict[int, dict[str, float]] = {}
    for occurrence in schedule.occurrences:
        factors.setdefault(occurrence.source_id, {})[occurrence.parameter_id] = occurrence.chain_factor
    history = list(sim.history)
    source_positions: dict[int, int] = {}
    for index, entry in enumerate(history):
        if entry["kind"] == "gate":
            source = int(entry["source"])
            if not 0 <= source < len(gates):
                raise ValueError(f"Execution tape references unknown source {source}")
            source_positions[source] = index
    missing = set(factors) - set(source_positions)
    if missing:
        raise ValueError(f"Gradient occurrences are absent from the execution tape: {sorted(missing)}")

    origin = _load_origin(sim)
    saved = _snapshot_state(sim)
    final_root, final_frontier = sim.root, sim.frontier
    old_workspace = set(getattr(sim, "_workspace_roots", set()))
    sim._workspace_roots = old_workspace | {origin["root"], final_root}
    local_gradient = {parameter_id: 0.0 for parameter_id in ids}
    try:
        adjoint_root = _observable_root(sim, observable, final_root)
        adjoint_frontier = final_frontier
        sim._workspace_roots = old_workspace | {origin["root"], final_root, adjoint_root}
        for index in reversed(range(len(history))):
            entry = history[index]
            if entry["kind"] == "layout":
                _set_cut(sim, adjoint_root, adjoint_frontier)
                _undo_layout(sim, entry)
                adjoint_root = sim.root
                adjoint_frontier = sim.frontier
                before = tuple(entry["layout_before"])
                sim.layout = sim.local.layout = QubitLayout(before, max(0, sim.layout.layout_id - 1))
                sim.placement_epoch = max(0, sim.placement_epoch - 1)
                sim._workspace_roots |= {adjoint_root}
                continue

            source = int(entry["source"])
            physical_gate = decode_gate(entry["physical"])
            if source in factors:
                forward_root, _layout = _replay_forward_to(
                    sim, history, index, origin, final_root, final_frontier,
                    adjoint_root, old_workspace,
                )
                x, z = _generator(physical_gate, sim)
                overlap = inner(sim, adjoint_root, forward_root, x, z,
                                physical_gate.controls, physical_gate.control_values,
                                reduce=False)
                derivative = float(overlap.imag)
                for parameter_id, factor in factors[source].items():
                    if parameter_id in local_gradient:
                        local_gradient[parameter_id] += derivative * factor

            inverse = inverse_gate(physical_gate, sim.assets)
            _set_cut(sim, adjoint_root, adjoint_frontier)
            adjoint_root = _apply_inverse_to_cut(sim, inverse, adjoint_frontier)
            adjoint_frontier = sim.frontier
            sim._workspace_roots = old_workspace | {origin["root"], final_root, adjoint_root}

        reduced = sim.transport.sum_array(np.asarray([local_gradient[p] for p in ids], dtype=np.float64))
        return {parameter_id: float(value) for parameter_id, value in zip(ids, reduced)}
    finally:
        _restore_state(sim, saved)
        if hasattr(sim.store, "reclaim_versions"):
            sim._memory_reclaim({sim.root, getattr(sim, "_rollback_root", sim.root)} |
                                sim.committed_roots | old_workspace | {origin["root"]})


def _share_child_budget(parent, child):
    """Transfer a new child's empty-cache baseline into the parent's ledger."""
    old_budget = child.budget
    charge = 0
    reserved = False
    error = None
    try:
        child.drain()
        child.cache.clear()
        charge = old_budget.used
    except Exception as exc:
        error = str(exc)
    if error is None:
        try:
            parent.budget.reserve(charge)
            reserved = True
        except MemoryError:
            # Parent was drained before the fork. Evicting locally avoids a
            # rank-dependent collective while still preserving stored versions.
            try:
                parent.cache.flush()
                parent.cache.clear()
                parent.budget.reserve(charge)
                reserved = True
            except Exception as exc:
                error = str(exc)
    try:
        child.transport.vote(error)
    except BaseException:
        if reserved:
            parent.budget.release(charge)
        raise

    old_budget.release(charge)
    child.budget = child.local.budget = parent.budget
    child.cache.budget = parent.budget
    child.io_engine.budget = parent.budget
    for store in (child.store, child.local.store):
        if hasattr(store, "budget"):
            store.budget = parent.budget
    engine_charge = child.io_engine.metadata_bytes
    return max(0, charge - engine_charge)


def _parameter_shift(sim, observable, requested, schedule):
    ids = _requested_ids(requested)
    if not getattr(sim, "_gradient_origin", None):
        raise ValueError("Parameter-shift requires the captured initial gradient origin")
    gates = sim.sequencer.operations
    by_source: OrderedDict[int, dict] = OrderedDict()
    for occurrence in schedule.occurrences:
        source = occurrence.source_id
        if not 0 <= source < len(gates):
            raise ValueError(f"Unknown source occurrence {source}")
        gate = gates[source]
        if occurrence.shifts is None:
            raise ValueError(f"Unsupported parameter-shift generator at source {source}")
        if tuple(occurrence.shifts) != shift_rule(gate):
            raise ValueError(f"Shift rule no longer matches source occurrence {source}")
        record = by_source.setdefault(source, {"gate": gate, "factors": {}, "shifts": tuple(occurrence.shifts)})
        record["factors"][occurrence.parameter_id] = record["factors"].get(occurrence.parameter_id, 0.0) + occurrence.chain_factor

    gradient = {parameter_id: 0.0 for parameter_id in ids}
    if not by_source:
        return gradient
    sim.drain()
    origin = sim._gradient_origin
    origin_data = _load_origin(sim)
    binding = sim.binding
    for source, record in by_source.items():
        gate = record["gate"]
        if gate.angle is None:
            raise ValueError(f"Source {source} has no parameterized angle")
        base_angle = gate.angle.evaluate(binding)
        factors = {p: f for p, f in record["factors"].items() if p in gradient}
        for shift, weight in record["shifts"]:
            child = None
            release_charge = 0
            try:
                prior_layout, prior_placement = sim.layout, sim.placement_epoch
                sim.layout = sim.local.layout = QubitLayout(tuple(origin_data["layout"]),
                                                            origin_data["layout_id"])
                sim.placement_epoch = origin_data["placement_epoch"]
                try:
                    child = sim.fork(origin, binding=binding)
                finally:
                    sim.layout = sim.local.layout = prior_layout
                    sim.placement_epoch = prior_placement
                release_charge = _share_child_budget(sim, child)
                child._gradient_origin = origin
                child._angle_overrides = {source: base_angle + shift}
                value = float(child.expectation(observable))
                for parameter_id, factor in factors.items():
                    gradient[parameter_id] += factor * weight * value
            finally:
                if child is not None:
                    try:
                        child.close()
                    finally:
                        if release_charge:
                            sim.budget.release(release_charge)
    return gradient


def execute_gradient(sim, observable, requested, schedule: GradientSchedule):
    """Execute a selected gradient method while restoring the parent cut."""
    sim._open()
    _check_schedule(sim, schedule)
    ids = _requested_ids(requested)
    if not ids:
        return {}
    sim.drain()
    parent_state = _snapshot_state(sim)
    if schedule.method == "adjoint":
        return adjoint_gradient(sim, observable, requested)
    if schedule.method == "recompute":
        return _recompute_adjoint(sim, observable, requested, schedule)
    if schedule.method == "parameter-shift":
        try:
            return _parameter_shift(sim, observable, requested, schedule)
        finally:
            _restore_state(sim, parent_state)
    raise ValueError(f"Unsupported planned gradient method {schedule.method!r}")
