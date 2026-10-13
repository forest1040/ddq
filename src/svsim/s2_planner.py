"""Bounded, conservative S2 source-window reorder proposals.

The score is a deterministic planning proxy. It is not a calibrated runtime
prediction and this module never changes the semantic GateOps.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence

from .ir import GateOp
from .layout import QubitLayout


_WINDOW_LIMIT = 32
_KNOWN_KINDS = {"H", "S", "SDG", "Z", "T", "TDG", "RX", "RY", "RZ", "CZ",
                "SWAP", "MCX", "DENSE_UNITARY", "PAULI_ROT"}


@dataclass(frozen=True)
class WindowPlan:
    source_ids: tuple[int, ...]
    groups: tuple[tuple[GateOp, ...], ...]
    baseline_passes: int
    planned_passes: int
    estimated_gain: float
    cost_model: str = "uncalibrated-heuristic-v1"


def _source_id(gate: GateOp, ordinal: int) -> int:
    if gate.op_id >= 0:
        return gate.op_id
    return ordinal


def _physical_support(gate: GateOp, layout: QubitLayout) -> frozenset[int]:
    return frozenset(layout.logical_to_physical[q] for q in gate.qubits)


def _local(gate: GateOp, layout: QubitLayout, block_bits: int) -> bool:
    try:
        targets = tuple(layout.logical_to_physical[q] for q in gate.targets)
    except (IndexError, TypeError):
        return False
    return bool(targets) and all(bit < block_bits for bit in targets)


def _representation(gate: GateOp) -> str:
    if gate.diagonal:
        return "diagonal"
    if gate.permutation:
        return "permutation"
    if gate.kind == "DENSE_UNITARY":
        return "dense"
    return "general"


def _locality_passes(gates: Sequence[GateOp], layout: QubitLayout, block_bits: int) -> int:
    if not gates:
        return 0
    labels = tuple(_local(gate, layout, block_bits) for gate in gates)
    return 1 + sum(left != right for left, right in zip(labels, labels[1:]))


def _score(gates: Sequence[GateOp], layout: QubitLayout, block_bits: int) -> tuple[int, int]:
    """Return (local/joint pass count, adjacent representation changes)."""
    passes = _locality_passes(gates, layout, block_bits)
    reps = tuple((_local(g, layout, block_bits), _representation(g)) for g in gates)
    changes = sum(a != b for a, b in zip(reps, reps[1:]))
    return passes, changes


def _groups(gates: Sequence[GateOp], layout: QubitLayout, block_bits: int) -> tuple[tuple[GateOp, ...], ...]:
    result: list[tuple[GateOp, ...]] = []
    run: list[GateOp] = []
    for gate in gates:
        if _local(gate, layout, block_bits):
            run.append(gate)
        else:
            if run:
                result.append(tuple(run)); run = []
    if run:
        result.append(tuple(run))
    return tuple(result)


def plan_window(gates: Sequence[GateOp], layout: QubitLayout, block_bits: int,
                beam_width: int = 4) -> WindowPlan:
    """Propose a legal, bounded reorder and report adjacent local fusion groups.

    Only the first 32 gates form the search window; the remaining suffix stays
    in source order. Overlapping non-diagonal gates retain source-order edges.
    Two explicitly proven diagonal gates may commute even with overlapping
    support. Any SWAP is a barrier, and an unknown or malformed operation makes
    the whole input retain source order.
    """
    sequence = tuple(gates)
    if not isinstance(layout, QubitLayout):
        raise TypeError("layout must be a QubitLayout")
    if not 1 <= block_bits <= len(layout.logical_to_physical):
        raise ValueError("block_bits must fit the layout")
    if beam_width < 1:
        raise ValueError("beam_width must be positive")
    # Keep metadata and search work bounded even if a caller asks for a wider beam.
    width = min(int(beam_width), 16)
    ids = tuple(_source_id(gate, i) for i, gate in enumerate(sequence))
    if len(set(ids)) != len(ids):
        raise ValueError("source ids must be unique within a window")

    window = sequence[:_WINDOW_LIMIT]
    n = len(window)
    valid = True
    supports: list[frozenset[int]] = []
    for gate in window:
        try:
            if gate.kind not in _KNOWN_KINDS or not gate.targets:
                valid = False
                break
            if any(q < 0 or q >= len(layout.logical_to_physical) for q in gate.qubits):
                valid = False
                break
            supports.append(_physical_support(gate, layout))
        except (IndexError, TypeError):
            valid = False
            break

    order = list(range(len(sequence)))
    if valid and n > 1:
        pred = [0] * n
        for later in range(n):
            for earlier in range(later):
                a, b = window[earlier], window[later]
                swap_barrier = a.kind == "SWAP" or b.kind == "SWAP"
                overlap = not supports[earlier].isdisjoint(supports[later])
                proven_commute = a.diagonal and b.diagonal
                if swap_barrier or (overlap and not proven_commute):
                    pred[later] |= 1 << earlier

        # Each beam member is a source-index path. Incremental proxy score is
        # pass changes (weight 4) plus representation changes (weight 1).
        beam: list[tuple[tuple[int, ...], int, int]] = [((), 0, 0)]
        for _depth in range(n):
            expanded: list[tuple[tuple[int, ...], int, int]] = []
            for path, pass_count, repr_changes in beam:
                used = 0
                for ix in path:
                    used |= 1 << ix
                ready = [ix for ix in range(n) if not used & (1 << ix) and pred[ix] & ~used == 0]
                for ix in ready:
                    next_path = path + (ix,)
                    added_pass = (1 if not path else int(
                        _local(window[path[-1]], layout, block_bits) != _local(window[ix], layout, block_bits)))
                    added_repr = int(bool(path) and (
                        _local(window[path[-1]], layout, block_bits), _representation(window[path[-1]])) != (
                        _local(window[ix], layout, block_bits), _representation(window[ix])))
                    expanded.append((next_path, pass_count + added_pass, repr_changes + added_repr))
            expanded.sort(key=lambda item: (4 * item[1] + item[2], item[0]))
            beam = expanded[:width]
            if not beam:
                # Defensive fallback; the source-order DAG is always acyclic.
                valid = False
                break
        if valid:
            winner = min(beam, key=lambda item: (4 * item[1] + item[2], item[0]))[0]
            candidate = [window[i] for i in winner]
            baseline_score = _score(window, layout, block_bits)
            candidate_score = _score(candidate, layout, block_bits)
            # Preserve source order unless the proxy strictly improves.
            if 4 * candidate_score[0] + candidate_score[1] < 4 * baseline_score[0] + baseline_score[1]:
                order[:n] = winner

    base = [sequence[i] for i in order]
    baseline_score = _score(sequence, layout, block_bits)
    planned_score = _score(base, layout, block_bits)
    return WindowPlan(
        source_ids=tuple(ids[i] for i in order),
        groups=_groups(base, layout, block_bits),
        baseline_passes=baseline_score[0],
        planned_passes=planned_score[0],
        estimated_gain=float(4 * (baseline_score[0] - planned_score[0]) + baseline_score[1] - planned_score[1]),
    )
