from svsim.ir import GateNormalizer, GlobalSequencer, GateOp
from svsim.layout import QubitLayout
from svsim.s2_planner import plan_window


def gates(*items):
    normalizer = GateNormalizer(8)
    sequencer = GlobalSequencer()
    result = []
    for kind, targets, kwargs in items:
        result.append(sequencer.append(normalizer.gate(kind, targets, **kwargs)))
    return tuple(result)


def test_reorders_disjoint_gates_to_reduce_local_joint_passes():
    source = gates(("H", (0,), {}), ("H", (6,), {}), ("S", (1,), {}))
    plan = plan_window(source, QubitLayout.identity(8), block_bits=2)
    assert plan.source_ids == (0, 2, 1)
    assert plan.planned_passes < plan.baseline_passes
    assert plan.estimated_gain > 0
    assert plan.groups == ((source[0], source[2]),)


def test_overlapping_gates_keep_order_unless_both_are_proven_diagonal():
    ordinary = gates(("H", (0,), {}), ("S", (0,), {}), ("H", (1,), {}))
    plan = plan_window(ordinary, QubitLayout.identity(8), 1)
    assert plan.source_ids[:2] == (0, 1)

    diagonal = gates(("S", (0,), {}), ("CZ", (0, 6), {}), ("S", (1,), {}))
    # The overlapping local and joint diagonal operations commute.
    plan = plan_window(diagonal, QubitLayout.identity(8), 2)
    assert plan.source_ids == (0, 2, 1)


def test_controls_above_block_bits_do_not_make_target_local_gate_joint():
    source = gates(("MCX", (0,), {"controls": (7,)}), ("H", (6,), {}), ("S", (1,), {}))
    plan = plan_window(source, QubitLayout.identity(8), block_bits=2)
    assert source[0] in plan.groups[0]
    assert all(g.kind != "H" or g.targets != (6,) for group in plan.groups for g in group)


def test_swap_is_a_hard_order_barrier():
    source = gates(("H", (0,), {}), ("SWAP", (0, 6), {}), ("H", (1,), {}), ("S", (2,), {}))
    plan = plan_window(source, QubitLayout.identity(8), block_bits=2)
    assert plan.source_ids.index(0) < plan.source_ids.index(1) < plan.source_ids.index(2)


def test_dense_and_unknown_overlap_are_conservative_and_semantic_ir_unchanged():
    source = gates(("DENSE_UNITARY", (0,), {"matrix": [[0, 1], [1, 0]]}),
                   ("H", (0,), {}), ("H", (6,), {}))
    before = tuple(source)
    plan = plan_window(source, QubitLayout.identity(8), block_bits=1)
    assert plan.source_ids == (0, 1, 2)
    assert tuple(source) == before
    unknown = GateOp("CUSTOM", (0,), op_id=10)
    untouched = (source[0], unknown, source[2])
    assert plan_window(untouched, QubitLayout.identity(8), 1).source_ids == (0, 10, 2)


def test_search_is_bounded_to_first_32_source_occurrences():
    normalizer = GateNormalizer(64)
    seq = GlobalSequencer()
    source = tuple(seq.append(normalizer.gate("H", (q,))) for q in range(33))
    plan = plan_window(source, QubitLayout.identity(64), block_bits=2, beam_width=10_000)
    assert len(plan.source_ids) == 33
    assert plan.source_ids[32] == 32
    assert plan.source_ids[32:] == (32,)
