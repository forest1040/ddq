"""Runtime-generated replan cost terms from exact invocation counts and measured evidence.

The executor delays and services one call per executor group, so the work
that distinguishes candidates is the number of remaining calls under each
policy. Counts are exact: the remaining source is chunked by the policy's
lookahead and planned with the same ``plan_window`` the scheduler uses.
Per-call costs come only from measured evidence (an evaluation baseline and
the observed excess); a term without evidence stays unknown.
"""
from __future__ import annotations

from dataclasses import replace

from .replan_cost import ReplanCosts
from .replan_candidates import proposal_context
from .s2_planner import plan_window

MIGRATION_FREE_SWAP = {'S0', 'S2'}  # S2 reorders and fuses within a window; S1 moves layout bits


def remaining_calls(sim, policy, start, end):
    """Executor invocations the async scheduler would make for sources start..end under ``policy``."""
    operations, calls, cursor = sim.sequencer.operations, 0, start
    while cursor <= end:
        stop = min(end, cursor + policy.lookahead_gates - 1)
        window = operations[cursor:stop + 1]
        if policy.swap_policy == 'S2':
            plan = plan_window(window, sim.layout, sim.block_bits, policy.beam_width)
            order = plan.source_ids
            groups = {g[0].op_id: g for g in plan.groups if len(g) > 1}
        else:
            order, groups = tuple(g.op_id for g in window), {}
        index = 0
        while index < len(order):
            group = groups.get(order[index])
            fused = group and tuple(g.op_id for g in group) == tuple(order[index:index + len(group)])
            index += len(group) if fused else 1
            calls += 1
        cursor = stop + 1
    return calls


def costs_for(sim, row, estimate, *, end, planning_seconds, drain_seconds, checkpoint_seconds, qualified=False):
    """ReplanCosts for one proposal row; ``estimate`` and ``qualified`` come from the cost registry."""
    start = sim.frontier + 1
    gates = max(0, end - start + 1)
    updates = dict(tuple(item) for item in row['updates'])
    candidate = replace(sim.policy, **updates)
    current_calls = remaining_calls(sim, sim.policy, start, end)
    candidate_calls = remaining_calls(sim, candidate, start, end)
    context = proposal_context(sim)
    if estimate is None:
        return ReplanCosts(context_hash=context), {'current_calls': current_calls, 'candidate_calls': candidate_calls}
    continue_lower = gates * estimate['service_lower_per_gate'] + current_calls * estimate['extra_lower_per_call']
    execution_upper = gates * estimate['service_upper_per_gate'] + candidate_calls * estimate['extra_upper_per_call']
    frontier_change = row['classification'] == 'FRONTIER_REPLAN'
    migration = 0.0 if candidate.swap_policy in MIGRATION_FREE_SWAP else None
    costs = ReplanCosts(continue_lower_seconds=continue_lower, execution_upper_seconds=execution_upper,
                        planning_seconds=planning_seconds, drain_seconds=drain_seconds,
                        migration_seconds=migration, calibration_seconds=0.0, validation_seconds=0.0,
                        checkpoint_seconds=checkpoint_seconds if frontier_change else 0.0, cleanup_seconds=0.0,
                        context_hash=context, qualified=qualified)
    return costs, {'current_calls': current_calls, 'candidate_calls': candidate_calls, 'remaining_gates': gates}
