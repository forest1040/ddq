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

from .journal import canonical, content_hash, gate_data
from .replan_cost import ReplanCosts
from .replan_candidates import proposal_context
from .remaining_work import cost_context, executor_condition, executor_groups, strip_ids

MIGRATION_FREE_SWAP = {'S0', 'S2'}  # S2 reorders and fuses within a window; S1 moves layout bits


def remaining_calls(sim, policy, start, end):
    """Executor invocations the async scheduler would make for sources start..end under ``policy``."""
    operations, calls, cursor = sim.sequencer.operations, 0, start
    while cursor <= end:
        stop = min(end, cursor + policy.lookahead_gates - 1)
        calls += sum(1 for _ in executor_groups(operations[cursor:stop + 1], sim.layout, sim.block_bits, policy))
        cursor = stop + 1
    return calls


def baseline_reason(sim, estimate, start, end):
    """Refuse missing/unstable evidence and extrapolation to an unmeasured executor class."""
    if estimate is None:return 'NO_BASELINE_EVIDENCE'
    quality=estimate.get('baseline_quality',{})
    if quality.get('certified') is not True:return 'UNCERTIFIED_BASELINE'
    if quality.get('scope')!='injected' or quality.get('production_profile') is not False:
        return 'INVALID_BASELINE_SCOPE'
    from .replan_candidates import MAX_LAYOUT_SCAN
    if end-start+1>MAX_LAYOUT_SCAN:return 'BASELINE_CLASS_SCAN_LIMIT'
    if sim.binding is None:return 'BASELINE_BINDING_UNKNOWN'
    operations,cursor=sim.sequencer.operations,start
    while cursor<=end:
        stop=min(end,cursor+sim.policy.lookahead_gates-1)
        for group in executor_groups(operations[cursor:stop+1],sim.layout,sim.block_bits,sim.policy):
            descriptors=[]
            for gate in group:
                data=gate_data(sim._physical(gate));data.pop('op_id',None);data.pop('source_op_id',None)
                descriptors.append(data)
            work_class=content_hash({'operations':descriptors,'layout':sim.layout.logical_to_physical,
                'n':sim.num_qubits,'b':sim.block_bits,'P':sim.size,'binding':sim.binding.binding_id})
            if len(group)!=1 or work_class!=quality.get('work_class'):return 'BASELINE_EXECUTOR_CLASS_MISMATCH'
        cursor=stop+1
    return None


def costs_for(sim, row, estimate, *, end, planning_seconds, drain_seconds, checkpoint_seconds, qualified=False,
              resolve=None):
    """ReplanCosts for one proposal row; ``estimate`` and ``qualified`` come from the cost registry."""
    start = sim.frontier + 1
    gates = max(0, end - start + 1)
    updates = dict(tuple(item) for item in row['updates'])
    if 'layout_swap' in updates:
        return layout_costs_for(sim, tuple(updates['layout_swap']), estimate, end=end, planning_seconds=planning_seconds,
                                drain_seconds=drain_seconds, checkpoint_seconds=checkpoint_seconds,
                                qualified=qualified, resolve=resolve)
    candidate = replace(sim.policy, **updates)
    current_calls = remaining_calls(sim, sim.policy, start, end)
    candidate_calls = remaining_calls(sim, candidate, start, end)
    context = proposal_context(sim)
    reason=baseline_reason(sim,estimate,start,end)
    if reason is not None:
        return ReplanCosts(context_hash=context), {'current_calls':current_calls,'candidate_calls':candidate_calls,
                                                  'baseline_reason':reason}
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


def layout_costs_for(sim, bits, estimate, *, end, planning_seconds, drain_seconds, checkpoint_seconds,
                     qualified=False, resolve=None):
    """ReplanCosts of migrating the physical layout by one (rank bit, local bit) swap at the current cut.

    Continue: the same lower bound as policy candidates (per-gate service and
    per-call excess from the degradation evidence). Candidate: every remaining
    executor class under the swapped layout (exact epoch shape) at the upper of
    the pre-calibrated ``executor-class-seconds`` profile, plus the evidence's
    per-call excess on the calls it applies to (only rank-crossing calls when the
    evidence is attributed to MPI), plus the profiled swap itself. Epoch
    publications and boundaries occur equally often on both sides and are
    omitted. Any class or swap the profile lacks leaves the candidate unknown.
    """
    from .executor_profile import swap_condition
    from .replan_candidates import MAX_LAYOUT_SCAN
    start = sim.frontier + 1
    gates = max(0, end - start + 1)
    context = proposal_context(sim)
    current_calls = remaining_calls(sim, sim.policy, start, end)
    counts = {'current_calls': current_calls, 'remaining_gates': gates, 'layout_swap': list(bits)}
    unknown = ReplanCosts(context_hash=context)
    reason=baseline_reason(sim,estimate,start,end)
    if reason is not None:
        return unknown, {**counts,'baseline_reason':reason,'layout_reason':reason}
    if resolve is None or gates > MAX_LAYOUT_SCAN:
        return unknown, {**counts, 'layout_reason': 'NO_EVIDENCE_OR_SCAN_LIMIT'}
    layout = sim.layout.swapped_physical(*bits)
    first_rank = sim.num_qubits - sim.ownership.rank_bits
    ccontext = cost_context(sim)
    classes, calls, crossing = {}, 0, 0
    operations, cursor = sim.sequencer.operations, start
    while cursor <= end:
        stop = min(end, cursor + sim.policy.lookahead_gates - 1)
        for group in executor_groups(operations[cursor:stop + 1], layout, sim.block_bits, sim.policy):
            condition = strip_ids(executor_condition(sim, group, layout))
            remote = any(layout.logical_to_physical[q] >= first_rank for g in group for q in g.qubits)
            key = canonical(condition)
            entry = classes.setdefault(key, [condition, 0, remote])
            entry[1] += 1; calls += 1; crossing += remote
        cursor = stop + 1
    counts.update(candidate_calls=calls, crossing_candidate_calls=crossing, candidate_classes=len(classes))
    execution, worst = 0.0, 0.0
    for condition, count, _ in classes.values():
        sample = resolve('executor-class-seconds', ccontext, condition)
        if sample is None:
            return unknown, {**counts, 'layout_reason': 'UNPROFILED_CANDIDATE_CLASS'}
        execution += count * sample.upper_seconds; worst = max(worst, sample.upper_seconds)
    swap = resolve('executor-class-seconds', ccontext, swap_condition(sim, *bits))
    if swap is None:
        return unknown, {**counts, 'layout_reason': 'UNPROFILED_MIGRATION'}
    charged = crossing if estimate.get('resource') == 'mpi' else calls
    execution += charged * estimate['extra_upper_per_call']
    continue_lower = gates * estimate['service_lower_per_gate'] + current_calls * estimate['extra_lower_per_call']
    counts.update(charged_excess_calls=charged, verify_service_upper_per_call=worst,
                  verify_extra_upper_per_call=estimate['extra_upper_per_call'] if charged else 0.0)
    return ReplanCosts(continue_lower_seconds=continue_lower, execution_upper_seconds=execution,
                       planning_seconds=planning_seconds, drain_seconds=drain_seconds,
                       migration_seconds=swap.upper_seconds, calibration_seconds=0.0, validation_seconds=0.0,
                       checkpoint_seconds=checkpoint_seconds, cleanup_seconds=0.0,
                       context_hash=context, qualified=qualified), counts
