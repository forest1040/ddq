# P4-005 comparison matrix validator

This extension adds `MatrixEntry` and `ComparisonMatrix` to
[`phase4_evaluation.py`](../src/svsim/phase4_evaluation.py). It wraps the existing
bounded `Campaign` reports and keeps their schema and validation behavior intact.

The matrix registers cases against the eight required Phase 4 groups. It reports
every required slot as `completed`, `failed`, `unsupported`,
`insufficient-budget`, `insufficient-sample`, or `unmeasured`, and returns an
explicit `remaining` list. A slot qualifies only with at least two variants,
two or more declared warmups, at least seven fully matched valid trial pairs,
and completion of the campaign's entire planned trial set. Unsupported routes,
resource rejection, missing trials, and numerical validation failures remain
visible as rows.

Execution has a separate `large-state` slot. It requires an MPI backend with at
least two ranks, `storage_kind=file`, and the calculated per-rank state shard
(`16 * 2**n / ranks`, rounded up) to exceed the declared `ram_per_rank_bytes`.
Transport has separate `same-host` and `cross-host-linux`
slots; these require `host_count=1`, or `host_count>=2` plus Linux build platform,
respectively. A small execution case or same-host run cannot satisfy either
special slot. Case IDs and group/slot registrations are unique within one
matrix, and each underlying Campaign continues to check raw case/variant/trial
identity.

The report labels its result `representative coverage only`. Its
`representative_coverage_complete` flag means that each representative slot
passed these checks. `phase4_acceptance.status` remains `not-assessed`: the
report does not check every required axis and condition, such as all S0/S1/S2,
swap, control, cache, lifecycle, or transport variants. Therefore, complete
representative coverage is not final matrix completion or Phase 4 acceptance.

## Scope and results

The validator provides preregistration, bounded coverage/status accounting, and
cross-case reporting. It does not execute campaigns or infer hardware scope
from measurements; host count and platform are declarations in the case
manifest, so evidence collection must record their provenance. Small local
results are not promoted into large-state or cross-host completion.

The unit tests cover missing mandatory groups, duplicate registrations and
case IDs, mismatched raw identity, missing planned trials, fewer than seven
matched pairs, one-variant and insufficient-warmup campaigns, failed numeric
validation, unsupported and budget statuses, large-state MPI/file guards, and
host-count type validation. A seven-pair campaign is also checked as
representative coverage only. No performance campaign was run for this change.

Remaining P4-005 work includes registering and running all required cases,
including the large-state and cross-host slots, then producing the final matrix
from verified raw results. The matrix report records missing work explicitly;
it does not claim Phase 4 complete.
