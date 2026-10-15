# P4-002 work-coverage fields

This note records the work-coverage extension to `RuntimeETAReport`. It is an
implementation artifact; it does not change the P4-002 completion status.

## Report fields and scope

`payload_byte_coverage`, `kernel_work_coverage`, and
`critical_chain_coverage` are serialized alongside the existing count-based
`coverage`, `known_work_count`, and `total_work_count` fields. The legacy count
coverage remains unchanged. All ETA estimates remain fail-closed, and reports
remain ineligible for control.

The first two fields are `WorkCoverage` objects. For either metric, `known_work`
is the sum of explicit per-unit weights multiplied by each task's counted
`count`, but only for task classes with an accepted measured cost. A fully
known denominator is reported in `total_work`; its ratio is `coverage`. If any
task's weight or snapshot scope is unknown, `total_work` and `coverage` are
null, while the known subtotal and reasons remain available. A known weight of
zero is different from a missing weight (`None`).

`payload_byte_coverage` has unit `bytes` and scope `MPI amplitude send payload (each direction once); excludes storage and control traffic`. It is not an amplitude-state volume estimate. Shard size, rank count,
gate count, and state size do not prove that bytes cross a route: caching,
virtual frames, fused kernels, and the selected communication route can change
actual transfers. Executor/window/future producers use route-specific exact weights where
provable; endpoint or placement-dependent transfer weights remain unknown.

`kernel_work_coverage` has unit `explicit kernel work units` and scope `rank-local main kernel amplitude visits; excludes Frame and packing`. It does not substitute semantic gate count for
physical kernel work. Virtual gates, fusion, and single-pass execution can
change the relation between semantic operations and kernel invocations. A
producer must supply exact per-unit `kernel_work_units`; otherwise the metric
is unknown.

The `CountedTask` fields `payload_bytes_per_unit` and
`kernel_work_per_unit` are populated by the shared executor route planner for
provable runtime work; uncertain routes retain null weights. See the
[executor stage implementation](phase4_executor_stage_dag_implementation.md). `remaining_work.task()` wires these weights from explicit condition
metadata and does not infer them from coarse shape fields. This keeps task
snapshot capture connected to the report while leaving unresolved measurements
visible as `UNESTIMATED_*` reasons.

For both weighted metrics, counted future work is multiplied by its count.
Endpoint tasks are included in the same task graph and can keep the denominator
unknown. An uncovered in-flight request makes weighted coverage null; a
residual task may cover it only when the snapshot explicitly marks that
relationship. Incomplete, stale, or unscanned snapshots also cannot claim a
complete denominator.

`critical_chain_coverage` uses seconds and the same dependency/resource lower
bound DAG as the current ETA model. It reports full coverage only when every
candidate task duration is measured and the snapshot scope is complete. If an
unknown duration could participate in the chain, the total and ratio are null
and `unknown_reasons` names the candidate. This is an all-or-unknown current measure. The fine-grained executor motifs
are represented inside their inclusive parent; their costs are never added
again to the parent duration.

## Validation

Regression coverage includes count-weighted metrics, zero versus missing
weights, unknown costs, counted future tasks, endpoint omissions, covered and
uncovered in-flight work, dependency/resource lower-bound oracle values, and
runtime snapshot extraction. Runtime snapshots now attach bounded route stage
DAGs and exact weights where provable; unknown storage/cache/Frame/placement
work retains explicit reasons. Snapshot metadata byte accounting serializes the
new fields under the existing bounded metadata check.

The shared executor stage DAG and separate storage coverage are described in
the linked implementation document. Cross-platform/short-run/overhead acceptance
remains independently validated by the runtime ETA campaign.
