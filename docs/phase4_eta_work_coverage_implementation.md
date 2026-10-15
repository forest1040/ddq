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

`payload_byte_coverage` has unit `bytes` and scope `explicit transfer payload
only`. It is not an amplitude-state volume estimate. Shard size, rank count,
gate count, and state size do not prove that bytes cross a route: caching,
virtual frames, fused kernels, and the selected communication route can change
actual transfers. Current coarse executor, future, and endpoint task producers
therefore leave the weight unknown unless they carry exact route-specific
`payload_bytes` metadata.

`kernel_work_coverage` has unit `explicit kernel work units` and scope `explicit
runtime kernel work only`. It does not substitute semantic gate count for
physical kernel work. Virtual gates, fusion, and single-pass execution can
change the relation between semantic operations and kernel invocations. A
producer must supply exact per-unit `kernel_work_units`; otherwise the metric
is unknown.

The `CountedTask` fields `payload_bytes_per_unit` and
`kernel_work_per_unit` are the extension point for later fine-grained stage
weights. `remaining_work.task()` wires these weights from explicit condition
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
and `unknown_reasons` names the candidate. This is an all-or-unknown current
measure; later fine-grained stage DAG weights can refine it.

## Validation

Regression coverage includes count-weighted metrics, zero versus missing
weights, unknown costs, counted future tasks, endpoint omissions, covered and
uncovered in-flight work, dependency/resource lower-bound oracle values, and
runtime snapshot extraction. Runtime snapshots currently emit explicit
unestimated reasons because their coarse task conditions do not prove exact
payload bytes or kernel work. Snapshot metadata byte accounting serializes the
new fields under the existing bounded metadata check.

This does not complete the remaining P4-002 acceptance work: stage-level
payload/kernel weights, fine-grained stage DAG coverage, and the other recorded
cross-platform/short-run/overhead validations remain separate work.
