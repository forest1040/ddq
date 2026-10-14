# Spec: `svsim.comparison_report` (Phase 5 比較評価)

Pure-Python module; no simulation, MPI, network or job submission. Python 3.12, NumPy allowed but not required.
Reuse `svsim.profile_document.read_document / write_document / wire_encode / wire_decode` and
`svsim.journal.content_hash`. Wire rule (already implemented by `wire_encode`): any key named
`*_count`, `*_bytes`, `*_calls`, `count`, `bytes`, `sample_count`, `warmup_count` holds a Python int and is
serialized as a decimal string; `qubits/ranks/threads/block_bits` are JSON ints. Never put floats or bools
under such keys. Unknown values are `None` plus a reason, never 0/inf.

## Inputs (all are wire-decoded dicts after `wire_decode(read_document(path))`)

### ExecutionPlan (kind `execution-plan`, produced by `svsim estimate`)
Relevant fields only (others exist and must be ignored):
```
plan_id: str, workload_id: str, profile_id: str, configuration_fingerprint: str,
time: {
  lower_seconds: float|None, expected_seconds: float|None, conditional_upper_seconds: float|None,
  evidence: str,            # e.g. 'derived', 'extrapolated', 'unknown'
  phases: None | {          # may be missing or None in older plans
     '<phase>': {lower_seconds, expected_seconds, conditional_upper_seconds}  # phases: setup, execute, flush, cleanup
  },
  reason: str
}
```

### BenchmarkRun (kind `benchmark-run`, `run_type == 'workload-execution'`, produced by `svsim benchmark`)
```
schema_version: 1, kind: 'benchmark-run', run_type: 'workload-execution', run_id: str,
workload_id: str, plan_id: str|None, profile_id: str, configuration_fingerprint: str,
case: {label, variant, qubits, block_bits, ranks, threads, storage, backend, mode, frame},
work: {lowered_gate_count, block_count, block_pass_count},
phases: ['setup','execute','flush','cleanup'],
trials: [{order_index: int, wall_seconds: {setup, execute, flush, cleanup, total: float},
          cpu_seconds: float, managed_peak_bytes: int, disk_payload_bytes: int, timestamp: str}],
summary: {'<phase>'|'total': {median_seconds, mean_seconds, min_seconds, max_seconds, p95_seconds}},
warmup_count: int, sample_count: int,
validation: {oracle: str, max_abs_error: float|None, passed: bool},
status: 'completed'|'partial'|'failed', omitted: [..], seed: int, provenance: {...}
```
Runs with `status != 'completed'` or `validation.passed is not True` are NOT compared (listed in
`not_compared` with reason `RUN_NOT_COMPLETED` / `RUN_VALIDATION_FAILED`).

### Campaign (kind `comparison-campaign`)
```
{ schema_version: 1, kind: 'comparison-campaign',
  cases: [{case_id: str (unique), plan: '<path>', run: '<path>', variant: str, group: str}],
  calibration_workload_ids: [str],      # workloads used to fit the time model -> not held-out
  baseline_variant: str | None,          # for A/B within each group
  small_time_threshold_seconds: float    # optional, default 0.05
}
```
Paths are relative to the campaign file's directory. Bound: ≤256 cases; each plan/run file ≤4 MiB
(use `read_document(path, max_bytes=4*1024**2)`). Reject (ValueError) unknown campaign fields, duplicate
case_id, wrong kind/schema_version, non-string ids.

## API
```python
def load_campaign(path) -> dict                 # validated, with resolved absolute paths
def build_report(campaign_path) -> dict         # wire-ENCODED comparison-report dict, report_id set
def render_markdown(report: dict) -> str        # accepts the encoded report
def save_report(report, path, overwrite=False)  # write_document
```
`build_report` reads each file once, records its SHA-256 (of raw bytes), and checks:
- plan `kind == 'execution-plan'`; run kind/run_type as above, else ValueError.
- `run.workload_id == plan.workload_id`, else case → not_compared `WORKLOAD_MISMATCH`.
- `run.plan_id` is not None and differs from `plan.plan_id` → not_compared `PLAN_MISMATCH`.
- `run.configuration_fingerprint != plan.configuration_fingerprint` → not_compared `CONFIGURATION_MISMATCH`.
- plan `expected_seconds is None` → not_compared `NO_TIME_PREDICTION` (still listed with observed values).

## Output: ComparisonReport (before wire encoding)
```
{ schema_version: 1, kind: 'comparison-report', report_id: content_hash(report without report_id),
  model: 'static-vs-observed-v1', campaign_sha256: str,
  cases: [{
     case_id, variant, group, workload_id, plan_id, run_id, plan_sha256, run_sha256,
     held_out: bool  (workload_id not in calibration_workload_ids),
     status: 'compared'|'not-compared', reason: str|None,
     predicted: {lower_seconds, expected_seconds, conditional_upper_seconds, evidence},
     observed: {median_seconds, min_seconds, max_seconds, p95_seconds, sample_count},
     absolute_error_seconds: expected - observed_median  (signed) | None,
     relative_error: (expected - observed_median)/observed_median | None (None if observed_median <= 0),
     within_interval: lower <= observed_median <= upper | None if any bound None,
     small_time: observed_median < small_time_threshold_seconds,
     phases: {phase: {predicted_expected_seconds, observed_median_seconds, relative_error|None,
                      observed_share: observed phase median / sum of observed phase medians}},
     observed_bottleneck: phase with largest observed median | None,
     predicted_bottleneck: phase with largest predicted expected | None (None if plan has no phases),
  }],
  accuracy: {
     'held_out': Stats, 'calibration': Stats, 'small_time': AbsStats,
     target: {statistic: 'held_out median |relative_error|', threshold: 0.30,
              meets_target: bool|None (None when held_out compared count==0)}
  },
  ab_comparisons: [ ... ],
  not_compared: [{case_id, reason}],
  notes: [str]  # fixed disclaimers, see below
}
Stats  = {case_count, median_abs_relative_error, p95_abs_relative_error, worst_abs_relative_error,
          worst_case_id, interval_coverage (fraction of cases with within_interval True among those
          where it is not None, or None), interval_case_count}
AbsStats = {case_count, median_abs_error_seconds, worst_abs_error_seconds, worst_case_id}
```
`held_out` and `calibration` Stats include only compared cases with `small_time == False`;
small-time compared cases go only into `small_time` (absolute error). Empty sets → counts 0, other fields None.
p95 = linear interpolation on sorted values at index (n-1)*0.95 (same as `benchmark_campaign._record`).
Never drop cases with large error.

### A/B
If `baseline_variant` is set: for every group, for each case in the group whose variant != baseline and
where a baseline case exists in the same group (first one by case_id order), produce
```
{group, baseline_case_id, case_id, baseline_variant, variant,
 observed_ratio: observed_median(variant)/observed_median(baseline) | None,
 observed_difference_seconds: variant - baseline,
 predicted_ratio: expected(variant)/expected(baseline) | None,
 observed_winner: 'baseline'|'variant'|'tie' (tie if |ratio-1| < 0.02, None if ratio None),
 predicted_winner: same rule on predicted_ratio,
 winner_agrees: bool|None,
 trial_ranges_overlap: bool  # [min,max] of total wall of both runs overlap -> difference not separated
}
```
Only cases whose run is completed+validated participate (prediction may be None).

### notes (always present, exact list order)
1. 'relative error uses the median of measured trials; sample extrema are not confidence intervals'
2. 'held-out cases exclude workloads used to fit the time model'
3. 'phase times are serial wall components on one main thread; no overlap is assumed'
4. 'the 30% median target is an evaluation goal, not a guarantee'

## Markdown
Japanese-or-English plain Markdown: title, a table of cases (case_id, variant, held-out, predicted
expected [lower, upper], observed median, relative error %, within interval, observed bottleneck),
an accuracy section, an A/B table, and the not-compared list with reasons, then the notes.
Format seconds with 4 significant digits; None renders as `n/a`.

## Tests (`tests/test_comparison_report.py`, pytest, use `tmp_path`)
Build synthetic plan/run dicts with helper functions (no simulator). Cover at least: exact error math;
held-out vs calibration split; small-time separate aggregation; interval coverage incl. None bounds;
mismatch reasons (workload/plan/config), failed validation excluded; A/B ratio, winner/tie, overlap;
duplicate case_id and unknown campaign field rejected; report roundtrip through write_document/read_document
keeps `*_count` fields as decimal strings and report_id is the content hash; markdown contains every case_id
and `n/a` for unknowns.
