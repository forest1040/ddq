# P4-002 final Linux v3 acceptance (2026-10-17)

**All eight predeclared cases completed.** Source `89e8ad2`, immutable image `sha256:3bee5df0c2960ab030ea1e9cb5528638929ab3cb6e45ee2ee073102f1d8cce14`. [Predeclared plan](../performance-v3-plan.md); [original six-condition acceptance](../../../phase4_p4_002_acceptance_2026-10-17.md). No TeX/PDF update was performed.

The serial batch ran on ent with a fixed read-only src/tests/examples snapshot, four CPU / 8 GiB container limits, one BLAS thread, cooperative I/O and node-local storage. All eight attempt statuses are complete / return code 0; cleanup independently verified each named container absent. Each attempt retains its manifest, driver log, source hashes and final JSON plus progress JSONL. All 284 source-file hashes per attempt match the integrated checkout. Raw git_rev is null because the exported snapshot has no .git; the externally captured source manifest provides provenance.

Reproduce the source/grid/pair/backend/cleanup audit and reductions from the repository root:

```sh
.venv/bin/python docs/internal/phase4_wave3_validation/linux-performance-v3/audit_results.py
```

[Integrity audit](integrity-audit.json) and [full reductions including all unknown rows and reasons](analysis-summary.json). Raw final JSON/JSONL equivalence was also independently reviewed. No source changes followed the final Mac fast suite (1363 passed / 38 skipped / 83 deselected) and Linux endpoint/actual-MPI suite (19 passed / 0 skipped). These data-only additions were verified by artifact integrity checks.

## Remaining ETA heldouts

Each configuration includes exactly 54 unique cells: rx-ry-low / rx-ry-var / mixed × S0/S1/S2 × file/RAM × fraction .4/.6/.8 × one trial. Total 216 checks, zero execution errors, **78 answered / 138 unknown**. Unknowns remain in the denominator and are never assigned zero seconds. upper / cz / mixed-var were predeclared omitted workloads; this is not a complete 108-cell matrix per configuration.

The collective query precedes the next materialize completion. ETA uses rank MAX, and actual is the MAX of per-rank materialize API wall times after the diagnostic/query barrier. Query and intervening diagnostic work are outside that actual interval; this is a gates-only next-completion oracle, not a continuously timed whole application from entry into the query. Monitoring wall cost is measured separately below. Explicit expectation/gradient/checkpoint API holdouts are [reported separately](../../p4_002_observed_eta/endpoint-heldout-20261017/README.md).

| Backend / ranks | Answered / all (unknown) | Relative error median / p95 / worst (%) | Absolute error median / p95 / worst (s) | Interval coverage |
|---|---|---|---|---|
| buffered-p1 | 30/54 (24) | 5.76 / 55.97 / 66.26 | 0.353002 / 7.245328 / 17.393543 | 27/30 (90.0%) |
| buffered-p2 | 9/54 (45) | 2.92 / 71.53 / 71.78 | 0.052389 / 12.625097 / 14.000079 | 6/9 (66.7%) |
| direct-p1 | 27/54 (27) | 4.13 / 111.18 / 239.82 | 0.135059 / 36.778464 / 65.326577 | 19/27 (70.4%) |
| direct-p2 | 12/54 (42) | 3.50 / 72.74 / 73.85 | 0.176564 / 33.918962 / 57.807054 | 9/12 (75.0%) |

All four configuration medians and all twelve answered workload medians meet the original 30% initial target. Tail errors are substantial: direct P1 reaches 239.82% relative error and 65.327 seconds absolute error. This is a known quality limit, not a claim that every prediction is within 30%. Raw uncertified class diagnostics preserve insufficient samples, held-out coverage failures and drift, with corresponding missing-cost tasks. In multi-rank conditions only 9/54 and 12/54 checks answer. There is no new response-rate floor or future 95% interval guarantee.

### Short remaining segment (fraction .8)

This is the final fifth of the gate sequence, not a guarantee of subsecond wall time. Actual distributions below use answered rows only; the JSON also retains actuals for unknown rows.

| Configuration | Answered / 18 | Actual wall median [min, max] (s) | Absolute error median / p95 / worst (s) | Relative error median / p95 / worst (%) |
|---|---|---|---|---|
| buffered-p1 | 10/18 | 4.476278 [1.055002, 27.528739] | 0.210160 / 6.220037 / 10.633341 | 5.06 / 42.60 / 66.26 |
| buffered-p2 | 3/18 | 1.474220 [1.464056, 19.677935] | 0.043009 / 12.604372 / 14.000079 | 2.92 / 64.32 / 71.15 |
| direct-p1 | 9/18 | 1.548420 [1.064397, 28.324168] | 0.044732 / 6.831294 / 10.657998 | 3.85 / 41.89 / 63.75 |
| direct-p2 | 5/18 | 5.783209 [1.465916, 24.631878] | 0.145069 / 11.540939 / 14.374159 | 2.51 / 47.44 / 58.36 |

## Monitoring overhead

Every configuration ran both fixed workloads with two warmup pairs followed by seven distinct measured pairs (indices 2–8): 16 warmup pairs and 56 measured pairs in total, all successful. Small is rx-ry-low (n=6, block_bits=2, 30 layers); long is mixed (n=8, block_bits=3, 20 layers). Both contain 120 gates but differ in state size and work. S2/file/cooperative and all other shared conditions remain in each raw artifact. Orders alternate using the fixed seed. CI is paired percentile bootstrap of the median, 5,000 replicates, 95%; differences crossing zero or ratios crossing one remain inconclusive. A complete campaign can therefore have an inconclusive performance result.

| Configuration / workload | Paired difference median [CI95] (s) | Paired ratio median [CI95] (on/off) | Result | Capture + evaluation median / p95 / worst (s) |
|---|---|---|---|---|
| buffered-p1 / mixed | 54.071651 [16.190862, 84.890641] | 1.946259 [1.345985, 2.780740] | eta-on-slower | 0.980491 / 1.233798 / 1.245446 |
| buffered-p1 / rx-ry-low | 0.024941 [-6.840469, 8.735065] | 1.003196 [0.686234, 1.647490] | inconclusive | 0.210529 / 0.345628 / 0.376822 |
| buffered-p2 / mixed | -15.231329 [-56.526865, 75.746538] | 0.845868 [0.557294, 2.427394] | inconclusive | 0.360989 / 0.452971 / 0.460613 |
| buffered-p2 / rx-ry-low | 0.230708 [-6.162877, 0.291593] | 1.032798 [0.740442, 1.041391] | inconclusive | 0.094841 / 0.117675 / 0.118327 |
| direct-p1 / mixed | -19.530006 [-48.048001, 40.933819] | 0.816096 [0.626931, 1.582518] | inconclusive | 0.739975 / 1.158110 / 1.226824 |
| direct-p1 / rx-ry-low | 0.257309 [-16.350139, 10.462790] | 1.032853 [0.324783, 1.432570] | inconclusive | 0.162588 / 0.384783 / 0.387502 |
| direct-p2 / mixed | 13.419538 [-73.470826, 47.820305] | 1.188610 [0.364277, 1.655044] | inconclusive | 0.354441 / 0.434446 / 0.437682 |
| direct-p2 / rx-ry-low | 0.605097 [0.119982, 10.861549] | 1.020306 [1.016906, 1.505377] | eta-on-slower | 0.123856 / 0.137491 / 0.138264 |

**Monitoring was slower in two groups** (buffered P1 mixed and direct P2 rx-ry-low); six groups are inconclusive. This is not an overhead reduction or free-monitoring claim. Absolute wall distributions, p95, sample standard deviations / coefficients of variation, pair order, every pair and both CI definitions remain in raw final JSON. The large wall variance limits interpretation; capture/evaluation timers alone do not explain every paired difference.

Capture and ETA evaluation totals are submetrics already inside ETA-on wall. Reported extraction is nested within capture wall, so it is not added again. Capture + evaluation is an accounting subtotal, not the complete end-to-end on/off overhead.

### Observed I/O

Both ETA flags and all warmup/measured pairs retain per-rank completed-storage evidence, sampled after the wall timer stops. All observations are complete, submitted/completed requests agree, outstanding requests are zero and event byte totals match exact counter deltas. Actual identities are buffered for buffered cases and linux-o-direct for direct cases. Empty cached FileBlockStore handle arrays are not used as proof; asynchronous handles are observed through completion traces. Metadata/SQLite I/O is outside the amplitude backend claim.

Measured-pair totals (both flags, summed across ranks) per configuration: rx-ry-low wrote 27,525,120 bytes / 6,720 events, mixed wrote 183,500,800 bytes / 44,800 events; read bytes/events were zero. These workloads therefore demonstrate writes, not uncached read performance. Backend identity claims apply to completed events within the materialize window, not every process descriptor.

## Acceptance and retained limits

The original six conditions are covered by the bounded implementation and meaningful functional oracles plus these real-runtime results and the explicit API endpoint holdouts. P4-002 is DONE under those original conditions. Unknowns, large tails, limited interval coverage, short-prepare certification failures, two measured slowdowns and the endpoint bounded raw queue drops remain explicit quality limits / improvement candidates. No new acceptance threshold was added and no failure was dropped. The aborted v2 attempt and prior endpoint baseline remain as separate historical evidence. P4-009 is independently DONE; P4-004 and the remainder of Phase 4 remain unfinished.
