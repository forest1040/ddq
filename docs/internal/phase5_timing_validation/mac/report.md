# Comparison Report (static-vs-observed-v1)

- **Report ID**: `38b3e89b4177a519210aae5aec4daace51bdb5d0bfd47d58594cb716496837c0`
- **Campaign SHA-256**: `7fe0538963763714a775fea4a8a057aacfbdec041ca81a0f474685822c263bb7`

## Cases

| Case ID | Variant | Held-Out | Predicted Expected [Lower, Upper] | Observed Median | Rel. Error % | Within Interval | Observed Bottleneck |
|---|---|---|---|---|---|---|---|
| held-a | default | True | 0.7444 [0.74, 1.034] | 0.747 | -0.3436% | True | execute |
| held-b | default | True | 1.906 [1.896, 2.489] | 1.945 | -1.998% | True | execute |
| held-c | default | True | n/a [n/a, n/a] | 0.3748 | n/a | n/a | execute |
| held-d | default | True | 0.9121 [0.905, 1.247] | 0.9083 | +0.4228% | True | execute |

## Accuracy

### Target Evaluation
- **Statistic**: held_out median |relative_error|
- **Threshold**: 0.3
- **Meets Target**: True

### Held-Out Evaluation
- **Case Count**: 3
- **Median |Relative Error|**: 0.004228
- **p95 |Relative Error|**: 0.01841
- **Worst |Relative Error|**: 0.01998
- **Worst Case ID**: held-b
- **Interval Coverage**: 1
- **Interval Case Count**: 3

### Calibration Evaluation
- **Case Count**: 0
- **Median |Relative Error|**: n/a
- **p95 |Relative Error|**: n/a
- **Worst |Relative Error|**: n/a
- **Worst Case ID**: n/a
- **Interval Coverage**: n/a
- **Interval Case Count**: 0

### Small-Time Evaluation
- **Case Count**: 0
- **Median |Error| (s)**: n/a
- **Worst |Error| (s)**: n/a
- **Worst Case ID**: n/a

## A/B Comparisons

No A/B comparisons performed.

## Not Compared

| Case ID | Reason |
|---|---|
| held-c | NO_TIME_PREDICTION |

## Notes

1. relative error uses the median of measured trials; sample extrema are not confidence intervals
2. held-out cases exclude workloads used to fit the time model
3. phase times are serial wall components on one main thread; no overlap is assumed
4. the 30% median target is an evaluation goal, not a guarantee
