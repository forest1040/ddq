# Comparison Report (static-vs-observed-v1)

- **Report ID**: `de8075443bc4c513e6857469dd5bd73dba07c58d10b9b8d2b10364a7b1e1ec41`
- **Campaign SHA-256**: `7fe0538963763714a775fea4a8a057aacfbdec041ca81a0f474685822c263bb7`

## Cases

| Case ID | Variant | Held-Out | Predicted Expected [Lower, Upper] | Observed Median | Rel. Error % | Within Interval | Observed Bottleneck |
|---|---|---|---|---|---|---|---|
| held-a | default | True | 0.7474 [0.7381, 0.9661] | 0.7528 | -0.7145% | True | execute |
| held-b | default | True | 1.914 [1.89, 2.408] | 1.964 | -2.546% | True | execute |
| held-c | default | True | n/a [n/a, n/a] | 0.3867 | n/a | n/a | execute |
| held-d | default | True | 0.9168 [0.9047, 1.175] | 0.9227 | -0.6468% | True | execute |

## Accuracy

### Target Evaluation
- **Statistic**: held_out median |relative_error|
- **Threshold**: 0.3
- **Meets Target**: True

### Held-Out Evaluation
- **Case Count**: 3
- **Median |Relative Error|**: 0.007145
- **p95 |Relative Error|**: 0.02363
- **Worst |Relative Error|**: 0.02546
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
