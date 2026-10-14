# Comparison Report (static-vs-observed-v1)

- **Report ID**: `0cacc755fdb52cbf15146d795277e733839f8118de0d90438f71603eb4f66c42`
- **Campaign SHA-256**: `8fc5fef68bd168713cde58d4e6b8b760b33ca2f9a3aa9d371b916f93992c4a0a`

## Cases

| Case ID | Variant | Held-Out | Predicted Expected [Lower, Upper] | Observed Median | Rel. Error % | Within Interval | Observed Bottleneck |
|---|---|---|---|---|---|---|---|
| held-a | default | True | 5.44 [5.083, 6.895] | 5.809 | -6.341% | True | execute |
| held-b | default | True | 13.83 [13.09, 17.28] | 27.49 | -49.69% | False | execute |
| held-c | default | True | n/a [n/a, n/a] | 2.877 | n/a | n/a | execute |
| held-d | default | True | 6.638 [6.177, 8.381] | 6.733 | -1.413% | True | execute |

## Accuracy

### Target Evaluation
- **Statistic**: held_out median |relative_error|
- **Threshold**: 0.3
- **Meets Target**: True

### Held-Out Evaluation
- **Case Count**: 3
- **Median |Relative Error|**: 0.06341
- **p95 |Relative Error|**: 0.4536
- **Worst |Relative Error|**: 0.4969
- **Worst Case ID**: held-b
- **Interval Coverage**: 0.6667
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
