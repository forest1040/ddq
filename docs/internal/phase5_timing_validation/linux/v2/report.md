# Comparison Report (static-vs-observed-v1)

- **Report ID**: `ba971f0ba2af2f6d6d0503b3cdc16a9416b956e78fd95b1a2b71dc20bb66a815`
- **Campaign SHA-256**: `6ba591a0579e3b927829de3441a4b5bbb420aa71c907f23216f3ebf47efa6404`

## Cases

| Case ID | Variant | Held-Out | Predicted Expected [Lower, Upper] | Observed Median | Rel. Error % | Within Interval | Observed Bottleneck |
|---|---|---|---|---|---|---|---|
| held-a | default | True | 5.723 [5.511, 64.89] | 11.19 | -48.85% | True | execute |
| held-b | default | True | 15.86 [15.36, 153.5] | 39.45 | -59.79% | True | execute |
| held-c | default | True | n/a [n/a, n/a] | 3.14 | n/a | n/a | execute |
| held-d | default | True | 7.34 [7.079, 79.74] | 15.86 | -53.72% | True | execute |

## Accuracy

### Target Evaluation
- **Statistic**: held_out median |relative_error|
- **Threshold**: 0.3
- **Meets Target**: False

### Held-Out Evaluation
- **Case Count**: 3
- **Median |Relative Error|**: 0.5372
- **p95 |Relative Error|**: 0.5918
- **Worst |Relative Error|**: 0.5979
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
