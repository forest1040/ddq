# Comparison Report (static-vs-observed-v1)

- **Report ID**: `04e93488cfcb596a180cceb8c84cad538ad6b0fcf541061a1f41109ad34eea2c`
- **Campaign SHA-256**: `73748916f2b469de718949f8c4d0e251886dc87026c2535759a534f405437fb7`

## Cases

| Case ID | Variant | Held-Out | Predicted Expected [Lower, Upper] | Observed Median | Rel. Error % | Within Interval | Observed Bottleneck |
|---|---|---|---|---|---|---|---|
| held-a | default | True | 1.571 [1.541, 2.864] | 1.724 | -8.903% | True | execute |
| held-b | default | True | 4.071 [3.975, 7.198] | 9.166 | -55.59% | False | execute |
| held-c | default | True | n/a [n/a, n/a] | 0.9779 | n/a | n/a | execute |
| held-d | default | True | 1.925 [1.889, 3.437] | 2.031 | -5.195% | True | execute |

## Accuracy

### Target Evaluation
- **Statistic**: held_out median |relative_error|
- **Threshold**: 0.3
- **Meets Target**: True

### Held-Out Evaluation
- **Case Count**: 3
- **Median |Relative Error|**: 0.08903
- **p95 |Relative Error|**: 0.5092
- **Worst |Relative Error|**: 0.5559
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
