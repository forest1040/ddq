#!/bin/bash
# usage: run_matrix.sh <outbase> <runner words> "<ranks> <storage> <qubits>" ...
base=$1; runner=$2; shift 2
for cfg in "$@"; do
  read -r ranks storage qubits <<< "$cfg"
  out=$base/eta-r$ranks-$storage
  RANKS=$ranks STORAGE=$storage QUBITS=$qubits bash "$(dirname "$0")/flow_eta.sh" "$out" "$base/sc-r$ranks-$storage" "$runner" >/dev/null 2>"$out.err"
  echo "r$ranks $storage exit=$?"
  python3 -c "
import json;d=json.load(open('$out/eta-report.json'));m=json.load(open('$out/model.json'))
print(' excluded',m['sync_latency']['excluded_unstable_trial_count'],{k:(round(d[k],3) if isinstance(d[k],float) else d[k]) for k in ('check_count','predicted_count','median_abs_relative_error','p95_abs_relative_error','worst_abs_relative_error','interval_coverage')})" 2>&1 | tail -1
done
