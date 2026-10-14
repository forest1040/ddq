#!/bin/bash
# Async executor calibration -> time model -> held-out runtime ETA checks.
# usage: flow_eta.sh <outdir> <scratch> "<runner words>"   e.g. "uv run --offline --no-sync"
# env: RANKS (MPI ranks, default 1), NODES (hosts, default 1), STORAGE (ram|file), BACKEND (buffered|direct),
#      MPIEXEC (launcher words for RANKS>1)
set -euo pipefail
O=$1; SCR=$2; read -r -a R <<< "$3"
RANKS=${RANKS:-1}; NODES=${NODES:-1}; STORAGE=${STORAGE:-ram}; BACKEND=${BACKEND:-buffered}
M=(); if [ "$RANKS" -gt 1 ]; then read -r -a M <<< "${MPIEXEC:-mpiexec} -n $RANKS"; fi
rm -rf "$O" "$SCR"; mkdir -p "$O/w" "$SCR"
"${R[@]}" svbench probe --storage-path "$SCR" --threads 2 --ranks "$RANKS" --nodes "$NODES" --output "$O/probe.json"
"${R[@]}" svsim calibration-suite --output-dir "$O/suite" --qubits "${QUBITS:-5,6,7,8}" --counts "${COUNTS:-1,4,12}" --ranks "$RANKS" --storage "$STORAGE" >/dev/null
a=(); for f in "$O"/suite/*.json; do a+=(--workload "$f"); done
${M[@]+"${M[@]}"} "${R[@]}" svsim benchmark "${a[@]}" --executor async --threads 2 --backend "$BACKEND" --profile "$O/probe.json" --scratch "$SCR" --output-dir "$O/cal" --ram-budget-per-rank 67108864 --disk-budget "${DISK_BUDGET:-268435456}" --wall-seconds "${CAL_WALL:-1200}" --block-bits 4 --repetitions 3 >/dev/null
r=(); for f in "$O"/cal/*.json; do r+=(--run "$f"); done
"${R[@]}" svsim calibrate-time "${r[@]}" --output "$O/model.json"
python3 - "$O/w" "$STORAGE" <<'PY'
import json,sys
o, storage = sys.argv[1], sys.argv[2]
H=lambda q:{"kind":"H","targets":[q]}; RX=lambda q:{"kind":"RX","targets":[q],"angle":0.37}
P={"kind":"PAULI_ROT","targets":[0,1,2],"paulis":["Y","X","Z"],"angle":0.37}
cases={'held-a':(7,[{"kind":"REPEAT","count":3,"body":[H(0),H(6),RX(2),P]}]),
       'held-b':(8,[{"kind":"REPEAT","count":6,"body":[RX(0),P,H(7)]}]),
       'held-d':(8,[{"kind":"REPEAT","count":2,"body":[H(0),RX(1),P,H(7)]}]),
       'held-e':(8,[{"kind":"REPEAT","count":10,"body":[H(0),RX(1),P,H(7)]}])}
for k,(n,ops) in cases.items():
    json.dump({"schema_version":1,"kind":"workload","qubits":n,"policies":{"storage":storage},"operations":ops},open(f'{o}/{k}.json','w'))
PY
w=(); for h in held-a held-b held-d held-e; do w+=(--workload "$O/w/$h.json"); done
${M[@]+"${M[@]}"} "${R[@]}" svsim benchmark "${w[@]}" --executor async --threads 2 --backend "$BACKEND" --eta-model "$O/model.json" --profile "$O/probe.json" --scratch "$SCR" --output-dir "$O/held" --ram-budget-per-rank 67108864 --disk-budget "${DISK_BUDGET:-268435456}" --wall-seconds "${HELD_WALL:-1200}" --block-bits 4 --repetitions "${HELD_REPS:-5}"
h=(); for f in "$O"/held/*.json; do h+=(--run "$f"); done
"${R[@]}" svsim eta-report "${h[@]}" --output "$O/eta-report.json"
rm -rf "$SCR"
