#!/bin/bash
# Phase 5 calibrate -> fit -> held-out estimate/measure -> report, inside the ddq Linux container.
set -euo pipefail
O=/output/phase5; R=(uv run --offline --no-sync)
rm -rf "$O" /scratch/p5; mkdir -p "$O/w" /scratch/p5
"${R[@]}" svbench probe --storage-path /scratch/p5 --output "$O/probe.json"
"${R[@]}" svsim calibration-suite --output-dir "$O/suite" --qubits "${QUBITS:-5,6,7,8}" --counts "${COUNTS:-1,4,12}" >/dev/null
a=(); for f in "$O"/suite/*.json; do a+=(--workload "$f"); done
"${R[@]}" svsim benchmark "${a[@]}" --profile "$O/probe.json" --scratch /scratch/p5 --output-dir "$O/cal" --ram-budget-per-rank 67108864 --disk-budget 67108864 --wall-seconds "${CAL_WALL:-600}" --block-bits 4 --repetitions 3 >/dev/null
r=(); for f in "$O"/cal/*.json; do r+=(--run "$f"); done
"${R[@]}" svsim calibrate-time "${r[@]}" --output "$O/model.json"
python - "$O" <<'PY'
import json,sys
o=sys.argv[1]
H=lambda q:{"kind":"H","targets":[q]}; RX=lambda q:{"kind":"RX","targets":[q],"angle":0.37}
P={"kind":"PAULI_ROT","targets":[0,1,2],"paulis":["Y","X","Z"],"angle":0.37}
cases={'held-a':(7,[{"kind":"REPEAT","count":3,"body":[H(0),H(6),RX(2),P]}]),
       'held-b':(8,[{"kind":"REPEAT","count":6,"body":[RX(0),P,H(7)]}]),
       'held-c':(6,[{"kind":"REPEAT","count":5,"body":[H(0),RX(5)]}]),
       'held-d':(8,[{"kind":"REPEAT","count":2,"body":[H(0),RX(1),P,H(7)]}])}
for k,(n,ops) in cases.items():
    json.dump({"schema_version":1,"kind":"workload","qubits":n,"operations":ops},open(f'{o}/w/{k}.json','w'))
model=json.load(open(f'{o}/model.json'))
json.dump({"schema_version":1,"kind":"comparison-campaign","calibration_workload_ids":model["calibration_workload_ids"],
           "cases":[{"case_id":k,"plan":f"{k}.plan.json","run":f"held/{k}.run.json","variant":"default","group":k} for k in cases]},
          open(f'{o}/campaign.json','w'))
PY
w=()
for h in held-a held-b held-c held-d; do
  "${R[@]}" svsim register --input "$O/w/$h.json" --output "$O/$h.reg.json"
  "${R[@]}" svsim estimate --workload "$O/$h.reg.json" --profile "$O/probe.json" --block-bits 4 --ram-budget-per-rank 67108864 --ram-budget-per-host 1073741824 --disk-budget-per-host 67108864 --time-model "$O/model.json" --output "$O/$h.plan.json" --planning-only
  w+=(--workload "$O/$h.reg.json" --plan "$O/$h.plan.json" --label "$h")
done
"${R[@]}" svsim benchmark "${w[@]}" --profile "$O/probe.json" --scratch /scratch/p5 --output-dir "$O/held" --ram-budget-per-rank 67108864 --disk-budget 67108864 --wall-seconds "${HELD_WALL:-300}" --block-bits 4 --repetitions "${HELD_REPS:-5}"
"${R[@]}" svsim report --campaign "$O/campaign.json" --output "$O/report.json" --markdown "$O/report.md"
rm -rf /scratch/p5
