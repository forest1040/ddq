#!/bin/zsh
# usage: flow.sh <workdir>  (run from repo root)
set -e
S=$1; R=(uv run --locked --extra mpi)
export OMP_NUM_THREADS=1 OPENBLAS_NUM_THREADS=1 VECLIB_MAXIMUM_THREADS=1
rm -rf $S/out; mkdir -p $S/out/scratch $S/out/w
"${R[@]}" svbench probe --storage-path $S/out/scratch --output $S/out/probe.json
"${R[@]}" svsim calibration-suite --output-dir $S/out/suite --counts 1,4,12 >/dev/null
a=(); for f in $S/out/suite/*.json; do a+=(--workload $f); done
"${R[@]}" svsim benchmark "${a[@]}" --profile $S/out/probe.json --scratch $S/out/scratch --output-dir $S/out/cal --ram-budget-per-rank 67108864 --disk-budget 67108864 --wall-seconds 400 --block-bits 4 --repetitions 3 >/dev/null
r=(); for f in $S/out/cal/*.json; do r+=(--run $f); done
"${R[@]}" svsim calibrate-time "${r[@]}" --output $S/out/model.json
python3 - $S/out <<'PY'
import json,sys
o=sys.argv[1]
H=lambda q:{"kind":"H","targets":[q]}; RX=lambda q:{"kind":"RX","targets":[q],"angle":0.37}
P={"kind":"PAULI_ROT","targets":[0,1,2],"paulis":["Y","X","Z"],"angle":0.37}
cases={'held-a':(7,[{"kind":"REPEAT","count":3,"body":[H(0),H(6),RX(2),P]}],'ram'),
       'held-b':(8,[{"kind":"REPEAT","count":6,"body":[RX(0),P,H(7)]}],'ram'),
       'held-c':(6,[{"kind":"REPEAT","count":5,"body":[H(0),RX(5)]}],'ram'),
       'held-d':(8,[{"kind":"REPEAT","count":2,"body":[H(0),RX(1),P,H(7)]}],'ram')}
for k,(n,ops,st) in cases.items():
    json.dump({"schema_version":1,"kind":"workload","qubits":n,"policies":{"storage":st},"operations":ops},open(f'{o}/w/{k}.json','w'))
PY
cal=$(python3 -c "import json,sys;print(json.dumps(json.load(open('$S/out/model.json'))['calibration_workload_ids']))")
w=(); cs=()
for h in held-a held-b held-c held-d; do
  "${R[@]}" svsim register --input $S/out/w/$h.json --output $S/out/$h.reg.json
  "${R[@]}" svsim estimate --workload $S/out/$h.reg.json --profile $S/out/probe.json --block-bits 4 --ram-budget-per-rank 67108864 --ram-budget-per-host 1073741824 --disk-budget-per-host 67108864 --time-model $S/out/model.json --output $S/out/$h.plan.json --planning-only
  w+=(--workload $S/out/$h.reg.json --plan $S/out/$h.plan.json --label $h)
  cs+=("{\"case_id\":\"$h\",\"plan\":\"$h.plan.json\",\"run\":\"held/$h.run.json\",\"variant\":\"default\",\"group\":\"$h\"}")
done
"${R[@]}" svsim benchmark "${w[@]}" --profile $S/out/probe.json --scratch $S/out/scratch --output-dir $S/out/held --ram-budget-per-rank 67108864 --disk-budget 67108864 --wall-seconds 200 --block-bits 4 --repetitions 5
echo "{\"schema_version\":1,\"kind\":\"comparison-campaign\",\"calibration_workload_ids\":$cal,\"cases\":[${(j:,:)cs}]}" > $S/out/campaign.json
"${R[@]}" svsim report --campaign $S/out/campaign.json --output $S/out/report.json --markdown $S/out/report.md
