#!/usr/bin/env bash
set -euo pipefail
ssh ent 'set -eu
mkdir -p /tmp/ddq-p4-linux-followup-20261015-tmori/wave3-performance-output-v2 /tmp/ddq-p4-linux-followup-20261015-tmori/wave3-performance-scratch-v2
cd /tmp/ddq-p4-linux-followup-20261015-tmori/wave3-performance-context-v2
find src tests examples -type f ! -path '"'"'*/__pycache__/*'"'"' ! -name '"'"'*.pyc'"'"' -print0 | sort -z | xargs -0 sha256sum > /tmp/ddq-p4-linux-followup-20261015-tmori/wave3-performance-output-v2/source-hashes.txt
for backend in buffered direct; do
 for ranks in 1 2; do
  docker run --rm --network none --hostname ddq-p4-vcoord-followup --cpus=4 --memory=8g --pids-limit=512 --shm-size=256m -v /tmp/ddq-p4-linux-followup-20261015-tmori/wave3-performance-context-v2/src:/workspace/src:ro -v /tmp/ddq-p4-linux-followup-20261015-tmori/wave3-performance-context-v2/tests:/workspace/tests:ro -v /tmp/ddq-p4-linux-followup-20261015-tmori/wave3-performance-context-v2/examples:/workspace/examples:ro -v /tmp/ddq-p4-linux-followup-20261015-tmori/wave3-performance-scratch-v2:/scratch -v /tmp/ddq-p4-linux-followup-20261015-tmori/wave3-performance-output-v2:/output -e TMPDIR=/scratch -e UV_CACHE_DIR=/scratch/uv-cache -e OMP_NUM_THREADS=1 -e OPENBLAS_NUM_THREADS=1 -e MKL_NUM_THREADS=1 -e NUMEXPR_NUM_THREADS=1 -e SVSIM_TEST_BACKEND="$backend" -e DDQ_BACKEND="$backend" -e DDQ_RANKS="$ranks" ddq-linux:dev-20261014 sh -c '"'"'set -eu
 if [ "$DDQ_RANKS" = 1 ]; then
  timeout --signal=TERM --kill-after=15s 600 uv run --offline --no-sync python -m pytest -q -p no:cacheprovider --junitxml=/output/new-functional-$DDQ_BACKEND.xml tests/test_bootstrap_namespace.py tests/test_private_volume_coordination.py tests/test_migration_scratch_recovery.py tests/test_checkpoint_payload_chunks.py tests/test_remaining_work_mpi.py tests/test_state_export_admission.py > /output/new-functional-$DDQ_BACKEND.txt 2>&1
 fi
 timeout --signal=TERM --kill-after=15s 3600 uv run --offline --no-sync mpiexec -n "$DDQ_RANKS" python examples/p4_002_overhead_campaign.py --backend "$DDQ_BACKEND" --scratch /scratch/overhead-$DDQ_BACKEND-p$DDQ_RANKS --output /output/overhead-$DDQ_BACKEND-p$DDQ_RANKS.json --warmups 2 --pairs 7 --seed 20261016
 timeout --signal=TERM --kill-after=15s 3600 uv run --offline --no-sync mpiexec -n "$DDQ_RANKS" python examples/p4_002_observed_eta.py --backend "$DDQ_BACKEND" --scratch /scratch/heldout-$DDQ_BACKEND-p$DDQ_RANKS --output /output/heldout-$DDQ_BACKEND-p$DDQ_RANKS.json --trials 1 --skip-overhead --policies S0,S1,S2 --stores file,ram
 '"'"' > /tmp/ddq-p4-linux-followup-20261015-tmori/wave3-performance-output-v2/campaign-$backend-p$ranks.txt 2>&1
 done
done
'
