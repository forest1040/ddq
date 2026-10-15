#!/usr/bin/env bash
# Run after the approved src/tests/examples transfer described in README.md.
set -euo pipefail
ssh ent 'set -eu
mkdir -p /tmp/ddq-p4-linux-followup-20261015-tmori/wave2-final-scratch /tmp/ddq-p4-linux-followup-20261015-tmori/wave2-final-output
cd /tmp/ddq-p4-linux-followup-20261015-tmori/context
find src tests examples -type f ! -path '\''*/__pycache__/*'\'' ! -name '\''*.pyc'\'' -print0 | sort -z | xargs -0 sha256sum > /tmp/ddq-p4-linux-followup-20261015-tmori/wave2-final-output/source-hashes.txt
{ date -u; uname -a; docker image inspect ddq-linux:dev-20261014 --format '\''{{.Id}} {{.Config.User}}'\''; } > /tmp/ddq-p4-linux-followup-20261015-tmori/wave2-final-output/host-and-image.txt
for backend in buffered direct; do
 docker run --rm --network none --hostname ddq-p4-vcoord-followup --cpus=4 --memory=8g --pids-limit=512 --shm-size=256m -v /tmp/ddq-p4-linux-followup-20261015-tmori/context/src:/workspace/src:ro -v /tmp/ddq-p4-linux-followup-20261015-tmori/context/tests:/workspace/tests:ro -v /tmp/ddq-p4-linux-followup-20261015-tmori/context/examples:/workspace/examples:ro -v /tmp/ddq-p4-linux-followup-20261015-tmori/wave2-final-scratch:/scratch -v /tmp/ddq-p4-linux-followup-20261015-tmori/wave2-final-output:/output -e UV_CACHE_DIR=/scratch/uv-cache -e TMPDIR=/scratch -e DDQ_MPI_TEST_TIMEOUT=600 -e SVSIM_TEST_BACKEND="$backend" -e OMP_NUM_THREADS=1 -e OPENBLAS_NUM_THREADS=1 -e MKL_NUM_THREADS=1 -e NUMEXPR_NUM_THREADS=1 ddq-linux:dev-20261014 sh -c '\''set -eu
id
python --version
mpirun --version | head -n 1
if [ "$SVSIM_TEST_BACKEND" = direct ]; then timeout --signal=TERM --kill-after=15s 30 uv run --offline --no-sync python -c '\''\'\'''\''import tempfile; from pathlib import Path; from svsim.transport import StorageFile; from svsim.catalog_admission import catalog_birth_identity,catalog_file_owner; p=Path(tempfile.mkdtemp(dir="/scratch"))/"catalog-probe.bin"; f=StorageFile(p,"w+b",backend="direct"); print("resolved_backend="+f.backend_identity,flush=True); assert f.backend_identity=="linux-o-direct"; assert f.write(0,b"v"*4096)==4096; f.flush(); assert f.read(0,4096)==b"v"*4096; f.close(); birth=catalog_birth_identity(p,p.lstat()); assert birth is not None; assert catalog_file_owner(p).startswith("catalog-v3:"); print("O_DIRECT write/fsync/readback and statx birthtime passed: "+birth,flush=True)'\''\'\'''\''; fi
timeout --signal=TERM --kill-after=15s 870 uv run --offline --no-sync pytest -m '\''\'\'''\''not slow'\''\'\'''\'' -q -rs -p no:cacheprovider --junitxml=/output/wave2-${SVSIM_TEST_BACKEND}.xml tests/test_catalog_file_identity.py tests/test_catalog_admission.py tests/test_checkpoint_recovery.py tests/test_uncommitted_recovery.py tests/test_private_ledger_recovery.py tests/test_uncommitted_gc.py tests/test_adaptive_evaluation.py tests/test_layout_migration.py tests/test_runtime_cost_registry.py tests/test_phase4_evaluation.py tests/test_replan_cost.py tests/test_volume_coordination_runtime.py'\'' > /tmp/ddq-p4-linux-followup-20261015-tmori/wave2-final-output/wave2-${backend}.txt 2>&1
done'

