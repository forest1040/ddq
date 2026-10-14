#!/usr/bin/env bash
set -euo pipefail
TASK_ROOT=/tmp/ddq-p4-writers-20261014.jopUVR
SOURCE_ROOT=$TASK_ROOT/context-final
mkdir -p "$TASK_ROOT/artifacts/final" "$TASK_ROOT/scratch-final"
cd "$SOURCE_ROOT"
find src tests examples -type f \( -name '*.py' -o -name '*.c' -o -name '*.h' \) -print0 | sort -z | xargs -0 sha256sum > "$TASK_ROOT/artifacts/final/source-hashes.txt"
NAME=ddq-p4-writers-jopUVR-final
cleanup() { docker rm -f "$NAME" >/dev/null 2>&1 || true; }
trap cleanup EXIT
result=0
start=$SECONDS
timeout --signal=TERM --kill-after=15s 180 docker run --rm --name "$NAME" --network none \
 --cpus=4 --memory=8g --pids-limit=512 --shm-size=256m \
 --mount "type=bind,src=$SOURCE_ROOT/src,dst=/workspace/src,readonly" \
 --mount "type=bind,src=$SOURCE_ROOT/tests,dst=/workspace/tests,readonly" \
 --mount "type=bind,src=$SOURCE_ROOT/examples,dst=/workspace/examples,readonly" \
 --mount "type=bind,src=$TASK_ROOT/scratch-final,dst=/scratch" \
 --mount "type=bind,src=$TASK_ROOT/artifacts/final,dst=/output" \
 -e UV_CACHE_DIR=/scratch/uv-cache ddq-linux:dev-20261014 sh -ec '
 uv run --offline --no-sync pytest -q -p no:cacheprovider --basetemp=/scratch/final --junitxml=/output/final.xml tests/test_catalog_capability.py tests/test_catalog_admission.py
 uv run --offline --no-sync python -c "import json; from pathlib import Path; from svsim.catalog_admission import get_sqlite_guard; Path(\"/output/capability.json\").write_text(json.dumps(get_sqlite_guard().capability(),indent=2)+\"\n\")"
 uv run --offline --no-sync pytest --collect-only -q -p no:cacheprovider tests/test_catalog_capability.py tests/test_admitted_files.py tests/test_catalog_admission.py tests/test_disk_reservation_ledger.py tests/test_trace_admission.py tests/test_checkpoint_atomic_admission.py tests/test_stored_recovery.py tests/test_stored_budget.py tests/test_telemetry.py tests/test_disk_coordinator.py tests/test_runtime_calibration.py tests/test_distributed_checkpoint.py tests/test_phase4_lifecycle.py > /output/collection.txt
 ' > "$TASK_ROOT/artifacts/final/final.txt" 2>&1 || result=$?
printf 'final\t%s\t%s\n' "$result" "$((SECONDS-start))" > "$TASK_ROOT/artifacts/final/status.tsv"
cat "$TASK_ROOT/artifacts/final/final.txt"
cat "$TASK_ROOT/artifacts/final/status.tsv"
exit "$result"
