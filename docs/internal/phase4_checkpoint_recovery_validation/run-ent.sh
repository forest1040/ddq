#!/usr/bin/env bash
set -euo pipefail
TASK_ROOT=/tmp/ddq-p4-recovery-20261014.WwrDGc
SOURCE_ROOT=$TASK_ROOT/context
IMAGE=ddq-linux:dev-20261014
mkdir -p "$TASK_ROOT/artifacts" "$TASK_ROOT/scratch"
chmod 0777 "$TASK_ROOT/artifacts" "$TASK_ROOT/scratch"
cd "$SOURCE_ROOT"
docker image inspect "$IMAGE" > "$TASK_ROOT/artifacts/image.json"
uname -a > "$TASK_ROOT/artifacts/kernel.txt"
df -B1 "$TASK_ROOT" > "$TASK_ROOT/artifacts/filesystem.txt"
find src tests examples -type f \( -name '*.py' -o -name '*.c' -o -name '*.h' \) -print0 | sort -z | xargs -0 sha256sum > "$TASK_ROOT/artifacts/source-hashes.txt"
COMMON=(--rm --network none --cpus=4 --memory=8g --pids-limit=512 --shm-size=256m
  --mount "type=bind,src=$SOURCE_ROOT/src,dst=/workspace/src,readonly"
  --mount "type=bind,src=$SOURCE_ROOT/tests,dst=/workspace/tests,readonly"
  --mount "type=bind,src=$SOURCE_ROOT/examples,dst=/workspace/examples,readonly"
  --mount "type=bind,src=$TASK_ROOT/scratch,dst=/scratch"
  --mount "type=bind,src=$TASK_ROOT/artifacts,dst=/output"
  -e UV_CACHE_DIR=/scratch/uv-cache -e DDQ_MPI_TEST_TIMEOUT=600)
REGRESSION_NAME=ddq-p4-recovery-WwrDGc-regression
NATIVE_NAME=ddq-p4-recovery-WwrDGc-native
cleanup() { docker rm -f "$REGRESSION_NAME" "$NATIVE_NAME" >/dev/null 2>&1 || true; }
trap cleanup EXIT
run_regression=0
start=$SECONDS
timeout --signal=TERM --kill-after=15s 1800 docker run "${COMMON[@]}" --name "$REGRESSION_NAME" "$IMAGE" \
 uv run --offline --no-sync pytest -q -p no:cacheprovider --basetemp=/scratch/regression --junitxml=/output/regression.xml \
 tests/test_checkpoint_recovery.py \
 tests/test_checkpoint_recovery_mpi.py \
 tests/test_checkpoint_reservation_ledger.py \
 tests/test_disk_reservation_ledger.py \
 tests/test_disk_failed_allocation.py \
 tests/test_catalog_admission.py \
 tests/test_storage.py \
 tests/test_storage_directory_lease.py \
 tests/test_dataset_lease.py \
 tests/test_checkpoint_atomic_admission.py \
 tests/test_admitted_files.py \
 tests/test_distributed_checkpoint.py \
 tests/test_stored_recovery.py \
 tests/test_stored_budget.py \
 tests/test_phase4_lifecycle.py \
 tests/test_disk_coordinator.py \
 tests/test_memory_store.py \
 > "$TASK_ROOT/artifacts/regression.txt" 2>&1 || run_regression=$?
printf 'regression\t%s\t%s\n' "$run_regression" "$((SECONDS-start))" >> "$TASK_ROOT/artifacts/status.tsv"
cleanup
run_native=0
start=$SECONDS
timeout --signal=TERM --kill-after=15s 600 docker run "${COMMON[@]}" --name "$NATIVE_NAME" \
 --security-opt "seccomp=$SOURCE_ROOT/docker/linux/seccomp-uring.json" -e DDQ_NATIVE_URING=1 "$IMAGE" \
 uv run --offline --no-sync pytest -q -p no:cacheprovider --basetemp=/scratch/native --junitxml=/output/native.xml \
 tests/test_native_uring_linux.py 'tests/test_native_uring_mpi_linux.py::test_native_io_uring_phase4_mpi[buffered-2]' \
 'tests/test_native_uring_mpi_linux.py::test_native_io_uring_phase4_mpi[direct-2]' \
 > "$TASK_ROOT/artifacts/native.txt" 2>&1 || run_native=$?
printf 'native\t%s\t%s\n' "$run_native" "$((SECONDS-start))" >> "$TASK_ROOT/artifacts/status.tsv"
cleanup
cat "$TASK_ROOT/artifacts/status.tsv"
cat "$TASK_ROOT/artifacts/regression.txt"
cat "$TASK_ROOT/artifacts/native.txt"
if [[ "$run_regression" != 0 || "$run_native" != 0 ]]; then exit 1; fi
