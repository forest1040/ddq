#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
IMAGE="${DDQ_LINUX_IMAGE:-ddq-linux:dev-20261014}"
EVIDENCE_DIR="${1:?usage: run_p4_coordinator_followup.sh <dedicated-evidence-directory>}"
EVIDENCE_DIR="$(mkdir -p "$EVIDENCE_DIR" && cd "$EVIDENCE_DIR" && pwd)"
SCRATCH_DIR="$EVIDENCE_DIR/scratch"
OUTPUT_DIR="$EVIDENCE_DIR/output"
mkdir -p "$SCRATCH_DIR" "$OUTPUT_DIR"

command -v docker >/dev/null
docker image inspect "$IMAGE" >/dev/null

SOURCE_FILES=(
  src/svsim
  tests/test_volume_coordination_runtime.py
  tests/mpi_volume_coordinator_runtime_worker.py
  tests/test_volume_coordinator.py
  tests/test_disk_coordinator.py
  tests/mpi_disk_coordinator_worker.py
  tests/test_uncommitted_recovery.py
  tests/test_checkpoint_recovery.py
  tests/test_transport.py
  tests/test_transport_buffers.py
  docker/linux/run_p4_coordinator_followup.sh
)
(cd "$ROOT_DIR" && find "${SOURCE_FILES[@]}" -type f -print0 | sort -z | xargs -0 sha256sum) \
  > "$OUTPUT_DIR/source-hashes.txt"
docker image inspect "$IMAGE" --format '{{json .}}' > "$OUTPUT_DIR/image.json"
{
  date -u '+%Y-%m-%dT%H:%M:%SZ'
  hostname
  uname -a
  id
  docker version --format 'client={{.Client.Version}} server={{.Server.Version}}'
  docker image inspect "$IMAGE" --format 'image_id={{.Id}} user={{.Config.User}}'
} > "$OUTPUT_DIR/host-and-image.txt"

run_pytest() {
  local backend="$1"
  local log_name="$2"
  shift 2
  docker run --rm --network none \
    --hostname ddq-p4-vcoord-followup \
    --cpus=4 --memory=8g --pids-limit=512 --shm-size=256m \
    -v "$ROOT_DIR/src:/workspace/src:ro" \
    -v "$ROOT_DIR/tests:/workspace/tests:ro" \
    -v "$ROOT_DIR/examples:/workspace/examples:ro" \
    -v "$ROOT_DIR/pyproject.toml:/workspace/pyproject.toml:ro" \
    -v "$ROOT_DIR/uv.lock:/workspace/uv.lock:ro" \
    -v "$ROOT_DIR/README.md:/workspace/README.md:ro" \
    -v "$SCRATCH_DIR:/scratch" \
    -v "$OUTPUT_DIR:/output" \
    -e UV_CACHE_DIR=/scratch/uv-cache -e TMPDIR=/scratch \
    -e DDQ_MPI_TEST_TIMEOUT=600 -e SVSIM_TEST_BACKEND="$backend" \
    -e DDQ_JUNIT_NAME="$log_name" \
    -e OMP_NUM_THREADS=1 -e OPENBLAS_NUM_THREADS=1 \
    -e MKL_NUM_THREADS=1 -e NUMEXPR_NUM_THREADS=1 \
    "$IMAGE" sh -c 'set -eu
      id
      python --version
      mpirun --version | head -n 1
      if [ "$SVSIM_TEST_BACKEND" = direct ]; then
        uv run --offline --no-sync python -c "import tempfile; from pathlib import Path; from svsim.transport import StorageFile; p=Path(tempfile.mkdtemp(dir=\"/scratch\"))/\"odirect-probe.bin\"; data=b\"v\"*4096; f=StorageFile(p,\"w+b\",backend=\"direct\"); print(\"requested_backend=direct resolved_backend=\"+f.backend_identity, flush=True); assert f.backend_identity == \"linux-o-direct\", f.backend_identity; assert f.write(0,data)==len(data); f.flush(); assert f.read(0,len(data))==data; f.close(); print(\"O_DIRECT aligned write/fsync/readback passed\", flush=True)"
      fi
      uv run --offline --no-sync pytest -q -p no:cacheprovider --junitxml="/output/${DDQ_JUNIT_NAME}.xml" "$@"' sh "$@"
}

run_pytest buffered volume-coordination-buffered \
  tests/test_volume_coordination_runtime.py \
  tests/test_volume_coordinator.py \
  tests/test_disk_coordinator.py \
  tests/test_uncommitted_recovery.py \
  tests/test_checkpoint_recovery.py \
  > "$OUTPUT_DIR/volume-coordination-buffered.txt" 2>&1

run_pytest direct volume-coordination-direct \
  tests/test_volume_coordination_runtime.py \
  tests/test_volume_coordinator.py \
  tests/test_uncommitted_recovery.py \
  tests/test_checkpoint_recovery.py \
  tests/test_transport.py \
  tests/test_transport_buffers.py \
  > "$OUTPUT_DIR/volume-coordination-direct.txt" 2>&1

printf 'Evidence: %s\n' "$OUTPUT_DIR"
