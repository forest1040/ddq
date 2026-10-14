#!/usr/bin/env bash
# Run bounded Linux validation in dedicated containers; never change the host.
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
IMAGE=${DDQ_LINUX_IMAGE:-ddq-linux:dev}
STAGE=${DDQ_LINUX_VALIDATION_STAGE:-all}
case "$STAGE" in
  all|native|regression) ;;
  *) printf 'Unknown DDQ_LINUX_VALIDATION_STAGE: %s\n' "$STAGE" >&2; exit 2 ;;
esac
RUN_DIR=$(mktemp -d "${1:-/tmp}/ddq-linux-validation.XXXXXX")
mkdir -p "$RUN_DIR/scratch" "$RUN_DIR/artifacts"
OWNED_NAME="ddq-linux-validation-$(basename "$RUN_DIR")"
cleanup() { docker rm -f "$OWNED_NAME" >/dev/null 2>&1 || true; }
trap cleanup EXIT

COMMON=(--rm --name "$OWNED_NAME" --network none
  --cpus=4 --memory=8g --pids-limit=512 --shm-size=256m
  --mount "type=bind,src=$ROOT_DIR/src,dst=/workspace/src,readonly"
  --mount "type=bind,src=$ROOT_DIR/tests,dst=/workspace/tests,readonly"
  --mount "type=bind,src=$ROOT_DIR/examples,dst=/workspace/examples,readonly"
  --mount "type=bind,src=$RUN_DIR/scratch,dst=/scratch"
  --mount "type=bind,src=$RUN_DIR/artifacts,dst=/output"
  -e UV_CACHE_DIR=/scratch/uv-cache -e DDQ_MPI_TEST_TIMEOUT=600)
NATIVE=(--security-opt "seccomp=$ROOT_DIR/docker/linux/seccomp-uring.json"
  -e DDQ_NATIVE_URING=1)

run_case() {
  local case_name=$1 seconds=$2
  shift 2
  local start=$SECONDS result=0
  timeout --signal=TERM --kill-after=15s "$seconds" docker run "${COMMON[@]}" "$@" \
    >"$RUN_DIR/artifacts/$case_name.log" 2>&1 || result=$?
  cleanup
  printf '%s\t%s\t%s\n' "$case_name" "$result" "$((SECONDS-start))" \
    >>"$RUN_DIR/artifacts/status.tsv"
  cat "$RUN_DIR/artifacts/$case_name.log"
  return "$result"
}

printf 'Artifacts: %s\n' "$RUN_DIR/artifacts"
docker image inspect "$IMAGE" >"$RUN_DIR/artifacts/image.json"
uname -a >"$RUN_DIR/artifacts/host-kernel.txt"
df -B1 "$RUN_DIR" >"$RUN_DIR/artifacts/host-filesystem.txt"
findmnt -T "$RUN_DIR" >"$RUN_DIR/artifacts/host-mount.txt"
docker version >"$RUN_DIR/artifacts/docker-version.txt"
sha256sum "$ROOT_DIR/uv.lock" "$ROOT_DIR/docker/linux/Dockerfile" \
  "$ROOT_DIR/pyproject.toml" "$ROOT_DIR/.dockerignore" \
  "$ROOT_DIR/docker/linux/run_validation.sh" \
  "$ROOT_DIR/docker/linux/seccomp-uring.json" \
  >"$RUN_DIR/artifacts/environment-hashes.txt"
find "$ROOT_DIR/src" "$ROOT_DIR/tests" "$ROOT_DIR/examples" -type f \
  \( -name '*.py' -o -name '*.c' -o -name '*.h' \) -print0 \
  | sort -z | xargs -0 sha256sum >"$RUN_DIR/artifacts/source-hashes.txt"

run_case toolchain 60 "$IMAGE" sh -ec \
  'uv --version; python --version; mpicc --showme; mpirun --version; cc --version; pkg-config --modversion liburing; dpkg-query -W liburing-dev libopenmpi-dev openmpi-bin libsqlite3-dev; python -c "import numpy, sqlite3; from mpi4py import MPI; print(numpy.__version__, sqlite3.sqlite_version); print(MPI.Get_library_version())"; cat /sys/fs/cgroup/cpu.max /sys/fs/cgroup/memory.max /sys/fs/cgroup/pids.max; sha256sum /usr/include/liburing.h /usr/include/liburing/io_uring.h; ldd /workspace/.venv/lib/python3.12/site-packages/mpi4py/MPI*.so'

# The default policy can reject setup. Preserve this separate capability result.
default_result=0
run_case probe-default 60 -e DDQ_NATIVE_URING=1 "$IMAGE" \
  uv run --offline --no-sync python examples/linux_native_io_probe.py --probe \
  || default_result=$?
if [[ "$default_result" != 0 && "$default_result" != 2 ]]; then
  exit "$default_result"
fi
run_case probe-native 60 "${NATIVE[@]}" "$IMAGE" \
  uv run --offline --no-sync python examples/linux_native_io_probe.py --probe
if [[ "$STAGE" != regression ]]; then
  run_case native 600 "${NATIVE[@]}" "$IMAGE" \
    uv run --offline --no-sync pytest -q -p no:cacheprovider \
    --basetemp=/scratch/native --junitxml=/output/native.xml \
    tests/test_native_uring_linux.py tests/test_native_uring_mpi_linux.py
fi
if [[ "$STAGE" != native ]]; then
  run_case regression 7200 "$IMAGE" \
    uv run --offline --no-sync pytest -q -p no:cacheprovider \
    --basetemp=/scratch/regression --junitxml=/output/regression.xml
fi
