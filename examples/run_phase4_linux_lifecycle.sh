#!/usr/bin/env bash
# Parent/operator starts this only after performance measurements finish.
# Only the three authorized source directories enter the container.
set -euo pipefail
if [[ $# -lt 3 ]]; then
  echo "usage: $0 EXPORTED_SOURCE_ROOT FRESH_RESULTS_DIR SUPPLIED_REVISION [PYTHON_ARGS...]" >&2
  exit 2
fi
if [[ $(id -u) == 0 ]]; then
  echo 'Run as the authorized nonroot host user.' >&2
  exit 2
fi
source_root=$(cd "$1" && pwd -P)
mkdir -p "$2"
results=$(cd "$2" && pwd -P)
scratch="$results/scratch"
revision=$3
shift 3
for directory in src tests examples; do
  [[ -d "$source_root/$directory" ]] || { echo "Missing authorized source directory: $directory" >&2; exit 2; }
done
[[ -z $(ls -A "$results") ]] || { echo 'Results directory must be fresh and empty.' >&2; exit 2; }
mkdir -p "$scratch"
image=ddq-linux:dev-20261014
image_id=$(docker image inspect --format '{{.Id}}' "$image")
command -v timeout >/dev/null || { echo 'GNU timeout is required for the outer deadline.' >&2; exit 2; }
container=ddq-phase4-lifecycle-$$
if docker container inspect "$container" >/dev/null 2>&1; then
  echo 'Refusing to reuse an existing container name.' >&2
  exit 2
fi
cleanup() { docker rm -f "$container" >/dev/null 2>&1 || true; }
trap cleanup EXIT
timeout --signal=TERM --kill-after=15s 1230s docker run --name "$container" --stop-timeout 10 --rm --pull never --cpus 4 --memory 8g --memory-swap 8g \
  --hostname ddq-phase4-lifecycle --network none --user "$(id -u):$(id -g)" \
  --tmpfs /full:rw,size=16m,mode=1777 \
  --mount "type=bind,src=$source_root/src,dst=/work/src,readonly" \
  --mount "type=bind,src=$source_root/tests,dst=/work/tests,readonly" \
  --mount "type=bind,src=$source_root/examples,dst=/work/examples,readonly" \
  --mount "type=bind,src=$results,dst=/results" \
  --mount "type=bind,src=$scratch,dst=/scratch" \
  --workdir /work --entrypoint python -e PYTHONPATH=/work/src -e PYTHONDONTWRITEBYTECODE=1 \
  -e SVSIM_FULL_FS=/full -e DDQ_IMAGE="$image" -e DDQ_IMAGE_ID="$image_id" \
  "$image_id" examples/linux_phase4_lifecycle.py "$@" --output /results \
    --source-revision "$revision"
