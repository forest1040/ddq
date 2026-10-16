#!/usr/bin/env bash
# Operator-only sequential final checkpoint acceptance. Does not submit jobs.
set -euo pipefail
if [[ $# != 3 || $(id -u) == 0 ]]; then
  echo "usage (nonroot): $0 EXPORTED_SOURCE_ROOT FRESH_RESULTS_DIR SUPPLIED_REVISION" >&2
  exit 2
fi
source_root=$(cd "$1" && pwd -P)
mkdir -p "$2"
results=$(cd "$2" && pwd -P)
revision=$3
[[ -n "$revision" && -z $(ls -A "$results") ]] || { echo 'Revision and fresh empty results required.' >&2; exit 2; }
for directory in src tests examples; do
  [[ -d "$source_root/$directory" ]] || { echo "Missing source directory: $directory" >&2; exit 2; }
done
command -v timeout >/dev/null
image=ddq-linux:dev-20261014
image_id=$(docker image inspect --format '{{.Id}}' "$image")
printf '%s\n' "$image_id" > "$results/image-id.txt"
printf '%s\n' "$revision" > "$results/supplied-revision.txt"
container=
cleanup() { if [[ -n "$container" ]]; then docker rm -f "$container" >/dev/null 2>&1 || true; fi; }
trap cleanup EXIT
overall=0
for backend in buffered direct; do
  scratch="$results/scratch-$backend"
  mkdir -p "$scratch"
  container=ddq-phase4-checkpoint-$backend-$$
  if docker container inspect "$container" >/dev/null 2>&1; then
    echo 'Refusing to reuse an existing container name.' >&2; exit 2
  fi
  printf '%s\trunning\n' "$backend" >> "$results/state.tsv"
  rc=0
  timeout --signal=TERM --kill-after=15s 1230s docker run --name "$container" --stop-timeout 10 --rm --pull never \
    --cpus 4 --memory 8g --memory-swap 8g --pids-limit 512 \
    --hostname ddq-phase4-checkpoint --network none --user "$(id -u):$(id -g)" \
    --tmpfs /full:rw,size=96m,mode=1777 \
    --mount "type=bind,src=$source_root/src,dst=/work/src,readonly" \
    --mount "type=bind,src=$source_root/tests,dst=/work/tests,readonly" \
    --mount "type=bind,src=$source_root/examples,dst=/work/examples,readonly" \
    --mount "type=bind,src=$results,dst=/results" \
    --mount "type=bind,src=$scratch,dst=/scratch" \
    --workdir /work --entrypoint /workspace/.venv/bin/python \
    -e PATH=/workspace/.venv/bin:/usr/local/bin:/usr/bin:/bin -e TMPDIR=/scratch \
    -e PYTHONPATH=/work/src -e PYTHONDONTWRITEBYTECODE=1 -e PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 \
    -e OMP_NUM_THREADS=1 -e OPENBLAS_NUM_THREADS=1 -e MKL_NUM_THREADS=1 -e VECLIB_MAXIMUM_THREADS=1 \
    -e SVSIM_FULL_FS=/full -e SVSIM_TEST_BACKEND="$backend" \
    -e DDQ_IMAGE_ID="$image_id" "$image_id" examples/linux_phase4_checkpoint_matrix.py \
      --revision "$revision" --backend "$backend" --deadline-seconds 1200 \
      > "$results/launch-$backend.log" 2>&1 || rc=$?
  cleanup
  container=
  printf '%s\t%s\n' "$backend" "$rc" >> "$results/status.tsv"
  printf '%s\tfinished\t%s\n' "$backend" "$rc" >> "$results/state.tsv"
  if [[ "$rc" != 0 ]]; then overall=1; fi
done
printf '%s\n' "$overall" > "$results/exit-status.txt"
exit "$overall"
