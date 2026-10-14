#!/usr/bin/env bash
set -euo pipefail
TASK_ROOT=/tmp/ddq-p4-writers-20261014.jopUVR
NAME=ddq-p4-writers-jopUVR-capture
cleanup() { docker rm -f "$NAME" >/dev/null 2>&1 || true; }
trap cleanup EXIT
timeout --signal=TERM --kill-after=15s 60 docker run --rm -i --name "$NAME" --network none \
 --cpus=4 --memory=8g --pids-limit=512 --shm-size=256m \
 --mount "type=bind,src=$TASK_ROOT/context-accepted/src,dst=/workspace/src,readonly" \
 --mount "type=bind,src=$TASK_ROOT/context-accepted/tests,dst=/workspace/tests,readonly" \
 --mount "type=bind,src=$TASK_ROOT/scratch-accepted,dst=/validation,readonly" \
 --mount "type=bind,src=$TASK_ROOT/artifacts/accepted,dst=/output" \
 ddq-linux:dev-20261014 uv run --offline --no-sync python - <<'PY'
import hashlib,json,sqlite3
from pathlib import Path
from svsim.catalog_admission import get_sqlite_guard
from pytest import main
out=Path('/output')
rows=[]
for path in sorted(Path('/validation/native').glob('test_native_io_uring_phase4_mp*/state-owner-*/rank-*/metadata/catalog.sqlite3')):
    con=sqlite3.connect(path.as_uri()+'?mode=ro',uri=True)
    try:
        row=con.execute("SELECT data FROM settings WHERE key='catalog_vfs_creation_capability'").fetchone()
        assert row
        capability=json.loads(row[0])
        assert capability['source_sha256']==hashlib.sha256(Path('/workspace/src/svsim/native/sqlite_budget_vfs.c').read_bytes()).hexdigest()
        rows.append({'catalog':str(path),'capability':capability})
    finally:con.close()
assert len(rows)==4,len(rows)
(out/'native-catalog-capabilities.json').write_text(json.dumps(rows,indent=2)+'\n')
(out/'capability-probe.json').write_text(json.dumps(get_sqlite_guard().capability(),indent=2)+'\n')
# Each platform has its own parameter expansion, so collect inside Linux too.
modules=['catalog_capability','admitted_files','catalog_admission','disk_reservation_ledger','disk_failed_allocation','trace_admission','checkpoint_atomic_admission','stored_recovery','stored_budget','telemetry','disk_coordinator','runtime_calibration','distributed_checkpoint','phase4_lifecycle']
import contextlib
with (out/'collection.txt').open('w') as stream,contextlib.redirect_stdout(stream):
    result=main(['--collect-only','-q','-p','no:cacheprovider',*[f'tests/test_{name}.py' for name in modules]])
assert result==0
print('Captured actual native catalog capabilities:',len(rows))
PY
