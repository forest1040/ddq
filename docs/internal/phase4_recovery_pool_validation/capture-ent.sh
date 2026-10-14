#!/usr/bin/env bash
set -euo pipefail
TASK_ROOT=/tmp/ddq-p4-recovery-pool-20261014.yDiiIL
NAME=ddq-p4-recovery-pool-yDiiIL-capture
cleanup() { docker rm -f "$NAME" >/dev/null 2>&1 || true; }
trap cleanup EXIT
timeout --signal=TERM --kill-after=15s 60 docker run --rm -i --name "$NAME" --network none \
 --cpus=4 --memory=8g --pids-limit=512 --shm-size=256m \
 --mount "type=bind,src=$TASK_ROOT/context/src,dst=/workspace/src,readonly" \
 --mount "type=bind,src=$TASK_ROOT/context/tests,dst=/workspace/tests,readonly" \
 --mount "type=bind,src=$TASK_ROOT/scratch,dst=/validation,readonly" \
 --mount "type=bind,src=$TASK_ROOT/artifacts,dst=/output" \
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
modules=['disk_recovery_pool', 'checkpoint_recovery', 'checkpoint_recovery_mpi', 'checkpoint_reservation_ledger', 'disk_reservation_ledger', 'disk_failed_allocation', 'catalog_capability', 'catalog_admission', 'checkpoint_atomic_admission', 'admitted_files', 'distributed_checkpoint', 'disk_coordinator', 'phase4_lifecycle']
import contextlib
with (out/'collection.txt').open('w') as stream,contextlib.redirect_stdout(stream):
    result=main(['--collect-only','-q','-p','no:cacheprovider',*[f'tests/test_{name}.py' for name in modules]])
assert result==0
receipts=[]
for path in sorted(Path('/validation/regression').rglob('*.sqlite3')):
    accepted=('test_dry_run_and_apply','test_missing_copy_and_partial','test_real_crashed_checkpoint','test_collective_checkpoint_rec','test_checkpoint_settlement_can')
    if not any(part.startswith(accepted) for part in path.parts):continue
    con=sqlite3.connect(path.as_uri()+'?mode=ro',uri=True)
    try:
        if not con.execute("SELECT 1 FROM sqlite_master WHERE name='checkpoint_recovery_receipts'").fetchone():continue
        for key,proof,evidence in con.execute('SELECT p.id,p.proof,r.evidence FROM checkpoint_recovery_receipts p JOIN reservations r ON r.id=p.id'):
            document=json.loads(proof)
            if document.get('version')!=1 or not document.get('metadata_only'):continue
            digest=hashlib.sha256(proof.encode()).hexdigest()
            assert evidence.endswith(':'+digest)
            receipts.append({'ledger':str(path),'reservation_id':key,'proof':document,'evidence':evidence})
    finally:con.close()
assert receipts,'No real recovery receipts captured'
(out/'recovery-receipts.json').write_text(json.dumps(receipts,indent=2)+'\n')
print('Captured actual native catalogs and recovery receipts:',len(rows),len(receipts))
PY
