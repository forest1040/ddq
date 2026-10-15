"""Real ENOSPC on a size-limited filesystem (P4-009). Skipped unless SVSIM_FULL_FS names the mount.

The mount must be a small, empty, writable filesystem that no other job uses, for example
``docker run --tmpfs /full:rw,size=8m,mode=1777`` or a small loop-mounted ext4. Nothing here
simulates a quota error: the filler process consumes physical blocks and the kernel returns ENOSPC.
"""
import errno
import hashlib
import json
import os
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import textwrap
from pathlib import Path

import pytest

from svsim.disk_budget import DiskAdmissionError, DiskBudget
from svsim.journal import Journal
from svsim.storage import FileBlockStore, StorageConfig
from svsim.uncommitted_recovery import recover_uncommitted_reservations
from test_disk_quota_sweep import Scenario, _files

FULL = os.environ.get('SVSIM_FULL_FS')
pytestmark = pytest.mark.skipif(not FULL, reason='SVSIM_FULL_FS must name a size-limited mount')

ROOT = Path(__file__).resolve().parents[1]
ENV = {**os.environ, 'OMP_NUM_THREADS': '1'}
QUOTA = 1 << 40  # logical quota never binds; only the physical filesystem does
BACKEND = os.environ.get('SVSIM_TEST_BACKEND', 'buffered')
KINDS = ['catalog', 'trace', 'journal', 'sidecar', 'extent']

FILLER = '''
    import os, sys
    path, leave = sys.argv[1], int(sys.argv[2])
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
    st = os.statvfs(path)
    while True:
        st = os.statvfs(path)
        free = st.f_bavail * st.f_frsize
        if free <= leave: break
        try: os.write(fd, b'\\0' * min(65536, max(4096, free - leave)))
        except OSError as e:
            if e.errno != 28: raise
            break
    os.fsync(fd)
    st = os.statvfs(path)
    print(st.f_bavail * st.f_frsize)
'''


def fill(path, leave=0):
    """External writer: grow ``path`` until at most ``leave`` bytes are free. Returns the free bytes left."""
    out = subprocess.run([sys.executable, '-c', textwrap.dedent(FILLER), str(path), str(leave)], capture_output=True,
                         text=True, check=True, timeout=120)
    return int(out.stdout)


def free_bytes():
    st = os.statvfs(FULL)
    return st.f_bavail * st.f_frsize


@pytest.fixture
def area():
    root = Path(tempfile.mkdtemp(dir=FULL, prefix='case-'))
    filler = Path(FULL) / ('filler-' + root.name)
    try:
        yield root, filler
    finally:
        filler.unlink(missing_ok=True)
        shutil.rmtree(root, ignore_errors=True)


PRELUDE = '''
    import json, os, subprocess, sys
    from svsim.disk_budget import DiskBudget
    from svsim.storage import FileBlockStore, StorageConfig
    root, kind, filler, leave = sys.argv[1], sys.argv[2], sys.argv[3], int(sys.argv[4])
    FILLER = %r
    os.makedirs(os.path.join(root, 'store'), exist_ok=True)
    budget = DiskBudget(os.path.join(root, 'ledger', 'ledger.sqlite3'), [os.path.join(root, 'store')],
                        quota_bytes=%d, shared_policy=True, bootstrap_bytes=1 << 20)
    store = FileBlockStore(StorageConfig(os.path.join(root, 'store'), 2, backend=%r), 4, disk_budget=budget)
    armed = [False]
    original_reserve = budget.reserve
    def reserve(*a, **k):
        key = original_reserve(*a, **k)
        if armed[0] and os.environ.get('FILL_AFTER_ADMIT') == '1':
            armed[0] = False   # an external job consumes the free space after admission, before the write
            subprocess.run([sys.executable, '-c', FILLER, filler, str(leave)], check=True, capture_output=True)
        return key
    budget.reserve = reserve
    from svsim.catalog_admission import CatalogConnection
    original_cover = CatalogConnection._reserve
    def catalog_reserve(self, *a, **k):   # a covering lease may reuse a live token without a new reserve
        result = original_cover(self, *a, **k)
        if armed[0] and os.environ.get('FILL_AFTER_ADMIT') == '1':
            armed[0] = False
            subprocess.run([sys.executable, '-c', FILLER, filler, str(leave)], check=True, capture_output=True)
        return result
    CatalogConnection._reserve = catalog_reserve
''' % (textwrap.dedent(FILLER), QUOTA, BACKEND)

# Each stage first commits a durable base, then arms the filler and attempts one new write.
OPS = {
    'catalog': '''
        with store.db.batch(): store.db.execute("INSERT INTO settings VALUES('old','1')")
        armed[0] = True
        with store.db.batch(): store.db.execute("INSERT INTO settings VALUES('big',?)", ('x' * 9000,))
    ''',
    'trace': '''
        from svsim.telemetry import StreamingExecutionTrace
        trace = StreamingExecutionTrace(os.path.join(root, 'store/traces/run.jsonl'), profile_id='p', plan_id='p',
                                        disk_budget_getter=lambda: budget)
        trace.record('first', metadata={'pad': 'a' * 100})
        trace.close()
        trace = StreamingExecutionTrace(os.path.join(root, 'store/traces/run.jsonl'), profile_id='p', plan_id='p',
                                        disk_budget_getter=lambda: budget)
        armed[0] = True
        trace.record('second', metadata={'pad': 'b' * 20000})
    ''',
    'journal': '''
        from svsim.journal import Journal
        journal = Journal(os.path.join(root, 'store/journal'), disk_budget_getter=lambda: budget)
        journal.append('gates', {'value': 'committed'})
        journal.close()
        journal = Journal(os.path.join(root, 'store/journal'), disk_budget_getter=lambda: budget)
        armed[0] = True
        journal.append('gates', {'value': 'z' * 20000})
    ''',
    'sidecar': '''
        from svsim.distributed_checkpoint import atomic_json
        path = os.path.join(root, 'store/ckpt.json')
        atomic_json(path, {'v': 1}, disk_budget=budget)
        armed[0] = True
        atomic_json(path, {'v': 2, 'pad': 'x' * 20000}, disk_budget=budget)
    ''',
    'extent': '''
        version = store.new_version()
        armed[0] = True
        store.write_version(version, __import__('mmap').mmap(-1, store.slot_bytes))
    ''',
}

# Reports the exception, then dies without any cleanup, exactly like an operator-killed writer.
EPILOGUE = '''
    import errno as _errno
    info = {'raised': None}
    try:
        EXEC
    except BaseException as exc:
        info = {'raised': type(exc).__name__, 'errno': getattr(exc, 'errno', None), 'msg': str(exc)[:200]}
    try: info['live'] = len(budget.live_reservations(linked=False))
    except BaseException as exc: info['live'] = 'unreadable: ' + type(exc).__name__
    print(json.dumps(info), flush=True)
    os._exit(0)
'''


def run_child(root, kind, filler, leave=0, **env):
    body = textwrap.dedent(PRELUDE)
    epilogue = textwrap.dedent(EPILOGUE).replace('EXEC', 'exec(%r)' % textwrap.dedent(OPS[kind]))
    child = subprocess.run([sys.executable, '-c', body + epilogue, str(root), kind, str(filler), str(leave)], cwd=ROOT,
                           env={**ENV, **{k: str(v) for k, v in env.items()}}, capture_output=True, text=True, timeout=180)
    assert child.returncode == 0, child.stderr
    return json.loads(child.stdout.strip().splitlines()[-1])


def namespace_digest(root):
    out = {}
    for p in sorted(Path(root, 'store').rglob('*')):
        if p.is_file():
            out[str(p.relative_to(root))] = hashlib.sha256(p.read_bytes()).hexdigest()
    return out


def is_disk_full(info):
    # tmpfs and ext4 surface a partial write as a short write; SQLite reports SQLITE_FULL as "database or disk is full"
    msg = info['msg'].lower()
    return info['errno'] == errno.ENOSPC or any(w in msg for w in ('full', 'no space', 'short write', 'incomplete'))


def durable_cut(root, kind):
    """What a reader sees after the failed write: only the base cut, never a torn or partial new one."""
    store = Path(root) / 'store'
    if kind == 'catalog':
        db = sqlite3.connect(store / 'metadata/catalog.sqlite3')
        try:
            assert db.execute('PRAGMA integrity_check').fetchone() == ('ok',)
            return sorted(r[0] for r in db.execute("SELECT key FROM settings WHERE key IN ('old','big')"))
        finally: db.close()
    if kind == 'sidecar':
        return json.loads((store / 'ckpt.json').read_text())
    if kind == 'journal':
        return [r['value'] for r in Journal(store / 'journal').replay('gates')]
    if kind == 'trace':
        lines = (store / 'traces/run.jsonl').read_bytes().split(b'\n')
        return [json.loads(line)['task_id'] for line in lines if line.endswith(b'}')]
    return None


BASE_CUT = {'catalog': ['old'], 'sidecar': {'v': 1}, 'journal': ['committed']}


def live_and_pending(root):
    ledger = Path(root) / 'ledger/ledger.sqlite3'
    budget = DiskBudget(ledger, [Path(root) / 'store'], quota_bytes=QUOTA, shared_policy=True, bootstrap_bytes=1 << 20)
    try: return budget.live_reservations(linked=False), budget.reserved_bytes
    finally: budget.close()


@pytest.mark.parametrize('kind', KINDS)
def test_external_writer_eats_space_after_admission_and_the_write_fails_safely(area, kind):
    root, filler = area
    info = run_child(root, kind, filler, FILL_AFTER_ADMIT=1)
    print('ENOSPC', kind, BACKEND, info)
    # the mount really is full now, filled by another process rather than by a simulated quota
    assert free_bytes() < 64 * 1024
    assert info['raised'] is not None, 'a full filesystem must surface as an error, never as silent success'
    assert is_disk_full(info), info
    filler.unlink()  # the operator frees space before recovery
    if kind in BASE_CUT:
        cut = durable_cut(root, kind)
        assert cut == BASE_CUT[kind] or (kind == 'journal' and cut[:1] == ['committed'] and len(cut) <= 1), cut
    if kind == 'trace':
        assert durable_cut(root, kind)[:1] == ['first']
    # recovery after the death of the writer: whatever token survived is settled once, offline
    live, pending = live_and_pending(root)
    ledger = root / 'ledger/ledger.sqlite3'
    before = namespace_digest(root)
    if kind == 'sidecar':
        # A bare atomic_json (no checkpoint intent) is not attributable offline by design: the caller settles it
        # explicitly. Checkpoint-linked sidecars under real ENOSPC are covered by test_checkpoint_stage_kills_mpi.
        report = recover_uncommitted_reservations(ledger)
        assert live == [] or (report['status'] == 'refused' and any('unknown-operation:atomic-json' in r for r in report['refusals']))
        budget = DiskBudget(ledger, [root / 'store'], quota_bytes=QUOTA, shared_policy=True, bootstrap_bytes=1 << 20)
        try:
            for token in budget.live_reservations(linked=False):
                budget.reconcile(token['id'], materialized_bytes=token['materialized_bytes'], evidence='test-enospc;owner-exited')
        finally: budget.close()
        assert namespace_digest(root) == before
    else:
        report = recover_uncommitted_reservations(ledger)
        if live:
            if report['status'] == 'refused' and any('catalog-journal-present' in r for r in report['refusals']):
                # a hot rollback journal is consumed by the next admitted writer, as for any crashed catalog commit
                budget = DiskBudget(ledger, [root / 'store'], quota_bytes=QUOTA, shared_policy=True, bootstrap_bytes=1 << 20)
                store = FileBlockStore(StorageConfig(str(root / 'store'), 2, backend=BACKEND), 4, create=False, disk_budget=budget)
                with store.db: store.db.execute("INSERT OR REPLACE INTO settings VALUES('probe','1')")
                store.close(); budget.close()
                report = recover_uncommitted_reservations(ledger)
                before = namespace_digest(root)
            assert report['status'] == 'dry-run', report['refusals']
            applied = recover_uncommitted_reservations(ledger, apply=True, confirm_plan=report['plan_sha256'])
            assert applied['status'] == 'applied' and applied['strict_capacity_guarantee'] is False
            assert namespace_digest(root) == before  # nothing deleted, truncated or rewritten
        else:
            assert report['status'] == 'nothing-to-do'
    assert live_and_pending(root) == ([], 0)
    # after reconcile the dataset accepts writes again
    if kind in ('catalog', 'extent', 'journal', 'sidecar', 'trace'):
        budget = DiskBudget(ledger, [root / 'store'], quota_bytes=QUOTA, shared_policy=True, bootstrap_bytes=1 << 20)
        try:
            if kind == 'journal':
                j = Journal(root / 'store/journal', disk_budget_getter=lambda: budget)
                j.append('gates', {'value': 'after'}); j.close()
                assert [r['value'] for r in Journal(root / 'store/journal').replay('gates')][-1] == 'after'
            else:
                store = FileBlockStore(StorageConfig(str(root / 'store'), 2, backend=BACKEND), 4, create=False, disk_budget=budget)
                with store.db: store.db.execute("INSERT OR REPLACE INTO settings VALUES('after','1')")
                store.close()
        finally: budget.close()


@pytest.mark.parametrize('mode', ['min_free', 'exhausted'])
@pytest.mark.parametrize('kind', KINDS)
def test_admission_refuses_on_short_physical_free_space_before_any_mutation(area, kind, mode):
    root, filler = area
    # Build the quiescent base on the same filesystem, then let an external job fill it.
    s = Scenario(root, kind)
    try:
        leave = 256 * 1024 if mode == 'min_free' else 4096
        left = fill(filler, leave)
        assert left <= leave
        if mode == 'min_free': s.budget.min_free_bytes = left - 512
        before = _files(root / 'data', s.budget)
        durable_before, reserved_before = s.durable(), s.budget.reserved_bytes
        with pytest.raises(DiskAdmissionError):
            getattr(s, '_op_' + kind)()
        assert _files(root / 'data', s.budget) == before, 'refusal mutated the namespace'
        assert s.durable() == durable_before and s.budget.reserved_bytes == reserved_before
        # once the external job releases the space the very same write is admitted and published
        filler.unlink(); s.budget.min_free_bytes = 0
        getattr(s, '_op_' + kind)()
    finally: s.close()

