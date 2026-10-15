"""Real-process kills at the extent segment write / fsync / catalog-publish stages (P4-009).

Both write paths are covered: the synchronous ``FileBlockStore.write_version`` and the
``AsyncStoreIO`` completion callback. After each ``os._exit`` the offline recovery
(``recover_uncommitted_reservations``) must settle the dead owner's tokens exactly once, the durable cut must be
whole (old, or old plus the one new extent), partial segment bytes stay charged as observed residual, and a
simulator resumed from the last committed snapshot must match the numpy oracle.

``SVSIM_TEST_BACKEND=direct`` runs the same matrix with the ``direct`` storage backend (O_DIRECT on Linux).
"""
import hashlib
import json
import os
import sqlite3
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

from svsim.disk_budget import DiskAdmissionError, DiskBudget
from svsim.storage import FileBlockStore, StorageConfig
from svsim.uncommitted_recovery import UncommittedRecoveryRefused, recover_uncommitted_reservations

ROOT = Path(__file__).resolve().parents[1]
ENV = {**os.environ, 'OMP_NUM_THREADS': '1'}
BACKEND = os.environ.get('SVSIM_TEST_BACKEND', 'buffered')
QUOTA = 64 * 1024**2
SLOT = 4096  # block_bits=2 -> 64 payload bytes, one aligned slot

# stage -> (new extent published in the catalog, new slot bytes already in the segment file,
#           token materialized before the kill). The extent token is released right after the publishing
#           commit, so a kill at the fsync stages (which run after publication) leaves no live token.
STAGES = {
    'before-segment-write': (False, False, False),
    'after-segment-write': (False, True, False),
    'before-catalog-commit': (False, True, True),
    'after-catalog-commit': (True, True, True),
    'before-segment-fsync': (True, True, True),
    'after-segment-fsync': (True, True, True),
}

CHILD = '''
    import mmap, os, sys
    from svsim.disk_budget import DiskBudget
    from svsim.storage import FileBlockStore, StorageConfig
    from svsim.transport import StorageFile
    from svsim.catalog_admission import CatalogConnection
    root, path, stage, backend = sys.argv[1:5]
    os.makedirs(os.path.join(root, 'store'), exist_ok=True)
    budget = DiskBudget(os.path.join(root, 'ledger', 'ledger.sqlite3'), [os.path.join(root, 'store')],
                        quota_bytes=%d, shared_policy=True)
    store = FileBlockStore(StorageConfig(os.path.join(root, 'store'), 2, backend=backend), 4, disk_budget=budget)
    def buffer(byte):
        region = mmap.mmap(-1, store.slot_bytes); region.write(bytes([byte]) * store.slot_bytes); return region
    if path == 'async':
        from svsim.async_io import WorkerIOEngine
        from svsim.async_store_io import AsyncStoreIO
        from svsim.cache import BufferLease, MemoryBudget
        engine = WorkerIOEngine(engine='cooperative')
        adapter = AsyncStoreIO(store, engine)
        lease = BufferLease(MemoryBudget(1 << 20), store.slot_bytes)
        def write(version, byte):
            lease.view[:] = bytes([byte]) * store.slot_bytes
            engine.wait(adapter.submit_write(version, lease))
        flush = adapter.flush
    else:
        def write(version, byte): store.write_version(version, buffer(byte))
        flush = store.flush
    old = store.new_version(); write(old, 0x11); flush()
    new = store.new_version()
    print(old, new, flush=True)
    state = {'armed': stage in ('before-segment-write', 'before-segment-fsync', 'after-segment-fsync')}
    def hook(cls, name, when, trigger):
        original = getattr(cls, name)
        def wrapper(self, *a, **k):
            if when == 'before' and state['armed'] == trigger: os._exit(0)
            result = original(self, *a, **k)
            if when == 'after' and state['armed'] == trigger: os._exit(0)
            return result
        setattr(cls, name, wrapper)
    def arm_after_write(cls):
        original = cls.write_from
        def wrapper(self, *a, **k):
            result = original(self, *a, **k)
            state['armed'] = 'written'
            return result
        cls.write_from = wrapper
    if stage == 'before-segment-write': hook(StorageFile, 'write_from', 'before', True)
    elif stage == 'after-segment-write': hook(StorageFile, 'write_from', 'after', True); state['armed'] = True
    elif stage == 'before-segment-fsync': hook(StorageFile, 'flush', 'before', True)
    elif stage == 'after-segment-fsync': hook(StorageFile, 'flush', 'after', True)
    else:  # catalog commit that publishes the extent: only commits after the segment bytes exist
        arm_after_write(StorageFile)
        hook(CatalogConnection, 'commit', 'before' if stage == 'before-catalog-commit' else 'after', 'written')
    write(new, 0x22)
    flush()
    print('completed', flush=True)
''' % QUOTA


def _run(script, *args, timeout=120):
    return subprocess.run([sys.executable, '-c', textwrap.dedent(script), *map(str, args)], cwd=ROOT, env=ENV,
                          capture_output=True, text=True, timeout=timeout)


def _budget(root, **kwargs):
    return DiskBudget(Path(root) / 'ledger/ledger.sqlite3', [Path(root) / 'store'], quota_bytes=QUOTA, shared_policy=True, **kwargs)


def _settle(root, config=None, num_qubits=4):
    """Offline recovery as an operator runs it: dry run, review, apply with the plan digest."""
    ledger = Path(root) / 'ledger/ledger.sqlite3'
    report = recover_uncommitted_reservations(ledger)
    if report['status'] == 'refused' and all('catalog-journal-present' in r for r in report['refusals']):
        # A non-empty rollback journal is refused; an admitted reopen lets SQLite replay it, then retry.
        budget = _budget(root)
        store = FileBlockStore(config or StorageConfig(str(Path(root) / 'store'), 2, backend=BACKEND), num_qubits,
                               create=False, disk_budget=budget)
        store.close(); budget.close()
        report = recover_uncommitted_reservations(ledger)
    assert report['status'] == 'dry-run', report['refusals']
    applied = recover_uncommitted_reservations(ledger, apply=True, confirm_plan=report['plan_sha256'])
    assert applied['status'] == 'applied' and applied['artifacts_preserved'] and applied['pending_bytes_after'] == 0
    assert applied['observed_usage_after_bytes'] == report['observed_usage_bytes']  # nothing freed blindly
    # settled exactly once: the reviewed plan cannot be replayed and nothing is left
    with pytest.raises(UncommittedRecoveryRefused):
        recover_uncommitted_reservations(ledger, apply=True, confirm_plan=report['plan_sha256'])
    assert recover_uncommitted_reservations(ledger)['status'] == 'nothing-to-do'
    return report, applied


@pytest.mark.parametrize('path', ['sync', 'async'])
@pytest.mark.parametrize('stage', list(STAGES))
def test_segment_stage_kill_recovers_whole_cut_and_charges_partial_segment(tmp_path, path, stage):
    published, slot_written, materialized = STAGES[stage]
    token_live = 'fsync' not in stage
    child = _run(CHILD, tmp_path, path, stage, BACKEND)
    assert child.returncode == 0 and 'completed' not in child.stdout, child.stderr
    old, new = child.stdout.split()[:2]
    segment = tmp_path / 'store/state/segment-000000.bin'
    assert segment.stat().st_size == SLOT * (2 if slot_written else 1)
    budget = _budget(tmp_path)
    try:
        live = budget.live_reservations(linked=False)
        extent = [t for t in live if t['operation'] == 'amplitude-extent']
        assert len(extent) == (1 if token_live else 0)
        if token_live:
            assert extent[0]['state'] == 'WRITING'  # the dead owner's slot token is still charged
            assert (extent[0]['materialized_bytes'] == SLOT) == materialized
    finally: budget.close()
    report, applied = _settle(tmp_path)  # other live tokens (catalog, trace...) of the dead owner are settled too
    entry = next((t for t in report['tokens'] if t['operation'] == 'amplitude-extent'), None)
    if token_live:
        assert entry['release_pending_bytes'] == (0 if materialized else SLOT)
        assert applied['observed_usage_after_bytes'] >= segment.stat().st_size  # written slot bytes remain charged
    else:  # published and released before the kill: no extent token is left to settle
        assert entry is None
    assert segment.stat().st_size == SLOT * (2 if slot_written else 1)  # recovery never truncates a segment

    # The durable cut is whole: the old extent always reads back; the new one exists iff it was published.
    budget = _budget(tmp_path)
    store = FileBlockStore(StorageConfig(str(tmp_path / 'store'), 2, backend=BACKEND), 4, create=False, disk_budget=budget)
    try:
        assert store.db.execute('PRAGMA integrity_check').fetchone() == ('ok',)
        import mmap
        region = mmap.mmap(-1, SLOT)
        store.read_version(old, region)
        assert bytes(region) == b'\x11' * SLOT
        if published:
            store.read_version(new, region)
            assert bytes(region) == b'\x22' * SLOT
        else:
            assert store.ref(new).segment is None
            with pytest.raises(OSError):
                store.read_version(new, region)
        # the claimed slot is a hole, never reused; a new extent lands past it and the store works on
        again = store.new_version()
        region.seek(0); region.write(b'\x33' * SLOT)
        store.write_version(again, region)
        assert store.ref(again).offset >= 2 * SLOT
        store.read_version(again, region)
        assert bytes(region) == b'\x33' * SLOT
        region.close()
    finally:
        store.close(); budget.close()


NUMERIC_CHILD = '''
    import os, sys
    import numpy as np
    from svsim.disk_budget import DiskBudget
    from svsim.storage import StorageConfig
    from svsim.stored_runtime import StoredSimulator
    from svsim.transport import StorageFile
    from svsim.catalog_admission import CatalogConnection
    root, stage, backend = sys.argv[1:4]
    os.makedirs(os.path.join(root, 'store'), exist_ok=True)
    budget = DiskBudget(os.path.join(root, 'ledger', 'ledger.sqlite3'), [os.path.join(root, 'store')],
                        quota_bytes=%d, shared_policy=True)
    cfg = StorageConfig(os.path.join(root, 'store'), 3, ram_budget_bytes=512 * 1024, metadata_reserve_bytes=64 * 1024,
                        alignment=4096 if backend == 'direct' else 64, backend=backend)
    sim = StoredSimulator(8, cfg, disk_budget=budget)
    for q in range(8): sim.gate('H', (q,))
    sim.gate('RZ', (7,), angle=0.37); sim.gate('CX', (7, 0))
    sim.materialize()
    expected = sim.state()
    np.save(os.path.join(root, 'expected.npy'), expected)
    snapshot = sim.commit_snapshot()
    print(snapshot.snapshot_id, flush=True)
    state = {'armed': False}
    def hook(cls, name, when, trigger):
        original = getattr(cls, name)
        def wrapper(self, *a, **k):
            if when == 'before' and state['armed'] == trigger: os._exit(0)
            result = original(self, *a, **k)
            if when == 'after' and state['armed'] == trigger: os._exit(0)
            return result
        setattr(cls, name, wrapper)
    original_write = StorageFile.write_from
    def write_from(self, *a, **k):
        if stage == 'before-segment-write' and state['armed']: os._exit(0)
        result = original_write(self, *a, **k)
        if state['armed']:
            if stage == 'after-segment-write': os._exit(0)
            state['armed'] = 'written'
        return result
    StorageFile.write_from = write_from
    if stage in ('before-catalog-commit', 'after-catalog-commit'):
        hook(CatalogConnection, 'commit', stage.split('-')[0], 'written')
    for q in range(8): sim.gate('RX', (q,), angle=0.21 + q)
    state['armed'] = True
    sim.materialize()
    sim.cache.flush()
    sim.store.flush()
    print('completed', flush=True)
''' % QUOTA


@pytest.mark.parametrize('stage', ['before-segment-write', 'after-segment-write', 'before-catalog-commit', 'after-catalog-commit'])
def test_simulator_killed_at_extent_stage_resumes_from_snapshot_matching_oracle(tmp_path, stage):
    np = pytest.importorskip('numpy')
    from svsim.stored_runtime import StoredSimulator
    child = _run(NUMERIC_CHILD, tmp_path, stage, BACKEND)
    assert child.returncode == 0 and 'completed' not in child.stdout, child.stderr
    snapshot_id = child.stdout.split()[0]
    expected = np.load(tmp_path / 'expected.npy')
    cfg = StorageConfig(str(tmp_path / 'store'), 3, ram_budget_bytes=512 * 1024, metadata_reserve_bytes=64 * 1024,
                        alignment=4096 if BACKEND == 'direct' else 64, backend=BACKEND)
    _settle(tmp_path, cfg, 8)
    budget = _budget(tmp_path)
    try:
        sim = StoredSimulator(8, cfg, disk_budget=budget, _snapshot=snapshot_id, _create=False)
        try:
            np.testing.assert_allclose(sim.state(), expected, rtol=0, atol=1e-13)
            sim.commit_snapshot()  # a new durable cut can still be made after recovery
        finally: sim.close()
    finally: budget.close()
