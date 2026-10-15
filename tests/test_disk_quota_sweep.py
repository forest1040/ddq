"""Quota boundary sweep: refusal precedes mutation at byte granularity; mid-write faults stay safe."""
import hashlib
import json
import os
import sqlite3
from pathlib import Path

import pytest

from svsim.catalog_admission import CatalogConnection
from svsim.disk_budget import DiskAdmissionError, DiskBudget
from svsim.distributed_checkpoint import atomic_json
from svsim.journal import Journal
from svsim.storage import FileBlockStore, StorageConfig
from svsim.telemetry import StreamingExecutionTrace

HIGH = 64 * 1024**2
BACKEND = os.environ.get('SVSIM_TEST_BACKEND', 'buffered')  # 'direct' runs the file store in O_DIRECT mode (Linux)
WINDOW = 40  # bytes on each side of the boundary, swept one byte at a time


def _files(root, budget):
    """Managed namespace data bytes, excluding the ledger, lease/lock files and empty inodes.

    A refused trace/journal record may follow an admitted 1-byte open probe, which leaves a
    zero-length file but never a data byte.
    """
    skip = {str(budget._ledger_path), str(budget._ledger_path) + '-journal'}
    out = {}
    for p in sorted(Path(root).rglob('*')):
        if p.is_file() and p.stat().st_size and str(p) not in skip and not p.name.endswith('.lock') and 'lease' not in p.name:
            out[str(p.relative_to(root))] = hashlib.sha256(p.read_bytes()).hexdigest()
    return out


class Scenario:
    """A fresh namespace with a quiescent durable cut and one pending write ``op``."""
    def __init__(self, tmp, kind):
        self.root = Path(tmp)
        self.kind = kind
        (self.root / 'data').mkdir()
        self.budget = DiskBudget(self.root / 'ledger.sqlite', [self.root / 'data'], quota_bytes=HIGH)
        self.closers = []
        getattr(self, '_setup_' + kind)()
        self.baseline = self.budget.reserved_bytes

    def _setup_catalog(self):
        db = CatalogConnection(self.root / 'data/catalog.sqlite', budget_getter=lambda: self.budget, require_native=True)
        db.execute('CREATE TABLE items(value BLOB)')
        with db: db.execute('INSERT INTO items VALUES(?)', (b'old',))
        db.close()
        self.durable = lambda: sqlite3.connect(self.root / 'data/catalog.sqlite').execute('SELECT value FROM items').fetchall()
        self.committed = [(b'old',)]

    def _op_catalog(self):
        db = CatalogConnection(self.root / 'data/catalog.sqlite', budget_getter=lambda: self.budget, require_native=True)
        try:
            with db: db.execute('INSERT INTO items VALUES(?)', (b'n' * 6000,))
        finally: db.close()
        self.committed = self.committed + [(b'n' * 6000,)]

    def _setup_trace(self):
        self.durable = lambda: None
        self.committed = None

    def _op_trace(self):
        t = StreamingExecutionTrace(self.root / 'data/trace.jsonl', profile_id='p', plan_id='p', disk_budget_getter=lambda: self.budget)
        try: t.record('new', metadata={'pad': 'x' * 300})
        finally: t.close()

    def _setup_journal(self):
        self.durable = lambda: None
        self.committed = None

    def _op_journal(self):
        j = Journal(self.root / 'data/journal', disk_budget_getter=lambda: self.budget)
        try: j.append('gates', {'value': 'x' * 300})
        finally: j.close()

    def _setup_sidecar(self):
        atomic_json(self.root / 'data/ckpt.json', {'v': 1}, disk_budget=self.budget)
        self.durable = lambda: json.loads((self.root / 'data/ckpt.json').read_text())
        self.committed = {'v': 1}

    def _op_sidecar(self):
        atomic_json(self.root / 'data/ckpt.json', {'v': 2, 'pad': 'x' * 400}, disk_budget=self.budget)
        self.committed = {'v': 2, 'pad': 'x' * 400}

    def _setup_extent(self):
        config = StorageConfig(str(self.root / 'data/store'), 2, backend=BACKEND)
        self.store = FileBlockStore(config, 4, disk_budget=self.budget)
        self.closers.append(self.store.close)
        self.version = self.store.new_version()
        self.durable = lambda: self.store.ref(self.version).segment
        self.committed = None

    def _op_extent(self):
        self.store.write_version(self.version, __import__('mmap').mmap(-1, self.store.slot_bytes))
        self.committed = 'published'

    def run(self, quota):
        self.budget.quota_bytes = quota
        before = _files(self.root / 'data', self.budget)
        before_durable = self.durable()
        try:
            getattr(self, '_op_' + self.kind)()
        except DiskAdmissionError:
            assert _files(self.root / 'data', self.budget) == before, 'refusal mutated the namespace'
            assert self.durable() == before_durable
            assert self.budget.reserved_bytes == self.baseline
            return False
        return True

    def close(self):
        for c in self.closers: c()
        self.budget.close()


def _try(factory, kind, quota):
    s = Scenario(factory.mktemp(kind), kind)
    try: return s.run(quota)
    finally: s.close()


def _boundary(factory, kind):
    lo, hi = 1, HIGH  # lo refuses, hi admits
    assert not _try(factory, kind, lo) and _try(factory, kind, hi)
    while hi - lo > 1:
        mid = (lo + hi) // 2
        if _try(factory, kind, mid): hi = mid
        else: lo = mid
    return hi


@pytest.mark.parametrize('kind', ['catalog', 'trace', 'journal', 'sidecar', 'extent'])
def test_quota_boundary_is_sharp_and_refusal_precedes_any_mutation(tmp_path_factory, kind):
    edge = _boundary(tmp_path_factory, kind)
    results = {q: _try(tmp_path_factory, kind, q) for q in range(edge - WINDOW, edge + WINDOW + 1)}
    # Monotone and sharp: every byte below the edge refuses, every byte from it admits.
    assert all(results[q] == (q >= edge) for q in results), results
    # Page-granular sweep further out in both directions keeps the same verdict.
    for q in [edge - n * 4096 for n in (1, 2, 8)] + [edge + n * 4096 for n in (1, 2, 8)]:
        if q > 0: assert _try(tmp_path_factory, kind, q) == (q >= edge)


@pytest.mark.parametrize('kind', ['catalog', 'sidecar', 'extent'])
def test_success_at_the_edge_publishes_exactly_the_new_cut(tmp_path_factory, kind):
    edge = _boundary(tmp_path_factory, kind)
    s = Scenario(tmp_path_factory.mktemp(kind + 'ok'), kind)
    try:
        assert s.run(edge)
        if kind == 'extent': assert s.store.ref(s.version).segment is not None
        else: assert s.durable() == s.committed
    finally: s.close()


def test_mid_write_extent_failure_keeps_the_old_cut_and_releases_the_token(tmp_path):
    if BACKEND == 'direct':
        pytest.skip('O_DIRECT transfers whole aligned pages, so a sub-page short write cannot occur')
    (tmp_path / 'store').mkdir()
    budget = DiskBudget(tmp_path / 'ledger.sqlite', [tmp_path / 'store'], quota_bytes=HIGH)
    store = FileBlockStore(StorageConfig(str(tmp_path / 'store'), 2, backend=BACKEND), 4, disk_budget=budget)
    try:
        version = store.new_version()
        base = budget.reserved_bytes
        handle = store._file(0)
        real = handle.write_from
        def short(offset, buffer):
            real(offset, buffer[:len(buffer) // 2]); return len(buffer) // 2
        handle.write_from = short
        with pytest.raises(OSError, match='Incomplete'):
            store.write_version(version, __import__('mmap').mmap(-1, store.slot_bytes))
        handle.write_from = real
        assert store.ref(version).segment is None  # never published
        assert budget.reserved_bytes == base  # token released; bytes are charged by inventory, not pending
        assert store.db.execute('PRAGMA integrity_check').fetchone() == ('ok',)
        store.write_version(version, __import__('mmap').mmap(-1, store.slot_bytes))  # a hole is left, the next extent still works
        assert store.ref(version).segment is not None
    finally: store.close(); budget.close()


@pytest.mark.parametrize('fail', ['file-fsync', 'rename'])
def test_mid_write_sidecar_failure_keeps_the_previous_target(tmp_path, monkeypatch, fail):
    (tmp_path / 'd').mkdir()
    budget = DiskBudget(tmp_path / 'ledger.sqlite', [tmp_path / 'd'], quota_bytes=HIGH)
    target = tmp_path / 'd' / 'ckpt.json'
    atomic_json(target, {'v': 1}, disk_budget=budget)
    base = budget.reserved_bytes
    old = target.read_bytes()
    def boom(*a): raise OSError('injected ' + fail)
    monkeypatch.setattr(os, 'fsync' if fail == 'file-fsync' else 'replace', boom)
    try:
        with pytest.raises(OSError, match='injected'):
            atomic_json(target, {'v': 2}, disk_budget=budget)
        monkeypatch.undo()
        assert target.read_bytes() == old
        assert not list(target.parent.glob('*.tmp'))
        assert budget.reserved_bytes == base
    finally: budget.close()


def test_catalog_native_page_cap_is_a_safe_mid_write_failure_not_a_refusal(tmp_path):
    (tmp_path / 'data').mkdir()
    budget = DiskBudget(tmp_path / 'ledger.sqlite', [tmp_path / 'data'], quota_bytes=HIGH)
    db = CatalogConnection(tmp_path / 'data/catalog.sqlite', budget_getter=lambda: budget, require_native=True)
    try:
        db.execute('CREATE TABLE items(value BLOB)')
        with db: db.execute('INSERT INTO items VALUES(?)', (b'old',))
        before = db.path.read_bytes()
        with pytest.raises(sqlite3.DatabaseError) as info:
            with db: db.execute('INSERT INTO items SELECT zeroblob(2000000)')
        assert not isinstance(info.value, DiskAdmissionError)  # admitted, then stopped by the page cap
        assert db.path.read_bytes() == before and not Path(str(db.path) + '-journal').exists()
        assert db.execute('PRAGMA integrity_check').fetchone() == ('ok',)
    finally: db.close(); budget.close()
