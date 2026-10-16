from contextlib import contextmanager

import pytest

from svsim.disk_budget import DiskBudget
from svsim.telemetry import StreamingExecutionTrace


class Budget:
    def __init__(self, *, reject=False):
        self.reject = reject
        self.active = 0
        self.calls = []
        self.reserves = []
        self.materialized = []
        self.released = []
        self._closed = False
        self._ledger_leases = set()

    def reserve(self, amount, **kwargs):
        self.reserves.append((amount, kwargs))
        if self.reject:
            raise OSError("disk admission rejected")
        self.active += 1
        return f"token-{len(self.reserves)}"

    @contextmanager
    def allocation(self, amount, **kwargs):
        self.calls.append((amount, kwargs))
        if self.reject:
            raise OSError("disk admission rejected")
        self.active += 1
        try:
            yield "token"
        except BaseException:
            if not kwargs.get("retain_on_error", False):
                self.active -= 1
            raise
        else:
            self.active -= 1

    def materialize(self, key, amount):
        self.materialized.append((key, amount))

    def release(self, key):
        self.released.append(key)
        self.active -= 1


class FaultStream:
    def __init__(self, stream, *, write_error=None, short_write=False, flush_error=None):
        self.stream = stream
        self.write_error = write_error
        self.short_write = short_write
        self.flush_error = flush_error

    def fileno(self):
        return self.stream.fileno()

    @property
    def closed(self):
        return self.stream.closed

    def write(self, data):
        if self.write_error:
            self.stream.write(data[:8])
            raise self.write_error
        if self.short_write:
            self.stream.write(data[:8])
            return 8
        return self.stream.write(data)

    def flush(self):
        self.stream.flush()
        if self.flush_error:
            raise self.flush_error

    def close(self):
        return self.stream.close()


def test_budget_rejection_does_not_create_trace_namespace(tmp_path):
    path = tmp_path / "missing" / "trace.jsonl"
    budget = Budget(reject=True)
    with pytest.raises(OSError, match="admission rejected"):
        StreamingExecutionTrace(path, profile_id="p", plan_id="p",
                                disk_budget_getter=lambda: budget)
    assert not path.parent.exists()
    assert budget.active == 0
    assert budget.calls[0][1]["operation"] == "trace-open"


def test_open_failure_releases_admission_token(tmp_path, monkeypatch):
    path = tmp_path / "trace.jsonl"
    budget = Budget()
    original_open = type(path).open

    def fail_open(self, *args, **kwargs):
        if self == path:
            raise OSError("open failed")
        return original_open(self, *args, **kwargs)

    monkeypatch.setattr(type(path), "open", fail_open)
    with pytest.raises(OSError, match="open failed"):
        StreamingExecutionTrace(path, profile_id="p", plan_id="p",
                                disk_budget_getter=lambda: budget)
    assert budget.active == 0
    assert path.parent.exists()
    assert not path.exists()


@pytest.mark.parametrize("fault", ["write_error", "short_write", "flush_error"])
def test_failed_append_rolls_back_and_keeps_the_covering_token_for_reuse(tmp_path, fault):
    path = tmp_path / "trace.jsonl"
    path.write_bytes(b'{"prior":true}\n')
    prior = path.read_bytes()
    budget = Budget()
    trace = StreamingExecutionTrace(path, profile_id="p", plan_id="p",
                                    disk_budget_getter=lambda: budget)
    kwargs = {fault: True} if fault == "short_write" else {fault: OSError(fault)}
    trace._stream = FaultStream(trace._stream, **kwargs)
    with pytest.raises(OSError):
        trace.record("task", bytes=7, metadata={"operation": "write", "transfer_bytes": 7})
    assert path.read_bytes() == prior
    assert trace.event_count == trace.write_count == trace.write_bytes == trace.flush_count == 0
    assert [kw["operation"] for _, kw in budget.reserves] == ["trace-append"]
    assert budget.reserves[0][1]["writing"] is True and budget.released == []
    trace._stream = trace._stream.stream
    trace.record("task", bytes=7, metadata={"operation": "write", "transfer_bytes": 7})
    assert len(budget.reserves) == 1  # rolled-back bytes do not consume the chunk
    trace.close()
    assert budget.released == ["token-1"] and budget.active == 0


def test_many_appends_share_one_chunk_token_and_close_releases_it(tmp_path):
    path = tmp_path / "trace.jsonl"
    path.write_bytes(b'{"older":true}\n')
    budget = Budget()
    trace = StreamingExecutionTrace(path, profile_id="p", plan_id="p",
                                    disk_budget_getter=lambda: budget)
    for _ in range(20):
        trace.record("task", bytes=5, metadata={"operation": "write", "transfer_bytes": 5})
    assert trace.event_count == trace.write_count == 20
    assert [amount for amount, _ in budget.reserves] == [64 * 1024]
    assert budget.materialized == []
    assert path.read_bytes().startswith(b'{"older":true}\n')
    trace.close()
    assert budget.released == ["token-1"] and budget.active == 0


def test_append_rejection_writes_nothing(tmp_path):
    path = tmp_path / "trace.jsonl"
    budget = Budget()
    trace = StreamingExecutionTrace(path, profile_id="p", plan_id="p", disk_budget_getter=lambda: budget)
    budget.reject = True
    with pytest.raises(OSError, match="admission rejected"):
        trace.record("task")
    assert path.read_bytes() == b"" and trace.event_count == 0
    trace.close()


def test_closed_trace_rejects_append_before_disk_admission(tmp_path):
    budget=Budget()
    trace=StreamingExecutionTrace(tmp_path/'trace.jsonl',profile_id='p',plan_id='p',disk_budget_getter=lambda:budget)
    trace.close()
    before=len(budget.calls)
    with pytest.raises(ValueError,match='closed trace'):trace.record('closed')
    assert len(budget.calls)==before


def test_rollback_failure_retains_real_writing_reservation_and_poisons_trace(tmp_path, monkeypatch):
    path = tmp_path / "trace.jsonl"
    path.write_bytes(b'{"prior":true}\n')
    before = path.read_bytes()
    budget = DiskBudget(tmp_path / "ledger.sqlite3", [tmp_path], quota_bytes=1 << 20,
                        usage=lambda: path.stat().st_size if path.exists() else 0,
                        free=lambda: 1 << 40)
    trace = StreamingExecutionTrace(path, profile_id="p", plan_id="p",
                                    disk_budget_getter=lambda: budget)
    trace._stream = FaultStream(trace._stream, short_write=True)
    real_ftruncate = __import__("os").ftruncate

    def fail_truncate(fd, size):
        if fd == trace._stream.fileno():
            raise OSError("truncate failed")
        return real_ftruncate(fd, size)

    monkeypatch.setattr("svsim.telemetry.os.ftruncate", fail_truncate)
    with pytest.raises(OSError, match="short write") as raised:
        trace.record("task", bytes=7, metadata={"operation": "write", "transfer_bytes": 7})
    assert isinstance(raised.value.__cause__, OSError)
    assert str(raised.value.__cause__) == "truncate failed"
    assert path.read_bytes() == before + b'{"trace_'  # injected partial record bytes
    assert trace.event_count == trace.write_count == trace.write_bytes == trace.flush_count == 0
    assert budget.snapshot()["states"]["WRITING"]["count"] == 1
    assert budget.reserved_bytes == 64 * 1024  # partial bytes remain covered by the retained chunk
    with pytest.raises(OSError, match="unusable"):
        trace.record("again")
    with pytest.raises(OSError, match="unusable"):
        list(trace.iter_events())
    assert path.read_bytes() == before + b'{"trace_'
    trace.close()
    budget.close()


def _real_budget(tmp_path, quota=1 << 20, path=None):
    return DiskBudget(tmp_path / "ledger.sqlite3", [tmp_path], quota_bytes=quota,
                      usage=lambda: path.stat().st_size if path and path.exists() else 0,
                      free=lambda: 1 << 40)


def test_real_ledger_commits_once_per_chunk_and_close_leaves_no_live_token(tmp_path):
    path = tmp_path / "trace.jsonl"
    budget = _real_budget(tmp_path, path=path)
    calls = []
    original = budget.reserve
    budget.reserve = lambda amount, **kw: calls.append(amount) or original(amount, **kw)
    trace = StreamingExecutionTrace(path, profile_id="p", plan_id="p", disk_budget_getter=lambda: budget)
    for i in range(400):
        trace.record(f"task-{i}", metadata={"operation": "write", "transfer_bytes": i})
    written = path.stat().st_size
    assert len(calls) == 1 + -(-written // (64 * 1024))  # trace-open probe plus one token per chunk
    assert budget.reserved_bytes <= 64 * 1024
    trace.close()
    assert budget.reserved_bytes == 0 and "WRITING" not in budget.snapshot()["states"]
    budget.close()


def test_crash_without_close_leaves_one_writing_token_for_recovery(tmp_path):
    path = tmp_path / "trace.jsonl"
    budget = _real_budget(tmp_path, path=path)
    trace = StreamingExecutionTrace(path, profile_id="p", plan_id="p", disk_budget_getter=lambda: budget)
    trace.record("task")
    trace._stream.close()  # simulate the owner vanishing: no trace.close(), no budget.close()
    budget.db.close()
    reopened = _real_budget(tmp_path, path=path)
    states = reopened.snapshot()["states"]
    assert states["WRITING"]["count"] == 1 and states["WRITING"]["bytes"] == 64 * 1024
    reopened.close()


def test_near_quota_falls_back_to_exact_record_and_rejects_before_writing(tmp_path):
    path = tmp_path / "trace.jsonl"
    budget = _real_budget(tmp_path, quota=4096, path=path)
    trace = StreamingExecutionTrace(path, profile_id="p", plan_id="p", disk_budget_getter=lambda: budget)
    trace.record("small")
    live = budget.snapshot()["states"]["WRITING"]
    assert live["count"] == 1 and live["bytes"] < 64 * 1024
    with pytest.raises(OSError):
        trace.record("x" * 8192)
    assert path.read_bytes().count(b"\n") == 1
    trace.close()
    budget.close()


def test_budget_close_releases_open_append_tokens(tmp_path):
    path = tmp_path / "trace.jsonl"
    budget = _real_budget(tmp_path, path=path)
    trace = StreamingExecutionTrace(path, profile_id="p", plan_id="p", disk_budget_getter=lambda: budget)
    trace.record("task")
    assert budget.reserved_bytes > 0
    budget.close()
    reopened = _real_budget(tmp_path, path=path)
    assert reopened.reserved_bytes == 0
    trace.close()  # releasing against a closed budget is a no-op
    reopened.close()


def test_journal_appends_share_chunk_tokens_and_release_on_close(tmp_path):
    from svsim.journal import Journal
    budget = _real_budget(tmp_path)
    calls = []
    original = budget.reserve
    budget.reserve = lambda amount, **kw: calls.append(kw["operation"]) or original(amount, **kw)
    journal = Journal(tmp_path / "journal", disk_budget_getter=lambda: budget)
    for i in range(300):
        journal.append("gates", {"value": i})
        journal.append("commits", {"value": i})
    size = sum(p.stat().st_size for p in (tmp_path / "journal").glob("*.jsonl"))
    assert calls == ["journal-open"] + ["journal-append"] * -(-size // (64 * 1024))
    assert budget.lease_reserved_bytes == budget.reserved_bytes > 0
    journal.close()
    assert budget.reserved_bytes == 0
    budget.close()


def test_real_process_exit_without_close_leaves_settleable_lease_tokens(tmp_path):
    import subprocess, sys, textwrap
    child = textwrap.dedent(f"""
        import os
        from pathlib import Path
        from svsim.catalog_admission import CatalogConnection
        from svsim.disk_budget import DiskBudget
        from svsim.journal import Journal
        from svsim.telemetry import StreamingExecutionTrace
        root = Path({str(tmp_path)!r})
        budget = DiskBudget(root / 'ledger.sqlite3', [root], quota_bytes=64 * 1024**2)
        trace = StreamingExecutionTrace(root / 'trace.jsonl', profile_id='p', plan_id='p', disk_budget_getter=lambda: budget)
        journal = Journal(root / 'journal', disk_budget_getter=lambda: budget)
        db = CatalogConnection(root / 'catalog.sqlite', budget_getter=lambda: budget)
        db.execute('CREATE TABLE t(v)')
        for i in range(50):
            trace.record(f't{{i}}'); journal.append('gates', {{'i': i}})
            with db: db.execute('INSERT INTO t VALUES(?)', (i,))
        os._exit(0)  # owner vanishes: no close of writers or budget
    """)
    subprocess.run([sys.executable, '-c', child], check=True, timeout=120)
    budget = DiskBudget(tmp_path / 'ledger.sqlite3', [tmp_path], quota_bytes=64 * 1024**2)
    rows = budget.db.execute("SELECT id,operation,bytes FROM reservations WHERE state='WRITING'").fetchall()
    assert sorted(op for _, op, _ in rows) == ['catalog-transaction', 'journal-append', 'trace-append']
    assert budget.lease_reserved_bytes == budget.reserved_bytes > 0
    for key, _, size in rows:  # explicit recovery settles each token once
        budget.reconcile(key, materialized_bytes=0, evidence='owner exited; files inventoried')
        with pytest.raises(OSError, match='already terminal'):
            budget.reconcile(key, materialized_bytes=0, evidence='repeat')
    assert budget.reserved_bytes == 0
    budget.close()
