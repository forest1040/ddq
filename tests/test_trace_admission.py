from contextlib import contextmanager

import pytest

from svsim.disk_budget import DiskBudget
from svsim.telemetry import StreamingExecutionTrace


class Budget:
    def __init__(self, *, reject=False):
        self.reject = reject
        self.active = 0
        self.calls = []
        self.materialized = []
        self.released = []

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
def test_failed_append_rolls_back_before_releasing_credit_and_keeps_stats(tmp_path, fault):
    path = tmp_path / "trace.jsonl"
    path.write_bytes(b'{"prior":true}\n')
    prior = path.read_bytes()
    budget = Budget()
    trace = StreamingExecutionTrace(path, profile_id="p", plan_id="p",
                                    disk_budget_getter=lambda: budget)
    kwargs = {fault: True} if fault == "short_write" else {fault: OSError(fault)}
    trace._stream = FaultStream(trace._stream, **kwargs)
    original_allocation = budget.allocation

    @contextmanager
    def checking_allocation(amount, **options):
        with original_allocation(amount, **options):
            try:
                yield "token"
            finally:
                if options.get("operation") == "trace-append":
                    assert path.read_bytes() == prior

    budget.allocation = checking_allocation
    with pytest.raises(OSError):
        trace.record("task", bytes=7, metadata={"operation": "write", "transfer_bytes": 7})
    assert path.read_bytes() == prior
    assert trace.event_count == trace.write_count == trace.write_bytes == trace.flush_count == 0
    assert budget.active == 0
    assert budget.released == ["token"]
    if fault == "flush_error":
        trace._stream.flush_error = None
    trace.close()


def test_success_appends_without_truncating_existing_trace_and_counts_after_flush(tmp_path):
    path = tmp_path / "trace.jsonl"
    path.write_bytes(b'{"older":true}\n')
    budget = Budget()
    trace = StreamingExecutionTrace(path, profile_id="p", plan_id="p",
                                    disk_budget_getter=lambda: budget)
    trace.record("task", bytes=5, metadata={"operation": "write", "transfer_bytes": 5})
    assert trace.event_count == trace.write_count == 1
    assert trace.write_bytes == 5
    assert budget.materialized[0][0] == "token"
    assert budget.materialized[0][1] == budget.calls[-1][0]
    assert path.read_bytes().startswith(b'{"older":true}\n')
    trace.close()
    assert budget.active == 0


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
    assert budget.reserved_bytes > 0
    with pytest.raises(OSError, match="unusable"):
        trace.record("again")
    with pytest.raises(OSError, match="unusable"):
        list(trace.iter_events())
    assert path.read_bytes() == before + b'{"trace_'
    trace.close()
    budget.close()
