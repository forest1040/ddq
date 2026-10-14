"""Deterministic CQE protocol tests; these are not Linux native measurements."""
import ctypes
import errno
from collections import deque
from concurrent.futures import CancelledError
from types import SimpleNamespace
import pytest
from svsim.async_io import WorkerIOEngine, BackpressureError
from svsim.cache import BufferLease, MemoryBudget
from svsim.uring_backend import NativeUringEngine


class Shim:
    def __init__(self): self.queued=[]; self.cq=deque(); self.closed=False
    def queue(self,*args): self.queued.append(args)
    def kick(self): pass
    def poll(self): return self.cq.popleft() if self.cq else None
    def close(self): self.closed=True


class Handle:
    fd=11
    alignment=4096
    capabilities=SimpleNamespace(backend='buffered')
    def _check_open(self): pass
    def _validate_range(self,offset,size):
        if offset<0 or size<0: raise ValueError('Invalid range')
    def _validate_direct_range(self,offset,size):
        if offset%self.alignment or size%self.alignment: raise ValueError('Unaligned range')


def setup():
    shim=Shim();budget=MemoryBudget(256*1024)
    engine=NativeUringEngine(max_requests=1,budget=budget,_shim=shim)
    lease=BufferLease(budget,4096)
    return shim,budget,engine,lease


def test_cancel_ack_cannot_release_original_pin():
    shim,budget,engine,lease=setup()
    request=engine.submit_read(Handle(),0,lease)
    assert engine.cancel(request)
    lease.close()
    shim.cq.append((2,0))
    assert engine.poll()==[]
    assert lease.pins==1 and not lease.closed and not request.done
    shim.cq.append((1,-errno.ECANCELED))
    assert engine.poll()==[request]
    assert isinstance(request.error,CancelledError) and lease.closed
    engine.close();assert shim.closed and budget.used==0


def test_original_terminal_cqe_before_cancel_ack():
    shim,budget,engine,lease=setup()
    request=engine.submit_write(Handle(),0,lease)
    assert engine.cancel(request)
    shim.cq.append((1,4096))
    assert engine.wait(request)==4096
    assert lease.pins==0 and engine.cancels=={2}
    shim.cq.append((2,-errno.ENOENT))
    engine.drain()
    lease.close();engine.close();assert budget.used==0


def test_short_buffered_io_resubmits_remaining_without_bounce_buffer():
    shim,budget,engine,lease=setup()
    request=engine.submit_read(Handle(),32,lease)
    first=shim.queued[-1]
    ctypes.memmove(first[2],b'A'*1024,1024)
    shim.cq.append((1,1024))
    assert engine.poll()==[]
    second=shim.queued[-1]
    assert second[2]==first[2]+1024 and second[3]==3072 and second[4]==1056
    ctypes.memmove(second[2],b'B'*3072,3072)
    shim.cq.append((1,3072))
    assert engine.wait(request)==4096
    assert bytes(lease.view)==b'A'*1024+b'B'*3072
    lease.close();engine.close();assert budget.used==0


@pytest.mark.parametrize('result,exception',[(0,OSError),(-errno.EIO,OSError)])
def test_negative_and_zero_cqe_are_terminal_error_completions(result,exception):
    shim,budget,engine,lease=setup()
    request=engine.submit_write(Handle(),0,lease)
    shim.cq.append((1,result))
    with pytest.raises(exception):engine.wait(request)
    assert lease.pins==0 and not engine.pending
    lease.close();engine.close();assert budget.used==0


def test_partial_direct_cqe_rejects_broken_alignment_and_fsync_requires_cqe():
    shim,budget,engine,lease=setup()
    handle=Handle();handle.capabilities=SimpleNamespace(backend='linux-o-direct')
    request=engine.submit_write(handle,0,lease)
    shim.cq.append((1,1))
    with pytest.raises(OSError,match='alignment'):engine.wait(request)
    request=engine.submit_flush(handle)
    assert not request.done
    shim.cq.append((3,0))
    assert engine.wait(request) is None
    lease.close();engine.close();assert budget.used==0


def test_backpressure_invalid_buffer_and_explicit_mac_rejection(monkeypatch):
    shim,budget,engine,lease=setup()
    request=engine.submit_read(Handle(),0,lease)
    with pytest.raises(BackpressureError):engine.submit_flush(Handle())
    shim.cq.append((1,4096));engine.wait(request)
    with pytest.raises(TypeError):engine.submit_write(Handle(),0,b'not writable')
    assert not engine.pending
    lease.close();engine.close();assert budget.used==0
    monkeypatch.setattr('svsim.uring_backend.sys.platform','darwin')
    with pytest.raises(NotImplementedError,match='Linux'):WorkerIOEngine(engine='io_uring')


def test_fatal_submission_does_not_unpin_uncertain_kernel_operation():
    shim,budget,engine,lease=setup()
    request=engine.submit_read(Handle(),0,lease)
    def fail(): raise OSError(errno.EIO,'injected enter error')
    shim.kick=fail
    with pytest.raises(RuntimeError,match='ownership uncertain'):engine.poll()
    assert lease.pins==1 and not request.done
    with pytest.raises(RuntimeError,match='poisoned'):engine.close()
    assert not shim.closed
    # Test-only cleanup after simulating a proven terminal kernel completion.
    engine.poisoned=None;shim.kick=lambda:None;shim.cq.append((1,-errno.EIO))
    engine.poll();lease.close();engine.close();assert budget.used==0
    from svsim.uring_backend import _FATAL_RINGS
    _FATAL_RINGS.remove(engine)


def test_native_request_metadata_and_whole_wall_include_validator():
    import time
    shim,budget,engine,lease=setup()
    handle=Handle()
    handle.backend_identity='linux-o-direct'
    request=engine.submit_write(handle,0,lease)
    def validate(req):
        assert req._lease.pins==1
        time.sleep(.005)
        req.mark_validated()
    request.add_done_callback(validate)
    shim.cq.append((1,4096))
    engine.wait(request)
    assert request.submitted_at>0 and request.wall_seconds>=request.elapsed_seconds
    assert request.wall_seconds>=.004 and request.queue_seconds is None
    assert request.storage_metadata['resolved_backend']=='linux-o-direct'
    assert request.storage_metadata['transfer_bytes']==4096
    assert request.storage_metadata['service_seconds_known'] is False
    assert request.validated and not lease.pins
    # Exercise the production collector adapter: the native elapsed interval
    # includes queueing and must never become an invented pure-service sample.
    from svsim.runtime_control import RuntimeControl
    from svsim.runtime_measurement import RuntimeMeasurementCollector
    from svsim.lifecycle import BenchmarkCondition
    collector=RuntimeMeasurementCollector()
    control=SimpleNamespace(
        sim=SimpleNamespace(config=SimpleNamespace(backend='direct'),
                            io_engine=engine,size=1,store=SimpleNamespace(slot_bytes=4096),
                            environment=[{'host':'test'}]),
        measurements=collector,profile_adapter=None,
        io_condition=lambda *args:BenchmarkCondition('native-write'),observe=lambda *args:None)
    RuntimeControl.io_completion(control,request)
    row=collector.snapshot()['observations'][0]
    assert row['service_seconds'] is None and row['queue_seconds'] is None
    assert row['protocol_seconds']==request.wall_seconds
    lease.close();engine.close()
    assert budget.used==0
