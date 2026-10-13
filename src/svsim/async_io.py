"""Bounded positional I/O with completion-owned buffers and main-thread callbacks."""
from concurrent.futures import ThreadPoolExecutor, CancelledError
from dataclasses import dataclass, field
import threading
import time
import os


class BackpressureError(RuntimeError):
    """No request credit is available; progress existing requests first."""


@dataclass
class IORequest:
    request_id: int
    operation: str
    future: object = None
    done: bool = False
    result: object = None
    error: BaseException | None = None
    elapsed_seconds: float = 0
    submitted_at: float = 0
    queue_seconds: float = 0
    wall_seconds: float = 0
    validated: bool = False
    storage_metadata: dict = field(default_factory=dict)
    _function: object = None
    _view: object = None
    _lease: object = None
    _callbacks: list = field(default_factory=list)

    def add_done_callback(self, callback):
        if self.done: callback(self)
        else: self._callbacks.append(callback)
        return self

    def mark_validated(self):
        """Mark completion validated after a done callback checks its result."""
        if not self.done:
            raise RuntimeError('I/O validation is only valid after completion callbacks')
        if self.error is not None:
            raise RuntimeError('Failed I/O cannot be marked validated')
        self.validated = True
        return self


class WorkerIOEngine:
    """Workers touch file descriptors only, never MPI, SQLite, or catalog state.

    THREAD_SINGLE callers must select cooperative execution, which executes one
    queued operation per poll on the creating thread. No worker is then created.
    """
    def __new__(cls, *args, **kwargs):
        if kwargs.get('engine') == 'io_uring':
            from .uring_backend import NativeUringEngine
            return NativeUringEngine(*args, **kwargs)
        return super().__new__(cls)

    def __init__(self, *, max_requests=4, workers=2, engine='auto',
                 mpi_thread_level=None, budget=None, request_bytes=512):
        if max_requests < 1 or workers < 1 or request_bytes < 1:
            raise ValueError('Invalid I/O request limits')
        if engine not in {'auto', 'worker', 'cooperative'}:
            raise ValueError('Unknown I/O engine')
        single = mpi_thread_level is not None and mpi_thread_level == 0
        if single and engine == 'worker':
            raise RuntimeError('MPI THREAD_SINGLE requires cooperative I/O')
        self.identity = 'cooperative' if single or engine == 'cooperative' else 'worker'
        self.max_requests, self.budget = max_requests, budget
        self.metadata_bytes = max_requests * request_bytes
        if budget is not None: budget.reserve(self.metadata_bytes)
        try:
            self.pool = ThreadPoolExecutor(max_workers=workers, thread_name_prefix='svsim-io') if self.identity == 'worker' else None
        except BaseException:
            if budget is not None: budget.release(self.metadata_bytes)
            raise
        self.pending, self._next_id = {}, 0
        self._thread = threading.get_ident()
        self.closed = False
        self.submitted = self.completed = self.peak_requests = 0

    def _check(self):
        if self.closed: raise ValueError('Closed I/O engine')
        if threading.get_ident() != self._thread: raise RuntimeError('I/O progress must run on its creating thread')

    def _submit(self, operation, handle, offset=0, buffer=None):
        self._check()
        if len(self.pending) >= self.max_requests: raise BackpressureError('I/O queue is full')
        lease = buffer if hasattr(buffer, 'acquire_pin') else None
        view = None
        try:
            if lease is not None: lease.acquire_pin()
            if buffer is not None:
                view = memoryview(lease.view if lease is not None else buffer).cast('B')
                if operation == 'read' and view.readonly: raise TypeError('Read buffer must be writable')
            request = IORequest(self._next_id, operation, _view=view, _lease=lease,
                                submitted_at=time.perf_counter())
            path=getattr(handle,'path',None)
            try:device=f'device-{os.stat(path).st_dev}' if path is not None else 'unknown'
            except OSError:device='unknown'
            request.storage_metadata={'resolved_backend':getattr(handle,'backend_identity','unknown'),
                'device_id':device,'alignment':getattr(handle,'alignment',None),
                'offset':offset,'transfer_bytes':len(view) if view is not None else 0,
                'submitted_in_flight':len(self.pending)+1}
            self._next_id += 1
            def execute():
                start = time.perf_counter()
                request.queue_seconds = max(0.0, start - request.submitted_at)
                try:
                    if operation == 'flush': return handle.flush()
                    count = (handle.read_into if operation == 'read' else handle.write_from)(offset, view)
                    if count != len(view): raise OSError(f'Incomplete {operation}: {count}/{len(view)} bytes')
                    return count
                finally: request.elapsed_seconds = time.perf_counter() - start
            request._function = execute
            if self.pool is not None: request.future = self.pool.submit(execute)
            self.pending[request.request_id] = request
            self.submitted += 1
            self.peak_requests = max(self.peak_requests, len(self.pending))
            return request
        except BaseException:
            if view is not None: view.release()
            if lease is not None: lease.release_pin()
            raise

    def submit_read(self, handle, offset, buffer): return self._submit('read', handle, offset, buffer)
    def submit_write(self, handle, offset, buffer): return self._submit('write', handle, offset, buffer)
    def submit_flush(self, handle): return self._submit('flush', handle)

    def cancel(self, request):
        self._check()
        if request.done: return False
        if request.request_id not in self.pending: raise ValueError('Unknown request')
        if request.future is not None: return request.future.cancel()
        request.error = CancelledError()
        request._function = None
        return True

    def poll(self):
        self._check()
        completions = []
        for request in list(self.pending.values()):
            if request.future is not None and not request.future.done(): continue
            try:
                if request.future is not None: request.result = request.future.result()
                elif request._function is not None: request.result = request._function()
            except BaseException as exc: request.error = exc
            # Callbacks inspect data and publish metadata before its pin is released.
            request.done = True
            try:
                for callback in request._callbacks:
                    try: callback(request)
                    except BaseException as exc:
                        if request.error is None: request.error = exc
            finally:
                request.wall_seconds = max(0.0, time.perf_counter() - request.submitted_at)
                request._callbacks.clear()
                request._function = None
                if request._view is not None: request._view.release(); request._view = None
                if request._lease is not None: request._lease.release_pin(); request._lease = None
                del self.pending[request.request_id]
                self.completed += 1
            completions.append(request)
            if self.pool is None: break
        return completions

    def wait(self, request):
        self._check()
        while not request.done:
            self.poll()
            if not request.done: time.sleep(0.0001)
        if request.error is not None: raise request.error
        return request.result

    def drain(self):
        self._check()
        errors = []
        while self.pending:
            for request in self.poll():
                if request.error is not None: errors.append(request.error)
            if self.pending: time.sleep(0.0001)
        if errors: raise errors[0]

    def close(self):
        if self.closed: return
        self._check()
        try: self.drain()
        finally:
            if self.pool is not None: self.pool.shutdown(wait=True)
            if self.budget is not None: self.budget.release(self.metadata_bytes)
            self.closed = True
