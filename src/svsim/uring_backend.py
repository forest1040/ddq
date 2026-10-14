"""Optional Linux liburing adapter; native capability is probed only explicitly.

Worker remains the auto baseline. A fatal io_uring_enter submission error cannot
prove which SQEs reached the kernel: such a ring is poisoned and its buffers are
retained. The process must stop; this adapter does not claim kernel-error recovery.
Ordinary negative CQEs are terminal completions and release ownership normally.
"""
import ctypes
import errno
import hashlib
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import threading
import time
from concurrent.futures import CancelledError
from .async_io import IORequest, BackpressureError
from .transport import _buffer_address

# Fatal rings cannot be garbage-collected while kernel ownership is uncertain.
_FATAL_RINGS = []


class NativeUringShim:
    def __init__(self, depth):
        if not sys.platform.startswith('linux'):
            raise NotImplementedError('io_uring requires Linux; macOS uses worker/cooperative I/O')
        compiler = shutil.which('cc')
        if compiler is None: raise NotImplementedError('io_uring requires an installed C compiler and liburing headers/library')
        source = Path(__file__).with_name('native')/'uring_backend.c'
        with tempfile.TemporaryDirectory(prefix='svsim-liburing-') as directory:
            library = Path(directory)/'uring_backend.so'
            command = [compiler, '-O2', '-fPIC', '-shared', str(source), '-luring', '-o', str(library)]
            result = subprocess.run(command, capture_output=True, text=True, timeout=30)
            if result.returncode:
                raise NotImplementedError('Cannot build io_uring adapter with installed liburing: '+result.stderr[-2000:])
            self.build_metadata = {
                'compiler': compiler,
                'flags': ['-O2', '-fPIC', '-shared', '-luring'],
                'source_sha256': hashlib.sha256(source.read_bytes()).hexdigest(),
                'binary_sha256': hashlib.sha256(library.read_bytes()).hexdigest(),
            }
            self.lib = ctypes.CDLL(str(library))
        lib = self.lib
        lib.sv_uring_open.argtypes = [ctypes.c_uint, ctypes.POINTER(ctypes.c_int)]
        lib.sv_uring_open.restype = ctypes.c_void_p
        lib.sv_uring_queue.argtypes = [ctypes.c_void_p, ctypes.c_int, ctypes.c_int, ctypes.c_void_p,
                                      ctypes.c_uint, ctypes.c_uint64, ctypes.c_uint64, ctypes.c_uint64]
        lib.sv_uring_queue.restype = ctypes.c_int
        lib.sv_uring_kick.argtypes = [ctypes.c_void_p]; lib.sv_uring_kick.restype = ctypes.c_int
        lib.sv_uring_poll.argtypes = [ctypes.c_void_p, ctypes.POINTER(ctypes.c_uint64), ctypes.POINTER(ctypes.c_int)]
        lib.sv_uring_poll.restype = ctypes.c_int
        lib.sv_uring_close.argtypes = [ctypes.c_void_p]; lib.sv_uring_close.restype = None
        error = ctypes.c_int()
        self.context = lib.sv_uring_open(depth, ctypes.byref(error))
        if not self.context:
            raise NotImplementedError(f'io_uring capability probe failed: {os.strerror(-error.value)} ({error.value})')

    def queue(self, operation, fd, address, length, offset, token, target=0):
        status = self.lib.sv_uring_queue(self.context, operation, fd, address, length, offset, token, target)
        if status == -errno.EAGAIN: raise BackpressureError('Native submission queue is full')
        if status < 0: raise OSError(-status, os.strerror(-status))

    def kick(self):
        status = self.lib.sv_uring_kick(self.context)
        if status == -errno.EAGAIN: return
        if status < 0: raise OSError(-status, os.strerror(-status))

    def poll(self):
        token, result = ctypes.c_uint64(), ctypes.c_int()
        status = self.lib.sv_uring_poll(self.context, ctypes.byref(token), ctypes.byref(result))
        if status < 0: raise OSError(-status, os.strerror(-status))
        return (token.value, result.value) if status else None

    def close(self): self.lib.sv_uring_close(self.context)


class NativeUringEngine:
    def __init__(self, *, max_requests=4, workers=2, engine='io_uring', mpi_thread_level=None,
                 budget=None, request_bytes=512, _shim=None):
        if max_requests < 1 or workers < 1 or request_bytes < 1: raise ValueError('Invalid native I/O limits')
        if _shim is None and not sys.platform.startswith('linux'):
            raise NotImplementedError('io_uring requires Linux; macOS uses worker/cooperative I/O')
        self._thread = threading.get_ident()
        # Native ring needs no workers, so SINGLE is compatible with main poll.
        self.identity, self.max_requests, self.budget = 'io_uring', max_requests, budget
        self.native_overhead_bytes = (2*max_requests+2)*4096
        self.metadata_bytes = max_requests*request_bytes + self.native_overhead_bytes
        if budget is not None: budget.reserve(self.metadata_bytes)
        try: self.shim = _shim if _shim is not None else NativeUringShim(2*max_requests+2)
        except BaseException:
            if budget is not None: budget.release(self.metadata_bytes)
            raise
        self.capacity = max_requests
        self.pending, self.cancels = {}, set()
        self._next_id = 0
        self.closed = False
        self.poisoned = None
        self.submitted = self.completed = self.peak_requests = 0

    def _poison(self, error):
        if self.poisoned is None: _FATAL_RINGS.append(self)
        self.poisoned = error

    def _check(self):
        if self.closed: raise ValueError('Closed native I/O engine')
        if threading.get_ident() != self._thread: raise RuntimeError('Native progress must run on its creating thread')
        if self.poisoned is not None: raise RuntimeError('Native ring is poisoned; outstanding buffers remain pinned') from self.poisoned
        if self.max_requests > self.capacity: raise ValueError('Growing native ring capacity requires restart')

    def _queue(self, request):
        remaining = request._length-request._position
        address = _buffer_address(request._view)+request._position if request._view is not None else 0
        self.shim.queue({'read':0,'write':1,'flush':2}[request.operation], request._handle.fd,
                        address, remaining, request._offset+request._position, request.request_id*2+1)

    def _submit(self, operation, handle, offset=0, buffer=None):
        self._check()
        if len(self.pending)>=self.max_requests: raise BackpressureError('Native I/O queue is full')
        if self._next_id >= (2**63-1): raise OverflowError('Native user_data generation exhausted')
        lease = buffer if hasattr(buffer,'acquire_pin') else None
        view = None
        try:
            if lease is not None: lease.acquire_pin()
            if buffer is not None:
                view = memoryview(lease.view if lease is not None else buffer).cast('B')
                if view.readonly: raise TypeError('Native I/O requires writable caller-owned storage')
                if len(view)>0xffffffff: raise ValueError('Native SQE payload exceeds unsigned 32-bit length')
                handle._check_open(); handle._validate_range(offset,len(view))
                if handle.capabilities.backend=='linux-o-direct':
                    handle._validate_direct_range(offset,len(view))
                    if _buffer_address(view)%handle.alignment: raise ValueError('Unaligned native direct I/O buffer')
            else: handle._check_open()
            if not isinstance(offset,int) or not 0<=offset<2**63: raise ValueError('Invalid native I/O offset')
            request=IORequest(self._next_id,operation,_view=view,_lease=lease,
                submitted_at=time.perf_counter(),queue_seconds=None)
            path=getattr(handle,'path',None)
            try:device=f'device-{os.stat(path).st_dev}' if path is not None else 'unknown'
            except OSError:device='unknown'
            request.storage_metadata={'resolved_backend':getattr(handle,'backend_identity','unknown'),
                'device_id':device,'alignment':getattr(handle,'alignment',None),
                'offset':offset,'transfer_bytes':len(view) if view is not None else 0,
                'submitted_in_flight':len(self.pending)+1,
                'timing_scope':'submission-to-cqe','service_seconds_known':False}
            self._next_id+=1
            request._handle=handle; request._offset=offset; request._position=0
            request._length=len(view) if view is not None else 0
            request._start=request.submitted_at; request._cancelled=False
            self._queue(request)
            self.pending[request.request_id]=request
            self.submitted+=1; self.peak_requests=max(self.peak_requests,len(self.pending))
            return request
        except BaseException:
            if view is not None: view.release()
            if lease is not None: lease.release_pin()
            raise

    def submit_read(self,handle,offset,buffer): return self._submit('read',handle,offset,buffer)
    def submit_write(self,handle,offset,buffer): return self._submit('write',handle,offset,buffer)
    def submit_flush(self,handle): return self._submit('flush',handle)

    def cancel(self,request):
        self._check()
        if request.done or request._cancelled: return False
        if self.pending.get(request.request_id) is not request: raise ValueError('Unknown native request')
        token=request.request_id*2+2
        self.shim.queue(3,-1,0,0,0,token,request.request_id*2+1)
        request._cancelled=True
        self.cancels.add(token)
        return True

    def _finish(self, request, error=None):
        request.error=error; request.done=True
        request.elapsed_seconds=time.perf_counter()-request._start
        request.result=None if request.operation=='flush' else request._position
        try:
            for callback in request._callbacks:
                try: callback(request)
                except BaseException as exc:
                    if request.error is None: request.error=exc
        finally:
            request.wall_seconds=max(0.0,time.perf_counter()-request.submitted_at)
            request._callbacks.clear()
            if request._view is not None: request._view.release(); request._view=None
            if request._lease is not None: request._lease.release_pin(); request._lease=None
            request._handle=None
            del self.pending[request.request_id]
            self.completed+=1
        return request

    def poll(self):
        self._check()
        try: self.shim.kick()
        except BaseException as exc:
            self._poison(exc)
            raise RuntimeError('Native submission failed; kernel ownership uncertain, stop this process') from exc
        complete=[]
        # Bound the poll batch; short completions are resubmitted next tick.
        for _ in range(2*self.capacity+2):
            try: completion=self.shim.poll()
            except BaseException as exc:
                self._poison(exc); raise
            if completion is None: break
            token,result=completion
            if token in self.cancels:
                self.cancels.remove(token)
                continue  # Cancel ACK never releases the original operation.
            request=self.pending.get((token-1)//2)
            if token%2!=1 or request is None:
                self._poison(RuntimeError('Unexpected/stale native CQE token'))
                raise self.poisoned
            error=None
            if result<0:
                error=CancelledError() if result==-errno.ECANCELED else OSError(-result,os.strerror(-result))
            elif request.operation!='flush':
                if result>request._length-request._position: error=OSError('Native CQE byte count exceeds request')
                else:
                    request._position+=result
                    if request._position<request._length:
                        if request._cancelled: error=CancelledError()
                        elif result==0: error=OSError('Incomplete native '+request.operation)
                        elif request._handle.capabilities.backend=='linux-o-direct' and result%request._handle.alignment:
                            error=OSError('Partial native direct I/O breaks alignment')
                        else:
                            try: self._queue(request)
                            except BaseException as exc: error=exc
                            if error is None: continue
            complete.append(self._finish(request,error))
        return complete

    def wait(self,request):
        self._check()
        while not request.done:
            self.poll()
            if not request.done: time.sleep(.0001)
        if request.error is not None: raise request.error
        return request.result

    def drain(self):
        self._check()
        errors=[]
        while self.pending or self.cancels:
            for request in self.poll():
                if request.error is not None: errors.append(request.error)
            if self.pending or self.cancels: time.sleep(.0001)
        if errors: raise errors[0]

    def close(self):
        if self.closed: return
        # Poisoned rings intentionally retain backing storage until process exit.
        self._check()
        try: self.drain()
        finally:
            if not self.pending and not self.cancels and self.poisoned is None:
                self.shim.close()
                if self.budget is not None: self.budget.release(self.metadata_bytes)
                self.closed=True
