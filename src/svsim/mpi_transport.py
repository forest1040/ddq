"""Synchronous, bounded MPI transport primitives.

MPI is deliberately imported only when :class:`MPITransport` is constructed.
"""
from __future__ import annotations

import hashlib
import json
import socket
import threading
import time
from typing import Any


class CollectiveError(RuntimeError):
    """An error reported by at least one participant in a collective round."""


def _json_bytes(value: Any, limit: int) -> bytes:
    try:
        raw = json.dumps(value, sort_keys=True, separators=(",", ":"),
                         ensure_ascii=False, allow_nan=False).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise ValueError(f"value is not canonical JSON: {exc}") from exc
    if len(raw) > limit:
        raise ValueError(f"metadata exceeds limit ({len(raw)} > {limit} bytes)")
    return raw


class MPITransport:
    """Collective typed-buffer MPI transport with bounded control messages."""

    def __init__(self, comm, *, tile_bytes: int = 262144,
                 metadata_limit: int = 65536, trace=None, observer=None) -> None:
        if tile_bytes <= 0 or metadata_limit <= 0:
            raise ValueError("tile_bytes and metadata_limit must be positive")
        if threading.current_thread() is not threading.main_thread():
            raise RuntimeError("MPITransport must be created on the main thread")
        from mpi4py import MPI
        self._MPI = MPI
        self._mpi = MPI.Is_initialized()
        if not self._mpi:
            MPI.Init_thread(required=MPI.THREAD_SINGLE)
            self._owns_init = True
        else:
            self._owns_init = False
        self._thread_id = threading.get_ident()
        self.thread_level = MPI.Query_thread()
        if self.thread_level < MPI.THREAD_SINGLE:
            raise RuntimeError("MPI does not provide a usable thread level")
        self.control = comm.Dup()
        self.data = comm.Dup()
        self.rank, self.size = self.control.Get_rank(), self.control.Get_size()
        self.tile_bytes, self.metadata_limit, self.trace = tile_bytes, metadata_limit, trace
        self.observer = observer
        self.observer_error = None
        self.delay_rank, self.delay_seconds = None, 0.0
        self.send_bytes = self.recv_bytes = self.metadata_bytes = 0
        self._closed = False

    def _check(self):
        if self._closed:
            raise ValueError("MPITransport is closed")
        if threading.get_ident() != self._thread_id:
            raise RuntimeError("MPITransport calls must run on its creating thread")

    def vote(self, error=None):
        self._check()
        message = None if error is None else str(error)[:self.metadata_limit]
        if message is not None and len(message.encode("utf-8")) > self.metadata_limit:
            message = message.encode("utf-8")[:self.metadata_limit].decode("utf-8", "ignore")
        reports = self.control.allgather(message)
        failures = [(i, text) for i, text in enumerate(reports) if text]
        if failures:
            raise CollectiveError("collective failure: " + "; ".join(f"rank {i}: {s}" for i, s in failures))
        return True

    def agree(self, value):
        self._check()
        raw = None
        error = None
        try:
            raw = _json_bytes(value, self.metadata_limit)
        except Exception as exc:
            error = str(exc)
        self.vote(error)
        digest = hashlib.sha256(raw).hexdigest()
        hashes = self.control.allgather(digest)
        if any(item != digest for item in hashes):
            raise CollectiveError("collective values differ between ranks")
        self.metadata_bytes += len(raw)
        return value

    def broadcast_json(self, value, root=0):
        self._check()
        if not 0 <= root < self.size:
            raise ValueError("root is outside communicator")
        error = None
        if self.rank == root:
            try:
                raw = _json_bytes(value, self.metadata_limit)
                length = len(raw)
            except Exception as exc:
                raw, length, error = b"", 0, str(exc)
        else:
            raw, length = None, 0
        self.vote(error)
        import numpy as np
        n = np.array([length], dtype=np.int64)
        self.control.Bcast([n, self._MPI.INT64_T], root=root)
        length = int(n[0])
        if length < 0 or length > self.metadata_limit:
            raise CollectiveError("broadcast metadata length exceeds limit")
        if self.rank != root:
            raw = bytearray(length)
        view = memoryview(raw).cast("B")
        for start in range(0, length, self.tile_bytes):
            part = view[start:min(start + self.tile_bytes, length)]
            self.control.Bcast([part, self._MPI.BYTE], root=root)
        try:
            result = json.loads(bytes(view).decode("utf-8"))
        except (UnicodeError, ValueError) as exc:
            raise CollectiveError(f"invalid JSON broadcast: {exc}") from exc
        self.metadata_bytes += length
        return result

    def handshake(self):
        import numpy as np
        from mpi4py import __version__ as mpi4py_version
        info = {"python": __import__("sys").version.split()[0], "numpy": np.__version__,
                "mpi4py": mpi4py_version, "mpi_library": self._MPI.Get_library_version().strip(),
                "host": socket.gethostname(),
                "thread_level": int(self.thread_level), "byteorder": __import__("sys").byteorder,
                "complex128_size": np.dtype(np.complex128).itemsize}
        gathered = self.control.allgather(info)
        keys = ("python", "numpy", "mpi4py", "mpi_library", "byteorder", "complex128_size")
        for key in keys:
            if any(item[key] != gathered[0][key] for item in gathered):
                raise CollectiveError(f"MPI handshake mismatch for {key}")
        self.vote()
        return gathered

    def set_delay(self, rank, seconds):
        """Collectively inject a bounded delay before the next payload round."""
        self._check()
        error = None
        try:
            if rank is not None and (not isinstance(rank, int) or isinstance(rank, bool) or
                                     not 0 <= rank < self.size):
                raise ValueError("delay rank must be a communicator rank")
            seconds = float(seconds)
            if not 0 <= seconds <= 0.1:
                raise ValueError("delay seconds must be between 0 and 0.1")
        except Exception as exc:
            error = str(exc)
            seconds = 0.0
        self.vote(error)
        self.agree({"delay_rank": rank, "delay_seconds": seconds})
        self.delay_rank, self.delay_seconds = rank, seconds

    def transfer(self, send, recv, source, destination, *, metadata=None,
                 performance_class="mpi-state"):
        self._check()
        overall_begin = time.perf_counter()
        import numpy as np
        local_error = None
        try:
            if not (0 <= source < self.size and 0 <= destination < self.size):
                raise ValueError("source/destination outside communicator")
        except Exception as exc:
            local_error = str(exc)
        self.vote(local_error)
        self.agree({"source": source, "destination": destination})
        local_error = None
        sv = rv = None
        try:
            if self.rank == source:
                if send is None: raise ValueError("source rank requires send buffer")
                sv = memoryview(send).cast("B")
                if sv.ndim != 1 or not sv.contiguous: raise ValueError("send buffer must be contiguous")
            elif send is not None:
                raise ValueError("only source may provide send buffer")
            if self.rank == destination:
                if recv is None: raise ValueError("destination rank requires receive buffer")
                rv = memoryview(recv).cast("B")
                if rv.readonly or not rv.contiguous: raise ValueError("receive buffer must be writable and contiguous")
            elif recv is not None:
                raise ValueError("only destination may provide receive buffer")
        except Exception as exc:
            local_error = str(exc)
        self.vote(local_error)
        if source == destination and self.rank == source:
            try:
                send_view = np.frombuffer(sv, dtype=np.uint8)
                recv_view = np.frombuffer(rv, dtype=np.uint8)
                if np.shares_memory(send_view, recv_view):
                    local_error = "local send and receive buffers must be disjoint"
            except Exception as exc:
                local_error = str(exc)
        self.vote(local_error)
        nsend = len(sv) if self.rank == source else -1
        counts = self.control.allgather(nsend)
        count = counts[source]
        local_error = None
        if count < 0 or any(c not in (-1, count) for c in counts):
            local_error = "inconsistent transfer count"
        if self.rank == destination and rv is not None and len(rv) != count:
            local_error = f"receive buffer has {len(rv)} bytes; expected {count}"
        self.vote(local_error)
        proposed = {"source": source, "destination": destination,
                    "metadata": metadata, "performance_class": performance_class}
        self.agree(proposed)
        header = self.broadcast_json({**proposed, "count": count} if self.rank == source else None,
                                     root=source)
        if self.delay_rank is not None:
            if self.rank == self.delay_rank and self.delay_seconds:
                time.sleep(self.delay_seconds)
            # Rank-local delay is a stage-entry delay: every peer waits here.
            self.control.Barrier()
        begin = time.perf_counter()
        if source == destination:
            if self.rank == source and count:
                rv[:] = sv
        else:
            for start in range(0, count, self.tile_bytes):
                stop = min(start + self.tile_bytes, count)
                if self.rank == source:
                    sb = [sv[start:stop], self._MPI.BYTE]
                    rb = [bytearray(), self._MPI.BYTE]
                    self.data.Sendrecv(sb, dest=destination, sendtag=0, recvbuf=rb,
                                       source=destination, recvtag=0)
                elif self.rank == destination:
                    sb = [bytearray(), self._MPI.BYTE]
                    rb = [rv[start:stop], self._MPI.BYTE]
                    self.data.Sendrecv(sb, dest=source, sendtag=0, recvbuf=rb,
                                       source=source, recvtag=0)
        elapsed = (time.perf_counter() - begin) * 1000
        overall_seconds = time.perf_counter() - overall_begin
        transferred = count if source != destination else 0
        if self.rank == source and transferred: self.send_bytes += transferred
        if self.rank == destination and transferred: self.recv_bytes += transferred
        self.metadata_bytes += len(_json_bytes(header, self.metadata_limit))
        if source != destination and self.rank == source and self.observer is not None:
            try:
                self.observer(source, destination, count, overall_seconds)
            except Exception as exc:
                self.observer_error = str(exc)
        if self.trace is not None:
            self.trace.record(f"{source}->{destination}", performance_class=performance_class,
                              transfer_ms=elapsed, bytes=transferred if self.rank == source else 0,
                              metadata={"source": source, "destination": destination,
                                        "peer": (destination if self.rank == source else
                                                 source if self.rank == destination else None),
                                        "send_bytes": transferred if self.rank == source else 0,
                                        "recv_bytes": transferred if self.rank == destination else 0,
                                        "metadata_bytes": len(_json_bytes(header, self.metadata_limit)),
                                        "request_metadata": metadata,
                                        "payload_mpi_elapsed_ms": elapsed,
                                        "overall_ms": overall_seconds * 1000})
        return count

    def send_bytes_to(self, *args, **kwargs):
        return self.transfer(*args, **kwargs)

    def recv_bytes_from(self, *args, **kwargs):
        return self.transfer(*args, **kwargs)

    def sum_array(self, local, out=None):
        self._check()
        import numpy as np
        error = None
        try:
            a = np.ascontiguousarray(local)
            if a.dtype.kind not in "ifc": raise TypeError("sum_array requires numeric dtype")
            mpi_type = self._mpi_dtype(a.dtype)
            if out is None: out = np.empty_like(a)
            if out.shape != a.shape or out.dtype != a.dtype or not out.flags.c_contiguous or not out.flags.writeable:
                raise ValueError("out must be writable contiguous array with matching shape and dtype")
            if not np.isfinite(a).all(): raise ValueError("sum_array input contains non-finite values")
        except Exception as exc:
            error = str(exc)
        self.vote(error)
        self.agree({"shape": list(a.shape), "dtype": a.dtype.str, "size": int(a.size)})
        self.data.Allreduce([a, mpi_type], [out, mpi_type], op=self._MPI.SUM)
        return out

    def _mpi_dtype(self, dtype):
        try: return self._MPI._typedict[dtype.char]
        except KeyError as exc: raise TypeError(f"unsupported MPI reduction dtype {dtype}") from exc

    def max_value(self, value):
        self._check()
        import numpy as np
        error = None
        try:
            local = float(value)
            if not np.isfinite(local): raise ValueError("max_value requires a finite value")
        except Exception as exc:
            error = str(exc)
            local = 0.0
        self.vote(error)
        result = self.control.allreduce(local, op=self._MPI.MAX)
        return float(result)

    def close(self):
        if self._closed: return
        self._check()
        self.data.Free()
        self.control.Free()
        self._closed = True

    def __enter__(self): return self
    def __exit__(self, *exc): self.close()
