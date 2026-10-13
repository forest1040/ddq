"""Main-thread, typed nonblocking MPI with bounded slots and collective credit.

Descriptor submission and acknowledgment must follow the coordinator's order.
Local polling never issues a collective, so completion permutations cannot
change control collective ordering. Slots are reused only after all ranks ack.
"""
from dataclasses import dataclass, field
import threading
import time
from .mpi_transport import MPITransport, CollectiveError
from .async_io import BackpressureError


@dataclass
class MPIRequest:
    slot: int
    generation: int
    count: int
    source: int
    destination: int
    requests: list = field(default_factory=list)
    views: list = field(default_factory=list)
    leases: list = field(default_factory=list)
    done: bool = False
    acknowledged: bool = False
    error: BaseException | None = None
    started: float = 0
    metadata: object = None
    performance_class: str = "mpi-state"


class MPIProgressEngine(MPITransport):
    def __init__(self, comm, *, max_requests=4, progress_callback=None, **kwargs):
        if max_requests < 1: raise ValueError('Invalid MPI request limit')
        super().__init__(comm, **kwargs)
        tag_ub = self.data.Get_attr(self._MPI.TAG_UB)
        if tag_ub is None:
            tag_ub = self._MPI.COMM_WORLD.Get_attr(self._MPI.TAG_UB)
        if tag_ub is None: tag_ub = 32767  # MPI guaranteed minimum tag upper bound
        if max_requests - 1 > tag_ub: self.close(); raise ValueError('MPI slots exceed TAG_UB')
        self.max_requests = max_requests
        self.progress_callback = progress_callback
        self.slots = {}
        self.generations = [0] * max_requests
        self.peak_requests = self.submitted = self.completed = 0

    def begin_transfer(self, send, recv, source, destination, *, metadata=None,
                       performance_class='mpi-state'):
        self._check()
        begin = time.perf_counter()
        error = None
        sv = rv = None
        leases = []
        request = None
        try:
            if not 0 <= source < self.size or not 0 <= destination < self.size:
                raise ValueError('source/destination outside communicator')
            for buffer, participant, receiving in ((send, source, False), (recv, destination, True)):
                if self.rank == participant:
                    if buffer is None: raise ValueError('Participant requires buffer')
                    lease = buffer if hasattr(buffer, 'acquire_pin') else None
                    if lease is not None: lease.acquire_pin(); leases.append(lease)
                    view = memoryview(lease.view if lease is not None else buffer).cast('B')
                    if receiving: rv = view
                    else: sv = view
                    if not view.contiguous or receiving and view.readonly:
                        raise ValueError('Buffer must be contiguous and receive must be writable')
                elif buffer is not None: raise ValueError('Only participant may supply buffer')
            if source == destination and self.rank == source:
                import numpy as np
                if np.shares_memory(np.frombuffer(sv, dtype='u1'), np.frombuffer(rv, dtype='u1')):
                    raise ValueError('Local transfer buffers must be disjoint')
        except Exception as exc: error = str(exc)
        try:
            self.vote(error)
            self.agree({'source': source, 'destination': destination, 'metadata': metadata,
                        'performance_class': performance_class})
            counts = self.control.allgather(len(sv) if self.rank == source else -1)
            count = counts[source]
            self.vote('Receive size differs' if self.rank == destination and len(rv) != count else None)
            free = self.control.allgather([s for s in range(self.max_requests) if s not in self.slots])
            common = set(free[0]).intersection(*free[1:])
            if not common: raise BackpressureError('MPI request slots are full; acknowledge completed requests')
            slot = min(common)
            generation = self.generations[slot]
            self.agree({'slot': slot, 'generation': generation, 'count': count})
            request = MPIRequest(slot, generation, count, source, destination,
                                 views=[v for v in (sv, rv) if v is not None], leases=leases,
                                 started=begin, metadata=metadata, performance_class=performance_class)
            post_error = None
            if source != destination and self.rank == destination:
                try: request.requests.append(self.data.Irecv([rv, self._MPI.BYTE], source=source, tag=slot))
                except Exception as exc: post_error = str(exc)
            self.vote(post_error)
            # Receivers are posted before send credits are granted. This is a
            # fixed admission collective, never a completion-driven collective.
            if self.delay_rank == self.rank and self.delay_seconds:
                time.sleep(self.delay_seconds)
            self.control.Barrier()
            if source == destination:
                if self.rank == source: rv[:] = sv
            post_error = None
            if source != destination and self.rank == source:
                try: request.requests.append(self.data.Isend([sv, self._MPI.BYTE], dest=destination, tag=slot))
                except Exception as exc: post_error = str(exc)
            self.vote(post_error)
            self.slots[slot] = request
            self.submitted += 1
            self.peak_requests = max(self.peak_requests, len(self.slots))
            return request
        except BaseException:
            # Admission/posting errors are exchanged before any participant
            # returns. Cancel and wait posted operations before unpinning.
            # Actual communicator/process failure remains a fatal MPI error;
            # standard MPI does not promise recovery without ULFM.
            if request is not None:
                for item in request.requests: item.Cancel()
                for item in request.requests: item.Wait()
            for view in (sv, rv):
                if view is not None: view.release()
            for lease in reversed(leases): lease.release_pin()
            raise

    def poll(self):
        self._check()
        complete = []
        for request in list(self.slots.values()):
            if request.done: continue
            try:
                done = all([item.Test() for item in request.requests])
            except Exception as exc:
                # Do not release a buffer with a still-live request on failure.
                request.error = exc
                raise
            if not done: continue
            request.done = True
            for view in request.views: view.release()
            request.views.clear()
            for lease in request.leases: lease.release_pin()
            request.leases.clear()
            if request.source != request.destination:
                if self.rank == request.source: self.send_bytes += request.count
                if self.rank == request.destination: self.recv_bytes += request.count
            complete.append(request)
            self.completed += 1
        return complete

    def wait(self, request):
        self._check()
        if request.acknowledged: return request.count
        if self.slots.get(request.slot) is not request or self.generations[request.slot] != request.generation:
            raise ValueError('Stale MPI request generation')
        while not request.done:
            if self.progress_callback is not None: self.progress_callback()
            self.poll()
            if not request.done: time.sleep(0.0001)
        self.agree({'ack_slot': request.slot, 'generation': request.generation})
        self.vote(request.error)
        del self.slots[request.slot]
        self.generations[request.slot] += 1
        request.acknowledged = True
        elapsed = time.perf_counter() - request.started
        remote = request.source != request.destination
        if remote and self.rank == request.source and self.observer is not None:
            try: self.observer(request.source, request.destination, request.count, elapsed)
            except Exception as exc: self.observer_error = str(exc)
        completion_observer = getattr(self, 'completion_observer', None)
        if completion_observer is not None and remote and self.rank == request.source:
            completion_observer(request, elapsed)
        if self.trace is not None:
            self.trace.record(f"{request.source}->{request.destination}",
                performance_class=request.performance_class, transfer_ms=elapsed*1000,
                bytes=request.count if remote and self.rank == request.source else 0,
                metadata={"source": request.source, "destination": request.destination,
                          "request_metadata": request.metadata, "slot": request.slot,
                          "generation": request.generation, "engine": "typed-nonblocking",
                          "send_bytes": request.count if remote and self.rank == request.source else 0,
                          "recv_bytes": request.count if remote and self.rank == request.destination else 0,
                          "overall_ms": elapsed*1000})
        return request.count

    def transfer(self, send, recv, source, destination, **kwargs):
        return self.wait(self.begin_transfer(send, recv, source, destination, **kwargs))

    def drain(self):
        self._check()
        for slot in sorted(self.slots): self.wait(self.slots[slot])

    def close(self):
        if self._closed: return
        if hasattr(self, 'slots'): self.drain()
        super().close()
