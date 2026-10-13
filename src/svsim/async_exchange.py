"""Partner batch with multiple typed MPI slots and bounded owned buffers."""
from contextlib import contextmanager, ExitStack

from .async_io import BackpressureError


@contextmanager
def fetch_partners(sim, partners, destination, *, root):
    requests = []
    outputs = []
    with ExitStack() as stack:
        try:
            for h in partners:
                view, where = sim.view(h, root=root)
                source = where['storage_rank']
                output = work = None
                error = None
                try:
                    if sim.rank == destination:
                        output = stack.enter_context(sim.cache.outputs(1))[0]
                    if sim.rank == source:
                        work = output if source == destination else stack.enter_context(sim.cache.outputs(1))[0]
                        reader = sim._reader(where['store_id'])
                        if reader is sim.store:
                            with sim.cache.pin(view.version_id) as cached:
                                work.view[:] = cached.view
                        else:
                            reader.read_version(view.version_id, work.view)
                        array = work.array(sim.store.payload_bytes)
                        try:
                            sim.local.phase2.resolve(array, view, h)
                        finally:
                            del array
                except Exception as exc:
                    error = str(exc)
                sim.transport.vote(error)
                if source != destination:
                    if view.version_id == 'zero':
                        if output is not None:
                            import numpy as np
                            np.frombuffer(output.view, dtype='u1').fill(0)
                    else:
                        while True:
                            try:
                                req = sim.transport.begin_transfer(
                                    work.view[:sim.store.payload_bytes] if work else None,
                                    output.view[:sim.store.payload_bytes] if output else None,
                                    source, destination, metadata={'root': root, 'h': h,
                                        'stream': 'partner-batch', 'version': view.version_id,
                                        'binding': sim.binding.binding_id if sim.binding else ''})
                                requests.append(req)
                                break
                            except BackpressureError:
                                sim.transport.wait(requests.pop(0))
                outputs.append(output)
            for request in requests:
                sim.transport.wait(request)
            requests.clear()
            yield outputs
        finally:
            # Collective submission failures are terminal to this group. Every
            # rank drains the already agreed prefix of transfers before exit.
            for request in requests:
                sim.transport.wait(request)
