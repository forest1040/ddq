import importlib.util
import os
import shutil
import subprocess
import sys
import pytest


@pytest.mark.parametrize('ranks', [1, 2, 4])
@pytest.mark.skipif(not shutil.which('mpiexec') or importlib.util.find_spec('mpi4py') is None,
                    reason='Local MPI runtime unavailable')
def test_multiple_real_nonblocking_slots_generations_and_peer_validation(ranks):
    script = r'''
from mpi4py import MPI
from svsim.mpi_progress import MPIProgressEngine
from svsim.async_io import BackpressureError
from svsim.cache import BufferLease, MemoryBudget
engine = MPIProgressEngine(MPI.COMM_WORLD, max_requests=2)
dest = engine.size - 1
budget = MemoryBudget(2**24)
send = BufferLease(budget, 2**20) if engine.rank == 0 else None
recv = BufferLease(budget, 2**20) if engine.rank == dest else None
if send is not None: send.view[:] = b'A' * 2**20
r1 = engine.begin_transfer(send, recv, 0, dest, metadata={'task': 1})
s2 = bytearray(b'abcdefgh') if engine.rank == 0 else None
r2buf = bytearray(8) if engine.rank == dest else None
r2 = engine.begin_transfer(s2, r2buf, 0, dest, metadata={'task': 2})
try: engine.begin_transfer(s2, r2buf, 0, dest)
except BackpressureError: pass
else: raise AssertionError('missing backpressure')
# Explicitly acknowledge in reverse order to exercise multiple live slots.
engine.wait(r2); engine.wait(r1)
if engine.rank == dest:
    assert r2buf == b'abcdefgh'
    assert bytes(recv.view) == b'A' * 2**20
r3 = engine.begin_transfer(s2, r2buf, 0, dest)
assert r3.generation == 1
engine.wait(r3)
try:
    engine.begin_transfer(s2, bytearray(7) if engine.rank == dest else None, 0, dest)
except Exception as exc: assert type(exc).__name__ == 'CollectiveError'
else: raise AssertionError('size mismatch not rejected')
assert engine.peak_requests == 2
for lease in (send, recv):
    if lease is not None: lease.close()
assert budget.used == 0
engine.close()
'''
    env = dict(os.environ, PYTHONPATH='src')
    env.setdefault('OMPI_ALLOW_RUN_AS_ROOT', '1')
    env.setdefault('OMPI_ALLOW_RUN_AS_ROOT_CONFIRM', '1')
    subprocess.run([shutil.which('mpiexec'), '-n', str(ranks), sys.executable, '-c', script],
                   env=env, check=True, timeout=45)


@pytest.mark.skipif(importlib.util.find_spec('mpi4py') is None, reason='mpi4py unavailable')
def test_comm_self_duplicate_tag_bound_fallback_and_local_transfer():
    from mpi4py import MPI
    from svsim.mpi_progress import MPIProgressEngine
    engine = MPIProgressEngine(MPI.COMM_SELF, max_requests=2)
    send, recv = bytearray(b'local'), bytearray(5)
    assert engine.transfer(send, recv, 0, 0) == 5
    assert recv == b'local'
    engine.close()
