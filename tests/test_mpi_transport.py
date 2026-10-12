"""MPI transport smoke tests; real multi-rank coverage is optional locally."""
import importlib.util
import os
import shutil
import subprocess
import sys

import pytest


def test_import_does_not_import_mpi4py():
    code = "import sys, svsim.mpi_transport; assert 'mpi4py' not in sys.modules"
    env = dict(os.environ, PYTHONPATH="src")
    subprocess.run([sys.executable, "-c", code], check=True, env=env, timeout=10)


def test_metadata_encoding_is_bounded_and_canonical():
    from svsim.mpi_transport import _json_bytes

    assert _json_bytes({"b": 1, "a": 2}, 32) == b'{"a":2,"b":1}'
    with pytest.raises(ValueError, match="exceeds limit"):
        _json_bytes({"payload": "x" * 20}, 10)


@pytest.mark.skipif(not (shutil.which("mpiexec") or shutil.which("mpirun")) or
                    importlib.util.find_spec("mpi4py") is None,
                    reason="MPI runtime and mpi4py are not installed")
def test_two_rank_transfer_reduction_and_handshake(tmp_path):
    launcher = shutil.which("mpiexec") or shutil.which("mpirun")
    script = r'''
import numpy as np
from mpi4py import MPI
from svsim.mpi_transport import MPITransport
c = MPI.COMM_WORLD
t = MPITransport(c, tile_bytes=3)
t.handshake()
observed = []
t.observer = lambda source, destination, count, seconds: observed.append((source, destination, count, seconds))
t.set_delay(1, 0.01)
send = bytearray(b"abcdefgh") if t.rank == 0 else None
recv = bytearray(8) if t.rank == 1 else None
assert t.transfer(send, recv, 0, 1, metadata={"round": 1}) == 8
if t.rank == 1: assert recv == b"abcdefgh"
if t.rank == 0:
    assert len(observed) == 1 and observed[0][:3] == (0, 1, 8)
    assert observed[0][3] >= 0.01
else:
    assert observed == []
t.set_delay(None, 0)
assert t.send_bytes == (8 if t.rank == 0 else 0)
assert t.recv_bytes == (8 if t.rank == 1 else 0)
local_send = bytearray(b"local") if t.rank == 0 else None
local_recv = bytearray(5) if t.rank == 0 else None
t.transfer(local_send, local_recv, 0, 0, metadata={"route": "local"})
if t.rank == 0: assert local_recv == b"local"
assert t.send_bytes == (8 if t.rank == 0 else 0)
assert t.recv_bytes == (8 if t.rank == 1 else 0)
local_shared = bytearray(b"alias") if t.rank == 0 else None
try:
    t.transfer(local_shared, local_shared, 0, 0)
except Exception as exc:
    assert type(exc).__name__ == "CollectiveError"
else:
    raise AssertionError("aliased local buffers did not fail collectively")
assert np.array_equal(t.sum_array(np.array([t.rank + 1], dtype=np.float64)), [3.0])
try:
    t.sum_array(np.ones(1 if t.rank == 0 else 2, dtype=np.float64))
except Exception as exc:
    assert type(exc).__name__ == "CollectiveError"
else:
    raise AssertionError("shape mismatch did not fail collectively")
bad_recv = bytearray(7 if t.rank == 1 else 0) if t.rank in (0, 1) else None
bad_send = bytearray(b"abcdefgh") if t.rank == 0 else None
try:
    t.transfer(bad_send, bad_recv if t.rank == 1 else None, 0, 1)
except Exception as exc:
    assert type(exc).__name__ == "CollectiveError"
else:
    raise AssertionError("bad receive count did not fail collectively")
assert t.max_value(float(t.rank)) == 1.0
assert t.broadcast_json({"ok": True} if t.rank == 0 else None) == {"ok": True}
t.close()
'''
    env = dict(os.environ, PYTHONPATH="src", UV_CACHE_DIR=str(tmp_path / "uv-cache"))
    # Open MPI refuses root execution unless these explicit opt-ins are present.
    env.setdefault("OMPI_ALLOW_RUN_AS_ROOT", "1")
    env.setdefault("OMPI_ALLOW_RUN_AS_ROOT_CONFIRM", "1")
    subprocess.run([launcher, "-n", "2", sys.executable, "-c", script],
                   check=True, env=env, timeout=30, cwd=os.getcwd())
