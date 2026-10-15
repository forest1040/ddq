"""Real MPI ranks killed (or starved of disk by an external writer) inside a checkpoint stage, then recovered.

``run BASE STAGE POINT VICTIM FAULT [FILLER]`` commits snapshot 1, applies one more gate, then commits snapshot 2 while
rank VICTIM faults at POINT of the atomic_json of STAGE (prepare | decision | decision-copy).
``recover BASE OUTCOME`` is a separate mpirun: collective reservation recovery, resume, numeric check, a new commit.
"""
import builtins
import os
import subprocess
import sys
from dataclasses import replace
from pathlib import Path

import numpy as np
from mpi4py import MPI

import svsim.distributed_checkpoint as dc
from svsim import DistributedConfig, DistributedSimulator
from svsim.checkpoint_recovery import recover_checkpoint_reservations
from svsim.disk_budget import DiskBudget
from svsim.uncommitted_recovery import recover_uncommitted_reservations

comm = MPI.COMM_WORLD
rank = comm.Get_rank()
mode, base = sys.argv[1], Path(sys.argv[2])
root = base / 'dataset'
CONFIG = dict(storage_scope='node-local', metadata_reserve_bytes=256 * 1024, ram_budget_bytes_per_rank=2 * 1024**2,
              disk_quota_bytes=16 * 1024**2, drain_reserve_bytes=1024**2,
              backend=os.environ.get('SVSIM_TEST_BACKEND', 'buffered'))
FILLER = '''
import os, sys
fd = os.open(sys.argv[1], os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
while True:
    st = os.statvfs(sys.argv[1])
    if st.f_bavail * st.f_frsize == 0: break
    try: os.write(fd, b"\\0" * 65536)
    except OSError as e:
        if e.errno != 28: raise
        break
os.fsync(fd)
'''


def more_gates(sim):
    sim.gate('RY', (4,), angle=.77)
    sim.materialize(binding=sim.bind())


if mode == 'run':
    stage, point, victim, fault = sys.argv[3], sys.argv[4], int(sys.argv[5]), sys.argv[6]
    filler = sys.argv[7] if len(sys.argv) > 7 else None
    config = DistributedConfig(str(root), 2, **CONFIG)
    with DistributedSimulator(6, config, comm=comm) as sim:
        sim.gate('H', (0,)); sim.gate('RY', (5,), angle=.31); sim.materialize(binding=sim.bind())
        sim.commit_snapshot()
        first_state = sim.state(allow_gather=True)
        ledger = str(sim.disk_budget._ledger_path)
        if rank == 0:
            np.save(base / 'expected-old.npy', first_state)
            (base / 'ledger.txt').write_text(ledger)
        more_gates(sim)
        second_state = sim.state(allow_gather=True)
        if rank == 0: np.save(base / 'expected-new.npy', second_state)
        comm.Barrier()
        live = {'on': False}
        original_atomic, original_replace, original_open, original_fsync = dc.atomic_json, os.replace, builtins.open, os.fsync

        def die(where):
            (base / f'fault-{rank}.txt').write_text(where)
            if fault == 'kill': os._exit(17)
            subprocess.run([sys.executable, '-c', FILLER, filler], check=True)  # external writer takes every free block

        def atomic_json(path, data, **kwargs):
            context = kwargs.get('checkpoint_context') or {}
            mine = context.get('stage') == stage and rank == victim and not (base / f'fault-{rank}.txt').exists()
            if mine and point == 'before': die('before')
            live['on'] = mine
            try: return original_atomic(path, data, **kwargs)
            finally: live['on'] = False

        class Partial:
            def __init__(self, stream): self.stream = stream
            def __enter__(self): return self
            def __exit__(self, *args): return self.stream.__exit__(*args)
            def write(self, raw):
                if fault == 'kill':
                    self.stream.write(raw[:13]); self.stream.flush(); original_fsync(self.stream.fileno())
                    die('partial')
                return self.stream.write(raw)
            def __getattr__(self, name): return getattr(self.stream, name)

        def fault_open(path, mode_='r', *args, **kwargs):
            if live['on'] and mode_ == 'xb':
                if point == 'reserved': live['on'] = False; die('reserved')
                stream = original_open(path, mode_, *args, **kwargs)
                return Partial(stream) if point == 'partial' else stream
            return original_open(path, mode_, *args, **kwargs)

        def fault_replace(src, dst, *a, **k):
            if live['on'] and point == 'pre-rename': live['on'] = False; die('pre-rename')
            result = original_replace(src, dst, *a, **k)
            if live['on'] and point == 'post-rename': live['on'] = False; die('post-rename')
            return result

        dc.atomic_json, builtins.open, os.replace = atomic_json, fault_open, fault_replace
        try:
            sim.commit_snapshot()
        except BaseException as exc:
            print(f'commit-error rank={rank} {type(exc).__name__}: {exc}'[:600].replace(chr(10), ' '), flush=True)  # the disk is full: no file
        finally:
            dc.atomic_json, builtins.open, os.replace = original_atomic, original_open, original_replace
        # whichever way the commit ended, this rank dies without cleanup (a failed commit demands recovery)
        os._exit(17)
else:
    outcome = sys.argv[3]
    ledger = (base / 'ledger.txt').read_text()
    config = replace(DistributedConfig(str(root), 2, **CONFIG), disk_admission_ledger=ledger)
    failed = None
    if rank == 0:  # offline, single process: tokens of a snapshot that never reached a global decision
        try:
            plan = recover_uncommitted_reservations(ledger)
            assert plan['status'] in ('dry-run', 'nothing-to-do'), plan['refusals']
            if plan['status'] == 'dry-run':
                applied = recover_uncommitted_reservations(ledger, apply=True, confirm_plan=plan['plan_sha256'])
                assert applied['status'] == 'applied' and applied['artifacts_preserved'], applied
        except BaseException as exc:
            failed = exc
    if comm.bcast(repr(failed) if failed else None, root=0):
        if failed: raise failed
        sys.exit(3)
    report = recover_checkpoint_reservations(config, comm=comm, apply=True)
    comm.Barrier()
    if rank == 0:
        budget = DiskBudget(ledger, [root], quota_bytes=config.disk_quota_bytes, drain_reserve_bytes=config.drain_reserve_bytes,
                            shared_policy=True)
        try:
            assert budget.live_reservations(linked=True) == [], budget.live_reservations(linked=True)
            assert budget.reserved_bytes == 0, budget.reserved_bytes
        finally: budget.close()
    comm.Barrier()
    expected = np.load(base / f'expected-{outcome}.npy')
    with DistributedSimulator.resume(config, None, comm=comm) as restored:
        observed = restored.state(allow_gather=True)
        if rank == 0: np.testing.assert_allclose(observed, expected, atol=1e-12)
        restored.commit_snapshot()  # the recovered dataset can checkpoint again
    if rank == 0: print('stage recovery PASS', outcome, flush=True); print('backend', config.backend, flush=True)
