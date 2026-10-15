"""Real checkpoint metadata writer killed at a selected persistence boundary."""
import os
import sys
from dataclasses import replace
from pathlib import Path

import numpy as np
from mpi4py import MPI
from svsim import DistributedConfig, DistributedSimulator
from svsim.disk_budget import DiskBudget
import svsim.checkpoint_payload as payload

base, stage, point = Path(sys.argv[1]), sys.argv[2], sys.argv[3]
config = DistributedConfig(str(base / 'dataset'), 2, storage_scope='node-local',
    metadata_reserve_bytes=256*1024, ram_budget_bytes_per_rank=2*1024**2,
    disk_quota_bytes=32*1024**2, backend=os.environ.get('SVSIM_TEST_BACKEND','buffered'))
with DistributedSimulator(5, config, comm=MPI.COMM_SELF) as sim:
    sim.gate('H',(0,)); sim.gate('RY',(4,),angle=.31)
    sim.materialize(binding=sim.bind()); sim.commit_snapshot()
    np.save(base / 'old.npy', sim.state(allow_gather=True))
    (base / 'ledger.txt').write_text(str(sim.disk_budget._ledger_path))
    sim.gate('RY',(3,),angle=.77); sim.materialize(binding=sim.bind())
    np.save(base / 'new.npy', sim.state(allow_gather=True))
    original_exchange, original_open = payload._exchange, Path.open
    original_reserve, original_release = DiskBudget.reserve, DiskBudget.release
    original_fsync, original_replace = os.fsync, os.replace
    live = {'armed':False, 'fd':None, 'tokens':set(), 'hit':False}

    def die():
        (base / 'fault.txt').write_text(point)
        os._exit(17)

    def exchange(transport,data,directory,name,**kwargs):
        mine = name.startswith('payload-' if stage == 'metadata-payload' else 'chunk-') and not live['hit']
        live['armed'] = mine
        try: return original_exchange(transport,data,directory,name,**kwargs)
        finally: live['armed'] = False

    def reserve(self,*args,**kwargs):
        key = original_reserve(self,*args,**kwargs)
        if live['armed'] and kwargs.get('operation') == 'checkpoint-' + stage:
            live['tokens'].add(key); live['hit'] = True
            if point == 'reserved': die()
        return key

    def release(self,key):
        if point == 'committed' and key in live['tokens']: return
        return original_release(self,key)

    class Partial:
        def __init__(self,stream): self.stream=stream
        def __getattr__(self,name): return getattr(self.stream,name)
        def write(self,raw):
            self.stream.write(raw[:13]); self.stream.flush(); original_fsync(self.stream.fileno()); die()

    def opened(path,mode='r',*args,**kwargs):
        stream=original_open(path,mode,*args,**kwargs)
        if live['armed'] and mode == 'xb':
            live['fd']=stream.fileno()
            return Partial(stream) if point == 'partial' else stream
        return stream

    def fsync(fd):
        mine=live['armed'] and fd == live['fd']
        if mine and point == 'pre-fsync': die()
        result=original_fsync(fd)
        if mine and point == 'post-fsync': die()
        return result

    def rename(src,dst,*args,**kwargs):
        mine=live['armed'] and str(src).endswith('.tmp')
        if mine and point == 'pre-rename': die()
        result=original_replace(src,dst,*args,**kwargs)
        if mine and point == 'post-rename': die()
        return result

    payload._exchange, Path.open = exchange, opened
    DiskBudget.reserve, DiskBudget.release = reserve, release
    os.fsync, os.replace = fsync, rename
    sim.commit_snapshot()
    assert live['hit'], 'checkpoint did not write the selected metadata family'
    if point == 'committed': die()
    raise AssertionError('fault point was not reached')
