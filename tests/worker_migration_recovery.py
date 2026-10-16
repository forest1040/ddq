"""Child-only crash hooks for source-bound migration scratch."""
import os
from pathlib import Path
import signal
import sys

from mpi4py import MPI
from svsim import StoredSimulator,StorageConfig,DistributedSimulator,DistributedConfig
from svsim.catalog_admission import CatalogConnection
import svsim.distributed_checkpoint as checkpoint

root=Path(sys.argv[1]);stage=sys.argv[2]
source_config=StorageConfig(str(root/'source'),1,ram_budget_bytes=4*1024**2,metadata_reserve_bytes=256*1024)
with StoredSimulator(3,source_config) as source:
    source.gate('H',(0,));source.gate('RY',(2,),angle=.31);source.materialize()
    snapshot=source.commit_snapshot()
config=DistributedConfig(str(root/'target'),1,ram_budget_bytes_per_rank=4*1024**2,
    metadata_reserve_bytes=256*1024,storage_scope='node-local')
with DistributedSimulator(3,config,comm=MPI.COMM_SELF) as target:
    target.materialize();old=target.commit_snapshot()
    print(target.disk_budget._ledger_path,flush=True)
    reserve=target.disk_budget.reserve
    def reserve_hook(amount,**kwargs):
        key=reserve(amount,**kwargs)
        if kwargs.get('operation')=='migration-open' and stage=='reserved':os.kill(os.getpid(),signal.SIGKILL)
        return key
    target.disk_budget.reserve=reserve_hook
    initialize=CatalogConnection.__init__
    def initialize_hook(self,path,*args,**kwargs):
        initialize(self,path,*args,**kwargs)
        if '.migration.' in str(path) and stage=='clone-open':os.kill(os.getpid(),signal.SIGKILL)
    CatalogConnection.__init__=initialize_hook
    backup=CatalogConnection.backup_from
    def backup_hook(self,source):
        if stage=='partial-copy' and '.migration.' in str(self.path):
            class PartialSource:
                def execute(self,*args,**kwargs):return source.execute(*args,**kwargs)
                def backup(self,destination):
                    def progress(status,remaining,total):
                        if remaining>0:os.kill(os.getpid(),signal.SIGKILL)
                    return source.backup(destination,pages=1,progress=progress)
            return backup(self,PartialSource())
        result=backup(self,source)
        if '.migration.' in str(self.path) and stage=='clone-copy':os.kill(os.getpid(),signal.SIGKILL)
        return result
    CatalogConnection.backup_from=backup_hook
    flush=target.cache.flush
    def flush_hook(*args,**kwargs):
        result=flush(*args,**kwargs)
        if stage=='block' and list(target.store.directory.glob('.migration.*.tmp')):os.kill(os.getpid(),signal.SIGKILL)
        return result
    target.cache.flush=flush_hook
    checkpoint.migrate_stored_snapshot(target,source_config,snapshot)
    raise AssertionError('kill hook did not run')
