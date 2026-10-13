"""Node-local storage ownership and durable disk admission."""
import importlib.util
import os
from pathlib import Path
import shutil
import subprocess
import sys
import pytest
from svsim.disk_budget import DiskBudget, DiskAdmissionError
from svsim.storage_placement import StoragePlacement, resolve_owner_root


def test_owner_resolution_never_uses_other_rank_root(tmp_path):
    a=tmp_path/'a'; b=tmp_path/'b'; a.mkdir(); b.mkdir()
    entries=[StoragePlacement.local('store',0,'host',a).to_data(),
             StoragePlacement.local('store',1,'host',b).to_data()]
    assert resolve_owner_root(entries,1,host='host') == b
    with pytest.raises(OSError,match='another host'):
        resolve_owner_root(entries,1,host='foreign')
    entries[1]['volume_id']='missing'
    with pytest.raises(OSError,match='volume unavailable'):
        resolve_owner_root(entries,1,host='host')


def test_disk_reservations_aggregate_ranks_and_preserve_drain(tmp_path):
    a=DiskBudget(tmp_path/'ledger', [tmp_path],quota_bytes=100,min_free_bytes=10,
                 drain_reserve_bytes=20,usage=lambda:0,free=lambda:200)
    b=DiskBudget(tmp_path/'ledger', [tmp_path],quota_bytes=100,min_free_bytes=10,
                 drain_reserve_bytes=20,usage=lambda:0,free=lambda:200)
    key=a.reserve(70)
    assert b.reserved_bytes == 70
    with pytest.raises(DiskAdmissionError): b.reserve(11)
    drain=b.reserve(30,drain=True)
    assert a.reserved_bytes == 100
    a.release(key); b.release(drain)
    with pytest.raises(RuntimeError):
        with a.allocation(20): raise RuntimeError('write fails')
    assert b.reserved_bytes == 0
    a.close(); b.close()


def test_disk_actual_free_checked_each_admission(tmp_path):
    free=[200]
    budget=DiskBudget(tmp_path/'ledger',[tmp_path],min_free_bytes=100,free=lambda:free[0])
    free[0]=109
    with pytest.raises(DiskAdmissionError,match='minimum free'): budget.reserve(10)
    budget.close()


@pytest.mark.parametrize('ranks',(1,2,4))
@pytest.mark.skipif(not shutil.which('mpiexec') or importlib.util.find_spec('mpi4py') is None,
                    reason='Requires local MPI')
def test_node_local_collective_lifecycle(tmp_path,ranks):
    root=Path(__file__).resolve().parents[1]
    env=dict(os.environ)
    env['PYTHONPATH']=str(root/'src')+os.pathsep+env.get('PYTHONPATH','')
    result=subprocess.run([shutil.which('mpiexec'),'-n',str(ranks),sys.executable,
        '-m','mpi4py',str(root/'tests/mpi_phase4_lifecycle_worker.py'),str(tmp_path)],
        env=env,cwd=root,capture_output=True,text=True,timeout=120)
    assert result.returncode == 0, result.stdout+'\n'+result.stderr


def test_ram_snapshot_flush_obeys_disk_admission(tmp_path):
    from svsim.block_store import MemoryBlockStore
    from svsim.cache import MemoryBudget
    from svsim.storage import StorageConfig
    store=MemoryBlockStore(StorageConfig(str(tmp_path/'store'),2),4,MemoryBudget(1024**2))
    version=store.new_version()
    store.write_version(version,memoryview(bytearray(store.slot_bytes)))
    admission=DiskBudget(tmp_path/'ledger',[store.directory],free=lambda:10)
    store.disk_budget=admission
    with pytest.raises(DiskAdmissionError): store.flush_to_file()
    assert version in store.resident
    assert store.ref(version).segment is None
    assert not list((store.directory/'state').glob('*.bin'))
    assert admission.reserved_bytes == 0
    admission._free=lambda:1024**2
    store.flush_to_file()
    assert not store.resident
    assert store.ref(version).segment is not None
    store.close(); admission.close()


def test_node_local_placement_rejects_network_filesystem(tmp_path,monkeypatch):
    monkeypatch.setattr('svsim.storage_placement.filesystem_type',lambda path:'nfs4')
    with pytest.raises(ValueError,match='local filesystem'):
        StoragePlacement.local('store',0,'host',tmp_path)


def test_disk_usage_tolerates_disappearing_sqlite_journal(tmp_path,monkeypatch):
    root=tmp_path/'owner'; root.mkdir()
    stable=root/'state.bin'; stable.write_bytes(b'1234')
    temporary=root/'catalog.sqlite3-journal'; temporary.write_bytes(b'not committed')
    budget=DiskBudget(tmp_path/'ledger',[root],quota_bytes=14,free=lambda:1024**2)
    original=Path.stat
    def vanishing(path,*,follow_symlinks=True):
        if path == temporary:
            path.unlink(missing_ok=True)
        return original(path,follow_symlinks=follow_symlinks)
    monkeypatch.setattr(Path,'stat',vanishing)
    assert budget._observed_usage() == 4
    key=budget.reserve(10)
    assert budget.reserved_bytes == 10
    budget.release(key); budget.close()
