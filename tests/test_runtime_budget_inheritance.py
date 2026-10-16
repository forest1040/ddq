"""Prefix execution keeps the caller's supplied admission authority."""
import numpy as np
import pytest

from svsim.disk_budget import DiskAdmissionError,DiskBudget
from svsim.parameters import ParameterSymbol
from svsim.storage import StorageConfig
from svsim.stored_runtime import StoredSimulator
from svsim.volume_coordinator import VolumeCoordinator


def test_stored_prefix_fork_keeps_supplied_shared_ledger_coordinator_and_capacity(tmp_path):
    quota=64*1024**2
    coordinator=VolumeCoordinator(tmp_path/'volume.sqlite3',quota_bytes=quota)
    budget=DiskBudget(tmp_path/'admission/ledger.sqlite3',[tmp_path],shared_policy=True,
                      quota_bytes=quota,volume_coordinator=coordinator)
    cfg=StorageConfig(str(tmp_path/'store'),2,ram_budget_bytes=512*1024,alignment=64)
    sim=StoredSimulator(4,cfg,disk_budget=budget,frame_policy='materialize')
    child=None
    try:
        theta=ParameterSymbol('theta','supplied-prefix-theta')
        sim.gate('RY',(0,),angle=theta)
        binding=sim.bind({theta:.37})
        prefix=sim.commit_prefix([theta],binding=binding)
        child=sim.fork(prefix,binding=binding)
        assert child.store.disk_budget is budget and child.store.disk_budget._coord is coordinator
        assert not (tmp_path/'store/metadata/disk-ledger.sqlite3').exists()
        expected=np.zeros(16,dtype=np.complex128);expected[0]=np.cos(.37/2);expected[1]=np.sin(.37/2)
        np.testing.assert_allclose(child.state(),expected,atol=1e-13)
        key=budget.reserve(48*1024**2,operation='trace-append',writing=True)
        try:
            before=budget.reserved_bytes
            with pytest.raises(DiskAdmissionError,match='quota'):
                child.store.disk_budget.reserve(24*1024**2,operation='amplitude-extent',writing=True)
            assert budget.reserved_bytes==before
        finally:budget.release(key)
        child.close();child=None
        assert not budget._closed
        np.testing.assert_allclose(sim.state(binding=binding),expected,atol=1e-13)
    finally:
        if child is not None:child.close()
        sim.close();budget.close();coordinator.close()


@pytest.mark.parametrize('coordinated',[False,True])
def test_private_prefix_child_owns_its_ledger_connection_after_parent_closes(tmp_path,coordinated):
    kwargs=({'volume_coordinator':str(tmp_path/'volume.sqlite3'),'disk_quota_bytes':64*1024**2}
            if coordinated else {})
    cfg=StorageConfig(str(tmp_path/'store'),2,ram_budget_bytes=512*1024,alignment=64,**kwargs)
    parent=StoredSimulator(4,cfg,frame_policy='materialize');child=None
    try:
        theta=ParameterSymbol('theta','private-lifetime-theta')
        parent.gate('RY',(0,),angle=theta)
        binding=parent.bind({theta:.37});prefix=parent.commit_prefix([theta],binding=binding)
        child=parent.fork(prefix,binding=binding)
        assert child.store.disk_budget is not parent.store.disk_budget
        assert child.store.disk_budget._ledger_path==parent.store.disk_budget._ledger_path
        parent.close()
        assert not child.store.disk_budget._closed
        expected=np.zeros(16,dtype=np.complex128);expected[0]=np.cos(.37/2);expected[1]=np.sin(.37/2)
        np.testing.assert_allclose(child.state(),expected,atol=1e-13)
        child.commit_snapshot()
    finally:
        if child is not None:child.close()
        parent.close()


@pytest.mark.parametrize('writer',['ram-flush','tile'])
def test_ram_flush_and_tile_use_the_supplied_volume_ledger(tmp_path,monkeypatch,writer):
    from svsim.block_store import MemoryBlockStore
    from svsim.cache import BufferLease,MemoryBudget
    from svsim.storage import FileBlockStore
    from svsim.tile_extent_writer import TileExtentWriter
    coordinator=VolumeCoordinator(tmp_path/'volume.sqlite3',quota_bytes=64*1024**2)
    budget=DiskBudget(tmp_path/'admission/ledger.sqlite3',[tmp_path],shared_policy=True,
        quota_bytes=64*1024**2,volume_coordinator=coordinator)
    cfg=StorageConfig(str(tmp_path/'store'),2,alignment=64)
    ram=MemoryBudget(1024**2)
    store=(MemoryBlockStore(cfg,4,ram,disk_budget=budget) if writer=='ram-flush' else
        FileBlockStore(cfg,4,disk_budget=budget))
    lease=BufferLease(ram,cfg.slot_bytes,cfg.alignment)
    try:
        lease.array()[:]=np.arange(4)+1j
        expected=lease.array().copy();version=store.new_version();admitted=[]
        original=budget.reserve
        def reserve(amount,**kwargs):
            admitted.append((amount,kwargs.get('operation')))
            return original(amount,**kwargs)
        monkeypatch.setattr(budget,'reserve',reserve)
        if writer=='ram-flush':
            store.write_version(version,lease.view)
            assert store.resident_bytes>0 and not list((store.directory/'state').iterdir())
            store.flush_to_file();assert store.durable
            operation='amplitude-extent'
        else:
            with TileExtentWriter(store,version,tile_bytes=64) as tile:
                tile.write(0,lease.view)
                tile.seal(lease.view)
            operation='tile-amplitude-extent'
        assert (cfg.slot_bytes,operation) in admitted
        assert store.disk_budget is budget and budget._coord is coordinator
        lease.array()[:]=0;store.read_version(version,lease.view)
        np.testing.assert_array_equal(lease.array(),expected)
        assert coordinator.claim(budget._ledger_path)['pending']==budget.reserved_bytes
    finally:lease.close();store.close();budget.close();coordinator.close()
