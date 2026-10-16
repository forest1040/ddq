"""Owned store-private ledger bootstrap with an optional persisted volume binding."""
from pathlib import Path
import sqlite3

from .disk_budget import DiskAdmissionError, DiskBudget


def preflight_private_config(config,directory,*,create=True):
    """Check policy, source volume and existing managed bytes without mkdir/open for writing."""
    from .bootstrap_preflight import ancestor,preflight
    directory=Path(directory).resolve();ledger=directory/'metadata/disk-ledger.sqlite3'
    pool=8*1024**2;excluded=[ledger,Path(str(ledger)+'-journal')]
    if not create and not ledger.is_file():
        raise DiskAdmissionError('Store-private ledger is missing; refusing to recreate admission history')
    if ledger.exists():
        bound=DiskBudget.read_volume_binding(ledger)
        requested=None if config.volume_coordinator is None else str(Path(config.volume_coordinator).resolve())
        if bound is not None and requested is None:
            raise DiskAdmissionError('Ledger is bound to a volume coordinator; open it with that coordinator')
        if bound is not None and bound!=requested:
            raise DiskAdmissionError('Ledger is bound to a different volume coordinator')
        if bound is None and requested is not None:
            raise DiskAdmissionError('An existing private ledger cannot acquire a new volume binding during reopen')
        con=sqlite3.connect(ledger.as_uri()+'?mode=ro',uri=True)
        try:
            if not con.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='private_policy'").fetchone():
                raise DiskAdmissionError('Store-private policy persisted none; explicit migration is required')
            saved=con.execute('SELECT quota_bytes,min_free_bytes,drain_reserve_bytes,bootstrap_bytes,max_page_count,max_records,volume_device,custom_accounting FROM private_policy WHERE singleton=1').fetchone()
            expected=(config.disk_quota_bytes,config.min_free_bytes,config.drain_reserve_bytes,
                      pool,1024,4096,ancestor(directory).stat().st_dev,0)
            if saved!=expected:raise DiskAdmissionError('Store-private ledger policy mismatch')
        except sqlite3.Error as exc:raise DiskAdmissionError('Private ledger policy is unreadable') from exc
        finally:con.close()
    if config.volume_coordinator is not None:
        candidate=Path(config.volume_coordinator).absolute()
        if candidate.resolve()!=candidate:
            raise DiskAdmissionError('Private coordinator path must not be aliased')
        path=candidate
        if ancestor(path.parent).stat().st_dev!=ancestor(directory).stat().st_dev:
            raise DiskAdmissionError('Private store coordinator must be on the storage volume')
        if path.exists():
            if path.is_symlink() or not path.is_file() or path.stat().st_nlink!=1:
                raise DiskAdmissionError('Private coordinator is not an exclusive regular file')
            con=sqlite3.connect(path.as_uri()+'?mode=ro',uri=True)
            try:
                row=con.execute('SELECT quota_bytes,min_free_bytes,drain_reserve_bytes FROM policy WHERE singleton=1').fetchone()
                if row!=(config.disk_quota_bytes,config.min_free_bytes,config.drain_reserve_bytes):
                    raise DiskAdmissionError('Private store coordinator policy mismatch')
            except sqlite3.Error as exc:raise DiskAdmissionError('Private coordinator policy is unreadable') from exc
            finally:con.close()
            # Its full metadata credit is checked by the coordinator itself.
        else:pool+=1024**2
    result=preflight(directory,pool_bytes=pool,quota_bytes=config.disk_quota_bytes,
        min_free_bytes=config.min_free_bytes,drain_reserve_bytes=config.drain_reserve_bytes,
        managed_roots=(directory,),excluded_paths=excluded)
    if config.volume_coordinator is not None:
        from .volume_coordinator import VolumeCoordinator
        coordinator=(VolumeCoordinator.open_bound(config.volume_coordinator) if ledger.exists() else
            VolumeCoordinator(config.volume_coordinator,quota_bytes=config.disk_quota_bytes,
                min_free_bytes=config.min_free_bytes,drain_reserve_bytes=config.drain_reserve_bytes))
        try:coordinator.pre_admit_ledger(ledger,metadata_bytes=8*1024**2,roots=(directory,))
        finally:coordinator.close()
    return result


def open_private_budget(config, directory, *, create):
    directory=Path(directory).resolve();ledger=directory/'metadata/disk-ledger.sqlite3'
    if not create and not ledger.is_file():
        raise DiskAdmissionError('Store-private ledger is missing; refusing to recreate admission history')
    coordinator=None
    try:
        if config.volume_coordinator is not None:
            from .volume_coordinator import VolumeCoordinator
            path=Path(config.volume_coordinator)
            existing=path.parent
            while not existing.exists():existing=existing.parent
            if existing.stat().st_dev!=directory.stat().st_dev:
                raise DiskAdmissionError('Private store coordinator must be on the storage volume')
            if ledger.exists():
                coordinator=VolumeCoordinator.open_bound(path)
                if (coordinator.quota_bytes,coordinator.min_free_bytes,coordinator.drain_reserve_bytes)!=(
                        config.disk_quota_bytes,config.min_free_bytes,config.drain_reserve_bytes):
                    raise DiskAdmissionError('Private store coordinator policy mismatch')
            else:
                coordinator=VolumeCoordinator(path,quota_bytes=config.disk_quota_bytes,
                    min_free_bytes=config.min_free_bytes,drain_reserve_bytes=config.drain_reserve_bytes)
        return DiskBudget(ledger,[directory],persist_private_policy=True,
            quota_bytes=config.disk_quota_bytes,min_free_bytes=config.min_free_bytes,
            drain_reserve_bytes=config.drain_reserve_bytes,volume_coordinator=coordinator,
            own_volume_coordinator=coordinator is not None)
    except BaseException:
        if coordinator is not None:coordinator.close()
        raise
