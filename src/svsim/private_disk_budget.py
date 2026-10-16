"""Owned store-private ledger bootstrap with an optional persisted volume binding."""
from pathlib import Path
import json
import sqlite3
import stat

from .disk_budget import DiskAdmissionError, DiskBudget


def _shared_shard_budget(config,directory,*,validate_only=False):
    """Inherit a shard's persisted shared ledger; never create missing history."""
    catalog=directory/'metadata/catalog.sqlite3'
    def existing(path):
        path=Path(path)
        if not path.is_absolute() or path.resolve()!=path:
            raise DiskAdmissionError('Persisted shard admission path is aliased')
        info=path.lstat()
        if not stat.S_ISREG(info.st_mode) or info.st_nlink!=1 or info.st_dev!=directory.stat().st_dev:
            raise DiskAdmissionError('Persisted shard admission file is not exclusive on the store volume')
        return path
    existing(catalog)
    for suffix in ('-journal','-wal','-shm'):
        side=Path(str(catalog)+suffix)
        if side.exists() and side.stat().st_size:
            raise DiskAdmissionError('Shard catalog needs admitted recovery before standalone open')
    con=sqlite3.connect(catalog.as_uri()+'?mode=ro',uri=True)
    try:
        con.execute('PRAGMA query_only=ON')
        row=con.execute("SELECT data FROM distributed_settings WHERE key='disk_admission_ledger'").fetchone()
        if row is None or not isinstance(row[0],str) or len(row[0])>16384:
            raise DiskAdmissionError('Shard has no bounded persisted shared admission locator')
        locator=json.loads(row[0])
        if not isinstance(locator,str):raise DiskAdmissionError('Invalid persisted shared admission locator')
        ledger=existing(locator)
    except (sqlite3.Error,ValueError,OSError) as exc:
        raise DiskAdmissionError('Store-private ledger is missing and shared shard binding is unproven') from exc
    finally:con.close()
    con=sqlite3.connect(ledger.as_uri()+'?mode=ro',uri=True)
    try:
        con.execute('PRAGMA query_only=ON')
        row=con.execute('''SELECT quota_bytes,min_free_bytes,drain_reserve_bytes,bootstrap_bytes,
            max_page_count,max_records,volume_device,custom_accounting FROM shared_policy WHERE singleton=1''').fetchone()
        roots=[Path(p) for p, in con.execute('SELECT path FROM roots LIMIT 257')]
        if (row is None or len(roots)>256 or row[6]!=directory.stat().st_dev or row[7]!=0 or
                not any(directory.is_relative_to(root) for root in roots)):
            raise DiskAdmissionError('Persisted shard ledger does not cover this store')
    except sqlite3.Error as exc:raise DiskAdmissionError('Persisted shard shared policy is unreadable') from exc
    finally:con.close()
    requested=(config.disk_quota_bytes,config.min_free_bytes,config.drain_reserve_bytes)
    if requested!=(None,0,0) and requested!=tuple(row[:3]):
        raise DiskAdmissionError('StorageConfig policy differs from the persisted shard ledger')
    binding=DiskBudget.read_volume_binding(ledger)
    if config.volume_coordinator is not None and str(Path(config.volume_coordinator).resolve())!=binding:
        raise DiskAdmissionError('StorageConfig coordinator differs from the persisted shard ledger')
    if validate_only:return directory
    coordinator=None
    try:
        if binding is not None:
            from .volume_coordinator import VolumeCoordinator
            coordinator=VolumeCoordinator.open_bound(binding)
        return DiskBudget(ledger,roots,quota_bytes=row[0],min_free_bytes=row[1],drain_reserve_bytes=row[2],
            bootstrap_bytes=row[3],max_page_count=row[4],max_records=row[5],shared_policy=True,
            volume_coordinator=coordinator,own_volume_coordinator=coordinator is not None)
    except BaseException:
        if coordinator is not None:coordinator.close()
        raise


def preflight_private_config(config,directory,*,create=True):
    """Check policy, source volume and existing managed bytes without mkdir/open for writing."""
    from .bootstrap_preflight import ancestor,preflight
    directory=Path(directory).resolve();ledger=directory/'metadata/disk-ledger.sqlite3'
    pool=8*1024**2;excluded=[ledger,Path(str(ledger)+'-journal')]
    if not create and not ledger.is_file():
        return _shared_shard_budget(config,directory,validate_only=True)
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
        return _shared_shard_budget(config,directory)
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
