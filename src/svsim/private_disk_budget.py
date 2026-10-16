"""Owned store-private ledger bootstrap with an optional persisted volume binding."""
from pathlib import Path

from .disk_budget import DiskAdmissionError, DiskBudget


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
