"""Read-only finite metadata-pool preflight before the first bootstrap mkdir.

This is a soft observation, not a filesystem metadata allocation guarantee.
The normal persistent ledger/VFS takes over as soon as it exists.
"""
import os
from pathlib import Path

from .disk_budget import DiskAdmissionError


def ancestor(path):
    current=Path(path).resolve()
    while not current.exists():current=current.parent
    if not current.is_dir():raise DiskAdmissionError('Bootstrap ancestor must be a directory')
    return current


def preflight(path,*,pool_bytes,quota_bytes=None,min_free_bytes=0,drain_reserve_bytes=0):
    root=ancestor(path)
    if quota_bytes is not None and pool_bytes+drain_reserve_bytes>quota_bytes:
        raise DiskAdmissionError('Namespace bootstrap exceeds host/device disk quota')
    st=os.statvfs(root)
    if st.f_bavail*st.f_frsize-pool_bytes-drain_reserve_bytes<min_free_bytes:
        raise DiskAdmissionError('Namespace bootstrap violates minimum free space')
    return root
