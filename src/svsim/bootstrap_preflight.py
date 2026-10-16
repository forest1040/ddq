"""Read-only finite metadata-pool preflight before the first bootstrap mkdir.

This is a soft observation, not a filesystem metadata allocation guarantee.
The normal persistent ledger/VFS takes over as soon as it exists.
"""
import os
from pathlib import Path
import stat

from .disk_budget import DiskAdmissionError

MAX_ENTRIES=16384


def ancestor(path):
    current=Path(path).resolve()
    while not current.exists():current=current.parent
    if not current.is_dir():raise DiskAdmissionError('Bootstrap ancestor must be a directory')
    return current


def inventory(roots,*,device,excluded_paths=()):
    """Observe only declared managed roots, never the surrounding ancestor tree."""
    excluded={Path(p).resolve() for p in excluded_paths};selected=[];total=allocated=entries=0
    for root in sorted({Path(p).absolute() for p in roots},key=lambda p:len(p.parts)):
        if any(root.is_relative_to(p) for p in selected):continue
        selected.append(root)
        try:info=root.lstat()
        except FileNotFoundError:continue
        if root.resolve()!=root or not stat.S_ISDIR(info.st_mode) or info.st_dev!=device:
            raise DiskAdmissionError('Bootstrap managed root is aliased or changed volume')
        stack=[root]
        while stack:
            try:
                with os.scandir(stack.pop()) as children:
                    for child in children:
                        entries+=1
                        if entries>MAX_ENTRIES:raise DiskAdmissionError('Bootstrap inventory entry bound exceeded')
                        info=child.stat(follow_symlinks=False);path=Path(child.path)
                        if info.st_dev!=device or stat.S_ISLNK(info.st_mode):
                            raise DiskAdmissionError('Bootstrap inventory is aliased or crosses volumes')
                        if stat.S_ISDIR(info.st_mode):stack.append(path)
                        elif stat.S_ISREG(info.st_mode):
                            if info.st_nlink!=1:
                                raise DiskAdmissionError('Bootstrap inventory contains hardlinked files')
                            if path in excluded:allocated+=info.st_size
                            else:total+=info.st_size
                        else:raise DiskAdmissionError('Bootstrap inventory contains an unsupported artifact')
            except OSError as exc:
                if isinstance(exc,DiskAdmissionError):raise
                raise DiskAdmissionError('Bootstrap inventory cannot be observed: '+str(exc)) from exc
    return total,allocated


def preflight(path,*,pool_bytes,quota_bytes=None,min_free_bytes=0,drain_reserve_bytes=0,
              managed_roots=(),excluded_paths=()):
    root=ancestor(path)
    used,allocated=inventory(managed_roots,device=root.stat().st_dev,excluded_paths=excluded_paths)
    if allocated>pool_bytes:
        raise DiskAdmissionError('Bootstrap metadata exceeds its finite pool')
    if quota_bytes is not None and used+pool_bytes+drain_reserve_bytes>quota_bytes:
        raise DiskAdmissionError('Namespace bootstrap exceeds host/device disk quota')
    st=os.statvfs(root)
    if st.f_bavail*st.f_frsize-max(0,pool_bytes-allocated)-drain_reserve_bytes<min_free_bytes:
        raise DiskAdmissionError('Namespace bootstrap violates minimum free space')
    return root
