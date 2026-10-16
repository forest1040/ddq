"""Atomic sidecar writes with admission held through the durability boundary."""

from contextlib import contextmanager
import os
from pathlib import Path
import stat
from uuid import uuid4


def checked_sidecar_path(budget,path,*,exclusive_leaf=True):
    """Keep admitted writes inside the inventory that will charge their bytes."""
    path=Path(path)
    if budget is None or not hasattr(budget,'db') or not hasattr(budget,'roots'):
        return path  # Unmanaged callers and admission-only test doubles.
    from .disk_budget import DiskAdmissionError
    from .export_admission import export_inventory_roots
    parent=path.absolute().parent.resolve()
    canonical=parent/path.name
    with budget._path_lock:
        roots=[Path(p) for (p,) in budget.db.execute('SELECT path FROM roots')]
        roots.extend(Path(row['path']) for row in export_inventory_roots(budget.db))
    covered=[root for root in roots if canonical.is_relative_to(root)]
    if not covered:raise DiskAdmissionError('Admitted sidecar destination is outside the registered inventory')
    root=max(covered,key=lambda candidate:len(candidate.parts))
    ancestor=parent
    while not ancestor.exists():ancestor=ancestor.parent
    device=root.stat().st_dev
    if ancestor.stat().st_dev!=device:
        raise DiskAdmissionError('Admitted sidecar destination is on another volume')
    if exclusive_leaf:
        try:info=canonical.lstat()
        except FileNotFoundError:pass
        else:
            if not stat.S_ISREG(info.st_mode) or info.st_nlink!=1 or info.st_dev!=device:
                raise DiskAdmissionError('Admitted sidecar destination must be an exclusive regular file')
    return canonical


@contextmanager
def file_allocation(budget, amount, temporary, *, drain=False, operation='sidecar-write',
                    owner=None,generation=None,checkpoint_link=None):
    """Keep recovery evidence if a temporary artifact cannot be cleaned up.

    This checks only artifact absence, not an authoritative checkpoint cut.
    Uncertain stat/cleanup results retain the live token for explicit recovery.
    """
    if budget is None:
        yield None
        return
    temporary=checked_sidecar_path(budget,temporary,exclusive_leaf=False)
    kwargs={'owner':owner,'generation':generation}
    if checkpoint_link is not None:kwargs['checkpoint_link']=checkpoint_link
    key=budget.reserve(amount,drain=drain,operation=operation,writing=True,**kwargs)
    try:yield key
    finally:
        try:Path(temporary).lstat()
        except FileNotFoundError:budget.release(key)
        except OSError:raise


def atomic_write_bytes(path, data, *, disk_budget=None, drain=False, operation='sidecar-write'):
    """Publish bytes after file fsync; return only after directory fsync.

    The destination must be within the caller's managed inventory for quota
    accounting. This soft reservation does not enforce a filesystem quota.
    After rename, an unsuccessful directory fsync leaves the new file in place
    and reports uncertain durability; it never rolls a checkpoint back.
    """
    if not isinstance(data, bytes):
        raise TypeError('Atomic sidecar data must be bytes')
    path=checked_sidecar_path(disk_budget,path)
    temporary=path.with_name(path.name+f'.{uuid4().hex}.tmp')
    created=False
    with file_allocation(disk_budget,len(data),temporary,drain=drain,operation=operation) as key:
        try:
            path.parent.mkdir(parents=True,exist_ok=True)
            try:stream=temporary.open('xb')
            except FileExistsError:
                if disk_budget is not None:disk_budget.release(key)
                raise
            created=True
            with stream:
                if stream.write(data)!=len(data):
                    raise OSError('Incomplete atomic sidecar write')
                stream.flush()
                if disk_budget is not None:disk_budget.materialize(key,len(data))
                os.fsync(stream.fileno())
            os.replace(temporary,path)
            fd=os.open(path.parent,os.O_RDONLY)
            try:os.fsync(fd)
            finally:os.close(fd)
        finally:
            if created:temporary.unlink(missing_ok=True)
