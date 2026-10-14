"""Atomic sidecar writes with admission held through the durability boundary."""

from contextlib import contextmanager
import os
from pathlib import Path
from uuid import uuid4


@contextmanager
def file_allocation(budget, amount, temporary, *, drain=False, operation='sidecar-write'):
    """Keep recovery evidence if a temporary artifact cannot be cleaned up.

    This checks only artifact absence, not an authoritative checkpoint cut.
    Uncertain stat/cleanup results retain the live token for explicit recovery.
    """
    if budget is None:
        yield None
        return
    key=budget.reserve(amount,drain=drain,operation=operation,writing=True)
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
    path=Path(path)
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
