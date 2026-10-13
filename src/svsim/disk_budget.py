"""Host/device admission for node-local immutable writes.

SQLite serializes ranks sharing a volume. Files on registered local roots are
measured at each admission, including catalogs, journals and failed writes.
Reservations are persistent: a crashed writer cannot silently return credit.
"""
from contextlib import contextmanager
from pathlib import Path
import os
import sqlite3
import stat
from uuid import uuid4


class DiskAdmissionError(OSError):
    pass


class DiskBudget:
    def __init__(self, ledger, roots, *, quota_bytes=None, min_free_bytes=0,
                 drain_reserve_bytes=0, usage=None, free=None):
        if any(x < 0 for x in (min_free_bytes, drain_reserve_bytes)):
            raise ValueError('Negative disk reserve')
        if quota_bytes is not None and quota_bytes <= 0:
            raise ValueError('Invalid disk quota')
        self.roots = tuple(dict.fromkeys(Path(p).resolve() for p in roots))
        if not self.roots:
            raise ValueError('Disk budget needs a local root')
        self.quota_bytes, self.min_free_bytes = quota_bytes, min_free_bytes
        self.drain_reserve_bytes = drain_reserve_bytes
        self._usage = usage or self._observed_usage
        self._free = free or self._observed_free
        ledger = Path(ledger); ledger.parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(ledger, timeout=30)
        self.db.execute('PRAGMA synchronous=FULL')
        self.db.execute('CREATE TABLE IF NOT EXISTS reservations(id TEXT PRIMARY KEY,bytes INTEGER)')
        self.db.execute('CREATE TABLE IF NOT EXISTS roots(path TEXT PRIMARY KEY)')
        self.db.executemany('INSERT OR IGNORE INTO roots VALUES(?)', [(str(p),) for p in self.roots])
        self.db.commit()

    def _observed_usage(self):
        total = 0
        roots=sorted((Path(p) for (p,) in self.db.execute('SELECT path FROM roots')),key=lambda p:len(p.parts))
        selected=[]
        for root in roots:
            if any(root.is_relative_to(parent) for parent in selected): continue
            selected.append(root)
            for path in root.rglob('*'):
                try: info=path.stat(follow_symlinks=False)
                except FileNotFoundError:
                    # SQLite rollback journals and atomic-write temporaries can
                    # disappear while another rank scans this shared volume.
                    continue
                if stat.S_ISREG(info.st_mode): total += info.st_size
        return total

    def _observed_free(self):
        stat = os.statvfs(self.roots[0])
        return stat.f_bavail * stat.f_frsize

    @property
    def reserved_bytes(self):
        return self.db.execute('SELECT COALESCE(SUM(bytes),0) FROM reservations').fetchone()[0]

    def reserve(self, amount, *, drain=False):
        if amount < 0:
            raise ValueError('Negative disk reservation')
        key = uuid4().hex
        with self.db:
            self.db.execute('BEGIN IMMEDIATE')
            pending = self.reserved_bytes
            margin = 0 if drain else self.drain_reserve_bytes
            if self.quota_bytes is not None and self._usage()+pending+amount+margin > self.quota_bytes:
                raise DiskAdmissionError('Host/device disk quota exceeded')
            if self._free()-pending-amount-margin < self.min_free_bytes:
                raise DiskAdmissionError('Host/device minimum free space violated')
            self.db.execute('INSERT INTO reservations VALUES(?,?)', (key, amount))
        return key

    def release(self, key):
        with self.db:
            self.db.execute('DELETE FROM reservations WHERE id=?', (key,))

    @contextmanager
    def allocation(self, amount, *, drain=False):
        key = self.reserve(amount, drain=drain)
        try:
            yield
        finally:
            # Actual bytes are now observed, including partial failed writes.
            self.release(key)

    def close(self):
        self.db.close()
