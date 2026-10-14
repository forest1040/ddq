"""4 KiB write+fsync latency in windows, plus SQLite FULL/DELETE vs WAL commit cost."""
import os, sqlite3, statistics, sys, time
from pathlib import Path
root = Path(sys.argv[1]); root.mkdir(parents=True, exist_ok=True)
def pct(v, q): v = sorted(v); return v[min(len(v)-1, int(q*(len(v)-1)))]
fd = os.open(root/'probe.bin', os.O_CREAT|os.O_RDWR, 0o600); buf = os.urandom(4096)
for window in range(6):
    lat = []
    for i in range(300):
        os.pwrite(fd, buf, (i % 64) * 4096); t = time.perf_counter(); os.fsync(fd); lat.append((time.perf_counter()-t)*1e3)
    print(f'fsync window {window}: median {statistics.median(lat):.2f} ms p95 {pct(lat,.95):.2f} max {max(lat):.1f}', flush=True)
os.close(fd)
for mode, sync in (('DELETE', 'FULL'), ('WAL', 'FULL'), ('WAL', 'NORMAL')):
    path = root/f'db-{mode}-{sync}.sqlite'
    db = sqlite3.connect(path, isolation_level=None); db.execute(f'PRAGMA journal_mode={mode}'); db.execute(f'PRAGMA synchronous={sync}')
    db.execute('CREATE TABLE t(k INTEGER PRIMARY KEY, v BLOB)')
    lat = []
    for i in range(400):
        t = time.perf_counter(); db.execute('INSERT INTO t VALUES(?,?)', (i, buf[:256])); lat.append((time.perf_counter()-t)*1e3)
    db.close()
    print(f'sqlite {mode}/{sync} autocommit insert: median {statistics.median(lat):.2f} ms p95 {pct(lat,.95):.2f} max {max(lat):.1f}', flush=True)
for p in root.iterdir(): p.unlink()
