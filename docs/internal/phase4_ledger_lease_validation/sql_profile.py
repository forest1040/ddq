"""Time every SQLite execute/commit by database file and statement for one trial."""
import collections, re, sqlite3, sys, time
from pathlib import Path
stats = collections.defaultdict(lambda: [0, 0.0])
_connect = sqlite3.connect
class Timed(sqlite3.Connection):
    def _key(self, sql): return (getattr(self, 'label', '?'), re.sub(r'\s+', ' ', sql.strip())[:70])
    def execute(self, sql, *a):
        t = time.perf_counter()
        try: return super().execute(sql, *a)
        finally: s = stats[self._key(sql)]; s[0] += 1; s[1] += time.perf_counter() - t
    def executemany(self, sql, *a):
        t = time.perf_counter()
        try: return super().executemany(sql, *a)
        finally: s = stats[self._key('MANY ' + sql)]; s[0] += 1; s[1] += time.perf_counter() - t
    def commit(self):
        t = time.perf_counter()
        try: return super().commit()
        finally: s = stats[(getattr(self, 'label', '?'), 'COMMIT')]; s[0] += 1; s[1] += time.perf_counter() - t
    def __exit__(self, *exc):
        t = time.perf_counter()
        try: return super().__exit__(*exc)
        finally: s = stats[(getattr(self, 'label', '?'), 'WITH-EXIT(commit)')]; s[0] += 1; s[1] += time.perf_counter() - t
def connect(database, *a, **k):
    k.setdefault('factory', Timed)
    con = _connect(database, *a, **k)
    name = str(database).split('?')[0].rstrip('/').split('/')[-1]
    try: con.label = f'{name}[iso={con.isolation_level!r}]'
    except Exception: pass
    return con
sqlite3.connect = connect
from mpi4py import MPI
from svsim.benchmark_run import RunCase, RunOptions, _trial, calibration_suite
from svsim.workload_spec import WorkloadSpec
w = WorkloadSpec(calibration_suite(4, (8,), (12,), classes=('h-local',))['cal-h-local-n8-g12'])
trial, _ = _trial(RunCase(w, 'x'), RunOptions(block_bits=4, ram_budget_bytes_per_rank=1 << 26, disk_budget_bytes=1 << 26),
                  Path(sys.argv[1]) / 't', MPI.COMM_SELF)
print('phases', {k: round(v, 2) for k, v in trial['wall_seconds'].items()}, 'probe_ms', round(trial['sync_probe_median_seconds']*1e3, 2))
rows = sorted(stats.items(), key=lambda kv: -kv[1][1])
total = sum(v[1] for v in stats.values())
print(f'sqlite total {total:.2f}s over {sum(v[0] for v in stats.values())} calls')
for (db, sql), (n, sec) in rows[:28]:
    print(f'{sec:7.2f}s {n:6d} {sec/n*1e3:7.2f}ms  {db:38s} {sql}')
