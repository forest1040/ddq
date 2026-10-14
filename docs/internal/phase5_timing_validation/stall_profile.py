"""Profile repeated identical trials; compare slowest vs fastest by self time."""
import cProfile, io, json, pstats, shutil, sys, time
from pathlib import Path
from mpi4py import MPI
from svsim.benchmark_run import RunCase, RunOptions, _trial, calibration_suite
from svsim.workload_spec import WorkloadSpec

scratch = Path(sys.argv[1]); trials = int(sys.argv[2])
w = WorkloadSpec(calibration_suite(4, (8,), (12,), classes=('h-local',))['cal-h-local-n8-g12'])
opts = RunOptions(block_bits=4, ram_budget_bytes_per_rank=1 << 26, disk_budget_bytes=1 << 26)
results = []
for i in range(trials):
    prof = cProfile.Profile()
    prof.enable()
    t = time.perf_counter()
    trial, _ = _trial(RunCase(w, 'x'), opts, scratch / f't{i}', MPI.COMM_SELF)
    prof.disable()
    results.append((trial['wall_seconds']['execute'], i, prof))
    print(f'trial {i}: execute {trial["wall_seconds"]["execute"]:.2f}s total {time.perf_counter()-t:.2f}s', flush=True)
results.sort(key=lambda r: r[0])
for label, (sec, i, prof) in (('FASTEST', results[0]), ('SLOWEST', results[-1])):
    out = io.StringIO()
    pstats.Stats(prof, stream=out).sort_stats('tottime').print_stats(14)
    print(f'===== {label} trial {i} execute={sec:.2f}s')
    print('\n'.join(l for l in out.getvalue().splitlines() if l.strip())[-1800:])
