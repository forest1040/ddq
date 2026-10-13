"""Measure Mac-local MPI variants through the bounded development scheduler."""
import argparse
from hashlib import sha256
import json
from pathlib import Path
import platform
import shutil
from statistics import median
import subprocess
import sys
import tempfile

from svsim import JobSpec, LocalDevelopmentRunner
from svsim.development_runner import JobState


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', required=True)
    parser.add_argument('--repetitions', type=int, default=3)
    parser.add_argument('--backend', choices=('buffered', 'direct'), default='buffered')
    args = parser.parse_args()
    if args.repetitions < 3:
        parser.error('at least three measured repetitions are required')
    root = Path(__file__).resolve().parents[1]
    launcher = shutil.which('mpiexec') or shutil.which('mpirun')
    if launcher is None:
        raise RuntimeError('Local MPI launcher is required')
    revision = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=root, text=True).strip()
    lock_hash = sha256((root/'uv.lock').read_bytes()).hexdigest()
    folder = Path(tempfile.mkdtemp(prefix='ddq-phase4-benchmark-'))
    rows = []
    with LocalDevelopmentRunner(folder/'reports', max_ram_bytes=64*1024**2,
                          max_disk_bytes=256*1024**2, max_concurrent_jobs=1) as runner:
        for ranks in (1, 2, 4):
            for runtime, policy in (('sync', 'S0'), ('async', 'S0'), ('async', 'S2')):
                # One unreported warmup per matched condition.
                for repetition in range(-1, args.repetitions):
                    spec = JobSpec(revision, lock_hash,
                        (sys.executable, '-m', 'mpi4py', str(root/'examples/phase4_benchmark_worker.py'),
                         '--runtime', runtime, '--policy', policy, '--backend', args.backend),
                        ranks=ranks, rank_ram_bytes=8*1024**2, rank_disk_bytes=32*1024**2,
                        launcher_argv=(launcher, '-n', str(ranks)),
                        scratch_root=str(folder/'scratch'), cwd=str(root),
                        walltime_seconds=120, test_case=f'{runtime}-{policy}-{ranks}',
                        env={'PYTHONPATH': str(root/'src'), 'OMP_NUM_THREADS': '1',
                             'OPENBLAS_NUM_THREADS': '1'})
                    record = runner.submit(spec).wait(130)
                    if record.state != JobState.COMPLETED:
                        raise RuntimeError(f'Local run failed; inspect {record.report_path}')
                    result = json.loads(Path(record.stdout_path).read_text())
                    if repetition >= 0:
                        result['repetition'] = repetition
                        rows.append(result)
    summaries = []
    for ranks in (1, 2, 4):
        for runtime, policy in (('sync', 'S0'), ('async', 'S0'), ('async', 'S2')):
            selected = [row for row in rows if (row['ranks'], row['runtime'], row['policy']) == (ranks, runtime, policy)]
            values = [row['execute_seconds'] for row in selected]
            summaries.append({'ranks': ranks, 'runtime': runtime, 'policy': policy,
                              'execute_median_seconds': median(values),
                              'execute_min_seconds': min(values), 'execute_max_seconds': max(values),
                              'read_bytes_median': median(sum(r['read_bytes'] for r in row['rank_metrics']) for row in selected),
                              'write_bytes_median': median(sum(r['write_bytes'] for r in row['rank_metrics']) for row in selected),
                              'managed_peak_bytes_max': max(r['peak'] for row in selected for r in row['rank_metrics'])})
    report = {'schema_version': 1, 'scope': 'same-host', 'platform': platform.platform(),
              'revision': revision, 'working_tree': 'phase4-uncommitted', 'lock_hash': lock_hash,
              'repetitions': args.repetitions, 'warmup_repetitions': 1, 'backend': args.backend,
              'rows': rows, 'summary': summaries}
    destination = Path(args.output)
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(json.dumps(report, ensure_ascii=False, indent=2)+'\n')
    print(json.dumps(summaries, indent=2))


if __name__ == '__main__':
    main()
