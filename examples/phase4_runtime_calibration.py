"""Run a finite Phase 4 runtime calibration locally; this script never submits jobs."""
import argparse
import json
from pathlib import Path
from svsim.profile_document import write_document
from svsim.runtime_calibration import MAX_REPORT_BYTES

from svsim.runtime_calibration import RuntimeCalibrationSpec, calibrate_runtime


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scratch", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--num-qubits", type=int, default=8)
    parser.add_argument("--block-bits", type=int, default=4)
    parser.add_argument("--ranks", type=int, nargs="+", default=[1])
    parser.add_argument("--seed", type=int, default=1)
    parser.add_argument("--ram-budget", type=int, required=True)
    parser.add_argument("--disk-budget", type=int, required=True)
    parser.add_argument("--walltime", type=float, required=True)
    parser.add_argument("--repetitions", type=int, default=5)
    parser.add_argument("--warmup", type=int, default=1)
    parser.add_argument("--backend", choices=("buffered", "direct"), default="buffered")
    parser.add_argument("--engine", choices=("worker", "cooperative"), default="worker")
    parser.add_argument("--max-io-requests", type=int, choices=(1, 2), default=1)
    parser.add_argument("--suite", choices=("kernel-storage", "memory-mpi", "storage-mpi"),
                        action="append", dest="suites")
    args = parser.parse_args()
    spec = RuntimeCalibrationSpec(
        scratch=args.scratch, num_qubits=args.num_qubits, block_bits=args.block_bits,
        ranks=tuple(args.ranks), seed=args.seed, ram_budget_bytes=args.ram_budget,
        disk_budget_bytes=args.disk_budget, walltime_seconds=args.walltime,
        repetitions=args.repetitions, warmup=args.warmup, backend=args.backend,
        engine=args.engine, max_io_requests=args.max_io_requests,
        suites=tuple(args.suites or ("kernel-storage",)))
    report = calibrate_runtime(spec)
    from mpi4py import MPI
    if MPI.COMM_WORLD.Get_rank() != 0:
        return
    destination = Path(args.output)
    destination.parent.mkdir(parents=True, exist_ok=True)
    write_document(destination,report,max_bytes=MAX_REPORT_BYTES)
    print(json.dumps({"status": report["status"], "summary": report["summary"]}, indent=2))
    return 0 if report['status']=='complete' else 4


if __name__ == "__main__":
    raise SystemExit(main())
