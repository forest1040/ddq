#!/usr/bin/env python3
"""Run one restartable, resource-bounded P4-002 Linux measurement case.

Run on the Linux host which has the immutable wave3 v3 source context and the
ddq-linux image. This driver never uses SSH. Each invocation runs exactly one
case and writes a new attempt directory, retaining partial JSONL on timeout.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import shlex
import subprocess
import sys
import time


ROOT_DEFAULT = Path("/tmp/ddq-p4-linux-followup-20261015-tmori")
BACKENDS = ("buffered", "direct")
RANKS = (1, 2)
REPRESENTATIVE_HELDOUT = ("rx-ry-low", "rx-ry-var", "mixed")
ALL_HELDOUT = ("rx-ry-low", "upper", "cz", "mixed", "rx-ry-var", "mixed-var")
OMITTED_HELDOUT = tuple(name for name in ALL_HELDOUT if name not in REPRESENTATIVE_HELDOUT)
WARMUPS, PAIRS, SEED, LONG_LAYERS = 2, 7, 20261016, 20
TIMEOUT_SECONDS = 3600
IMAGE = "ddq-linux:dev-20261014"


def case_names():
    return tuple(f"{stage}-{backend}-p{ranks}"
                 for stage in ("overhead", "heldout")
                 for backend in BACKENDS for ranks in RANKS)


def manifest(case, context, timeout_seconds, source_hash_manifest_sha256, image=IMAGE):
    stage, backend, rank_tag = case.split("-")
    ranks = int(rank_tag[1:])
    revision_path = context / "SOURCE_REVISION"
    revision = revision_path.read_text(encoding="utf-8").strip() if revision_path.is_file() else None
    common = {"case": case, "backend": backend, "ranks": ranks,
              "timeout_seconds": timeout_seconds, "source_context": str(context),
              "image": image, "source_revision": revision,
              "source_hash_manifest_sha256": source_hash_manifest_sha256}
    if stage == "overhead":
        return {**common, "stage": stage, "workloads": ["rx-ry-low", "mixed"],
                "mixed_layers": LONG_LAYERS, "warmups": WARMUPS, "measured_pairs": PAIRS,
                "seed": SEED, "timeout_scope": "one complete overhead workload campaign"}
    return {**common, "stage": stage, "workloads": list(REPRESENTATIVE_HELDOUT),
            "omitted_workloads": list(OMITTED_HELDOUT), "policies": ["S0", "S1", "S2"],
            "stores": ["file", "ram"], "fractions": [0.4, 0.6, 0.8], "trials": 1,
            "expected_checks": len(REPRESENTATIVE_HELDOUT) * 3 * 2 * 3,
            "timeout_scope": "one representative held-out matrix"}


def source_hashes(context):
    records = []
    for area in ("src", "tests", "examples"):
        for path in sorted((context / area).rglob("*")):
            if not path.is_file() or "__pycache__" in path.parts or path.suffix == ".pyc":
                continue
            digest = hashlib.sha256(path.read_bytes()).hexdigest()
            records.append(f"{digest}  {path.relative_to(context)}")
    return records


def next_attempt(output_root, case):
    case_root = output_root / case
    case_root.mkdir(parents=True, exist_ok=True)
    index = 1
    while (case_root / f"attempt-{index:02d}").exists():
        index += 1
    path = case_root / f"attempt-{index:02d}"
    path.mkdir()
    return path, index


def command_for(case, *, context, scratch, output, timeout_seconds, image, container_name):
    stage, backend, rank_tag = case.split("-")
    ranks = int(rank_tag[1:])
    if stage == "overhead":
        result = f"/output/overhead-{case}.json"
        scratch_path = f"/scratch/{case}"
        workload_arg = "rx-ry-low,mixed"
        command = ("uv run --offline --no-sync mpiexec -n {ranks} python "
                   "examples/p4_002_overhead_campaign.py --backend {backend} "
                   "--scratch {scratch} --output {output} --workloads {workloads} "
                   "--long-layers {layers} --warmups 2 --pairs 7 --seed 20261016").format(
                       ranks=ranks, backend=backend, scratch=shlex.quote(scratch_path),
                       output=shlex.quote(result), workloads=workload_arg, layers=LONG_LAYERS)
        expected = result
    else:
        result = f"/output/heldout-{case}.json"
        scratch_path = f"/scratch/{case}"
        workloads = ",".join(REPRESENTATIVE_HELDOUT)
        command = ("uv run --offline --no-sync mpiexec -n {ranks} python "
                   "examples/p4_002_observed_eta.py --backend {backend} "
                   "--scratch {scratch} --output {output} --workloads {workloads} "
                   "--trials 1 --skip-overhead --policies S0,S1,S2 --stores file,ram").format(
                       ranks=ranks, backend=backend, scratch=shlex.quote(scratch_path),
                       output=shlex.quote(result), workloads=workloads)
        expected = result

    container_script = "set -eu; timeout --signal=TERM --kill-after=15s {timeout}s sh -c {inner}".format(
        timeout=timeout_seconds, inner=shlex.quote(command))
    return [
        "docker", "run", "--rm", "--name", container_name,
        "--network", "none", "--hostname", "ddq-p4-vcoord-v3",
        "--cpus=4", "--memory=8g", "--pids-limit=512", "--shm-size=256m", "--stop-timeout=15",
        "-v", f"{context / 'src'}:/workspace/src:ro",
        "-v", f"{context / 'tests'}:/workspace/tests:ro",
        "-v", f"{context / 'examples'}:/workspace/examples:ro",
        "-v", f"{scratch}:/scratch",
        "-v", f"{output}:/output",
        "-w", "/workspace",
        "-e", "TMPDIR=/scratch", "-e", "UV_CACHE_DIR=/scratch/uv-cache",
        "-e", "OMP_NUM_THREADS=1", "-e", "OPENBLAS_NUM_THREADS=1",
        "-e", "MKL_NUM_THREADS=1", "-e", "NUMEXPR_NUM_THREADS=1",
        "-e", f"SVSIM_TEST_BACKEND={backend}", "-e", f"DDQ_BACKEND={backend}",
        "-e", f"DDQ_RANKS={ranks}", image, "sh", "-c", container_script,
    ], expected


def cleanup_container(container_name):
    """Remove only this attempt's container and verify it is absent afterward."""
    result = {"container_name": container_name, "verified_absent": False, "status": "cleanup-failed"}
    list_command = ["docker", "ps", "--all", "--quiet", "--filter", f"name=^/{container_name}$"]

    def list_ids():
        listed = subprocess.run(list_command, capture_output=True, text=True, check=False)
        return listed, [line.strip() for line in listed.stdout.splitlines() if line.strip()]

    try:
        listed, ids = list_ids()
        result["initial_list"] = {"return_code": listed.returncode, "stdout": listed.stdout,
                                  "stderr": listed.stderr}
        if listed.returncode != 0:
            result["status"] = "container-list-failed"
            return result
        if not ids:
            result.update({"status": "already-absent", "verified_absent": True})
            return result

        removed = subprocess.run(["docker", "rm", "--force", *ids],
                                 capture_output=True, text=True, check=False)
        result["remove"] = {"return_code": removed.returncode, "stdout": removed.stdout,
                            "stderr": removed.stderr}
        verified, remaining = list_ids()
        result["verification_list"] = {"return_code": verified.returncode, "stdout": verified.stdout,
                                       "stderr": verified.stderr}
        if verified.returncode != 0:
            result["status"] = "cleanup-verification-failed"
        elif not remaining:
            result.update({"status": "removed" if removed.returncode == 0 else "absent-after-remove-race",
                           "verified_absent": True})
        else:
            result["status"] = "container-remains"
            result["remaining_ids"] = remaining
        return result
    except Exception as exc:
        result["error"] = f"{type(exc).__name__}: {exc}"
        result["status"] = "cleanup-error"
        return result


def run_with_container_cleanup(command, *, container_name, log):
    """Run the timeout-wrapped Docker command and always clean up this named case."""
    return_code = 127
    launch_error = None
    try:
        try:
            result = subprocess.run(command, stdout=log, stderr=subprocess.STDOUT, check=False)
            return_code = result.returncode
        except OSError as exc:
            launch_error = f"{type(exc).__name__}: {exc}"
    finally:
        cleanup = cleanup_container(container_name)
    return return_code, launch_error, cleanup


def classify_artifact(case, output, expected_pairs):
    if case.startswith("overhead-"):
        path = output / f"overhead-{case}.json"
        if not path.exists():
            return "artifact-missing", None
        try:
            document = json.loads(path.read_text(encoding="utf-8"))
            results = document.get("results", {})
            expected_workloads = {"rx-ry-low", "mixed"}
            artifacts_complete = document.get("status") in ("complete", "incomplete") and \
                expected_workloads <= results.keys() and all(
                    len(results[name].get("warmup_pairs", ())) == WARMUPS and
                    {row.get("pair_index") for row in results[name].get("warmup_pairs", ())} ==
                        set(range(WARMUPS)) and
                    len(results[name].get("measured_pairs", ())) == expected_pairs and
                    {row.get("pair_index") for row in results[name].get("measured_pairs", ())} ==
                        set(range(WARMUPS, WARMUPS + expected_pairs))
                    for name in expected_workloads)
            if not artifacts_complete:
                return "artifact-incomplete", document.get("status")
            return ("complete" if document.get("status") == "complete" else "complete-with-failed-pairs",
                    document.get("status"))
        except (OSError, json.JSONDecodeError, TypeError, AttributeError):
            return "artifact-invalid", None

    path = output / f"heldout-{case}.json"
    if not path.exists():
        return "artifact-missing", None
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
        rows = document.get("rows", [])
        expected = len(REPRESENTATIVE_HELDOUT) * 3 * 2 * 3
        _, backend, rank_tag = case.split("-")
        ranks = int(rank_tag[1:])
        expected_conditions = {
            (workload, policy, store, fraction, 0, "cooperative", backend, ranks)
            for workload in REPRESENTATIVE_HELDOUT
            for policy in ("S0", "S1", "S2")
            for store in ("file", "ram")
            for fraction in (0.4, 0.6, 0.8)
        }
        observed_conditions = {
            (row.get("workload"), row.get("policy"), row.get("store"), row.get("frac"),
             row.get("trial"), row.get("engine"), row.get("backend"), row.get("ranks"))
            for row in rows if isinstance(row, dict)
        }
        if (len(rows) != expected or document.get("summary", {}).get("overall", {}).get("n_checks") != expected
                or observed_conditions != expected_conditions):
            return "artifact-incomplete", document.get("kind")
        # Trial errors/unknown ETA remain measured rows, not driver failures.
        return "complete", document.get("kind")
    except (OSError, json.JSONDecodeError, TypeError, AttributeError):
        return "artifact-invalid", None


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--case", choices=case_names(), help="run exactly one independent case")
    parser.add_argument("--list-cases", action="store_true")
    parser.add_argument("--root", type=Path, default=ROOT_DEFAULT,
                        help="test root containing wave3-performance-context-v3")
    parser.add_argument("--timeout", type=int, default=TIMEOUT_SECONDS,
                        help="hard timeout in seconds for this case only")
    parser.add_argument("--image", default=IMAGE)
    args = parser.parse_args(argv)
    if args.list_cases:
        print(json.dumps({"cases": list(case_names()), "heldout_workloads": list(REPRESENTATIVE_HELDOUT),
                          "omitted_workloads": list(OMITTED_HELDOUT), "warmups": WARMUPS,
                          "measured_pairs": PAIRS, "seed": SEED, "long_layers": LONG_LAYERS}, indent=2))
        return 0
    if not args.case:
        parser.error("--case is required unless --list-cases is used")
    if args.timeout < 1:
        parser.error("--timeout must be positive")

    context = args.root / "wave3-performance-context-v3"
    output_root = args.root / "wave3-performance-output-v3"
    scratch_root = args.root / "wave3-performance-scratch-v3"
    for required in (context / "src", context / "tests", context / "examples"):
        if not required.is_dir():
            parser.error(f"missing immutable source context directory: {required}")
    hashes = source_hashes(context)
    hash_text = "\n".join(hashes) + "\n"
    manifest_data = manifest(args.case, context, args.timeout,
                             hashlib.sha256(hash_text.encode()).hexdigest(), image=args.image)
    case_output, attempt = next_attempt(output_root, args.case)
    output = case_output / "output"
    scratch = scratch_root / args.case / f"attempt-{attempt:02d}"
    output.mkdir()
    scratch.mkdir(parents=True, exist_ok=True)
    (case_output / "case-manifest.json").write_text(json.dumps(manifest_data, indent=2) + "\n",
                                                    encoding="utf-8")
    (case_output / "source-hashes.txt").write_text(hash_text, encoding="utf-8")
    status_path = case_output / "status.json"
    started = time.time()
    status = {"case": args.case, "attempt": attempt, "status": "running",
              "started_unix_seconds": started, "manifest": manifest_data}
    status_path.write_text(json.dumps(status, indent=2) + "\n", encoding="utf-8")
    container_name = f"ddq-p4-v3-{args.case}-a{attempt:02d}"
    cmd, result_path = command_for(args.case, context=context, scratch=scratch,
                                   output=output, timeout_seconds=args.timeout, image=args.image,
                                   container_name=container_name)
    outer = ["timeout", "--signal=TERM", "--kill-after=30s", str(args.timeout), *cmd]
    status["command"] = outer
    status["container_name"] = container_name
    status["expected_result_in_container"] = result_path
    with (case_output / "driver.log").open("w", encoding="utf-8") as log:
        return_code, launch_error, cleanup = run_with_container_cleanup(
            outer, container_name=container_name, log=log)
    if launch_error is not None:
        status["launch_error"] = launch_error
    status["container_cleanup"] = cleanup
    artifact_status, artifact_kind = classify_artifact(args.case, output, PAIRS)
    status.update({"return_code": return_code,
                   "elapsed_seconds": time.time() - started,
                   "artifact_status": artifact_status,
                   "artifact_kind_or_campaign_status": artifact_kind})
    if return_code == 124:
        status["status"] = "timeout"
    elif return_code in (137, 143):
        status["status"] = "terminated-or-killed"
    elif return_code != 0:
        status["status"] = "driver-failed"
    else:
        status["status"] = artifact_status
    if not cleanup["verified_absent"]:
        status["execution_status"] = status["status"]
        status["status"] = "cleanup-failed"
    status_path.write_text(json.dumps(status, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(status, indent=2))
    return 0 if status["status"] in ("complete", "complete-with-failed-pairs") else 1


if __name__ == "__main__":
    raise SystemExit(main())
