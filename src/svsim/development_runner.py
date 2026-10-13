"""Bounded, single-host development job runner.

The runner launches argv directly (never through a shell) and is intentionally
separate from the simulator's runtime scheduler.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
from enum import Enum
import json
import os
from pathlib import Path
import queue
import signal
import subprocess
import threading
import time
import uuid
from typing import Mapping, Sequence


class JobState(str, Enum):
    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


@dataclass(frozen=True)
class JobSpec:
    revision: str
    lock_hash: str
    argv: tuple[str, ...]
    ranks: int = 1
    rank_ram_bytes: int = 0
    host_ram_bytes: int = 0
    rank_disk_bytes: int = 0
    host_disk_bytes: int = 0
    threads: int = 1
    walltime_seconds: float = 3600
    scratch_root: str = "."
    scratch_policy: str = "job-directory"
    test_case: str = "unspecified"
    launcher_argv: tuple[str, ...] = ()
    cwd: str | None = None
    env: Mapping[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.revision.strip() or not self.lock_hash.strip():
            raise ValueError("revision and lock_hash are required")
        if not self.argv or any(not isinstance(x, str) or not x for x in self.argv):
            raise ValueError("argv must contain non-empty strings")
        if self.ranks < 1 or self.threads < 1 or self.walltime_seconds <= 0:
            raise ValueError("ranks, threads and walltime must be positive")
        for name in ("rank_ram_bytes", "host_ram_bytes", "rank_disk_bytes", "host_disk_bytes"):
            if getattr(self, name) < 0:
                raise ValueError(f"{name} cannot be negative")
        if self.host_ram_bytes and self.rank_ram_bytes and self.host_ram_bytes < self.ranks * self.rank_ram_bytes:
            raise ValueError("host_ram_bytes must cover all rank_ram_bytes allocations")
        if self.scratch_policy != "job-directory":
            raise ValueError("only job-directory scratch policy is supported")
        # MPI launchers receive a direct argv, but a declared rank count must
        # agree with the request so resource reservations describe execution.
        args = self.launcher_argv
        for i, arg in enumerate(args):
            if arg in ("-n", "-np", "--np", "--ntasks"):
                if i + 1 >= len(args) or not args[i + 1].isdigit() or int(args[i + 1]) != self.ranks:
                    raise ValueError("MPI launcher rank count must match ranks")
            elif arg.startswith(("--np=", "--ntasks=")):
                value = arg.partition("=")[2]
                if not value.isdigit() or int(value) != self.ranks:
                    raise ValueError("MPI launcher rank count must match ranks")

    @property
    def command(self) -> tuple[str, ...]:
        return self.launcher_argv + self.argv


@dataclass
class JobRecord:
    job_id: str
    spec: JobSpec
    state: JobState = JobState.QUEUED
    submitted_at: float = field(default_factory=time.time)
    started_at: float | None = None
    finished_at: float | None = None
    exit_code: int | None = None
    timed_out: bool = False
    cancelled: bool = False
    error: str | None = None
    report_path: str | None = None
    stdout_path: str | None = None
    stderr_path: str | None = None


class JobHandle:
    def __init__(self, runner: "LocalDevelopmentRunner", record: JobRecord):
        self._runner, self.job_id = runner, record.job_id

    @property
    def record(self) -> JobRecord:
        return self._runner.get(self.job_id)

    def wait(self, timeout: float | None = None) -> JobRecord:
        return self._runner.wait(self.job_id, timeout)

    def cancel(self) -> bool:
        return self._runner.cancel(self.job_id)


class LocalDevelopmentRunner:
    """A bounded FIFO local queue with per-host resource reservations."""
    _DONE = {JobState.COMPLETED, JobState.FAILED, JobState.CANCELLED}

    def __init__(self, report_root: str | os.PathLike[str], *, max_ram_bytes: int,
                 max_disk_bytes: int, max_concurrent_jobs: int = 1,
                 terminate_grace_seconds: float = 2.0, max_report_bytes: int = 1_000_000):
        if min(max_ram_bytes, max_disk_bytes, max_concurrent_jobs) <= 0:
            raise ValueError("resource limits and concurrency must be positive")
        self.root = Path(report_root).resolve()
        self.root.mkdir(parents=True, exist_ok=True)
        self.max_ram, self.max_disk = max_ram_bytes, max_disk_bytes
        self.max_concurrent = max_concurrent_jobs
        self.grace, self.max_report_bytes = terminate_grace_seconds, max_report_bytes
        self._cv = threading.Condition()
        self._records: dict[str, JobRecord] = {}
        self._pending: queue.Queue[str | None] = queue.Queue()
        self._active: dict[str, subprocess.Popen[bytes]] = {}
        self._reserved_ram = self._reserved_disk = 0
        self._reserved: set[str] = set()
        self._threads = [threading.Thread(target=self._worker, daemon=True) for _ in range(max_concurrent_jobs)]
        for t in self._threads:
            t.start()

    @staticmethod
    def _budget(spec: JobSpec, rank_value: int, host_value: int) -> int:
        return host_value or rank_value * spec.ranks

    def submit(self, spec: JobSpec) -> JobHandle:
        ram = self._budget(spec, spec.rank_ram_bytes, spec.host_ram_bytes)
        disk = self._budget(spec, spec.rank_disk_bytes, spec.host_disk_bytes)
        if ram > self.max_ram or disk > self.max_disk:
            raise ValueError("job resource request exceeds local runner limits")
        if spec.cwd and not Path(spec.cwd).is_dir():
            raise ValueError("job cwd must be an existing directory")
        jid = uuid.uuid4().hex
        rec = JobRecord(jid, spec)
        with self._cv:
            self._records[jid] = rec
            self._pending.put(jid)
            self._cv.notify_all()
        return JobHandle(self, rec)

    def get(self, job_id: str) -> JobRecord:
        with self._cv:
            return self._records[job_id]

    def list_jobs(self) -> list[JobRecord]:
        with self._cv:
            return list(self._records.values())

    def wait(self, job_id: str, timeout: float | None = None) -> JobRecord:
        deadline = None if timeout is None else time.monotonic() + timeout
        with self._cv:
            rec = self._records[job_id]
            while rec.state not in self._DONE:
                remaining = None if deadline is None else deadline - time.monotonic()
                if remaining is not None and remaining <= 0:
                    raise TimeoutError(job_id)
                self._cv.wait(remaining)
            return rec

    def cancel(self, job_id: str) -> bool:
        with self._cv:
            rec = self._records[job_id]
            if rec.state in self._DONE:
                return False
            rec.cancelled = True
            proc = self._active.get(job_id)
            if proc is None:
                rec.state = JobState.CANCELLED
                rec.finished_at = time.time()
                self._release(rec)
                self._write_report(rec)
                self._cv.notify_all()
                return True
        self._signal_group(proc, signal.SIGTERM)
        threading.Thread(target=self._kill_after_grace, args=(proc,), daemon=True).start()
        return True

    def _kill_after_grace(self, proc: subprocess.Popen[bytes]) -> None:
        try:
            proc.wait(timeout=self.grace)
        except subprocess.TimeoutExpired:
            self._signal_group(proc, signal.SIGKILL)

    def _release(self, rec: JobRecord) -> None:
        if rec.job_id not in self._reserved:
            return
        self._reserved.remove(rec.job_id)
        self._reserved_ram -= self._budget(rec.spec, rec.spec.rank_ram_bytes, rec.spec.host_ram_bytes)
        self._reserved_disk -= self._budget(rec.spec, rec.spec.rank_disk_bytes, rec.spec.host_disk_bytes)

    def _worker(self) -> None:
        while True:
            jid = self._pending.get()
            if jid is None:
                return
            with self._cv:
                rec = self._records[jid]
                if rec.state == JobState.CANCELLED:
                    continue
                ram = self._budget(rec.spec, rec.spec.rank_ram_bytes, rec.spec.host_ram_bytes)
                disk = self._budget(rec.spec, rec.spec.rank_disk_bytes, rec.spec.host_disk_bytes)
                while (rec.state != JobState.CANCELLED and
                       (self._reserved_ram + ram > self.max_ram or self._reserved_disk + disk > self.max_disk)):
                    self._cv.wait()
                if rec.state == JobState.CANCELLED:
                    continue
                self._reserved.add(jid)
                self._reserved_ram += ram
                self._reserved_disk += disk
                rec.state, rec.started_at = JobState.RUNNING, time.time()
                self._cv.notify_all()
            self._run(rec)

    @staticmethod
    def _signal_group(proc: subprocess.Popen[bytes], sig: int) -> None:
        try:
            os.killpg(proc.pid, sig)
        except ProcessLookupError:
            pass

    def _run(self, rec: JobRecord) -> None:
        jobdir = self.root / rec.job_id
        scratch_base = Path(rec.spec.scratch_root).expanduser().resolve()
        scratch_dir = scratch_base / rec.job_id
        captured = {"stdout": 0, "stderr": 0}
        writers: list[threading.Thread] = []
        deadline = time.monotonic() + rec.spec.walltime_seconds
        try:
            jobdir.mkdir()
            scratch_base.mkdir(parents=True, exist_ok=True)
            scratch_dir.mkdir()
            rec.stdout_path, rec.stderr_path = str(jobdir / "stdout.log"), str(jobdir / "stderr.log")
            rec.report_path = str(jobdir / "report.json")
            env = os.environ.copy()
            env.update(rec.spec.env)
            env["SVSIM_JOB_SCRATCH"] = str(scratch_dir)
            proc = subprocess.Popen(rec.spec.command, cwd=rec.spec.cwd, env=env,
                                    stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                    start_new_session=True, shell=False)
            with self._cv:
                self._active[rec.job_id] = proc
                if rec.cancelled:
                    self._signal_group(proc, signal.SIGTERM)
            def pump(pipe, name, path):
                with open(path, "wb") as out:
                    while True:
                        chunk = pipe.read(65536)
                        if not chunk:
                            break
                        room = max(0, self.max_report_bytes - captured[name])
                        if room:
                            out.write(chunk[:room])
                            captured[name] += min(room, len(chunk))
                pipe.close()
            for name, pipe, path in (("stdout", proc.stdout, rec.stdout_path), ("stderr", proc.stderr, rec.stderr_path)):
                t = threading.Thread(target=pump, args=(pipe, name, path), daemon=True)
                writers.append(t); t.start()
            try:
                code = proc.wait(timeout=max(0.0, deadline - time.monotonic()))
            except subprocess.TimeoutExpired:
                rec.timed_out = True
                self._signal_group(proc, signal.SIGTERM)
                try:
                    proc.wait(timeout=self.grace)
                except subprocess.TimeoutExpired:
                    self._signal_group(proc, signal.SIGKILL)
                    proc.wait()
                code = proc.returncode
            if rec.cancelled:
                self._signal_group(proc, signal.SIGTERM)
                try:
                    proc.wait(timeout=self.grace)
                except subprocess.TimeoutExpired:
                    self._signal_group(proc, signal.SIGKILL)
                    proc.wait()
            # A descendant may inherit the pipes after the launcher exits.
            # Bound pump joins and close pipes before returning the reservation.
            remaining = max(0.0, deadline - time.monotonic())
            if rec.cancelled:
                remaining = min(remaining, self.grace)
            for t in writers: t.join(timeout=remaining)
            if any(t.is_alive() for t in writers):
                self._signal_group(proc, signal.SIGKILL)
                for pipe in (proc.stdout, proc.stderr):
                    try: pipe.close()
                    except OSError: pass
                for t in writers: t.join(timeout=self.grace)
            if proc.poll() is None:
                self._signal_group(proc, signal.SIGKILL)
                proc.wait()
            code = proc.returncode
            rec.exit_code = code
            rec.state = JobState.CANCELLED if rec.cancelled else (JobState.FAILED if rec.timed_out or code else JobState.COMPLETED)
        except Exception as exc:  # spawn errors are reported like failed jobs
            rec.error = f"{type(exc).__name__}: {exc}"
            rec.state = JobState.CANCELLED if rec.cancelled else JobState.FAILED
        finally:
            rec.finished_at = time.time()
            with self._cv:
                self._active.pop(rec.job_id, None)
                self._release(rec)
                self._write_report(rec)
                self._cv.notify_all()

    def _write_report(self, rec: JobRecord) -> None:
        if not rec.report_path:
            folder = self.root / rec.job_id
            folder.mkdir(exist_ok=True)
            rec.report_path = str(folder / "report.json")
        payload = {"job_id": rec.job_id, "state": rec.state.value, "revision": rec.spec.revision,
                   "lock_hash": rec.spec.lock_hash, "command": list(rec.spec.command),
                   "ranks": rec.spec.ranks, "rank_ram_bytes": rec.spec.rank_ram_bytes,
                   "host_ram_bytes": rec.spec.host_ram_bytes, "rank_disk_bytes": rec.spec.rank_disk_bytes,
                   "host_disk_bytes": rec.spec.host_disk_bytes, "threads": rec.spec.threads,
                   "walltime_seconds": rec.spec.walltime_seconds, "scratch_policy": rec.spec.scratch_policy,
                   "scratch_root": rec.spec.scratch_root,
                   "test_case": rec.spec.test_case, "submitted_at": rec.submitted_at,
                   "started_at": rec.started_at, "finished_at": rec.finished_at,
                   "exit_code": rec.exit_code, "timed_out": rec.timed_out, "cancelled": rec.cancelled,
                   "error": rec.error, "stdout": rec.stdout_path, "stderr": rec.stderr_path}
        Path(rec.report_path).write_text(json.dumps(payload, indent=2) + "\n")

    def close(self, *, cancel: bool = True) -> None:
        if cancel:
            with self._cv:
                ids = [r.job_id for r in self._records.values() if r.state not in self._DONE]
            for jid in ids: self.cancel(jid)
        for _ in self._threads: self._pending.put(None)
        for t in self._threads: t.join()

    def __enter__(self) -> "LocalDevelopmentRunner": return self
    def __exit__(self, *_exc) -> None: self.close()
