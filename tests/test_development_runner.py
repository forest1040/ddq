import json
import importlib.util
import os
import shutil
import sys
import time

import pytest

from svsim.development_runner import JobSpec, JobState, LocalDevelopmentRunner


@pytest.fixture(autouse=True)
def _isolate_default_scratch(tmp_path, monkeypatch):
    """Keep JobSpec's default relative scratch inside a writable test directory."""
    cwd = tmp_path / "cwd"
    cwd.mkdir()
    monkeypatch.chdir(cwd)


def spec(code, **kwargs):
    return JobSpec("rev-a", "lock-a", (sys.executable, "-c", code), **kwargs)


def test_runs_and_writes_bounded_report(tmp_path):
    with LocalDevelopmentRunner(tmp_path, max_ram_bytes=100, max_disk_bytes=100,
                                max_report_bytes=8) as runner:
        rec = runner.submit(spec("print('hello from child')", rank_ram_bytes=10,
                                 rank_disk_bytes=10, test_case="smoke")).wait(5)
    assert rec.state is JobState.COMPLETED
    assert rec.exit_code == 0
    assert open(rec.stdout_path, "rb").read() == b"hello fr"
    report = json.loads(open(rec.report_path).read())
    assert report["test_case"] == "smoke"
    assert report["state"] == "COMPLETED"


def test_walltime_and_nonzero_exit_are_failed(tmp_path):
    with LocalDevelopmentRunner(tmp_path, max_ram_bytes=100, max_disk_bytes=100,
                                terminate_grace_seconds=.1) as runner:
        failed = runner.submit(spec("raise SystemExit(7)", walltime_seconds=3)).wait(5)
        timed = runner.submit(spec("import time; time.sleep(5)", walltime_seconds=.1)).wait(5)
    assert failed.state is JobState.FAILED and failed.exit_code == 7
    assert timed.state is JobState.FAILED and timed.timed_out


def test_cancel_queued_job_and_resource_admission(tmp_path):
    with LocalDevelopmentRunner(tmp_path, max_ram_bytes=10, max_disk_bytes=10,
                                max_concurrent_jobs=1) as runner:
        first = runner.submit(spec("import time; time.sleep(.3)", host_ram_bytes=10, host_disk_bytes=10))
        second = runner.submit(spec("print('should not run')", host_ram_bytes=10, host_disk_bytes=10))
        assert second.cancel()
        assert second.wait(2).state is JobState.CANCELLED
        assert first.wait(3).state is JobState.COMPLETED


def test_rejects_shell_metacharacters_as_argv_not_shell(tmp_path):
    payload = "x; touch SHOULD_NOT_EXIST"
    with LocalDevelopmentRunner(tmp_path, max_ram_bytes=10, max_disk_bytes=10) as runner:
        rec = runner.submit(JobSpec("r", "h", (sys.executable, "-c", "import sys; print(sys.argv[1])", payload))).wait(5)
    assert rec.state is JobState.COMPLETED
    assert open(rec.stdout_path).read().strip() == payload
    assert not (tmp_path / "SHOULD_NOT_EXIST").exists()


def test_uses_declared_scratch_root_and_checks_resource_and_mpi_counts(tmp_path):
    import pytest
    scratch = tmp_path / "scratch"
    code = "import os; print(os.environ['SVSIM_JOB_SCRATCH'])"
    with LocalDevelopmentRunner(tmp_path / "reports", max_ram_bytes=10, max_disk_bytes=10) as runner:
        rec = runner.submit(spec(code, scratch_root=str(scratch))).wait(5)
    assert rec.state is JobState.COMPLETED
    scratch_path = open(rec.stdout_path).read().strip()
    assert scratch_path.startswith(str(scratch.resolve()))
    assert os.path.isdir(scratch_path)
    with pytest.raises(ValueError, match="host_ram_bytes"):
        JobSpec("r", "h", ("true",), ranks=2, rank_ram_bytes=10, host_ram_bytes=19)
    with pytest.raises(ValueError, match="rank count"):
        JobSpec("r", "h", ("true",), ranks=2, launcher_argv=("mpiexec", "-n", "3"))


def test_descendant_inherited_pipes_do_not_hold_worker_past_walltime(tmp_path):
    code = "import subprocess,sys; subprocess.Popen([sys.executable,'-c','import time; time.sleep(10)'])"
    started = time.monotonic()
    with LocalDevelopmentRunner(tmp_path, max_ram_bytes=10, max_disk_bytes=10) as runner:
        rec = runner.submit(spec(code, walltime_seconds=.3)).wait(3)
    assert time.monotonic() - started < 2
    assert rec.state is JobState.COMPLETED


def test_two_rank_mpi_when_available(tmp_path):
    launcher = shutil.which("mpiexec") or shutil.which("mpirun")
    if not launcher or importlib.util.find_spec("mpi4py") is None:
        import pytest
        pytest.skip("local MPI launcher and mpi4py are not both installed")
    env = {"OMPI_ALLOW_RUN_AS_ROOT": "1", "OMPI_ALLOW_RUN_AS_ROOT_CONFIRM": "1"}
    mpi_code = "from mpi4py import MPI; print(f'rank={MPI.COMM_WORLD.Get_rank()} size={MPI.COMM_WORLD.Get_size()}')"
    job = JobSpec("local", "lock", (sys.executable, "-c", mpi_code), ranks=2,
                  rank_ram_bytes=1, rank_disk_bytes=1,
                  launcher_argv=(launcher, "-n", "2"), env=env, walltime_seconds=30)
    with LocalDevelopmentRunner(tmp_path, max_ram_bytes=2, max_disk_bytes=2) as runner:
        rec = runner.submit(job).wait(35)
    assert rec.state is JobState.COMPLETED
    output = open(rec.stdout_path).read()
    assert "rank=0 size=2" in output and "rank=1 size=2" in output
