import json

import pytest

from svsim.development_runner import JobSpec
from svsim.pbs_adapter import PBSAdapter, PBSAllocation, PBSNodeAllocation


def test_renders_resources_and_metadata_without_submit_api(tmp_path):
    spec = JobSpec("r1", "sha", ("python", "run.py", "a b"), ranks=4,
                   rank_ram_bytes=1 << 30, rank_disk_bytes=1 << 30, threads=2,
                   walltime_seconds=65, test_case="mpi-small")
    alloc = PBSAllocation((
        PBSNodeAllocation("n1", 4, 8 << 30, "/scratch/n1", 8 << 30),
        PBSNodeAllocation("n2", 4, 8 << 30, "/scratch/n2", 8 << 30),
    ), queue="workq")
    adapter = PBSAdapter()
    rendered = adapter.render(spec, alloc)
    assert "#PBS -l select=1:ncpus=4:mpiprocs=2:mem=2gb+1:ncpus=4:mpiprocs=2:mem=2gb" in rendered.script
    assert "'a b'" in rendered.script
    assert "qsub" not in rendered.script
    assert 'cd "$SVSIM_JOB_SCRATCH"' in rendered.script
    assert 'PBS_O_WORKDIR' not in rendered.script
    assert not hasattr(adapter, "submit")
    assert rendered.metadata["placement_estimate"] == [{"node": "n1", "ranks": 2}, {"node": "n2", "ranks": 2}]
    assert rendered.metadata["host_placement_guaranteed"] is False
    adapter.write(rendered, tmp_path / "job.pbs", tmp_path / "job.json")
    assert json.loads((tmp_path / "job.json").read_text())["revision"] == "r1"


@pytest.mark.parametrize("allocation", [
    PBSAllocation(()),
    PBSAllocation((PBSNodeAllocation("offline", 8, 8 << 30, "/scratch", 8 << 30, online=False),)),
    PBSAllocation((PBSNodeAllocation("tiny", 1, 1 << 20, "/scratch", 1 << 20),)),
])
def test_rejects_invalid_or_insufficient_allocation(allocation):
    spec = JobSpec("r", "h", ("python", "x"), ranks=2,
                   rank_ram_bytes=1 << 30, rank_disk_bytes=1 << 30)
    with pytest.raises(ValueError):
        PBSAdapter().render(spec, allocation)


def test_rejects_unsafe_queue_and_relative_scratch():
    spec = JobSpec("r", "h", ("true",))
    with pytest.raises(ValueError):
        PBSAdapter().render(spec, PBSAllocation((PBSNodeAllocation("n", 2, 1 << 30,
                                                                   "/scratch", 1 << 30),), "x;bad"))
    with pytest.raises(ValueError):
        PBSAdapter().render(spec, PBSAllocation((PBSNodeAllocation("n", 2, 1 << 30,
                                                                   "relative", 1 << 30),)))


def test_rejects_launcher_hosts_outside_scheduler_allocation():
    job = JobSpec("r", "h", ("python", "x"), ranks=2,
                  launcher_argv=("mpiexec", "-n", "2", "--host", "elsewhere"))
    allocation = PBSAllocation((PBSNodeAllocation("n", 4, 4 << 30, "/scratch", 4 << 30),))
    with pytest.raises(ValueError, match="explicit hosts"):
        PBSAdapter().render(job, allocation)
