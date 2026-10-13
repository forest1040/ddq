import json
from pathlib import Path

from svsim.cli import svbench_main, svsim_main
from svsim.profile_document import ProfileDocument


def setup(tmp_path):
    workload = tmp_path / 'workload.json'
    workload.write_text(json.dumps({'kind':'workload','schema_version':1,'qubits':4,
                                    'operations':[{'kind':'H','targets':[0]}]}))
    profile = tmp_path / 'profile.json'
    ProfileDocument.create({}, {}, {'ranks':1,'nodes':1,'threads':1}).save(profile)
    return workload, profile


def test_estimate_writes_partial_report_and_has_explicit_exit_codes(tmp_path):
    workload, profile = setup(tmp_path)
    output = tmp_path / 'estimate.json'
    args = ['estimate','--workload',str(workload),'--profile',str(profile),
            '--output',str(output),'--block-bits','2','--ram-budget-per-rank',str(1<<30),
            '--ram-budget-per-host',str(1<<32),'--disk-budget-per-host',str(1<<30)]
    assert svsim_main(args) == 5
    report = json.loads(output.read_text())
    assert report['capacity_feasible'] is True
    assert report['time']['expected_seconds'] is None
    assert svsim_main(args) == 2  # no implicit overwrite
    assert svsim_main(args + ['--overwrite','--planning-only']) == 0
    assert set(p.name for p in tmp_path.iterdir()) == {'workload.json','profile.json','estimate.json'}


def test_invalid_workload_creates_no_output_and_register_roundtrips(tmp_path):
    workload, _ = setup(tmp_path)
    registered = tmp_path / 'registered.json'
    assert svsim_main(['register','--input',str(workload),'--output',str(registered)]) == 0
    data = json.loads(registered.read_text())
    assert data['workload_id']
    workload.write_text('{"kind":"workload","kind":"workload"}')
    bad = tmp_path / 'bad.json'
    assert svsim_main(['register','--input',str(workload),'--output',str(bad)]) == 2
    assert not bad.exists()


def test_probe_is_read_only_and_all_requires_explicit_limits(tmp_path, monkeypatch):
    import svsim.cli as cli
    def forbidden(*args, **kwargs): raise AssertionError('probe ran a benchmark')
    monkeypatch.setattr(cli, 'run_calibration', forbidden)
    output = tmp_path / 'probe.json'
    assert svbench_main(['probe','--output',str(output),'--storage-path',str(tmp_path)]) == 0
    document = ProfileDocument.load(output)
    assert document.calibrations == ()
    assert document.data['software']['mpi_initialized'] is False
    assert document.data['provenance']['uv_lock_sha256']
    import pytest
    with pytest.raises(SystemExit) as exc:
        svbench_main(['all','--scratch',str(tmp_path),'--output',str(output)])
    assert exc.value.code == 2


def test_real_bounded_cli_campaign_and_raw_trial_manifest(tmp_path):
    import hashlib
    output = tmp_path / 'measured.json'
    scratch = tmp_path / 'scratch'
    assert svbench_main(['all','--scratch',str(scratch),'--output',str(output),
                         '--block-bits','3','--ram-budget','1000000','--disk-budget','32768',
                         '--wall-seconds','5','--repetitions','2']) == 0
    profile = ProfileDocument.load(output)
    assert len(profile.calibrations) == 10
    manifest = json.loads(Path(str(output)+'.run.json').read_text())
    assert manifest['kind'] == 'benchmark-run'
    assert manifest['raw_trials']['sha256'] == hashlib.sha256(output.read_bytes()).hexdigest()
    assert manifest['profile_id'] == profile.profile_id
    assert not list(scratch.iterdir())
