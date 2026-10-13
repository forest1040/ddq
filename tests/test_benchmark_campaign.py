from svsim.benchmark_campaign import BenchmarkOptions, run_calibration
from svsim.profile_document import probe_profile


def test_campaign_records_raw_ram_and_kernel_trials_and_keeps_profile_immutable(tmp_path):
    original = probe_profile()
    result = run_calibration(original, tmp_path, BenchmarkOptions(
        block_bits=3, repetitions=2, warmup=1, ram_budget_bytes=1_000_000,
        disk_budget_bytes=32_768, walltime_seconds=5, suites=("ram", "kernel"), seed=7,
    ))
    assert result.profile_id != original.profile_id
    assert original.calibrations == ()
    rows = result.calibrations
    assert {row["operation"] for row in rows} >= {
        "ram-copy", "kernel-h", "kernel-rx", "kernel-pauli-rotation",
    }
    assert all(row["sample_count"] == 2 and len(row["trials"]) == 2 for row in rows)
    assert all(row["trials"][0]["count"] == 1 for row in rows)
    assert all(row["evidence"] == "measured" and row["quality"] == "validated" for row in rows)
    rotation = next(row for row in rows if row["operation"] == "kernel-pauli-rotation")
    assert rotation["condition"]["paulis"] == ["Y", "X", "Z"]
    assert [row["condition"]["x_mask"] for row in rows if row["operation"] == "kernel-pauli-inner"] == [3, 5]
    assert all(row["condition"]["z_mask"] == 5 for row in rows if row["operation"] == "kernel-pauli-inner")
    assert not list(tmp_path.iterdir())


def test_campaign_storage_respects_budget_and_cleans_its_file(tmp_path):
    profile = probe_profile()
    result = run_calibration(profile, tmp_path, BenchmarkOptions(
        block_bits=3, repetitions=2, warmup=0, ram_budget_bytes=100_000,
        disk_budget_bytes=16_384, walltime_seconds=5, suites=("storage",),
    ))
    assert {row["operation"] for row in result.calibrations} == {
        "storage-read", "storage-write", "storage-fsync",
    }
    assert not list(tmp_path.iterdir())


def test_campaign_rejects_ram_preflight_before_allocating(tmp_path):
    profile = probe_profile()
    try:
        run_calibration(profile, tmp_path, BenchmarkOptions(
            block_bits=6, repetitions=1, warmup=0, ram_budget_bytes=100,
            disk_budget_bytes=4096, suites=("kernel",),
        ))
    except ValueError as exc:
        assert "RAM preflight" in str(exc)
    else:
        raise AssertionError("expected conservative RAM preflight to reject the campaign")
    assert not list(tmp_path.iterdir())


def test_storage_preflight_rejects_truncated_block(tmp_path):
    profile = probe_profile()
    try:
        run_calibration(profile, tmp_path, BenchmarkOptions(
            block_bits=9, repetitions=1, warmup=0, ram_budget_bytes=1_000_000,
            disk_budget_bytes=4096, suites=("storage",),
        ))
    except ValueError as exc:
        assert "storage preflight needs" in str(exc)
    else:
        raise AssertionError("expected requested block to exceed disk budget")
    assert not list(tmp_path.iterdir())


def test_options_bound_raw_trial_growth_and_validate_suite_combinations(tmp_path):
    try:
        BenchmarkOptions(repetitions=1001)
    except ValueError as exc:
        assert "trial counts" in str(exc)
    else:
        raise AssertionError("expected repetitions cap")
    try:
        BenchmarkOptions(suites=("mixed", "kernel"))
    except ValueError as exc:
        assert "requires both kernel and storage" in str(exc)
    else:
        raise AssertionError("expected mixed suite requirements")

    profile = probe_profile()
    try:
        run_calibration(profile, tmp_path, BenchmarkOptions(
            block_bits=3, repetitions=1000, warmup=0, ram_budget_bytes=100_000,
            disk_budget_bytes=4096, suites=("ram",),
        ))
    except ValueError as exc:
        assert "including raw trials" in str(exc)
    else:
        raise AssertionError("expected raw trial memory reserve to reject campaign")
    assert not list(tmp_path.iterdir())
