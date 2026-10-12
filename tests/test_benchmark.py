from svsim.benchmark import MockTransport, run_benchmarks
from svsim.lifecycle import BenchmarkCondition
from svsim.telemetry import storage_condition, storage_device_id
from svsim.transport import StorageFile


def test_run_benchmarks_returns_measured_conditions_and_removes_scratch(tmp_path):
    report = run_benchmarks(tmp_path, block_bytes=1024, samples=2, ram_copy_bytes=512)
    assert report.profile.benchmarks == report.measurements
    assert {s.condition.performance_class for s in report.measurements} == {
        "ram_copy", "storage-read", "storage-write",
    }
    assert all(s.condition.io_depth == 1 and s.sample_count == 2 for s in report.measurements)
    assert report.warmup_count == 1
    storage_samples = [s for s in report.measurements if s.condition.performance_class.startswith("storage-")]
    for sample in storage_samples:
        condition = sample.condition
        runtime_condition = storage_condition(
            condition.details["operation"], condition.block_bytes,
            backend=condition.details["backend"], device_id=storage_device_id(tmp_path),
            pattern=condition.details["pattern"], alignment=condition.details["alignment"],
            flush="deferred", concurrency=1,
        )
        assert report.profile.matching_benchmark(runtime_condition) is sample
        assert condition.details["warmup"] is None
        assert condition.details["flush"] == "deferred"
    assert not list(tmp_path.glob("svsim-bench-*.bin"))


def test_mock_transport_tracks_virtual_durations_without_sleep(tmp_path):
    path = tmp_path / "data.bin"
    with StorageFile(path, "w+b") as raw:
        mock = MockTransport(raw, bytes_per_second=100, delay_multipliers=(1, 3))
        assert mock.write_from(0, b"abcd") == 4
        target = bytearray(4)
        assert mock.read_into(0, target) == 4
        assert target == b"abcd"
        assert mock.last_duration_seconds == 0.12
        assert mock.virtual_elapsed_seconds == 0.16


def test_mock_buffer_methods_delegate_without_read_or_write_allocation(tmp_path, monkeypatch):
    path = tmp_path / "data.bin"
    with StorageFile(path, "w+b") as raw:
        mock = MockTransport(raw)
        monkeypatch.setattr(raw, "read", lambda *_: (_ for _ in ()).throw(AssertionError("read allocates")))
        monkeypatch.setattr(raw, "write", lambda *_: (_ for _ in ()).throw(AssertionError("write allocates")))
        assert mock.write_from(0, b"abcd") == 4
        target = bytearray(4)
        assert mock.read_into(0, target) == 4
        assert target == b"abcd"
