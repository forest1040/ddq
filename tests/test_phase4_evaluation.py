import hashlib
import json

import pytest

from svsim.journal import canonical
from svsim.phase4_evaluation import Campaign, CaseManifest, Trial, Variant, paired_order


def case(**overrides):
    values = dict(case_id="small-1", source={"sha256": "a" * 64},
                  initial_state={"sha256": "b" * 64}, binding={"theta": 0.25},
                  exact_witness={"id": "w1"}, observable={"z": 0}, endpoint="durable-result",
                  n=8, b=4, ranks=2, resources={"ram_bytes": 4096, "disk_bytes": 8192},
                  backend="buffered", build={"revision": "deadbeef"}, seed=42,
                  tolerances={"rtol": 1e-8, "atol": 1e-10}, warmups=2, trials=7,
                  budget_seconds=30.0)
    values.update(overrides)
    return CaseManifest(**values)


def variants():
    return (Variant("base", {"runtime": "sync", "policy": "S0"}),
            Variant("candidate", {"runtime": "sync", "policy": "S1"}))


def measured(variant, index, seconds, *, valid=True, manifest=None, scope="primary-e2e",
            ranks=None, metrics=None):
    raw = {"trial": index, "wall_seconds": seconds, "scope": scope,
           "case_manifest_hash": (manifest or case()).manifest_hash,
           "variant_id": variant, "trial_index": index}
    return Trial.completed(variant, index, seconds, raw,
                           rank_wall_seconds=tuple(ranks if ranks is not None else [seconds] * (manifest or case()).ranks),
                           metrics=metrics or {}, validator_passed=valid)


def test_report_uses_only_hash_verified_valid_matched_pairs_and_is_deterministic():
    manifest = case(trials=9)
    rows = []
    for i, (a, b) in enumerate(zip((10, 11, 9, 10, 12, 8, 11), (8, 9, 8, 9, 10, 7, 9))):
        rows.extend((measured("base", i, a, manifest=manifest,
                              metrics={"execute_seconds": a * .6, "managed_peak_bytes_max": 100}),
                     measured("candidate", i, b, manifest=manifest,
                              metrics={"execute_seconds": b * .6, "managed_peak_bytes_max": 90})))
    # A completed but validator-rejected result and unmatched candidate are retained,
    # while neither can contribute to the paired benefit summary.
    rows.extend((measured("base", 7, 1, valid=False, manifest=manifest),
                 measured("candidate", 8, 1, manifest=manifest)))
    campaign = Campaign(manifest, variants(), tuple(rows))
    first = campaign.compare(bootstrap_samples=500).to_json()
    second = campaign.compare(bootstrap_samples=500).to_json()
    assert first == second
    report = json.loads(first)
    comparison = report["comparisons"][0]
    assert comparison["matched_pairs"] == 7
    assert comparison["claim"] == "unconfirmed"
    assert comparison["difference"]["median"] < 0
    assert report["extrapolation"] == "none"
    assert report["rows"][0]["statuses"]["completed"] == 8
    assert report["rows"][0]["statuses"]["validation_failed"] == 1
    assert report["rows"][0]["status"] == "failed"
    paired_metrics = comparison["paired_metrics"]
    assert paired_metrics["execute_seconds"]["difference"]["median"] < 0
    assert paired_metrics["managed_peak_bytes_max"]["difference"]["median"] < 0


def test_raw_hash_validator_and_variant_axis_guardrails():
    raw = {"measurement": 1}
    with pytest.raises(ValueError, match="raw_sha256"):
        Trial("base", 0, "completed", wall_seconds=1, validator_passed=True,
              raw=raw, raw_sha256="0" * 64)
    with pytest.raises(ValueError, match="exactly one"):
        Campaign(case(), variants()[:1] + (Variant("bad", {"runtime": "async", "policy": "S1"}),))
    with pytest.raises(ValueError, match="finite"):
        case(budget_seconds=float("inf"))


def test_pair_schedule_is_seeded_and_balances_first_position():
    vs = variants()
    a = paired_order(vs, 7, 11)
    assert a == paired_order(vs, 7, 11)
    firsts = [a[2 * i][0] for i in range(7)]
    assert firsts.count("base") in (3, 4)
    assert all(firsts[i] != firsts[i + 1] for i in range(len(firsts) - 1))
    three = (variants()[0], variants()[1], Variant("third", {"runtime": "sync", "policy": "S2"}))
    triple_order = paired_order(three, 9, 11)
    firsts = [triple_order[3 * i][0] for i in range(9)]
    assert {name: firsts.count(name) for name in ("base", "candidate", "third")} == {
        "base": 3, "candidate": 3, "third": 3}


def test_budget_and_campaign_row_bounds():
    with pytest.raises(ValueError, match="trial count"):
        case(trials=101)
    with pytest.raises(ValueError, match="16 variants"):
        Campaign(case(), tuple(Variant(str(i), {"policy": str(i)}) for i in range(17)))
    assert case(warmups=100).warmups == 100
    with pytest.raises(ValueError, match="invalid dimensions"):
        case(warmups=101)


def test_unsupported_and_insufficient_budget_rows_are_preserved():
    manifest = case(trials=1)
    rows = (Trial("base", 0, "unsupported", reason="route unavailable"),
            Trial("candidate", 0, "insufficient-budget", reason="disk preflight"))
    report = Campaign(manifest, variants(), rows).compare(bootstrap_samples=100).to_dict()
    assert [row["status"] for row in report["rows"]] == ["unsupported", "insufficient-budget"]
    assert report["comparisons"][0]["matched_pairs"] == 0


def test_raw_pair_identity_and_trial_budget_are_enforced():
    wrong = {"trial": 0, "wall_seconds": 2, "scope": "primary-e2e",
             "case_manifest_hash": "wrong", "variant_id": "base", "trial_index": 0}
    item = Trial("base", 0, "completed", wall_seconds=2, validator_passed=True,
                 raw=wrong, raw_sha256=hashlib.sha256(canonical(wrong).encode()).hexdigest())
    with pytest.raises(ValueError, match="identity"):
        Campaign(case(), variants(), (item,))
    too_slow = measured("base", 0, 3601)
    failed = Campaign(case(), variants(), (too_slow,)).compare(bootstrap_samples=100).to_dict()
    assert failed["rows"][0]["status"] == "failed"


def test_completed_raw_hash_covers_measurements_and_direct_trial_mismatch_fails_validation():
    manifest = case(trials=1)
    raw = {"case_manifest_hash": manifest.manifest_hash, "variant_id": "base",
           "trial_index": 0, "scope": "primary-e2e", "measurement": {
               "wall_seconds": 1.0, "rank_wall_seconds": [1.0, 1.0],
               "metrics": {"execute_seconds": .25}, "validator_passed": True}}
    good = Trial.completed("base", 0, 1, {k: v for k, v in raw.items() if k != "measurement"},
                           rank_wall_seconds=(1, 1), metrics={"execute_seconds": .25})
    assert good.raw["measurement"]["wall_seconds"] == 1
    assert good.raw_sha256 == hashlib.sha256(canonical(good.raw).encode()).hexdigest()
    mismatch = dict(raw)
    mismatch["measurement"] = {**raw["measurement"], "metrics": {"execute_seconds": .5}}
    bad = Trial("base", 0, "completed", wall_seconds=1, rank_wall_seconds=(1, 1),
                metrics={"execute_seconds": .25}, validator_passed=True,
                raw=mismatch, raw_sha256=hashlib.sha256(canonical(mismatch).encode()).hexdigest())
    report = Campaign(manifest, variants(), (bad,)).compare(bootstrap_samples=100).to_dict()
    assert report["rows"][0]["status"] == "failed"
    assert "raw-measurement-mismatch-or-missing" in report["rows"][0]["trials"][0]["validation_errors"]


def test_paired_metric_zero_baseline_and_overflowing_ratio_are_finite():
    manifest = case(trials=2, ranks=1, budget_seconds=1e308)
    rows = (
        measured("base", 0, 1, manifest=manifest, ranks=(1,), metrics={"zero": 0, "tiny": 1e-308}),
        measured("candidate", 0, 2, manifest=manifest, ranks=(2,), metrics={"zero": 1, "tiny": 1e308}),
        measured("base", 1, 1, manifest=manifest, ranks=(1,), metrics={"zero": 0, "tiny": 1e-308}),
        measured("candidate", 1, 2, manifest=manifest, ranks=(2,), metrics={"zero": 1, "tiny": 1e308}),
    )
    comparison = Campaign(manifest, variants(), rows).compare(bootstrap_samples=100).to_dict()["comparisons"][0]
    assert comparison["paired_metrics"]["zero"]["ratio"] is None
    assert comparison["paired_metrics"]["tiny"]["ratio"] is None
    assert comparison["paired_metrics"]["tiny"]["difference"]["median"] == 1e308


def test_manifest_and_variant_defensively_freeze_input_mappings():
    source = {"hash": "abc", "nested": {"values": [1, 2]}}
    manifest = case(source=source)
    source["nested"]["values"].append(3)
    assert manifest.source["nested"]["values"] == (1, 2)
    with pytest.raises(TypeError, match="immutable"):
        manifest.source["hash"] = "changed"
    with pytest.raises(TypeError, match="immutable"):
        manifest.source |= {"new": "value"}


def test_dimensions_scope_rank_max_resource_budget_and_validation_status():
    with pytest.raises(ValueError, match="dimensions"):
        case(n=5, b=6)
    manifest = case(trials=1, resources={"ram_per_rank_bytes": 100,
                                         "disk_per_rank_bytes": 100})
    base = measured("base", 0, 1, manifest=manifest, metrics={
        "managed_peak_bytes_max": 100, "scratch_bytes_max": 10})
    candidate = measured("candidate", 0, .5, manifest=manifest, scope="steady-state",
                         metrics={"managed_peak_bytes_max": 50, "scratch_bytes_max": 10})
    report = Campaign(manifest, variants(), (base, candidate)).compare(bootstrap_samples=100).to_dict()
    cmp = report["comparisons"][0]
    assert cmp["matched_pairs"] == 0
    assert cmp["scope_mismatches"] == 1
    assert cmp["claim"] == "unconfirmed"
    over = measured("base", 0, 1, manifest=manifest, metrics={
        "managed_peak_bytes_max": 201, "scratch_bytes_max": 10})
    failed_report = Campaign(manifest, variants(), (over,)).compare(bootstrap_samples=100).to_dict()
    base_row = failed_report["rows"][0]
    assert base_row["status"] == "failed"
    assert "managed-memory-budget-exceeded" in base_row["trials"][0]["validation_errors"]

    bad_max = measured("base", 0, 1, manifest=manifest, ranks=(0.9, 0.95))
    assert Campaign(manifest, variants(), (bad_max,)).compare(bootstrap_samples=100).to_dict()["rows"][0]["status"] == "failed"
    near_max = measured("base", 0, 1, manifest=manifest, ranks=(1, 1 + 5e-10),
                        metrics={"managed_peak_bytes_max": 100, "scratch_bytes_max": 100})
    assert Campaign(manifest, variants(), (near_max,)).compare(bootstrap_samples=100).to_dict()["rows"][0]["status"] == "completed"


def test_boundary_metrics_report_size_and_unmeasured_rows():
    with pytest.raises(ValueError, match="rank wall seconds"):
        Trial("base", 0, "failed", rank_wall_seconds=(float("nan"),))
    with pytest.raises(ValueError, match="raw exceeds size limit"):
        Trial.completed("base", 0, 1, {"oversized": "x" * 20_000})
    manifest = case(trials=1)
    report = Campaign(manifest, variants(), ()).compare(bootstrap_samples=100).to_dict()
    assert all(row["status"] == "unmeasured" and row["statuses"]["unmeasured"] == 1
               for row in report["rows"])


def test_finite_arithmetic_overflow_cannot_escape_into_json():
    manifest = case(trials=1, budget_seconds=1e308, ranks=1)
    first = measured("base", 0, 1e-308, manifest=manifest, ranks=(1e-308,))
    # Finite inputs whose ratio overflows still yield finite, valid JSON.
    second = measured("candidate", 0, 1e308, manifest=manifest, ranks=(1e308,))
    report = Campaign(manifest, variants(), (first, second)).compare(bootstrap_samples=100).to_json()
    assert '"ratio":null' in report
    json.loads(report)


def test_small_sample_and_zero_crossing_are_unconfirmed():
    manifest = case(trials=2)
    rows = (measured("base", 0, 10, manifest=manifest),
            measured("candidate", 0, 9, manifest=manifest),
            measured("base", 1, 10, manifest=manifest),
            measured("candidate", 1, 11, manifest=manifest))
    comparison = Campaign(manifest, variants(), rows).compare(bootstrap_samples=500).to_dict()["comparisons"][0]
    assert comparison["claim"] == "unconfirmed"
    assert "small sample; interval is diagnostic" in comparison["diagnostics"]
    assert "interval crosses zero" in comparison["diagnostics"]
