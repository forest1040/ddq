"""Health baselines must distinguish execution settings omitted by legacy cells."""
from dataclasses import replace
from types import SimpleNamespace

import pytest

from svsim.async_scheduler import AsyncPolicy
from svsim.distributed import DistributedConfig
from svsim.executor_profile import document, merge, production_health_context, validate


def runtime():
    return SimpleNamespace(config=DistributedConfig('/calibration', 2), policy=AsyncPolicy())


def test_health_identity_ignores_directory_and_control_mode_but_keeps_execution_settings():
    sim = runtime()
    baseline = production_health_context(sim)
    sim.config = replace(sim.config, directory='/observation')
    sim.policy = replace(sim.policy, mode='auto', cooldown_seconds=0, minimum_gain_fraction=.3)
    assert production_health_context(sim) == baseline
    for changes in ({'amplitude_store': 'ram'}, {'frame_policy': 'materialize'},
                    {'ram_budget_bytes_per_rank': 128 * 1024**2}, {'metadata_reserve_bytes': 256 * 1024},
                    {'tile_bytes': 128 * 1024}, {'alignment': 8192}, {'mcx_exchange': 'full'}):
        other = SimpleNamespace(config=replace(sim.config, **changes), policy=sim.policy)
        assert production_health_context(other) != baseline
    for changes in ({'prefetch_blocks': 0}, {'lookahead_gates': 4}, {'max_mpi_requests': 8}):
        other = SimpleNamespace(config=sim.config, policy=replace(sim.policy, **changes))
        assert production_health_context(other) != baseline


def test_health_execution_context_is_content_addressed_and_survives_merge():
    context = production_health_context(runtime())
    original = document([], 'fingerprint', health_context=context)
    assert validate(original) == original
    merged = merge([original, original])
    assert merged['health_context'] == context
    assert merged['profile_id'] == original['profile_id']
    changed = {**context, 'policy': {**context['policy'], 'prefetch_blocks': 0}}
    with pytest.raises(ValueError, match='content'):
        validate({**original, 'health_context': changed})
    with pytest.raises(ValueError, match='health execution'):
        merge([original, document([], 'fingerprint', health_context=changed)])
    with pytest.raises(ValueError, match='health execution'):
        merge([original, document([], 'fingerprint')])


def test_legacy_profiles_remain_readable_without_becoming_health_certificates():
    legacy = document([], 'fingerprint')
    assert 'health_context' not in validate(legacy)
    assert merge([legacy, legacy]) == legacy
