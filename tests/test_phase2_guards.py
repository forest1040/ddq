"""Binding witness guards across stored prefixes and snapshot recovery."""
import json
import math

import pytest

from svsim.parameters import ExactPi, ParameterSymbol, exact_pi_coefficient
from svsim.storage import StorageConfig
from svsim.stored_runtime import StoredSimulator


def _sim(path):
    config = StorageConfig(str(path), 1, ram_budget_bytes=1 << 18,
                           metadata_reserve_bytes=1 << 16, alignment=64)
    return config, StoredSimulator(2, config, frame_policy='keep')


def test_float_near_exact_projection_does_not_gain_witness(tmp_path):
    theta = ParameterSymbol('theta', 'guard-theta')
    near = math.nextafter(math.pi / 2, math.inf)
    _, sim = _sim(tmp_path / 'near')
    try:
        binding = sim.bind({theta: near})
        assert binding.exact_witnesses == ()
        assert exact_pi_coefficient(theta, binding) is None
    finally:
        sim.close()


def test_snapshot_resume_preserves_witness_and_rejects_malformed_projection(tmp_path):
    theta = ParameterSymbol('theta', 'snapshot-theta')
    config, sim = _sim(tmp_path / 'snapshot')
    try:
        sim.gate('RZ', (0,), angle=theta)
        binding = sim.bind({theta: ExactPi(1, 2)})
        sim.materialize(binding=binding)
        manifest = sim.commit_snapshot()
        with StoredSimulator.resume(config, manifest) as resumed:
            assert resumed.binding.exact_witnesses == (('snapshot-theta', ExactPi(1, 2)),)

        row = sim.store.db.execute('SELECT data FROM snapshots WHERE id=?', (manifest.snapshot_id,)).fetchone()
        data = json.loads(row[0])
        data['binding_witnesses'] = [['snapshot-theta', 1, 1]]
        with sim.store.db:
            sim.store.db.execute('UPDATE snapshots SET data=? WHERE id=?',
                                 (json.dumps(data, sort_keys=True, separators=(',', ':')), manifest.snapshot_id))
        with pytest.raises(ValueError, match='projection mismatch'):
            StoredSimulator.resume(config, manifest.snapshot_id)
    finally:
        sim.close()


def test_prefix_fork_rejects_changed_witness_with_same_numeric_projection(tmp_path):
    theta = ParameterSymbol('theta', 'prefix-theta')
    _, sim = _sim(tmp_path / 'prefix')
    try:
        sim.gate('RZ', (1,), angle=theta)
        witnessed = sim.bind({theta: ExactPi(1, 2)})
        prefix = sim.commit_prefix((), binding=witnessed)
        same_float_legacy = sim.bind({theta: math.pi / 2})
        assert same_float_legacy.values == witnessed.values
        with pytest.raises(ValueError, match='witness'):
            sim.fork(prefix, binding=same_float_legacy)
    finally:
        sim.close()
