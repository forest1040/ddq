import json

import pytest

from svsim.workload_spec import WorkloadSpec


def workload(operations=None, **extra):
    return {"kind": "workload", "schema_version": 1, "qubits": 5,
            "operations": operations or [{"kind": "H", "targets": [0]}], **extra}


def test_repeat_counts_are_compressed_and_roundtrip_identity_is_stable(tmp_path):
    source = workload([{"kind": "REPEAT", "count": "1000000000000",
                        "body": [{"kind": "H", "targets": [0]}]}])
    first = WorkloadSpec(source)
    assert first.explicit_node_count == 2
    assert len(first.gates) == 1
    assert first.gates[0].count == 10**12
    assert first.to_dict()["workload_id"] == first.workload_id
    path = tmp_path / "workload.json"
    path.write_text(json.dumps(first.to_dict()))
    loaded = WorkloadSpec.load(path)
    assert loaded.workload_id == first.workload_id
    assert loaded.gates == first.gates


@pytest.mark.parametrize("bad", [
    {"kind": "workload", "schema_version": True, "qubits": 1, "operations": []},
    {"kind": "wrong", "schema_version": 1, "qubits": 1, "operations": []},
    {"kind": "workload", "schema_version": 1, "qubits": True, "operations": []},
    {"kind": "workload", "schema_version": 1, "qubits": 1, "operations": [], "extra": 1},
])
def test_strict_top_level_schema_and_integer_types(bad):
    with pytest.raises(ValueError):
        WorkloadSpec(bad)


def test_expression_symbols_require_stable_parameter_id():
    expr = {"op": "symbol", "symbol": {"name": "theta"}}
    with pytest.raises(ValueError, match="parameter_id"):
        WorkloadSpec(workload([{"kind": "RX", "targets": [0], "angle": expr}]))
    expr["symbol"]["parameter_id"] = "theta-1"
    spec = WorkloadSpec(workload([{"kind": "RX", "targets": [0], "angle": expr}], bindings={"theta-1": .2}))
    assert spec.gates[0].gate.dependencies == frozenset({"theta-1"})


def test_repeat_depth_explicit_nodes_and_condition_count_are_bounded():
    nested = [{"kind": "REPEAT", "count": 1, "body": []}]
    for _ in range(33):
        nested = [{"kind": "REPEAT", "count": 1, "body": nested}]
    with pytest.raises(ValueError):
        WorkloadSpec(workload(nested))
    too_many = [{"kind": "H", "targets": [0]}] * 4097
    with pytest.raises(ValueError, match="node limit"):
        WorkloadSpec(workload(too_many))
    distinct = [{"kind": "RX", "targets": [0], "angle": float(i)} for i in range(257)]
    with pytest.raises(ValueError, match="condition limit"):
        WorkloadSpec(workload(distinct))


def test_repeat_count_and_iterations_reject_boolean_and_unbounded_values():
    with pytest.raises(ValueError):
        WorkloadSpec(workload([{"kind": "REPEAT", "count": True, "body": []}]))
    with pytest.raises(ValueError):
        WorkloadSpec(workload([], iterations=False))
    nested = [{"kind": "H", "targets": [0]}]
    count = "9" * 500
    for _ in range(10):
        nested = [{"kind": "REPEAT", "count": count, "body": nested}]
    with pytest.raises(ValueError, match="aggregate repeat"):
        WorkloadSpec(workload(nested))


def test_qpe_foundation_refuses_uncertified_or_nonfoundation_scenario():
    algorithm = {"kind": "ipea-trotter", "bias_certified": True}
    with pytest.raises(ValueError, match="certification"):
        WorkloadSpec(workload(algorithm=algorithm))
    algorithm["bias_certified"] = False
    algorithm["trotter_order"] = 4
    with pytest.raises(ValueError, match="foundation QPE"):
        WorkloadSpec(workload(algorithm=algorithm))


def test_forced_decomposition_reports_unsupported_capabilities():
    source = workload([{"kind": "DENSE_UNITARY", "targets": [0], "matrix": [
        [[1, 0], [0, 0]], [[0, 0], [1, 0]]]}],
        policies={"execution": "forced-decomposition"})
    _, unsupported = WorkloadSpec(source).lowered()
    assert unsupported == ["UNSUPPORTED_DECOMPOSITION:DENSE_UNITARY"]
    rz = WorkloadSpec(workload([{"kind": "RZ", "targets": [0], "angle": .37}],
                               policies={"execution": "forced-decomposition", "basis": "clifford-t"}))
    _, unsupported = rz.lowered()
    assert unsupported == ["UNSUPPORTED_CONTINUOUS_CLIFFORD_T_SYNTHESIS"]


def test_random_state_requires_reproducible_seed_and_numeric_precision_rejects_bool():
    with pytest.raises(ValueError,match='explicit seed'):
        WorkloadSpec(workload(initial_state='seeded-random'))
    spec = WorkloadSpec(workload(initial_state='seeded-random',seed=(1<<64)-1))
    assert spec.to_dict()['seed'] == str((1<<64)-1)
    assert WorkloadSpec(spec.to_dict()).workload_id == spec.workload_id
    with pytest.raises(ValueError): WorkloadSpec(workload(algorithm={'kind':'ipea-trotter','accuracy_mha':True}))
