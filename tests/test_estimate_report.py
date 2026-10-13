import pytest

from svsim.estimate_report import ExecutionPlanDocument
from svsim.profile_document import ProfileDocument
from svsim.resource_estimator import EstimateOptions, estimate
from svsim.workload_spec import WorkloadSpec


def test_plan_codec_checks_identity_and_preserves_large_counts_and_unknowns(tmp_path):
    workload = WorkloadSpec({'schema_version':1,'kind':'workload','qubits':50,
                             'operations':[{'kind':'H','targets':[0]}]})
    report = estimate(workload,ProfileDocument.create({}, {}, {}),EstimateOptions(block_bits=12))
    plan = ExecutionPlanDocument(report)
    path = tmp_path/'plan.json'
    plan.save(path)
    loaded = ExecutionPlanDocument.load(path)
    assert loaded.plan_id == plan.plan_id
    assert loaded.data['resources']['state_payload_bytes'] == 16*(1<<50)
    assert loaded.data['time']['expected_seconds'] is None
    report['resources']['state_payload_bytes'] = '1'
    with pytest.raises(ValueError,match='content hash'): ExecutionPlanDocument(report)
    with pytest.raises(FileExistsError): loaded.save(path)
