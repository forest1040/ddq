"""Immutable execution-plan codec, independent of runtime/checkpoint schemas."""
from copy import deepcopy

from .journal import content_hash
from .profile_document import read_document, wire_decode, wire_encode, write_document
from .workload_spec import bounded_tree


class ExecutionPlanDocument:
    def __init__(self, data):
        bounded_tree(data)
        allowed = {'schema_version','kind','model_version','plan_id','workload_id','profile_id',
                   'configuration_fingerprint','options','capacity_feasible','execution_supported',
                   'calibration_coverage','capacity_reasons','capacity_unknown','unsupported_capabilities',
                   'resources','work','time','algorithm','provenance','source_templates','analysis'}
        if not isinstance(data,dict) or set(data) != allowed:
            raise ValueError('execution plan has missing or unknown fields')
        if type(data['schema_version']) is not int or data['schema_version'] != 1 or data['kind'] != 'execution-plan':
            raise ValueError('unsupported plan kind/schema_version')
        normalized = wire_encode(data)
        payload = {key:value for key,value in normalized.items() if key != 'plan_id'}
        if data['plan_id'] != content_hash(payload): raise ValueError('plan_id content hash mismatch')
        decoded = wire_decode(normalized)
        if decoded['capacity_feasible'] is not None and type(decoded['capacity_feasible']) is not bool:
            raise ValueError('capacity_feasible must be boolean or null')
        if type(decoded['execution_supported']) is not bool: raise ValueError('execution_supported must be boolean')
        if decoded['calibration_coverage']['status'] not in {'complete','partial','unknown'}:
            raise ValueError('unknown calibration coverage status')
        for key in ('lower_seconds','expected_seconds','conditional_upper_seconds','reference_kernel_seconds'):
            value = decoded['time'][key]
            if value is not None and (type(value) not in (int,float) or value < 0):
                raise ValueError('plan time must be nonnegative or unknown')
        self._data = decoded

    @classmethod
    def from_dict(cls,data): return cls(data)

    @classmethod
    def load(cls,path): return cls(read_document(path,max_bytes=4*1024**2))

    @property
    def data(self): return deepcopy(self._data)

    @property
    def plan_id(self): return self._data['plan_id']

    def to_dict(self): return wire_encode(self._data)

    def save(self,path,overwrite=False): write_document(path,self.to_dict(),overwrite=overwrite)
