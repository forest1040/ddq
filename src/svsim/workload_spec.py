"""Bounded semantic workload documents; repetitions never become gate lists."""
from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass, replace
import json
import math

from .ir import GateNormalizer, GateOp, FallbackDecomposer
from .journal import content_hash, decode_expr, gate_data
from .parameters import ParameterBinding
from .profile_document import read_document, wire_encode

MAX_INPUT_BYTES = 16 * 1024**2
MAX_NODES = 4096
MAX_DEPTH = 32
MAX_CONDITIONS = 256


def integer(value, name, minimum=0, maximum=None):
    if isinstance(value, str):
        if len(value) > 512 or not value.isascii() or not value.isdecimal():
            raise ValueError(f'{name} must be a bounded decimal integer')
        value = int(value)
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise ValueError(f'{name} must be an integer >= {minimum}')
    if maximum is not None and value > maximum:
        raise ValueError(f'{name} exceeds {maximum}')
    return value


def fields(data, allowed, name):
    if not isinstance(data, dict) or set(data) - set(allowed):
        raise ValueError(f'{name}: invalid object or unknown fields')


def bounded_tree(value, depth=0, counter=None):
    """Reject pathological metadata before invoking recursive expression code."""
    counter = [0] if counter is None else counter
    counter[0] += 1
    if depth > MAX_DEPTH or counter[0] > 65536:
        raise ValueError('analysis metadata tree limit exceeded')
    if isinstance(value, dict):
        for key, child in value.items():
            if not isinstance(key, str): raise ValueError('JSON object keys must be strings')
            bounded_tree(child, depth + 1, counter)
    elif isinstance(value, (list, tuple)):
        for child in value: bounded_tree(child, depth + 1, counter)
    elif isinstance(value, float) and not math.isfinite(value):
        raise ValueError('numbers must be finite')
    elif value is not None and not isinstance(value, (str, int, float, bool)):
        raise ValueError('non-JSON workload value')


def _angle(value):
    if not isinstance(value, dict):
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise ValueError('angle must be a number or Expr document')
        return value
    op = value.get('op')
    expected = {'const': {'op', 'value'}, 'pi': {'op', 'numerator', 'denominator'},
                'symbol': {'op', 'symbol'}}.get(op, {'op', 'args'})
    fields(value, expected, 'angle')
    if set(value) != expected: raise ValueError('incomplete angle expression')
    if op == 'symbol':
        fields(value['symbol'], {'name', 'parameter_id', 'bounds'}, 'symbol')
        if not isinstance(value['symbol'].get('parameter_id'), str) or not value['symbol']['parameter_id']:
            raise ValueError('symbol requires a stable parameter_id')
    elif op not in {'const', 'pi'}:
        if op not in {'neg', 'sin', 'cos', 'add', 'mul', 'div'}: raise ValueError('unknown expression')
        if not isinstance(value['args'], list): raise ValueError('expression args must be a list')
        for child in value['args']: _angle(child)
    elif op == 'const':
        _angle(value['value'])
    return decode_expr(value)


@dataclass(frozen=True)
class CountedGate:
    gate: GateOp
    count: int
    template_id: str


class WorkloadSpec:
    """One validated source document and a bounded histogram of semantic gates."""
    def __init__(self, data):
        bounded_tree(data)
        if len(json.dumps(data, allow_nan=False).encode()) > MAX_INPUT_BYTES:
            raise ValueError('workload exceeds input byte limit')
        fields(data, {'kind', 'schema_version', 'workload_id', 'qubits', 'dtype', 'operations',
                      'bindings', 'iterations', 'algorithm', 'policies', 'initial_state', 'seed'}, 'workload')
        if data.get('kind') != 'workload' or type(data.get('schema_version')) is not int or data['schema_version'] != 1:
            raise ValueError('unsupported workload kind/schema_version')
        self.qubits = integer(data.get('qubits'), 'qubits', 1, 1024)
        if data.get('dtype', 'complex128') != 'complex128': raise ValueError('only complex128 is supported')
        self.iterations = integer(data.get('iterations', 1), 'iterations', 1)
        self.algorithm = deepcopy(data.get('algorithm', {'kind': 'circuit'}))
        fields(self.algorithm, {'kind', 'accuracy_mha', 'trotter_order', 'phase_estimation', 'bias_certified'}, 'algorithm')
        if self.algorithm.get('kind') not in {'circuit', 'vqe', 'ipea-trotter'}: raise ValueError('unknown algorithm')
        if self.algorithm['kind'] == 'ipea-trotter':
            self.algorithm.setdefault('accuracy_mha', 1.0)
            self.algorithm.setdefault('trotter_order', 2)
            self.algorithm.setdefault('phase_estimation', 'iterative')
            self.algorithm.setdefault('bias_certified', False)
            if (type(self.algorithm['accuracy_mha']) not in (int,float) or self.algorithm['accuracy_mha'] != 1 or
                    type(self.algorithm['trotter_order']) is not int or self.algorithm['trotter_order'] != 2 or
                    self.algorithm['phase_estimation'] != 'iterative'):
                raise ValueError('foundation QPE scenario requires 1 mHa, symmetric second order, iterative QPE')
            if self.algorithm['bias_certified'] is not False: raise ValueError('no Trotter error certification is implemented')
        self.policies = {'storage': 'ram', 'frame': 'materialize', 'swap': 'S0',
                         'execution': 'auto-native', 'basis': 'clifford-t-rz', 'gradient': 'none'}
        fields(data.get('policies', {}), self.policies, 'policies')
        self.policies.update(data.get('policies', {}))
        choices = {'storage': {'ram','file'}, 'frame': {'materialize','keep'}, 'swap': {'S0','S1','S2'},
                   'execution': {'auto-native','forced-decomposition'}, 'basis': {'clifford-t-rz','clifford-t'},
                   'gradient': {'none','adjoint','recompute','parameter-shift'}}
        for key, values in choices.items():
            if self.policies[key] not in values: raise ValueError(f'unknown policy {key}')
        if data.get('initial_state', 'zero') not in {'zero','seeded-random'}: raise ValueError('unknown initial state')
        if data.get('initial_state') == 'seeded-random' and 'seed' not in data:
            raise ValueError('seeded-random requires an explicit seed')
        if 'seed' in data: integer(data['seed'], 'seed', 0, (1 << 64) - 1)
        bindings = data.get('bindings', {})
        if not isinstance(bindings, dict) or any(isinstance(v, bool) or not isinstance(v, (int,float)) for v in bindings.values()):
            raise ValueError('bindings must map stable parameter IDs to finite numbers')
        binding = ParameterBinding('static-workload', tuple(bindings.items()))
        normalizer = GateNormalizer(self.qubits)
        aggregate = {}
        node_count = 0
        self.source_templates = []

        def visit(nodes, multiplier, path=()):
            nonlocal node_count
            if not isinstance(nodes, list): raise ValueError('operations/body must be a list')
            for index, node in enumerate(nodes):
                node_count += 1
                if node_count > MAX_NODES: raise ValueError('explicit IR node limit exceeded')
                location = path + (index,)
                if not isinstance(node, dict): raise ValueError('operation must be an object')
                if node.get('kind') == 'REPEAT':
                    fields(node, {'kind','count','body'}, 'repeat')
                    repeated = multiplier * integer(node.get('count'), 'repeat count')
                    if repeated.bit_length() > 4096: raise ValueError('aggregate repeat count limit exceeded')
                    visit(node.get('body'), repeated, location)
                    continue
                fields(node, {'kind','targets','controls','control_values','angle','paulis','matrix'}, 'gate')
                for key in ('targets','controls','control_values'):
                    vals = node.get(key, [] if key != 'targets' else None)
                    if vals is not None and (not isinstance(vals, list) or any(type(q) is not int for q in vals)):
                        raise ValueError(f'{key} must be an integer list')
                angle = _angle(node['angle']) if 'angle' in node else None
                targets = node.get('targets', [])
                matrix = node.get('matrix')
                if matrix is not None:
                    if len(targets) > 4: raise ValueError('dense assets are limited to four targets')
                    import numpy as np
                    # Inline complex elements are [real, imaginary]; assets are bounded independently of n.
                    width = 1 << len(targets)
                    if not isinstance(matrix, list) or len(matrix) != width or any(not isinstance(row,list) or len(row)!=width for row in matrix):
                        raise ValueError('invalid dense matrix dimensions')
                    matrix = np.array([[complex(*v) if isinstance(v,list) and len(v)==2 else complex(v) for v in row] for row in matrix])
                gate = normalizer.gate(node.get('kind',''), targets, controls=node.get('controls',()),
                                       control_values=node.get('control_values'), angle=angle,
                                       paulis=node.get('paulis',()), matrix=matrix)
                if gate.angle: gate.angle.evaluate(binding)  # Validate bindings without allocating any state.
                identity = content_hash(gate_data(gate))
                template_id = 'template:' + '.'.join(map(str, location))
                self.source_templates.append({'template_id':template_id,'gate_fingerprint':identity,'count':multiplier})
                if identity not in aggregate:
                    if len(aggregate) >= MAX_CONDITIONS: raise ValueError('condition limit exceeded')
                    aggregate[identity] = [gate, 0, template_id]
                aggregate[identity][1] += multiplier

        visit(data.get('operations'), self.iterations)
        self.gates = tuple(CountedGate(*values) for values in aggregate.values() if values[1])
        self.binding = binding
        self.assets = normalizer.assets
        self.explicit_node_count = node_count
        normalized = deepcopy(data)
        normalized.pop('workload_id', None)
        normalized.update(qubits=self.qubits, dtype='complex128', iterations=str(self.iterations),
                          algorithm=self.algorithm, policies=self.policies)
        if 'seed' in normalized: normalized['seed'] = str(integer(normalized['seed'],'seed'))
        self.workload_id = content_hash(wire_encode(normalized))
        if 'workload_id' in data and data['workload_id'] != self.workload_id: raise ValueError('workload_id mismatch')
        self._data = normalized

    @classmethod
    def load(cls, path): return cls(read_document(path, MAX_INPUT_BYTES))

    def to_dict(self): return wire_encode({**deepcopy(self._data), 'workload_id': self.workload_id})

    def lowered(self):
        """Return bounded counted lowered IR and explicit unsupported capabilities."""
        result, unsupported = [], []
        for entry in self.gates:
            gate = entry.gate
            if self.policies['execution'] == 'forced-decomposition':
                try: lowered = FallbackDecomposer().decompose(gate)
                except NotImplementedError:
                    unsupported.append('UNSUPPORTED_DECOMPOSITION:' + gate.kind)
                    continue
                if self.policies['basis'] == 'clifford-t' and any(g.kind == 'RZ' for g in lowered):
                    unsupported.append('UNSUPPORTED_CONTINUOUS_CLIFFORD_T_SYNTHESIS')
                    continue
            else: lowered = (gate,)
            for derived in lowered:
                # Preserve Expr rather than disguising symbolic/exact inputs as floats.
                result.append(CountedGate(replace(derived, source_op_id=None), entry.count, entry.template_id))
                if len(result) > MAX_NODES: raise ValueError('lowered IR node limit exceeded')
        return tuple(result), sorted(set(unsupported))
