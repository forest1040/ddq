"""Immutable symbolic expression DAGs; evaluation never executes user code."""
from __future__ import annotations
from dataclasses import dataclass
from hashlib import sha256
import json
import math
from uuid import uuid4


class Arithmetic:
    def __add__(self, other): return Expr('add', (expression(self), expression(other)))
    def __radd__(self, other): return expression(other) + self
    def __neg__(self): return Expr('neg', (expression(self),))
    def __sub__(self, other): return self + -expression(other)
    def __rsub__(self, other): return expression(other) - self
    def __mul__(self, other): return Expr('mul', (expression(self), expression(other)))
    def __rmul__(self, other): return expression(other) * self
    def __truediv__(self, other): return Expr('div', (expression(self), expression(other)))
    def __rtruediv__(self, other): return expression(other) / self


@dataclass(frozen=True)
class ParameterSymbol(Arithmetic):
    name: str
    parameter_id: str = ''
    bounds: tuple[float, float] | None = None

    def __post_init__(self):
        if not self.parameter_id: object.__setattr__(self, 'parameter_id', uuid4().hex)
        if self.bounds is not None:
            bounds = tuple(float(v) for v in self.bounds)
            if len(bounds) != 2 or not all(math.isfinite(v) for v in bounds) or bounds[0] > bounds[1]:
                raise ValueError('Invalid parameter bounds')
            object.__setattr__(self, 'bounds', bounds)


@dataclass(frozen=True)
class Expr(Arithmetic):
    op: str
    args: tuple = ()

    def __post_init__(self):
        object.__setattr__(self, 'args', tuple(self.args))
        arities = {'const': 1, 'symbol': 1, 'neg': 1, 'sin': 1, 'cos': 1,
                   'add': 2, 'mul': 2, 'div': 2}
        if self.op not in arities or len(self.args) != arities[self.op]:
            raise ValueError('Invalid expression node')
        if self.op == 'const' and not math.isfinite(float(self.args[0])):
            raise ValueError('Parameters must be finite')
        if self.op == 'symbol' and not isinstance(self.args[0], ParameterSymbol):
            raise TypeError('Expected parameter symbol')
        if self.op not in {'const', 'symbol'} and not all(isinstance(a, Expr) for a in self.args):
            raise TypeError('Expression children must be Expr nodes')

    @property
    def dependencies(self):
        if self.op == 'symbol': return frozenset((self.args[0].parameter_id,))
        if self.op == 'const': return frozenset()
        return frozenset().union(*(a.dependencies for a in self.args))

    def evaluate(self, binding: ParameterBinding):
        if self.op == 'const': return float(self.args[0])
        if self.op == 'symbol':
            symbol = self.args[0]
            value = binding.value(symbol.parameter_id)
            if symbol.bounds is not None and not symbol.bounds[0] <= value <= symbol.bounds[1]:
                raise ValueError(f'Parameter {symbol.name} outside bounds')
            return value
        values = [a.evaluate(binding) for a in self.args]
        match self.op:
            case 'add': value = values[0] + values[1]
            case 'mul': value = values[0] * values[1]
            case 'div': value = values[0] / values[1]
            case 'neg': value = -values[0]
            case 'sin': value = math.sin(values[0])
            case 'cos': value = math.cos(values[0])
        if not math.isfinite(value): raise ValueError('Non-finite expression result')
        return value

    def derivative(self, parameter_id: str):
        if self.op == 'const': return expression(0)
        if self.op == 'symbol': return expression(int(self.args[0].parameter_id == parameter_id))
        a = self.args[0]
        da = a.derivative(parameter_id)
        if self.op == 'neg': return -da
        if self.op == 'sin': return Expr('cos', (a,)) * da
        if self.op == 'cos': return -Expr('sin', (a,)) * da
        b = self.args[1]
        db = b.derivative(parameter_id)
        if self.op == 'add': return da + db
        if self.op == 'mul': return da * b + a * db
        return (da * b - a * db) / (b * b)


def expression(value):
    if isinstance(value, Expr): return value
    if isinstance(value, ParameterSymbol): return Expr('symbol', (value,))
    return Expr('const', (float(value),))


@dataclass(frozen=True)
class ParameterBinding:
    circuit_hash: str
    values: tuple[tuple[str, float], ...] = ()

    def __post_init__(self):
        values = tuple(sorted((k.parameter_id if isinstance(k, ParameterSymbol) else str(k), float(v))
                              for k, v in self.values))
        if len({k for k, _ in values}) != len(values): raise ValueError('Duplicate parameter IDs')
        if not all(math.isfinite(v) for _, v in values): raise ValueError('Non-finite binding')
        object.__setattr__(self, 'values', values)

    @property
    def binding_id(self):
        return sha256(json.dumps((self.circuit_hash, self.values), separators=(',', ':')).encode()).hexdigest()

    def value(self, parameter_id):
        for key, value in self.values:
            if key == parameter_id: return value
        raise ValueError(f'Missing binding for {parameter_id}')

    def shifted(self, parameter_id, delta):
        self.value(parameter_id)
        return ParameterBinding(self.circuit_hash, tuple((k, v + delta if k == parameter_id else v)
                                                        for k, v in self.values))
