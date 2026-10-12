"""Immutable symbolic expression DAGs; evaluation never executes user code."""
from __future__ import annotations
from dataclasses import dataclass
from hashlib import sha256
import json
import math
from fractions import Fraction
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
class ExactPi(Arithmetic):
    """An exact rational multiple of pi (``numerator / denominator * pi``)."""
    numerator: int
    denominator: int = 1

    def __post_init__(self):
        if isinstance(self.numerator, bool) or not isinstance(self.numerator, int):
            raise TypeError('ExactPi numerator must be an integer')
        if isinstance(self.denominator, bool) or not isinstance(self.denominator, int):
            raise TypeError('ExactPi denominator must be an integer')
        if self.denominator == 0: raise ValueError('ExactPi denominator cannot be zero')
        f = Fraction(self.numerator, self.denominator)
        object.__setattr__(self, 'numerator', f.numerator)
        object.__setattr__(self, 'denominator', f.denominator)

    @property
    def value(self):
        try: value = math.pi * (self.numerator / self.denominator)
        except OverflowError as exc: raise ValueError('ExactPi projection is not finite') from exc
        if not math.isfinite(value): raise ValueError('ExactPi projection is not finite')
        return value


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
        arities = {'const': 1, 'pi': 1, 'symbol': 1, 'neg': 1, 'sin': 1, 'cos': 1,
                   'add': 2, 'mul': 2, 'div': 2}
        if self.op not in arities or len(self.args) != arities[self.op]:
            raise ValueError('Invalid expression node')
        if self.op == 'const' and not math.isfinite(float(self.args[0])):
            raise ValueError('Parameters must be finite')
        if self.op == 'symbol' and not isinstance(self.args[0], ParameterSymbol):
            raise TypeError('Expected parameter symbol')
        if self.op == 'pi' and not isinstance(self.args[0], ExactPi):
            raise TypeError('Expected ExactPi')
        if self.op not in {'const', 'pi', 'symbol'} and not all(isinstance(a, Expr) for a in self.args):
            raise TypeError('Expression children must be Expr nodes')

    @property
    def dependencies(self):
        if self.op == 'symbol': return frozenset((self.args[0].parameter_id,))
        if self.op in {'const', 'pi'}: return frozenset()
        return frozenset().union(*(a.dependencies for a in self.args))

    def evaluate(self, binding: ParameterBinding):
        if self.op == 'const': return float(self.args[0])
        if self.op == 'pi': return self.args[0].value
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
        if self.op in {'const', 'pi'}: return expression(0)
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
    if isinstance(value, ExactPi): return Expr('pi', (value,))
    return Expr('const', (float(value),))


@dataclass(frozen=True)
class ParameterBinding:
    circuit_hash: str
    values: tuple[tuple[str, float | ExactPi], ...] = ()
    exact_witnesses: tuple[tuple[str, ExactPi], ...] = ()

    def __post_init__(self):
        witness_items = tuple((k.parameter_id if isinstance(k, ParameterSymbol) else str(k), v)
                              for k, v in self.exact_witnesses)
        if len({k for k, _ in witness_items}) != len(witness_items):
            raise ValueError('Duplicate exact witnesses')
        if not all(isinstance(v, ExactPi) for _, v in witness_items):
            raise TypeError('Exact witnesses must be ExactPi values')
        witnesses = dict(witness_items)
        raw = []
        for k, v in self.values:
            key = k.parameter_id if isinstance(k, ParameterSymbol) else str(k)
            if isinstance(v, ExactPi):
                if key in witnesses and witnesses[key] != v: raise ValueError('Conflicting exact witness')
                witnesses[key] = v
                v = v.value
            raw.append((key, float(v)))
        values = tuple(sorted(raw))
        if len({k for k, _ in values}) != len(values): raise ValueError('Duplicate parameter IDs')
        if not all(math.isfinite(v) for _, v in values): raise ValueError('Non-finite binding')
        by_value = dict(values)
        normalized = tuple(sorted((str(k), v if isinstance(v, ExactPi) else ExactPi(*v))
                                  for k, v in witnesses.items()))
        if len({k for k, _ in normalized}) != len(normalized): raise ValueError('Duplicate exact witnesses')
        for key, witness in normalized:
            if key not in by_value or by_value[key] != witness.value:
                raise ValueError(f'Exact witness projection mismatch for {key}')
        object.__setattr__(self, 'values', values)
        object.__setattr__(self, 'exact_witnesses', normalized)

    @property
    def binding_id(self):
        if not self.exact_witnesses:
            return sha256(json.dumps((self.circuit_hash, self.values), separators=(',', ':')).encode()).hexdigest()
        payload = ('binding-v2', self.circuit_hash, self.values,
                   tuple((k, 'ExactPi', v.numerator, v.denominator) for k, v in self.exact_witnesses))
        return sha256(json.dumps(payload, separators=(',', ':')).encode()).hexdigest()

    @property
    def witness_hash(self):
        return sha256(json.dumps(tuple((k, 'ExactPi', v.numerator, v.denominator)
                                      for k, v in self.exact_witnesses), separators=(',', ':')).encode()).hexdigest()

    def value(self, parameter_id):
        for key, value in self.values:
            if key == parameter_id: return value
        raise ValueError(f'Missing binding for {parameter_id}')

    def shifted(self, parameter_id, delta):
        self.value(parameter_id)
        delta_value = delta.value if isinstance(delta, ExactPi) else float(delta)
        values = tuple((k, v + delta_value if k == parameter_id else v) for k, v in self.values)
        old = dict(self.exact_witnesses)
        if isinstance(delta, ExactPi) and parameter_id in old:
            old[parameter_id] = ExactPi(old[parameter_id].numerator * delta.denominator + delta.numerator * old[parameter_id].denominator,
                                        old[parameter_id].denominator * delta.denominator)
            values = tuple((k, old[parameter_id].value if k == parameter_id else v) for k, v in values)
        else:
            old.pop(parameter_id, None)
        return ParameterBinding(self.circuit_hash, values, tuple(old.items()))


_MAX_EXACT_BITS = 4096


def _bounded(value: Fraction) -> Fraction | None:
    return value if value.numerator.bit_length() <= _MAX_EXACT_BITS and value.denominator.bit_length() <= _MAX_EXACT_BITS else None


def exact_pi_coefficient(expr, binding: ParameterBinding) -> Fraction | None:
    """Prove an expression is a rational multiple of pi; unknown is returned as None."""
    expr = expression(expr)
    witnesses = dict(binding.exact_witnesses)

    seen = 0

    def walk(node, depth=0):
        nonlocal seen
        seen += 1
        if seen > 10000 or depth > 256: return ('unknown', None)
        if node.op == 'pi':
            p = node.args[0]
            return ('pi', Fraction(p.numerator, p.denominator))
        if node.op == 'const':
            try: return ('rat', _bounded(Fraction.from_float(float(node.args[0]))))
            except (OverflowError, ValueError): return ('unknown', None)
        if node.op == 'symbol':
            sym = node.args[0]
            if sym.parameter_id in witnesses:
                p = witnesses[sym.parameter_id]
                return ('pi', Fraction(p.numerator, p.denominator))
            try: return ('rat', _bounded(Fraction.from_float(binding.value(sym.parameter_id))))
            except (ValueError, OverflowError): return ('unknown', None)
        if node.op == 'neg':
            kind, v = walk(node.args[0], depth + 1); return (kind, -v) if v is not None else ('unknown', None)
        if node.op in ('sin', 'cos'):
            kind, v = walk(node.args[0], depth + 1)
            if kind == 'pi' and v is not None and (v * 2).denominator == 1:
                # Reduce integer/half-turns exactly; no floating trig is involved.
                turn = int(v * 2) % 4
                answer = (Fraction((0, 1, 0, -1)[turn]) if node.op == 'sin'
                          else Fraction((1, 0, -1, 0)[turn]))
                return ('rat', answer)
            return ('unknown', None)
        k1, a = walk(node.args[0], depth + 1); k2, b = walk(node.args[1], depth + 1)
        if a is None or b is None: return ('unknown', None)
        try:
            if node.op == 'add' and k1 == k2: return (k1, _bounded(a + b))
            if node.op == 'add' and k1 == 'rat' and a == 0 and k2 == 'pi': return ('pi', b)
            if node.op == 'add' and k2 == 'rat' and b == 0 and k1 == 'pi': return ('pi', a)
            if node.op == 'mul' and k1 == 'rat' and k2 == 'rat': return ('rat', _bounded(a * b))
            if node.op == 'mul' and k1 == 'rat' and k2 == 'pi': return ('pi', _bounded(a * b))
            if node.op == 'mul' and k2 == 'rat' and k1 == 'pi': return ('pi', _bounded(a * b))
            if node.op == 'div' and k1 == 'rat' and k2 == 'rat' and b != 0: return ('rat', _bounded(a / b))
            if node.op == 'div' and k1 == 'pi' and k2 == 'pi' and b != 0: return ('rat', _bounded(a / b))
            if node.op == 'div' and k2 == 'rat' and b != 0: return (k1, _bounded(a / b))
        except (OverflowError, ZeroDivisionError): pass
        return ('unknown', None)

    kind, value = walk(expr)
    return value if kind == 'pi' or (kind == 'rat' and value == 0) else None
