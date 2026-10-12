"""Immutable scalar phases; exact pi multiples never use tolerance snapping."""
from dataclasses import dataclass
from fractions import Fraction
import cmath
import math
import operator


@dataclass(frozen=True)
class PhaseValue:
    z8: int = 0
    pi_numerator: int = 0
    pi_denominator: int = 1
    legacy: complex = 1+0j

    def __post_init__(self):
        for name in ('z8','pi_numerator','pi_denominator'):
            object.__setattr__(self,name,operator.index(getattr(self,name)))
        if self.pi_denominator == 0: raise ValueError('Zero phase denominator')
        p = Fraction(self.pi_numerator, self.pi_denominator) % 2
        object.__setattr__(self, 'z8', self.z8 % 8)
        object.__setattr__(self, 'pi_numerator', p.numerator)
        object.__setattr__(self, 'pi_denominator', p.denominator)
        if not math.isfinite(self.legacy.real) or not math.isfinite(self.legacy.imag):
            raise ValueError('Non-finite phase')

    def times(self, other):
        p = Fraction(self.pi_numerator,self.pi_denominator)+Fraction(other.pi_numerator,other.pi_denominator)
        return PhaseValue(self.z8+other.z8,p.numerator,p.denominator,self.legacy*other.legacy)

    def evaluate(self):
        p = (Fraction(self.z8,4)+Fraction(self.pi_numerator,self.pi_denominator)) % 2
        exact = {Fraction(0):1, Fraction(1,2):1j, Fraction(1):-1, Fraction(3,2):-1j}
        return self.legacy*(exact[p] if p in exact else cmath.exp(1j*math.pi*float(p)))

    def data(self):
        return {'z8':self.z8,'pi_numerator':self.pi_numerator,'pi_denominator':self.pi_denominator,
                'legacy':[self.legacy.real,self.legacy.imag]}

    @classmethod
    def from_data(cls,data):
        data = dict(data)
        data['legacy'] = complex(*data['legacy'])
        return cls(**data)
