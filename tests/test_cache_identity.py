import pytest

from svsim.cache_identity import RawCacheIdentity, ResolvedCacheIdentity
from svsim.phase import PhaseValue
from svsim.parameters import ExactPi, ParameterBinding


def test_raw_identity_and_resolved_identity_use_only_dependencies():
    raw = RawCacheIdentity('d', 's', 'v', 'rank0', 'complex128', '<c16')
    a = ParameterBinding('c', (('used', 1.0), ('unused', 2.0)))
    b = ParameterBinding('c', (('used', 1.0), ('unused', 9.0)))
    def resolved(binding):
        return ResolvedCacheIdentity.build(raw, source_coordinate='x', layout='l', placement='p',
                                           frame='f', log=('g',), phase='0',
                                           dependencies=('used',), binding=binding)
    assert resolved(a) == resolved(b)
    assert resolved(a).parameter_values == (('used', 1.0),)
    changed = ParameterBinding('c', (('used', 2.0), ('unused', 2.0)))
    assert resolved(a) != resolved(changed)


def test_resolved_identity_checks_exact_witness_and_rejects_missing_dependency():
    raw = RawCacheIdentity('d', 's', 'v', 'rank0', 'complex128', '<c16')
    exact = ParameterBinding('c', (('theta', ExactPi(1, 2)),))
    float_only = ParameterBinding('c', (('theta', 1.5707963267948966),))
    kwargs = dict(source_coordinate='x', layout='l', placement='p', frame='f', dependencies=('theta',))
    assert ResolvedCacheIdentity.build(raw, binding=exact, **kwargs) != ResolvedCacheIdentity.build(raw, binding=float_only, **kwargs)
    with pytest.raises(ValueError, match='Unresolved'):
        ResolvedCacheIdentity.build(raw, binding=ParameterBinding('c'), **kwargs)
    with pytest.raises(ValueError, match='without a binding'):
        ResolvedCacheIdentity.build(raw, binding=None, **kwargs)


def test_identity_requires_raw_coordinates_and_freezes_nested_metadata():
    with pytest.raises(ValueError, match='dataset'):
        RawCacheIdentity('', 's', 'v', 'owner', 'f', 'd')
    raw = RawCacheIdentity('d', 's', 'v', 'owner', 'f', 'd')
    log = {'ops': ['RZ'], 'weights': [1.0]}
    identity = ResolvedCacheIdentity.build(raw, source_coordinate='x', layout='l', placement='p',
                                           frame='f', log=log, phase={'exact': ExactPi(1, 3)})
    log['ops'].append('X')
    assert identity.log == (('ops', ('RZ',)), ('weights', (1.0,)))
    with pytest.raises(TypeError, match='Unsupported mutable'):
        ResolvedCacheIdentity.build(raw, source_coordinate='x', layout='l', placement='p',
                                    frame='f', phase=object())


def test_phase_value_identity_preserves_z8_pi_and_legacy_complex_components():
    raw = RawCacheIdentity('d', 's', 'v', 'owner', 'complex128', '<c16')
    def identity(phase):
        return ResolvedCacheIdentity.build(raw, source_coordinate='x', layout='l', placement='p',
                                           frame='f', phase=phase)

    z8 = identity(PhaseValue(z8=1))
    pi = identity(PhaseValue(pi_numerator=1, pi_denominator=2))
    legacy = identity(PhaseValue(legacy=2+3j))
    assert len({z8, pi, legacy}) == 3
    assert legacy.phase[1][3][1] == ('complex', 2.0, 3.0)
