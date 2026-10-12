import numpy as np
import pytest
from svsim import *
from svsim.kernels import eager_reference


PAULI = {'I':np.eye(2), 'X':np.array([[0,1],[1,0]]), 'Y':np.array([[0,-1j],[1j,0]]), 'Z':np.diag([1,-1])}


def dense_pauli(text):
    out = np.array([[1]])
    for char in reversed(text): out = np.kron(out,PAULI[char])
    return out


@pytest.mark.parametrize('n', [8,10,12])
def test_block_pauli_inner_against_dense_and_layout(n):
    rng = np.random.default_rng(n)
    bra = rng.normal(size=1 << n)+1j*rng.normal(size=1 << n)
    ket = rng.normal(size=1 << n)+1j*rng.normal(size=1 << n)
    bra /= np.linalg.norm(bra); ket /= np.linalg.norm(ket)
    layout = QubitLayout.identity(n).swapped_physical(0,n-1)
    from svsim.layout import index_bit_swap
    left = StateHandle.from_vector(index_bit_swap(bra,0,n-1),3,layout=layout,role='adjoint')
    right = StateHandle.from_vector(index_bit_swap(ket,0,n-1),3,layout=layout)
    inner = BlockPauliInner()
    for text in ['Y'+'I'*(n-1), 'IXZ'+'I'*(n-4)+'Y', 'I'*(n-1)+'Z', 'XYZ'+'I'*(n-3)]:
        term = PauliTerm.from_string(text,.7)
        # Apply independent tensor oracle; 12Q dense matrix unnecessary.
        tensor = ket.reshape((2,)*n)
        for q, char in enumerate(text):
            tensor = np.moveaxis(np.tensordot(PAULI[char],tensor,axes=(1,n-1-q)),0,n-1-q)
        expected = .7*np.vdot(bra,tensor.reshape(-1))
        assert inner.evaluate(left,right,term) == pytest.approx(expected,abs=1e-13)
    with pytest.raises(ValueError): right.blocks[0].setflags(write=True)
    with pytest.raises(ValueError): inner.evaluate(left,right,PauliTerm(1 << n,0))


@pytest.mark.parametrize('n',[8,10,12])
def test_adjoint_symbolic_chain_rule_controlled_and_finite_difference(n):
    sim = DeferredSimulator(n,3)
    theta, phi = ParameterSymbol('theta'), ParameterSymbol('phi')
    for q in [0,2,n-1]: sim.gate('H',(q,))
    sim.gate('RY',(0,),angle=theta)
    sim.gate('CX',(0,n-1)); sim.gate('RX',(n-1,),angle=2*theta+phi)
    sim.gate('RZ',(2,),controls=(n-1,),control_values=(0,),angle=phi/2)
    sim.gate('DENSE_UNITARY',(0,),matrix=np.array([[1,1j],[1j,1]])/np.sqrt(2))
    sim.gate('RZ',(0,),angle=theta*phi)
    observable = ObservableIR((PauliTerm.from_string('Z'+'I'*(n-1),.4),
                               PauliTerm.from_string('Y'+'I'*(n-2)+'X',.7)))
    binding = sim.bind({theta:.21,phi:.43})
    state = sim.state(binding)
    inner = BlockPauliInner()
    handle = StateHandle.from_simulator(sim)
    direct = 0
    for term, text in zip(observable.terms, ['Z'+'I'*(n-1),'Y'+'I'*(n-2)+'X']):
        tensor = state.reshape((2,)*n)
        for q,c in enumerate(text): tensor = np.moveaxis(np.tensordot(PAULI[c],tensor,axes=(1,n-1-q)),0,n-1-q)
        direct += term.coefficient*np.vdot(state,tensor.reshape(-1)).real
    assert inner.expectation(handle,observable) == pytest.approx(direct,abs=1e-13)
    gradients = adjoint_gradient(sim.sequencer.operations,n,sim.assets,binding,observable,block_bits=3)
    for symbol in [theta,phi]:
        values = []
        for delta in [1e-6,-1e-6]:
            shifted = binding.shifted(symbol.parameter_id,delta)
            vector = eager_reference(sim.sequencer.operations,n,sim.assets,shifted)
            values.append(inner.expectation(StateHandle.from_vector(vector,3),observable))
        assert gradients[symbol.parameter_id] == pytest.approx((values[0]-values[1])/2e-6,abs=2e-9)


def test_pauli_y_phase_and_hermitian_validation():
    state = StateHandle.from_vector(np.array([1,1j])/np.sqrt(2),1)
    assert BlockPauliInner().expectation(state,ObservableIR((PauliTerm.from_string('Y'),))) == pytest.approx(1)
    with pytest.raises(ValueError): ObservableIR((PauliTerm(0,0,1j),))
    with pytest.raises(ValueError): PauliTerm.from_string('Q')
