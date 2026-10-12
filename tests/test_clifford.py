import cmath
import numpy as np
import pytest
from svsim.clifford import CliffordTableau, anchor_phase
from svsim.frame import FrameAsset, frame_from_word
from svsim.ir import GateOp
from svsim.pauli_rotation import SignedPauli
from svsim.streaming_kernels import apply_local


def operator(b, word):
    result=np.eye(1 << b,dtype=np.complex128)
    for gate in word:
        for col in range(1 << b): apply_local(result[:,col],gate,None,block_bits=b)
    return result


def pauli(b,p):
    a=np.zeros((1 << b,1 << b),dtype=np.complex128)
    for j in range(1 << b): a[j^p.x_mask,j]=(-1)**p.sign_bit * 1j**((p.x_mask&p.z_mask).bit_count()) * (-1)**((p.z_mask&j).bit_count())
    return a


def random_word(b,rng,count):
    result=[]
    for _ in range(count):
        k=rng.choice(['H','S','SDG','Z','X']+(['CX','CZ','SWAP'] if b>1 else []))
        qs=tuple(int(i) for i in rng.choice(b,2 if k in ['CX','CZ','SWAP'] else 1,replace=False))
        result.append(GateOp('MCX',(qs[1],),(qs[0],),(int(rng.integers(2)),)) if k=='CX' else GateOp('MCX' if k=='X' else k,qs))
    return tuple(result)


@pytest.mark.parametrize('b',[1,2,3,4])
def test_tableau_synthesis_anchor_and_inverse(b):
    rng=np.random.default_rng(324+b)
    for _ in range(12):
        word=random_word(b,rng,16)
        u=operator(b,word)
        frame,phase=frame_from_word(b,word)
        assert CliffordTableau.from_bytes(frame.tableau.canonical_bytes()) == frame.tableau
        assert np.allclose(cmath.exp(1j*phase*cmath.pi/4)*operator(b,frame.word)*frame.multiplier,u,atol=1e-12)
        assert frame.tableau.compose(frame.tableau.inverse())==CliffordTableau.identity(b)
        for __ in range(6):
            p=SignedPauli(int(rng.integers(1 << b)),int(rng.integers(1 << b)),int(rng.integers(2)))
            assert np.allclose(u@pauli(b,p)@u.conj().T,pauli(b,frame.conjugate_forward(p)),atol=1e-12)
            assert np.allclose(u.conj().T@pauli(b,p)@u,pauli(b,frame.conjugate_inverse(p)),atol=1e-12)


def test_left_update_phase_and_bounded_word():
    frame=FrameAsset.identity(1); phase=0; word=[]
    for _ in range(9):
        for k in ['H','S']:
            gate=GateOp(k,(0,)); word.append(gate)
            frame,delta=frame.left_update(gate); phase=(phase+delta)%8
            assert np.allclose(cmath.exp(1j*phase*cmath.pi/4)*operator(1,frame.word)*frame.multiplier,operator(1,word),atol=1e-12)
    assert frame.tableau==CliffordTableau.identity(1)
    assert phase==3
    assert frame.word==()


def test_large_tableau_metadata_without_amplitudes():
    b=24; frame=FrameAsset.identity(b)
    for gate in [GateOp('H',(0,)),GateOp('S',(0,)),GateOp('MCX',(23,),(0,),(1,)),GateOp('H',(23,))]: frame,_=frame.left_update(gate)
    assert len(frame.tableau.rows)==48
    assert len(frame.word)<10*b*b
    assert len(frame.tableau.canonical_bytes())<1024
    assert frame.tableau.inverse().compose(frame.tableau)==CliffordTableau.identity(b)


def test_random_left_updates_preserve_absolute_phase():
    b=3
    word=random_word(b,np.random.default_rng(772),60)
    frame=FrameAsset.identity(b)
    phase=0
    for i,gate in enumerate(word):
        frame,delta=frame.left_update(gate)
        phase=(phase+delta)%8
        expected=operator(b,word[:i+1])
        actual=cmath.exp(1j*phase*cmath.pi/4)*operator(b,frame.word)*frame.multiplier
        assert np.allclose(actual,expected,atol=1e-12)
        assert len(frame.word) < 10*b*b


def test_symplectic_validation():
    with pytest.raises(ValueError): CliffordTableau(1,(SignedPauli(1,0),SignedPauli(1,0)))
    with pytest.raises(ValueError): CliffordTableau.from_bytes(b'\x02\x00\x00\x00\x00\x00')
    with pytest.raises(ValueError): CliffordTableau.from_bytes(b'\x01\x00\xff\xff\xff\xff')
    with pytest.raises(ValueError): CliffordTableau(0,(),format_version=2)
    for correction in (-1,8,1.0):
        with pytest.raises(ValueError): FrameAsset(CliffordTableau.identity(1),(),correction)
