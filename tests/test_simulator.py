from dataclasses import replace
import numpy as np
import pytest
from svsim import *
from svsim.kernels import eager_reference, gate_matrix


def random_state(n, rng):
    state = rng.normal(size=1 << n) + 1j*rng.normal(size=1 << n)
    return state / np.linalg.norm(state)


def random_unitary(k, rng):
    a = rng.normal(size=(1 << k, 1 << k)) + 1j*rng.normal(size=(1 << k, 1 << k))
    q, r = np.linalg.qr(a)
    return q @ np.diag(np.diag(r)/abs(np.diag(r)))


@pytest.mark.parametrize('n', [8, 10, 12])
@pytest.mark.parametrize('block_bits', [2, 5])
def test_semantic_pipeline_matches_independent_eager(n, block_bits):
    rng = np.random.default_rng(130+n)
    initial = random_state(n, rng)
    sim = DeferredSimulator(n, block_bits, initial_state=initial)
    theta = ParameterSymbol('theta')
    # Different frames in conditional groups, relative phases, upper mixing.
    sim.gate('H', (0,)); sim.gate('S', (1,)); sim.gate('CX', (n-1,0))
    sim.gate('RZ', (n-1,), angle=.73)
    sim.gate('H', (n-1,)); sim.gate('CX', (1,n-1))
    sim.gate('RZ', (0,), angle=theta); sim.gate('RZ', (0,), angle=2*theta-.1)
    sim.gate('H', (0,))
    sim.gate('DENSE_UNITARY', (1,), matrix=random_unitary(1, rng))
    sim.gate('DENSE_UNITARY', (n-1,0), matrix=random_unitary(2, rng))
    sim.gate('MCX', (n-1,), controls=(0,1,n-2), control_values=(0,1,0))
    sim.gate('MCX', (0,), controls=(n-1,n-2,1), control_values=(1,0,1))
    sim.gate('CZ', (1,n-2)); sim.gate('SWAP', (0,n-1))
    for _ in range(10):
        kind = rng.choice(['H','S','T','RX','RY','RZ','CX','CZ','SWAP'])
        qs = tuple(int(x) for x in rng.choice(n, 2, replace=False))
        if kind in {'CX','CZ','SWAP'}: sim.gate(kind, qs)
        elif kind in {'RX','RY','RZ'}: sim.gate(kind, qs[:1], angle=float(rng.normal()))
        else: sim.gate(kind, qs[:1])
    assert sim.frontier == -1
    np.testing.assert_array_equal(np.concatenate(list(sim.blocks.values())), initial)
    binding = sim.bind({theta: .31})
    expected = eager_reference(sim.sequencer.operations, n, sim.assets, binding, initial)
    np.testing.assert_allclose(sim.state(binding), expected, atol=2e-14)
    np.testing.assert_allclose(sim.state(binding), expected, atol=2e-14) # no double execution
    assert len(sim.block_log.records) == len(sim.sequencer.operations)
    assert sim.dag.tasks
    paths = {e.metadata['path'] for e in sim.trace.events}
    assert {'FRAME_UPDATE', 'PHASE_UPDATE','NATIVE_JOINT', 'FUSED_RZ', 'NATIVE_MCX_CONDITIONAL_SWAP'} <= paths
    assert sim.trace.profile_id == sim.profile.profile_id == sim.plan.profile_id
    assert sim.plan.circuit_hash == sim.circuit_hash
    for h in sim.blocks:
        view = sim.block_view(h)
        assert view.base_version.binding_id == binding.binding_id
        assert theta.parameter_id in view.base_version.parameter_dependencies
        assert view.base_version.frontier == len(sim.sequencer.operations)-1


@pytest.mark.parametrize('controls', range(7))
@pytest.mark.parametrize('target', [0, 3, 7])
def test_mcx_masks_open_controls_local_upper_partial_exchange(controls, target):
    rng = np.random.default_rng(22)
    initial = random_state(8, rng)
    sim = DeferredSimulator(8, 3, initial_state=initial)
    qs = tuple(q for q in range(8) if q != target)[:controls]
    values = tuple(i % 2 for i in range(controls))
    gate = sim.gate('MCX', (target,), controls=qs, control_values=values)
    assert gate.matrix_asset_ref is None
    expected = eager_reference([gate], 8, sim.assets, initial_state=initial)
    np.testing.assert_allclose(sim.state(), expected, atol=1e-15)
    sim2 = DeferredSimulator(8,3,initial_state=expected)
    sim2.gate('MCX', (target,), controls=qs, control_values=values)
    np.testing.assert_allclose(sim2.state(), initial, atol=1e-15)


def test_frame_sharing_cow_and_relative_phase_joint_alignment():
    rng = np.random.default_rng(41)
    initial = random_state(4, rng)
    sim = DeferredSimulator(4, 2, initial_state=initial)
    gates = [sim.gate('H', (0,)), sim.gate('CX', (3,0)), sim.gate('RZ', (3,), angle=.4), sim.gate('H', (3,))]
    sim.binding = sim.bind()
    sim._apply(gates[0])
    assert len(set(sim.frame_ids.values())) == 1
    shared = sim.frame_ids[0]
    sim._apply(gates[1])
    assert sim.frame_ids[0] == shared and sim.frame_ids[2] != shared
    sim._apply(gates[2])
    assert sim.phases[0] != sim.phases[2]
    handle = StateHandle.from_simulator(sim)
    # Unnormalized blocks preserve the full-state norm.
    assert sum(np.vdot(b,b).real for b in handle.blocks) == pytest.approx(1)
    sim._apply(gates[3]); sim.frontier = 3
    for h in sim.blocks: sim._flush(h)
    np.testing.assert_allclose(np.concatenate(list(sim.blocks.values())), eager_reference(gates,4,sim.assets,initial_state=initial), atol=1e-15)
    assert len(sim.frames.frames) == 3


@pytest.mark.parametrize('a,b', [(0,7),(1,2),(6,7)])
def test_physical_layout_swap_preserves_logical_state(a,b):
    rng = np.random.default_rng(12)
    initial = random_state(8,rng)
    sim = DeferredSimulator(8,3,initial_state=initial)
    sim.gate('H',(0,)); sim.gate('CX',(7,1))
    before = sim.state()
    sim.physical_index_bit_swap(a,b)
    np.testing.assert_array_equal(sim.state(),before)
    sim.physical_index_bit_swap(a,b)
    np.testing.assert_array_equal(sim.state(),before)
    logical = DeferredSimulator(8,3,initial_state=before)
    logical.gate('SWAP',(a,b))
    assert not np.allclose(logical.state(),before)


@pytest.mark.parametrize('kind,targets,kwargs', [
    ('RX',(1,),{'angle':.43}), ('RY',(1,),{'angle':-.31}), ('RZ',(7,),{'angle':2*np.pi}),
    ('X',(1,),{}), ('CZ',(0,7),{}), ('SWAP',(1,7),{}),
    ('CX',(7,1),{}), ('MCX',(7,),{'controls':(1,), 'control_values':(0,)}), ('SDG',(1,),{}), ('TDG',(7,),{}),
])
def test_fallback_exact_named_basis(kind,targets,kwargs):
    rng = np.random.default_rng(24)
    initial = random_state(8,rng)
    sim = DeferredSimulator(8,3,initial_state=initial,execution='fallback')
    op = sim.gate(kind,targets,**kwargs,approximation_error=1e-9)
    parts = FallbackDecomposer().decompose(op)
    assert {g.kind for g in parts} <= {'H','S','T','RZ','MCX'}
    assert sum(g.approximation_error for g in parts) == op.approximation_error
    assert all(g.source_op_id == op.op_id for g in parts)
    expected = eager_reference([op],8,sim.assets,initial_state=initial)
    np.testing.assert_allclose(sim.state(),expected,atol=1e-15)
    assert sim.trace.events[0].metadata['path'] == 'FALLBACK'


def test_parameter_expression_bindings_guards_and_no_partial_mutation():
    theta, other = ParameterSymbol('theta'), ParameterSymbol('theta')
    assert theta.parameter_id != other.parameter_id
    sim = DeferredSimulator(8,3)
    sim.gate('H',(0,)); sim.gate('RZ',(0,),angle=2*theta+other); sim.gate('RZ',(0,),angle=-theta)
    with pytest.raises(ValueError,match='Missing binding'): sim.state(sim.bind({theta:1}))
    assert sim.frontier == -1
    binding = sim.bind({theta:.2,other:.4})
    assert binding.binding_id == sim.bind({other:.4,theta:.2}).binding_id
    expr = (2*theta+other)/3
    assert expr.evaluate(binding) == pytest.approx(.8/3)
    assert expr.derivative(theta.parameter_id).evaluate(binding) == pytest.approx(2/3)
    expected = eager_reference(sim.sequencer.operations,8,sim.assets,binding)
    np.testing.assert_allclose(sim.state(binding),expected,atol=1e-15)
    with pytest.raises(ValueError,match='another binding'): sim.state(sim.bind({theta:.3,other:.4}))
    with pytest.raises(ValueError,match='different circuit'): sim.state(ParameterBinding('wrong',binding.values))
    with pytest.raises(ValueError): Expr('eval',('bad',))
    with pytest.raises(ValueError): ParameterBinding('',(('a',float('nan')),))


def test_matrix_validation_asset_immutability_and_target_order():
    registry = MatrixAssetRegistry()
    u = np.array([[0,1],[1,0]],complex)
    key = registry.register(u,1)
    u[:] = 0
    stored = registry.get(key).matrix
    np.testing.assert_array_equal(stored,[[0,1],[1,0]])
    with pytest.raises(ValueError): stored.setflags(write=True)
    assert registry.register([[0,1],[1,0]],1) == key
    for bad in [np.ones((2,2)), np.eye(3), [[np.nan,0],[0,1]]]:
        with pytest.raises(ValueError): registry.register(bad,1)
    with pytest.raises(ValueError): UnitaryValidator(max_bytes=16).validate(np.eye(2),1)
    sim = DeferredSimulator(8,3)
    sim.gate('DENSE_UNITARY',(7,0),matrix=np.kron(np.eye(2),[[0,1],[1,0]]))
    expected = np.zeros(256); expected[128] = 1
    np.testing.assert_array_equal(sim.state(),expected)


@pytest.mark.parametrize('kind,targets,kwargs', [('MCX',(0,),{'controls':(0,)}), ('MCX',(0,),{'controls':(1,), 'control_values':()}), ('H',(8,),{}), ('H',(0,),{'angle':1}), ('CX',(0,),{}), ('foo',(0,),{})])
def test_invalid_gate_rejected(kind,targets,kwargs):
    with pytest.raises(ValueError): GateNormalizer(8).gate(kind,targets,**kwargs)


def test_frontiers_no_double_execution_and_joint_dependencies():
    sim = DeferredSimulator(8,3)
    sim.gate('H',(0,)); sim.gate('H',(7,)); sim.gate('CX',(0,7)); sim.gate('T',(1,))
    sim.materialize(frontier=0)
    assert sim.frontier == 0
    assert sim.dag.tasks[2].predecessors == (1,)
    assert any(r.kind == 'JoinRef' for r in sim.block_log.view(0))
    expected = eager_reference(sim.sequencer.operations,8,sim.assets)
    np.testing.assert_allclose(sim.state(),expected,atol=1e-15)
    assert sum(len(e.metadata['source_ops']) for e in sim.trace.events) == 4
    with pytest.raises(ValueError): sim.materialize(frontier=0)
    with pytest.raises(ValueError): sim.append_gate(sim.sequencer.operations[0])


def test_negative_control_phase_is_not_discarded():
    sim = DeferredSimulator(8,3)
    sim.gate('H',(7,)); sim.gate('RZ',(0,),controls=(7,),control_values=(0,),angle=2*np.pi)
    state = sim.state()
    assert state[0] == pytest.approx(-1/np.sqrt(2))
    assert state[128] == pytest.approx(1/np.sqrt(2))


def test_execute_more_gates_after_physical_layout_transition():
    rng = np.random.default_rng(51)
    initial = random_state(8,rng)
    sim = DeferredSimulator(8,3,initial_state=initial)
    theta = ParameterSymbol('theta')
    sim.gate('RX',(1,),angle=theta)
    sim.state(sim.bind({theta:.3}))
    sim.physical_index_bit_swap(0,7)
    sim.gate('CX',(7,1)); sim.gate('H',(0,)); sim.gate('RZ',(0,),angle=theta)
    binding = sim.bind({theta:.3})
    expected = eager_reference(sim.sequencer.operations,8,sim.assets,binding,initial)
    np.testing.assert_allclose(sim.state(binding),expected,atol=1e-15)
    assert sim.block_log.records[-1].layout_id == 1
    assert any(e.task_id == 'layout-swap' for e in sim.trace.events)
    assert sim.plan.layout_id == '1'


def test_materialized_symbolic_state_reuses_immutable_binding():
    sim = DeferredSimulator(8,3)
    theta = ParameterSymbol('theta',bounds=[-1,1])
    sim.gate('RY',(0,),angle=theta)
    binding = sim.bind({theta:.3})
    expected = sim.state(binding)
    np.testing.assert_array_equal(sim.state(),expected)
    source = [expression(theta),expression(1)]
    expr = Expr('add',source)
    source.clear()
    assert expr.evaluate(binding) == pytest.approx(1.3)
    assert theta.bounds == (-1,1)
    with pytest.raises(ValueError): ParameterSymbol('x',bounds=(1,-1))
