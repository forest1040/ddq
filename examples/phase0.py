"""Run with: uv run python examples/phase0.py"""
from svsim import (
    DeferredSimulator, ParameterSymbol, ObservableIR, PauliTerm,
    StateHandle, BlockPauliInner, adjoint_gradient,
)

sim = DeferredSimulator(num_qubits=8, block_bits=3)
theta = ParameterSymbol('theta')
sim.gate('H', (0,))
sim.gate('CX', (0, 7))
sim.gate('RY', (0,), angle=theta)
sim.gate('RZ', (7,), angle=theta)
sim.gate('RZ', (7,), angle=2 * theta)
binding = sim.bind({theta: 0.3})
state = sim.state(binding)
observable = ObservableIR((PauliTerm.from_string('ZIIIIIIZ'),))
expectation = BlockPauliInner().expectation(StateHandle.from_simulator(sim), observable)
gradients = adjoint_gradient(sim.sequencer.operations, 8, sim.assets, binding, observable, block_bits=3)
print(f'amplitudes={len(state)}, expectation={expectation:.12f}')
print(f'd/dtheta={gradients[theta.parameter_id]:.12f}')
print('execution paths:', [event.metadata['path'] for event in sim.trace.events])
