"""Bounded single-pass composition of physical index bit swaps."""
from dataclasses import replace

from .storage import StoredBlockView


def permute_index(index, pairs):
    for a, b in pairs:
        different = ((index >> a) ^ (index >> b)) & 1
        index ^= (different << a) | (different << b)
    return index


def source_labels(destination, block_bits, num_qubits, pairs):
    """Upper source labels depend only on lower bits mapped into upper bits."""
    inverse = tuple(reversed(pairs))
    moving = [q for q in range(block_bits)
              if permute_index(1 << q, inverse) >= (1 << block_bits)]
    base = destination << block_bits
    return sorted({permute_index(base | sum(((v >> j) & 1) << q
                                          for j, q in enumerate(moving)), inverse) >> block_bits
                   for v in range(1 << len(moving))})


def execute_fused_swap(sim, pairs):
    pairs = tuple(tuple(pair) for pair in pairs)
    if not pairs or len(pairs) > 2:
        raise ValueError('Fused SWAP accepts one or two ordered bit pairs')
    if any(len(pair) != 2 or any(type(q) is not int or not 0 <= q < sim.num_qubits for q in pair)
           for pair in pairs):
        raise ValueError('Invalid physical bit pair')
    sim.transport.agree({'fused_pairs': pairs})
    parent = sim.root
    new = sim._new_root()
    sim._admit(5, output_blocks=sim.ownership.blocks_per_rank)
    b = sim.block_bits
    inverse = tuple(reversed(pairs))
    low_mask = (1 << b) - 1
    try:
        for h in range(sim.ownership.num_blocks):
            owner = sim.ownership.shard_owner(h)
            with sim._buffers(1, owner) as buffers:
                out = buffers[0].array(sim.store.payload_bytes) if buffers else None
                try:
                    for label in source_labels(h, b, sim.num_qubits, pairs):
                        error = None
                        with sim.fetch(label, owner, root=parent) as fetched:
                            if out is not None:
                                source = fetched.array(sim.store.payload_bytes)
                                try:
                                    for offset in range(1 << b):
                                        index = permute_index((h << b) | offset, inverse)
                                        if index >> b == label:
                                            out[offset] = source[index & low_mask]
                                except Exception as exc:
                                    error = str(exc)
                                finally:
                                    del source
                        sim.transport.vote(error)
                    error = None
                    if buffers:
                        try:
                            sim._publish(new, h, StoredBlockView(sim.cache.adopt(buffers[0]),
                                                                sim.frontier, sim.frontier))
                        except Exception as exc:
                            error = str(exc)
                    sim.transport.vote(error)
                finally:
                    del out
        sim._commit_stage(new, sim.frontier)
    except BaseException:
        sim.root = sim.local.root = parent
        raise
    for a, c in pairs:
        if a == c:
            continue
        before = sim.layout
        sim.layout = sim.local.layout = before.swapped_physical(a, c)
        sim.placement_epoch += 1
        # Adjoint uses the actual ordered composition in reverse.
        sim.history.append({'kind': 'layout', 'bits': [a, c],
                            'layout_before': before.logical_to_physical,
                            'frontier': sim.frontier, 'fused': True})
    sim.transport.agree({'layout': sim.layout.logical_to_physical,
                         'placement': sim.placement_epoch})
    sim.trace.record('fused-swap', performance_class='swap',
                     metadata={'path': 'FUSED_INDEX_BIT_SWAP', 'pairs': pairs,
                               'state_passes': 1, 'sequential_passes': len(pairs)})
    return sim
