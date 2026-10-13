"""Bounded P=1 file-backed physical SWAP transpose."""
from contextlib import ExitStack
from hashlib import sha256
import math
import numpy as np

from .cache import BufferLease
from .storage import StoredBlockView
from .tile_extent_writer import TileExtentWriter
from .tile_resolver import certify, require_identity_view, source_index


def _view(sim, root, h):
    if getattr(sim, 'ownership', None) is None:
        return sim.store.view(root, h), None
    view, where = sim.view(h, root=root)
    if where['store_id'] != sim.store_id or where['storage_rank'] != sim.rank:
        raise ValueError('UNSUPPORTED_TILE_OWNER: source extent is not local')
    return view, where


def _source_read(store, view, aligned, out):
    np.frombuffer(out, dtype='u1').fill(0)
    if view.version_id in {'zero', 'initial-one'}:
        if view.version_id == 'initial-one' and aligned == 0:
            out[:16] = np.complex128(1).tobytes()
        return
    ref = store.ref(view.version_id)
    handle = store._file(ref.segment)
    count = handle.read_into(ref.offset+aligned, out)
    if count != len(out): raise OSError('Short aligned source fragment')


def _preflight(sim, root, cert, scratch):
    store = sim.store
    # Two lazy passes: metadata gates first, then bounded checksum verification.
    for h in range(store.num_blocks):
        view, where = _view(sim, root, h)
        require_identity_view(view, sim.local if hasattr(sim, 'local') else sim)
        if getattr(sim, 'ownership', None) is not None and (where is None or where['store_id'] != sim.store_id or where['storage_rank'] != sim.rank):
            raise ValueError('UNSUPPORTED_TILE_OWNER: source extent is not local')
    sim.cache.flush()
    sim.cache.clear()
    for h in range(store.num_blocks):
        view, _ = _view(sim, root, h)
        if view.version_id in {'zero', 'initial-one'}: continue
        ref = store.ref(view.version_id)
        if ref.segment is None: raise OSError('Tile source extent is not durable')
        handle = store._file(ref.segment)
        digest = sha256()
        for offset in range(0, store.payload_bytes, cert.alignment):
            count = handle.read_into(ref.offset+offset, scratch[:cert.alignment])
            if count != cert.alignment: raise OSError('Short source checksum read')
            digest.update(scratch[:min(cert.alignment, store.payload_bytes-offset)])
        if digest.hexdigest() != ref.checksum: raise OSError('Block checksum mismatch')


def execute_tile_swap(sim, pairs, tile_bytes=4096):
    """Apply ordered physical swaps with one source page and one output tile.

    P=1 supports FileBlockStore plus identity Frame, empty deferred log, and
    resolved scalar phase. P>1, remote extents and other Frame resolvers are
    explicitly rejected before a private root or output extent is created.
    """
    cert = certify(sim, pairs, tile_bytes)
    store = sim.store
    alignment = cert.alignment
    tile_bytes = min(tile_bytes, store.slot_bytes)
    tile_bytes -= tile_bytes % alignment
    if tile_bytes <= 0: raise ValueError('Tile smaller than storage alignment')
    parent = sim.root
    low_mask = (1 << sim.block_bits)-1
    b = sim.block_bits
    # Admission is explicit and managed. The destination tile is reused as the
    # writer checksum scratch after its final write; the source page is reused
    # as the preflight checksum scratch.
    with ExitStack() as stack:
        destination = stack.enter_context(BufferLease(sim.budget, tile_bytes, alignment))
        source = stack.enter_context(BufferLease(sim.budget, alignment, alignment))
        _preflight(sim, parent, cert, source.view)
        distributed = getattr(sim, 'ownership', None) is not None
        if distributed:
            sim._admit(0, output_blocks=sim.ownership.blocks_per_rank)
            new = sim._new_root(parent)
        else:
            new = store.new_root(parent)
        prior = (sim.root, sim.layout, getattr(sim, 'placement_epoch', None), len(getattr(sim, 'history', ())))
        try:
            dst, src = destination.view, source.view
            for h in range(store.num_blocks):
                version = store.new_version()
                with TileExtentWriter(store, version, tile_bytes=tile_bytes) as writer:
                    for out_start in range(0, store.slot_bytes, tile_bytes):
                        nbytes = min(tile_bytes, store.slot_bytes-out_start)
                        np.frombuffer(dst[:nbytes], dtype='u1').fill(0)
                        logical_end = min(out_start+nbytes, store.payload_bytes)
                        loaded = None
                        source_view = None
                        source_aligned = 0
                        for out_byte in range(out_start, logical_end, 16):
                            out_offset = out_byte//16
                            si = source_index((h << b) | out_offset, cert.pairs)
                            sh, so = si >> b, si & low_mask
                            source_byte = so*16
                            aligned = (source_byte//alignment)*alignment
                            key = (sh, aligned)
                            if key != loaded:
                                source_view, _ = _view(sim, parent, sh)
                                _source_read(store, source_view, aligned, src)
                                loaded, source_aligned = key, aligned
                            pos = source_byte-source_aligned
                            value = np.frombuffer(src[pos:pos+16], dtype='<c16', count=1)[0]
                            dst[out_byte-out_start:out_byte-out_start+16] = np.complex128(value*source_view.phase).tobytes()
                        writer.write(out_start, dst[:nbytes])
                    writer.seal(dst)
                view = StoredBlockView(version, getattr(sim, 'frontier', sim.local.frontier if distributed else -1),
                                       getattr(sim, 'frontier', sim.local.frontier if distributed else -1))
                if distributed:
                    sim._publish(new, h, view, store_id=sim.store_id, storage_rank=sim.rank)
                else:
                    store.publish(new, ((h, view),))

            before = sim.layout
            next_layout = before
            history_entries = []
            for a, c in cert.pairs:
                if a != c:
                    history_entries.append({'kind':'layout','bits':[a,c],
                        'layout_before':next_layout.logical_to_physical,
                        'frontier':getattr(sim, 'frontier', -1), 'tile':True})
                    next_layout = next_layout.swapped_physical(a,c)
            # Record telemetry before crossing the root visibility cut.
            sim.trace.record('tile-swap', performance_class='swap', metadata={
                'path':'P1_FILE_TILE_TRANSPOSE','pairs':cert.pairs,'tile_bytes':tile_bytes,
                'rank_count':1,'inverse_witness':cert.inverse_witness,'state_passes':1})
            if distributed:
                sim.layout = sim.local.layout = next_layout
                sim.placement_epoch += sum(a != c for a,c in cert.pairs)
                sim.history.extend(history_entries)
                sim._commit_stage(new, sim.frontier)
            else:
                store.seal(parent)
                sim.root = new
                sim.layout = next_layout
            return sim
        except BaseException:
            if distributed:
                sim.root, sim.layout, sim.placement_epoch, history_len = prior
                sim.local.root = sim.root
                sim.local.layout = sim.layout
                del sim.history[history_len:]
            else:
                sim.root = parent
                sim.layout = prior[1]
            raise
