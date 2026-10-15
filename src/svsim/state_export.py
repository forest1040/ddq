"""Bounded, admitted state export publication (files and whole shard directories)."""
from contextlib import contextmanager
import ctypes
import json
import os
from pathlib import Path
import shutil
import re
import sys
from uuid import uuid4

from .export_admission import OPERATIONS, export_lease, intent_for, prepare_export


def fsync_directory(path):
    fd=os.open(path,os.O_RDONLY)
    try:os.fsync(fd)
    finally:os.close(fd)


def ensure_parent(parent,root):
    missing=[];current=parent
    while not current.exists():
        missing.append(current);current=current.parent
    parent.mkdir(parents=True,exist_ok=True)
    # Persist new ancestor entries before publishing the output generation.
    for directory in missing:fsync_directory(directory)
    if missing:fsync_directory(current)


def write_exact(stream,data):
    if stream.write(data)!=getattr(data,'nbytes',len(data)):raise OSError('Incomplete state export write')


@contextmanager
def admitted_export_file(path,amount,*,budget,dataset,owner):
    target,root=prepare_export(budget,path)
    generation=uuid4().hex
    temporary=target.with_name(f'.{target.name}.{generation}.tmp')
    with export_lease(root) as lease:
        intent=intent_for(dataset,root,target,temporary,'file')
        key=budget.reserve(amount,operation=OPERATIONS['file'],owner=owner,generation=generation,
                           writing=True,checkpoint_link=intent)
        created=published=False
        try:
            ensure_parent(target.parent,root)
            with temporary.open('x+b',buffering=0) as stream:
                created=True
                stream.truncate(amount)
                yield stream
                stream.flush()
                budget.materialize(key,amount)
                os.fsync(stream.fileno())
            lease.assert_current()
            os.replace(temporary,target);published=True
            fsync_directory(target.parent)
            budget.release(key);key=None
        finally:
            if key is not None and not published:
                if created:temporary.unlink(missing_ok=True)
                fsync_directory(target.parent)
                budget.release(key)


def exchange_directories(first,second):
    """One atomic publication when replacing an existing nonempty export directory."""
    libc=ctypes.CDLL(None,use_errno=True)
    if sys.platform=='darwin':
        function=libc.renamex_np
        result=function(os.fsencode(first),os.fsencode(second),2)  # RENAME_SWAP
    elif sys.platform.startswith('linux') and hasattr(libc,'renameat2'):
        result=libc.renameat2(-100,os.fsencode(first),-100,os.fsencode(second),2)  # RENAME_EXCHANGE
    else:raise OSError('Atomic directory replacement is unavailable on this platform')
    if result:
        code=ctypes.get_errno();raise OSError(code,os.strerror(code))


def check_export_directory(target):
    if not target.exists():return
    if not target.is_dir():raise ValueError('Distributed export target must be a directory')
    count=0
    for entry in target.iterdir():
        count+=1
        if count>1025:raise ValueError('Existing export has too many entries')
        if (entry.is_symlink() or not entry.is_file() or entry.stat().st_nlink!=1
                or not (entry.name=='manifest.json' or re.fullmatch(r'rank-[0-9]{4}\.bin',entry.name))):
            raise ValueError('Existing output is not an exclusive state export directory')


def export_distributed(sim,path,binding):
    """Reserve every shard and manifest before creating any output generation."""
    error=None;target=root=lease=lease_identity=None;keys=[];stream=None;temporary=None;published=False
    budget=sim.disk_budget
    try:
        try:
            if len({row['host'] for row in sim.environment})!=1:
                raise ValueError('State export requires a proven same-host output namespace')
            target,root=prepare_export(budget,path)
            check_export_directory(target)
            pending_lease=export_lease(root);lease_identity=pending_lease.__enter__();lease=pending_lease
        except Exception as exc:error=str(exc)
        sim.transport.vote(error)
        generation=sim.transport.broadcast_json(uuid4().hex if sim.rank==0 else None)
        temporary=target.with_name(f'.{target.name}.{generation}.tmp')
        shard=temporary/f'rank-{sim.rank:04d}.bin'
        amount=sim.ownership.blocks_per_rank*sim.store.payload_bytes
        manifest=json.dumps({'n':sim.num_qubits,'b':sim.block_bits,'ranks':sim.size,
            'order':'physical-shard','layout':sim.layout.logical_to_physical,'root':sim.root},
            sort_keys=True,separators=(',',':')).encode()
        error=None
        try:
            for kind,name,size in [('shard',shard.name,amount)]+([('manifest','manifest.json',len(manifest))] if sim.rank==0 else []):
                intent=intent_for(sim.store.directory,root,target/name,temporary/name,kind)
                key=budget.reserve(size,operation=OPERATIONS[kind],owner=sim.run_id,generation=generation,
                                   writing=True,checkpoint_link=intent)
                keys.append((key,size))
        except Exception as exc:error=str(exc)
        sim.transport.vote(error)
        sim.materialize(binding=binding)
        # Materialization can change the logical root/layout, so serialize only now.
        manifest=json.dumps({'n':sim.num_qubits,'b':sim.block_bits,'ranks':sim.size,
            'order':'physical-shard','layout':sim.layout.logical_to_physical,'root':sim.root},
            sort_keys=True,separators=(',',':')).encode()
        error=None
        try:
            if sim.rank==0:
                ensure_parent(target.parent,root);temporary.mkdir()
        except Exception as exc:error=str(exc)
        sim.transport.vote(error)
        error=None
        try:stream=shard.open('xb',buffering=0)
        except Exception as exc:error=str(exc)
        sim.transport.vote(error)
        for h in range(sim.ownership.num_blocks):
            with sim.fetch(h,sim.ownership.shard_owner(h)) as buf:
                error=None
                if buf:
                    try:write_exact(stream,buf.view[:sim.store.payload_bytes])
                    except Exception as exc:error=str(exc)
                sim.transport.vote(error)
        error=None
        try:
            stream.flush();os.fsync(stream.fileno());stream.close();stream=None
            budget.materialize(keys[0][0],amount)
            if sim.rank==0:
                if len(manifest)!=keys[1][1]:raise OSError('Export manifest changed its admitted size')
                with (temporary/'manifest.json').open('xb',buffering=0) as out:
                    write_exact(out,manifest);os.fsync(out.fileno())
                budget.materialize(keys[1][0],len(manifest));fsync_directory(temporary)
        except Exception as exc:error=str(exc)
        sim.transport.vote(error)
        error=None
        if sim.rank==0:
            try:
                lease_identity.assert_current();check_export_directory(target)
                if target.exists():exchange_directories(temporary,target)
                else:os.replace(temporary,target)
                published=True
                fsync_directory(target.parent)
                if temporary.exists():shutil.rmtree(temporary)
                fsync_directory(target.parent)
            except Exception as exc:error=str(exc)
        # No rank releases its reservation before global publication and cleanup.
        sim.transport.vote(error)
        for key,_ in keys:budget.release(key)
        keys=[]
        return Path(path)
    finally:
        close_error=None
        if stream is not None:
            try:stream.close()
            except Exception as exc:close_error=str(exc)
        # On an ordinary pre-publication failure all ranks still participate.
        # A process kill deliberately leaves its purpose/intent and generation.
        if temporary is not None:
            close_errors=sim.transport.control.allgather(close_error)
            cleanup_error=None
            if sim.rank==0 and not published and temporary is not None:
                try:
                    if temporary.exists():shutil.rmtree(temporary)
                    if target.parent.exists():fsync_directory(target.parent)
                except Exception as exc:cleanup_error=str(exc)
            clean=sim.transport.broadcast_json(cleanup_error is None and not any(close_errors) and not published if sim.rank==0 else None)
            if clean:
                for key,_ in keys:budget.release(key)
        if lease is not None:lease.__exit__(None,None,None)
