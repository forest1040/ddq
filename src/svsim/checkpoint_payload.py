"""Bounded typed metadata streaming to owner-local checkpoint payload files."""
from hashlib import sha256
import json
import os
from pathlib import Path
from uuid import uuid4
import numpy as np
from .cache import BufferLease


def encoded_chunks(data, chunk_bytes):
    encoder=json.JSONEncoder(ensure_ascii=False,sort_keys=True,separators=(',',':'),allow_nan=False)
    for part in encoder.iterencode(data):
        raw=part.encode('utf-8')
        for start in range(0,len(raw),chunk_bytes):
            yield raw[start:start+chunk_bytes]


CHUNK_ENTRIES=512
CHUNKED_KEYS=('gates','history')


def _encoded(data):
    # Same bytes as encoded_chunks, through the C encoder (chunks and format-2 payloads are bounded).
    return json.dumps(data,ensure_ascii=False,sort_keys=True,separators=(',',':'),allow_nan=False).encode('utf-8')


def compact_payload(data,cache=None,sources=None):
    """Payload with ``gates``/``history`` replaced by content-addressed chunk references, and the chunks.

    Chunks are fixed index ranges of CHUNK_ENTRIES entries named by the sha256
    of their canonical JSON, so a checkpoint only writes chunks that changed
    (the tail) instead of the whole growing lists. ``cache`` (caller-owned dict)
    keeps the references of full chunks below the previous commit, valid while
    ``sources[key]`` (the identity of the live list) is unchanged: entries before
    a published cut are never rewritten in place (rollbacks stop at the cut).
    """
    data={k:v for k,v in data.items() if k not in {'metadata_payload','payload_format'}};chunks={}
    for key in CHUNKED_KEYS:
        items=data.pop(key);refs=[]
        identity=(sources or {}).get(key)
        cached=cache.get(key) if cache is not None and identity is not None else None
        reuse=cached[1] if cached and cached[0]==identity else []
        for index,start in enumerate(range(0,len(items),CHUNK_ENTRIES)):
            if index<len(reuse):refs.append(reuse[index]);continue
            raw=_encoded(items[start:start+CHUNK_ENTRIES]);digest=sha256(raw).hexdigest()
            refs.append({'sha256':digest,'bytes':len(raw),'entries':len(items[start:start+CHUNK_ENTRIES])})
            chunks[digest]=items[start:start+CHUNK_ENTRIES]
        if cache is not None and identity is not None:
            cache[key]=(identity,[r for r in refs if r['entries']==CHUNK_ENTRIES])
        data[key+'_chunks']=refs
    data['payload_format']=2
    return data,chunks


def chunk_name(digest):
    return f'chunk-{digest}.json'


def expand_payload(directory,data,rank=0):
    """Inline the chunked lists of a format-2 payload, verifying every chunk's size and sha256."""
    if data.get('payload_format')!=2:return data
    data=dict(data)
    for key in CHUNKED_KEYS:
        items=[]
        for ref in data.pop(key+'_chunks'):
            raw=(Path(directory)/f'rank-{rank:04d}'/'manifests'/chunk_name(ref['sha256'])).read_bytes()
            if len(raw)!=ref['bytes'] or sha256(raw).hexdigest()!=ref['sha256']:
                raise OSError('Checkpoint metadata chunk checksum mismatch')
            chunk=json.loads(raw)
            if len(chunk)!=ref['entries']:raise OSError('Checkpoint metadata chunk length mismatch')
            items.extend(chunk)
        data[key]=items
    return data


def exchange_chunks(transport,refs,chunks,directory,**kwargs):
    """Stream to every rank only the chunks some rank does not hold yet (content-addressed, write-once)."""
    refs=transport.broadcast_json(refs if transport.rank==0 else None)
    folder=Path(directory)/f'rank-{transport.rank:04d}'/'manifests'
    local=[r['sha256'] for r in refs if not ((folder/chunk_name(r['sha256'])).is_file() and
                                             (folder/chunk_name(r['sha256'])).stat().st_size==r['bytes'])]
    missing=set().union(*transport.control.allgather(local))
    for ref in refs:
        if ref['sha256'] in missing:
            _exchange(transport,chunks[ref['sha256']] if transport.rank==0 else None,directory,
                      chunk_name(ref['sha256']),load=False,fast=True,**kwargs)
            missing.discard(ref['sha256'])


def payload_reference(data):
    return data['metadata_payload']


def decision_record(data):
    return {key:data[key] for key in ('schema_version','semantics_version','snapshot_id',
        'commit_decision','root','frontier','size','prepares','metadata_payload')}


def read_payload(directory, decision, rank=0):
    ref=decision['metadata_payload']
    if ref['name'] != f"payload-{decision['snapshot_id']}.json":
        raise OSError('Invalid checkpoint payload name')
    path=Path(directory)/f'rank-{rank:04d}'/'manifests'/ref['name']
    digest=sha256(); count=0
    with path.open('rb') as stream:
        while chunk:=stream.read(16384): digest.update(chunk); count+=len(chunk)
    if count != ref['bytes'] or digest.hexdigest() != ref['sha256']:
        raise OSError('Checkpoint metadata payload checksum mismatch')
    with path.open() as stream: data=json.load(stream)
    data['metadata_payload']=ref
    if decision_record(data) != decision: raise OSError('Checkpoint payload/decision mismatch')
    return expand_payload(directory,data,rank)


def exchange_payload(transport,data,directory,*,disk_budget=None,budget=None,cache=None,checkpoint_context=None):
    """Collectively stream JSON; failures drain the stream before the final vote.

    Serialization is bounded by the existing in-memory semantic metadata.
    The transfer buffer is a managed lease, never a whole serialized manifest.
    Each owner fsyncs its copy before the coordinator publishes COMMIT.
    """
    if transport.rank == 0: data={key:value for key,value in data.items() if key != 'metadata_payload'}
    name=transport.broadcast_json(f"payload-{data['snapshot_id']}.json" if transport.rank == 0 else None)
    # A format-2 payload holds references only and is small; older payloads stream token by token.
    fast=transport.broadcast_json(data.get('payload_format')==2 or any(k.endswith('_chunks') for k in data)
                                  if transport.rank == 0 else None)
    data,header=_exchange(transport,data,directory,name,fast=fast,disk_budget=disk_budget,budget=budget,cache=cache,
                          checkpoint_context=checkpoint_context)
    data['metadata_payload']=header
    return data


def _exchange(transport,data,directory,name,*,load=True,fast=False,disk_budget=None,budget=None,cache=None,
              checkpoint_context=None):
    """Stream one JSON value from rank 0 to ``rank-NNNN/manifests/<name>`` on every rank (write-once)."""
    rank=transport.rank
    limit=max(1,min(transport.tile_bytes,transport.metadata_limit//4))
    header=None; error=None
    if rank == 0:
        try:
            digest=sha256(); count=0
            raw=_encoded(data) if fast else None
            for chunk in ((raw[i:i+limit] for i in range(0,len(raw),limit)) if fast else encoded_chunks(data,limit)):
                digest.update(chunk); count+=len(chunk)
            context=checkpoint_context or ({key:data[key] for key in ('dataset_id','snapshot_id')}
                                          if isinstance(data,dict) and 'dataset_id' in data else None)
            header={'name':name,'bytes':count,'sha256':digest.hexdigest(),'checkpoint_context':context}
        except Exception as exc: error=str(exc)
    transport.vote(error)
    header=transport.broadcast_json(header)
    dataset_root=Path(directory).resolve()
    directory=dataset_root/f'rank-{rank:04d}'/'manifests'
    generation=uuid4().hex
    path=directory/header['name']; temporary=path.with_name('.'+path.name+'.'+generation+'.tmp')
    stream=None; key=None; lease=None; error=None; existing=False
    try:
        from .checkpoint_recovery import _checked_path
        _, prior=_checked_path(dataset_root,str(path.relative_to(dataset_root)),missing=True)
        if prior is not None and prior.st_size == header['bytes']:
            saved=sha256()
            with path.open('rb') as source:
                while piece:=source.read(limit): saved.update(piece)
            existing=saved.hexdigest() == header['sha256']
        if not existing:
            if path.exists() or path.is_symlink():
                raise OSError('Checkpoint metadata conflicts with its write-once target')
            if disk_budget is not None:
                context=header['checkpoint_context']
                if context is None: raise ValueError('Checkpoint metadata needs dataset and snapshot identity')
                stage='metadata-payload' if header['name'].startswith('payload-') else 'metadata-chunk'
                key=disk_budget.reserve(header['bytes'],drain=True,writing=True,
                    operation='checkpoint-'+stage,owner=uuid4().hex,generation=generation,
                    checkpoint_link={'dataset_root':str(dataset_root),**context,'rank':rank,'stage':stage,
                                     'target':str(path),'temporary':str(temporary),'digest':header['sha256']})
            directory.mkdir(parents=True,exist_ok=True)
            stream=temporary.open('xb')
        if budget is not None:
            if cache is not None: cache.ensure(BufferLease.allocation_cost(limit,4096))
            lease=BufferLease(budget,limit,4096)
            buffer=lease.view
        else: buffer=memoryview(bytearray(limit))
    except Exception as exc: error=str(exc)
    try:
        transport.vote(error)
        iterator=(iter([raw[i:i+limit] for i in range(0,len(raw),limit)]) if fast else iter(encoded_chunks(data,limit))) \
            if rank == 0 else None
        digest=sha256(); count=0
        while True:
            chunk=next(iterator,None) if rank == 0 else None
            n=np.array([len(chunk) if chunk is not None else 0],dtype=np.int64)
            transport.control.Bcast([n,transport._MPI.INT64_T],root=0)
            size=int(n[0])
            if size == 0: break
            if not 0 < size <= limit: raise OSError('Invalid checkpoint metadata chunk size')
            if rank == 0: buffer[:size]=chunk
            transport.control.Bcast([buffer[:size],transport._MPI.BYTE],root=0)
            digest.update(buffer[:size]); count+=size
            if error is None and not existing:
                try:
                    if stream.write(buffer[:size]) != size: raise OSError('Truncated checkpoint metadata write')
                except Exception as exc: error=str(exc)
        transport.metadata_bytes+=count
        if count != header['bytes'] or digest.hexdigest() != header['sha256']:
            error='Checkpoint metadata transfer checksum mismatch'
        if error is None and not existing:
            try:
                stream.flush(); os.fsync(stream.fileno()); stream.close(); stream=None
                os.replace(temporary,path)
                fd=os.open(directory,os.O_RDONLY)
                try: os.fsync(fd)
                finally: os.close(fd)
            except Exception as exc: error=str(exc)
        transport.vote(error)
        error=None
        if rank != 0 and load:
            try:
                with path.open() as source: data=json.load(source)
            except Exception as exc: error=str(exc)
        transport.vote(error)
        return data,{key:header[key] for key in ('name','bytes','sha256')}
    finally:
        if stream is not None: stream.close()
        if lease is not None: lease.close()
        temporary.unlink(missing_ok=True)
        if disk_budget is not None and key is not None: disk_budget.release(key)


def distribute_payload(transport,data,directory,*,chunked,expand=True,chunk_cache=None,sources=None,**kwargs):
    """Rank 0's checkpoint data to every rank's payload file, returned expanded on every rank.

    ``chunked`` (format 2) sends only missing write-once chunks plus a payload of
    references; otherwise (format 1, older checkpoints) the whole payload is streamed.
    """
    if not chunked:return exchange_payload(transport,data,directory,**kwargs)
    compacted,chunks=compact_payload(data,chunk_cache,sources) if transport.rank == 0 else (None,None)
    refs=[ref for key in CHUNKED_KEYS for ref in compacted[key+'_chunks']] if transport.rank == 0 else None
    context=transport.broadcast_json({key:data[key] for key in ('dataset_id','snapshot_id')}
                                     if transport.rank==0 else None)
    exchange_chunks(transport,refs,chunks,directory,checkpoint_context=context,**kwargs)
    payload=exchange_payload(transport,compacted,directory,**kwargs)
    return expand_payload(directory,payload,transport.rank) if expand else payload
