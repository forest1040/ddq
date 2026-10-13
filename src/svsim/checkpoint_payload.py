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
    return data


def exchange_payload(transport,data,directory,*,disk_budget=None,budget=None,cache=None):
    """Collectively stream JSON; failures drain the stream before the final vote.

    Serialization is bounded by the existing in-memory semantic metadata.
    The transfer buffer is a managed lease, never a whole serialized manifest.
    Each owner fsyncs its copy before the coordinator publishes COMMIT.
    """
    rank=transport.rank
    limit=max(1,min(transport.tile_bytes,transport.metadata_limit//4))
    header=None; error=None
    if rank == 0:
        try:
            data={key:value for key,value in data.items() if key != 'metadata_payload'}
            digest=sha256(); count=0
            for chunk in encoded_chunks(data,limit): digest.update(chunk); count+=len(chunk)
            header={'name':f"payload-{data['snapshot_id']}.json",'bytes':count,'sha256':digest.hexdigest()}
        except Exception as exc: error=str(exc)
    transport.vote(error)
    header=transport.broadcast_json(header)
    directory=Path(directory)/f'rank-{rank:04d}'/'manifests'
    path=directory/header['name']; temporary=path.with_suffix('.'+uuid4().hex+'.tmp')
    stream=None; key=None; lease=None; error=None; existing=False
    try:
        if path.is_file() and path.stat().st_size == header['bytes']:
            saved=sha256()
            with path.open('rb') as source:
                while piece:=source.read(limit): saved.update(piece)
            existing=saved.hexdigest() == header['sha256']
        if not existing:
            if disk_budget is not None: key=disk_budget.reserve(header['bytes'],drain=True)
            directory.mkdir(parents=True,exist_ok=True)
            stream=temporary.open('wb')
        if budget is not None:
            if cache is not None: cache.ensure(BufferLease.allocation_cost(limit,4096))
            lease=BufferLease(budget,limit,4096)
            buffer=lease.view
        else: buffer=memoryview(bytearray(limit))
    except Exception as exc: error=str(exc)
    try:
        transport.vote(error)
        iterator=iter(encoded_chunks(data,limit)) if rank == 0 else None
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
        if rank != 0:
            try:
                with path.open() as source: data=json.load(source)
            except Exception as exc: error=str(exc)
        transport.vote(error)
        data['metadata_payload']=header
        return data
    finally:
        if stream is not None: stream.close()
        if lease is not None: lease.close()
        temporary.unlink(missing_ok=True)
        if disk_budget is not None and key is not None: disk_budget.release(key)
