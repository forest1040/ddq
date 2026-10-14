"""Checksummed JSONL and canonical, typed semantic serialization."""
from dataclasses import asdict
from hashlib import sha256
import json
import os
from pathlib import Path
from contextlib import nullcontext
from .disk_budget import AppendReservation
from .ir import GateOp
from .parameters import Expr, ParameterSymbol, ExactPi


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False)


def content_hash(value): return sha256(canonical(value).encode()).hexdigest()


def expr_data(expr):
    if expr is None: return None
    if expr.op == 'symbol': return {'op':'symbol','symbol':asdict(expr.args[0])}
    if expr.op == 'const': return {'op':'const','value':expr.args[0]}
    if expr.op == 'pi': return {'op':'pi','numerator':expr.args[0].numerator,'denominator':expr.args[0].denominator}
    return {'op':expr.op, 'args':[expr_data(a) for a in expr.args]}


def decode_expr(data):
    if data is None: return None
    if data['op'] == 'symbol': return Expr('symbol',(ParameterSymbol(**data['symbol']),))
    if data['op'] == 'const': return Expr('const',(data['value'],))
    if data['op'] == 'pi': return Expr('pi',(ExactPi(data['numerator'],data['denominator']),))
    return Expr(data['op'],tuple(decode_expr(a) for a in data['args']))


def gate_data(gate):
    data = asdict(gate)
    data['angle'] = expr_data(gate.angle)
    if not gate.paulis: data.pop('paulis')  # Preserve legacy circuit hashes.
    return data


def decode_gate(data):
    data = dict(data)
    data['angle'] = decode_expr(data.get('angle'))
    return GateOp(**data)


class Journal:
    def __init__(self, directory, *, disk_budget_getter=lambda:None, drain_getter=lambda:False):
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True)
        self.streams = {}
        self.protected_cursor = lambda kind: 0
        self._disk_budget_getter,self._drain_getter=disk_budget_getter,drain_getter
        self._append=AppendReservation(disk_budget_getter,operation='journal-append',drain_getter=drain_getter)

    def append(self, kind, payload):
        if kind not in {'gates','local','shared','joint','commits','frame'}: raise ValueError('Invalid journal')
        stream = self.streams.get(kind)
        if stream is None:
            path = self.directory/f'{kind}.jsonl'
            if path.exists() and path.stat().st_size:
                with open(path,'r+b') as repair:
                    end = repair.seek(0,os.SEEK_END)
                    repair.seek(end-1)
                    if repair.read(1) != b'\n':
                        pos,last = end,0
                        while pos:
                            start = max(0,pos-4096)
                            repair.seek(start)
                            part = repair.read(pos-start)
                            index = part.rfind(b'\n')
                            if index >= 0:
                                last = start+index+1; break
                            pos = start
                        if last < self.protected_cursor(kind): raise OSError('Incomplete committed journal tail')
                        repair.truncate(last)
            stream = self.streams[kind] = open(path,'ab',buffering=0)
        record = {'schema_version':1,'payload':payload}
        record['checksum'] = content_hash(record)
        raw = (canonical(record)+'\n').encode()
        if len(raw)>1024**2: raise ValueError('Journal record exceeds bounded 1 MiB envelope')
        # One chunked token covers the record before it is written; a short
        # write keeps the bytes covered for the caller's rollback/recovery.
        self._append.admit(len(raw))
        if stream.write(raw) != len(raw): raise OSError('Incomplete journal write')
        self._append.wrote(len(raw))
        return stream.tell()

    def rollback(self, cursors):
        for path in self.directory.glob('*.jsonl'):
            kind = path.stem
            size = cursors.get(kind,0)
            if type(size) is not int or not 0<=size<=path.stat().st_size:
                raise ValueError('Rollback cursor would extend the journal')
            if size < self.protected_cursor(kind): raise RuntimeError('Cannot truncate committed journal')
            stream = self.streams.pop(kind,None)
            if stream: stream.close()
            with open(path,'r+b') as repair: repair.truncate(size)

    def cursors(self):
        return {p.stem:p.stat().st_size for p in self.directory.glob('*.jsonl')}

    def replay(self, kind, cursor=None):
        path = self.directory/f'{kind}.jsonl'
        if not path.exists():
            if cursor: raise OSError('Missing committed journal')
            return
        with open(path,'rb') as stream:
            while cursor is None or stream.tell() < cursor:
                raw = stream.readline()
                if not raw: break
                if not raw.endswith(b'\n'):
                    if cursor is None: break
                    raise OSError('Incomplete committed journal')
                if cursor is not None and stream.tell() > cursor: raise OSError('Invalid committed cursor')
                try:
                    record = json.loads(raw)
                    checksum = record.pop('checksum')
                    if record.get('schema_version') != 1 or checksum != content_hash(record): raise ValueError('Checksum/schema mismatch')
                except (ValueError, KeyError) as exc: raise OSError('Corrupt journal record') from exc
                yield record['payload']
            if cursor is not None and stream.tell() != cursor: raise OSError('Truncated committed journal')

    def flush(self):
        for stream in self.streams.values(): os.fsync(stream.fileno())

    def close(self):
        for stream in self.streams.values(): stream.close()
        self.streams.clear()
        self._append.release()
