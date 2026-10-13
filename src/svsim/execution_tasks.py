"""Bounded private-epoch task coverage and explicit publication states.

This ledger accounts only window metadata. Actual Block, MPI and I/O memory
remains in the shared MemoryBudget via their BufferLease owners. Task ordinals
are positions in the planned source window, never sortable source IDs.
"""
from dataclasses import dataclass
from enum import Enum


class TaskState(str, Enum):
    PLANNED = 'planned'
    RESERVED = 'reserved'
    INPUT_READY = 'input_ready'
    IN_FLIGHT = 'in_flight'
    COMPUTED = 'computed'
    STAGED = 'staged'
    COMMITTED = 'committed'
    FAILED = 'failed'
    DISCARDED = 'discarded'


_NEXT = {
    TaskState.PLANNED: {TaskState.RESERVED, TaskState.FAILED},
    TaskState.RESERVED: {TaskState.INPUT_READY, TaskState.FAILED},
    TaskState.INPUT_READY: {TaskState.IN_FLIGHT, TaskState.FAILED},
    TaskState.IN_FLIGHT: {TaskState.COMPUTED, TaskState.FAILED},
    TaskState.COMPUTED: {TaskState.STAGED, TaskState.FAILED},
    TaskState.STAGED: {TaskState.COMMITTED, TaskState.FAILED},
    TaskState.FAILED: {TaskState.DISCARDED},
    TaskState.COMMITTED: set(),
    TaskState.DISCARDED: set(),
}


@dataclass(slots=True)
class ExecutionTask:
    source_id: int
    ordinal: int
    binding_id: str = ''
    layout_id: object = 0
    placement_epoch: int = 0
    plan_version: int = 0
    policy_version: int = 0
    state: TaskState = TaskState.PLANNED
    input_root: str | None = None
    output_root: str | None = None


class EpochLedger:
    """Publication requires exact source coverage, despite arbitrary completion.

    ``stage`` is called only after the executor has validated Block coverage,
    kernels and request completion for that physical group. It records those
    completed transitions rather than claiming to execute the kernels itself.
    ``commit`` is called after authoritative GlobalCut publication succeeds.
    """
    def __init__(self, source_ids, *, budget=None, metadata_bytes_per_task=256,
                 binding_id='', layout_id=0, placement_epoch=0,
                 plan_version=0, policy_version=0):
        ids = tuple(source_ids)
        if not ids or any(not isinstance(i, int) or isinstance(i, bool) for i in ids):
            raise ValueError('Source window requires integer source IDs')
        if len(ids) != len(set(ids)): raise ValueError('Duplicate source ID in window')
        if metadata_bytes_per_task < 1: raise ValueError('Invalid task metadata accounting')
        self.source_ids = ids
        self.budget = budget
        self.metadata_bytes = len(ids) * metadata_bytes_per_task
        if budget is not None: budget.reserve(self.metadata_bytes)
        try:
            self.tasks = {sid: ExecutionTask(sid, ordinal, binding_id, layout_id,
                                           placement_epoch, plan_version, policy_version)
                          for ordinal, sid in enumerate(ids)}
        except BaseException:
            if budget is not None: budget.release(self.metadata_bytes)
            raise
        self.committed = self.closed = self.failed = False
        self.error = None

    def _check(self):
        if self.closed: raise ValueError('Closed epoch ledger')

    def transition(self, source_id, state):
        self._check()
        if source_id not in self.tasks: raise ValueError('Source ID outside planned window')
        task = self.tasks[source_id]
        state = TaskState(state)
        if state not in _NEXT[task.state]:
            raise ValueError(f'Invalid task transition {task.state.value}->{state.value}')
        task.state = state
        return task

    def stage(self, source_ids, input_root, output_root):
        self._check()
        if self.failed or self.committed: raise ValueError('Epoch is terminal')
        ids = tuple(source_ids)
        if not ids or len(ids) != len(set(ids)): raise ValueError('Empty or duplicate physical group')
        if not isinstance(input_root, str) or not input_root or not isinstance(output_root, str) or not output_root:
            raise ValueError('Group requires input/output roots')
        for sid in ids:
            if sid not in self.tasks: raise ValueError('Source ID outside planned window')
            if self.tasks[sid].state not in {TaskState.PLANNED, TaskState.COMPUTED}:
                raise ValueError('Source group already staged or not computed')
        for sid in ids:
            task = self.tasks[sid]
            if task.state == TaskState.PLANNED:
                for state in (TaskState.RESERVED, TaskState.INPUT_READY,
                              TaskState.IN_FLIGHT, TaskState.COMPUTED):
                    self.transition(sid, state)
            task.input_root, task.output_root = input_root, output_root
            self.transition(sid, TaskState.STAGED)
        return tuple(self.tasks[sid] for sid in ids)

    def validate_coverage(self):
        self._check()
        if self.failed: raise RuntimeError('Failed epoch cannot publish')
        missing = [sid for sid in self.source_ids if self.tasks[sid].state != TaskState.STAGED]
        if missing: raise ValueError(f'Incomplete source coverage: {missing}')
        if any(task.output_root is None or task.input_root is None for task in self.tasks.values()):
            raise ValueError('Missing private root identity')
        return True

    def commit(self):
        self.validate_coverage()
        for sid in self.source_ids: self.transition(sid, TaskState.COMMITTED)
        self.committed = True
        return tuple(self.tasks[sid] for sid in self.source_ids)

    @property
    def private_roots(self):
        return tuple(dict.fromkeys(task.output_root for task in self.tasks.values()
                                   if task.output_root is not None))

    def fail(self, error=None):
        self._check()
        if self.committed: raise ValueError('Authoritative committed epoch cannot be discarded')
        self.failed = True
        self.error = error
        for task in self.tasks.values():
            if task.state == TaskState.DISCARDED: continue
            if task.state != TaskState.FAILED: self.transition(task.source_id, TaskState.FAILED)
            self.transition(task.source_id, TaskState.DISCARDED)
        # Caller removes these private roots through its transactional lifecycle;
        # the ledger itself has no catalog/storage side effects.
        return self.private_roots

    def close(self):
        if self.closed: return
        if not self.committed and not self.failed: self.fail('Epoch closed before publication')
        if self.budget is not None: self.budget.release(self.metadata_bytes)
        self.closed = True

    def __enter__(self): return self
    def __exit__(self, *exc): self.close()
