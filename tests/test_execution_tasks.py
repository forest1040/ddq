import pytest
from svsim.cache import MemoryBudget
from svsim.execution_tasks import EpochLedger, TaskState


def test_arbitrary_completion_order_exact_coverage_and_metadata_release():
    budget = MemoryBudget(4096)
    ledger = EpochLedger([7, 1, 9], budget=budget, binding_id='binding',
                         layout_id=2, placement_epoch=3, plan_version=4, policy_version=5)
    assert budget.used == 768
    ledger.stage([9], 'input', 'root-c')
    with pytest.raises(ValueError, match='Incomplete'): ledger.commit()
    assert not ledger.committed
    ledger.stage([7, 1], 'input', 'root-ab')
    result = ledger.commit()
    assert [t.source_id for t in result] == [7, 1, 9]
    assert [t.ordinal for t in result] == [0, 1, 2]
    assert all(t.state == TaskState.COMMITTED and t.binding_id == 'binding' for t in result)
    with pytest.raises(ValueError, match='committed'): ledger.fail()
    ledger.close(); ledger.close()
    assert budget.used == 0


def test_duplicate_unknown_hole_and_failure_never_publish():
    with pytest.raises(ValueError, match='Duplicate'): EpochLedger([1, 1])
    ledger = EpochLedger([3, 8])
    with pytest.raises(ValueError, match='outside'): ledger.stage([4], 'in', 'out')
    ledger.stage([3], 'in', 'private')
    with pytest.raises(ValueError, match='already'): ledger.stage([3], 'in', 'other')
    assert ledger.fail('write failure') == ('private',)
    assert all(t.state == TaskState.DISCARDED for t in ledger.tasks.values())
    with pytest.raises(RuntimeError, match='Failed'): ledger.commit()
    ledger.close()


def test_task_transition_validation_and_close_discards_unpublished_roots():
    budget = MemoryBudget(1024)
    ledger = EpochLedger([99], budget=budget)
    with pytest.raises(ValueError, match='Invalid'): ledger.transition(99, TaskState.COMPUTED)
    for state in (TaskState.RESERVED, TaskState.INPUT_READY, TaskState.IN_FLIGHT, TaskState.COMPUTED):
        ledger.transition(99, state)
    ledger.stage([99], 'durable', 'private')
    ledger.close()
    assert ledger.tasks[99].state == TaskState.DISCARDED and budget.used == 0
