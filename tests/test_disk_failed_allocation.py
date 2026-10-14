"""Failed-write retention is explicit and does not change legacy contexts."""
import pytest

from svsim.disk_budget import DiskBudget


@pytest.fixture
def budget(tmp_path):
    result=DiskBudget(tmp_path/'ledger.sqlite',[tmp_path],usage=lambda:0,free=lambda:10**9)
    try:yield result
    finally:result.close()


def test_failed_write_can_retain_recovery_evidence(budget):
    with pytest.raises(OSError,match='failure'):
        with budget.allocation(10,retain_on_error=True) as key:
            budget.materialize(key,4)
            raise OSError('write failure')
    assert budget.reserved_bytes==6
    assert budget.reservation(key)['state']=='WRITING'
    assert budget.reservation(key)['materialized_bytes']==4


def test_retaining_context_releases_success_and_verified_cleanup(budget):
    with budget.allocation(10,retain_on_error=True) as success:pass
    assert budget.reservation(success) is None
    with pytest.raises(OSError):
        with budget.allocation(10,retain_on_error=True) as failed:
            budget.release(failed)  # caller has verified its cleanup
            raise OSError('write failure')
    assert budget.reservation(failed) is None and budget.reserved_bytes==0


def test_retention_option_is_validated_before_reservation(budget):
    with pytest.raises(ValueError,match='retain-on-error'):
        with budget.allocation(10,retain_on_error=1):pass
    assert budget.db.execute('SELECT count(*) FROM reservations').fetchone()[0]==0
