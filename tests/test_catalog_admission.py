"""Real SQLite reservations precede mutations and cover cursor/backup paths."""
import sqlite3
import pytest

from svsim.catalog_admission import CatalogConnection, CatalogAdmissionError
from svsim.disk_budget import DiskBudget, DiskAdmissionError
from svsim.journal import Journal
from svsim.telemetry import StreamingExecutionTrace


@pytest.fixture
def catalog(tmp_path):
    budget=DiskBudget(tmp_path/'ledger.sqlite',[tmp_path],quota_bytes=32*1024**2)
    db=CatalogConnection(tmp_path/'catalog.sqlite',budget_getter=lambda:budget,require_native=True)
    db.execute('CREATE TABLE items(value BLOB)')
    try: yield db,budget
    finally: db.close();budget.close()


def test_quota_rejection_precedes_begin_and_preserves_committed_rows(catalog):
    db,budget=catalog
    with db: db.execute('INSERT INTO items VALUES(?)',(b'old',))
    mutations=db.mutation_count
    budget.quota_bytes=1
    with pytest.raises(DiskAdmissionError): db.execute('INSERT INTO items VALUES(?)',(b'new',))
    assert not db.in_transaction
    assert db.mutation_count==mutations and budget.reserved_bytes==0
    assert db.execute('SELECT value FROM items').fetchall()==[(b'old',)]


def test_cursor_and_script_share_reservations_and_rollback(catalog):
    db,budget=catalog
    db.cursor().executescript('INSERT INTO items VALUES(1); INSERT INTO items VALUES(2);')
    with pytest.raises(RuntimeError,match='rollback'):
        with db:
            db.cursor().executemany('INSERT INTO items VALUES(?)',[(3,),(4,)])
            assert budget.reserved_bytes>0
            raise RuntimeError('rollback')
    assert db.execute('SELECT value FROM items ORDER BY value').fetchall()==[(1,),(2,)]
    assert budget.reserved_bytes==0 and not db.admission_status()['active']


def test_generated_sql_payload_is_stopped_by_native_page_cap(catalog):
    db,budget=catalog
    with db: db.execute('INSERT INTO items VALUES(?)',(b'old',))
    before=db.path.stat().st_size
    with pytest.raises(sqlite3.DatabaseError):
        with db: db.execute('INSERT INTO items SELECT zeroblob(2000000)')
    assert db.path.stat().st_size<=before+db.growth_bytes
    assert db.execute('SELECT value FROM items').fetchall()==[(b'old',)]
    assert budget.reserved_bytes==0


@pytest.mark.parametrize('sql',[
    'PRAGMA journal_mode=WAL','PRAGMA writable_schema=ON',
    'PRAGMA synchronous=OFF','SELECT svsim_disk_guard_config(99999999,99999999,1)',
])
def test_caller_cannot_change_guard_or_durability(catalog,sql):
    db,budget=catalog
    with pytest.raises(CatalogAdmissionError): db.execute(sql)
    assert not db.admission_status()['active'] and budget.reserved_bytes==0


def test_backup_reserves_complete_destination_before_native_copy(catalog,tmp_path):
    db,budget=catalog
    source=sqlite3.connect(tmp_path/'source.sqlite')
    source.execute('CREATE TABLE items(value BLOB)')
    source.execute('INSERT INTO items VALUES(?)',(b'x'*400000,));source.commit()
    try: db.backup_from(source)
    finally: source.close()
    assert db.execute('SELECT length(value) FROM items').fetchone()[0]==400000
    assert budget.reserved_bytes==0 and not db.admission_status()['active']


def test_second_quiescent_connection_preserves_shared_catalog(catalog):
    db,budget=catalog
    other=CatalogConnection(db.path,budget_getter=lambda:budget,require_native=True)
    try:
        with other: other.execute('INSERT INTO items VALUES(5)')
        assert db.execute('SELECT value FROM items').fetchone()==(5,)
        with db:
            with pytest.raises(CatalogAdmissionError,match='Another connection'):
                other.execute('INSERT INTO items VALUES(6)')
    finally: other.close()
    assert budget.reserved_bytes==0


def test_open_failure_releases_startup_reservation(tmp_path):
    budget=DiskBudget(tmp_path/'ledger.sqlite',[tmp_path],quota_bytes=32*1024**2)
    try:
        for _ in range(2):
            with pytest.raises(sqlite3.OperationalError):
                CatalogConnection(tmp_path/'missing'/'catalog.sqlite',budget_getter=lambda:budget,require_native=True)
            assert budget.reserved_bytes==0
    finally: budget.close()


def test_trace_and_semantic_journal_refuse_growth_before_write(catalog,tmp_path):
    _,budget=catalog
    trace=StreamingExecutionTrace(tmp_path/'trace.jsonl',profile_id='p',plan_id='p',disk_budget_getter=lambda:budget)
    journal=Journal(tmp_path/'journal',disk_budget_getter=lambda:budget)
    trace.record('old');journal.append('gates',{'value':'old'})
    before_trace=trace.path.read_bytes();before_journal=(journal.directory/'gates.jsonl').read_bytes()
    budget.quota_bytes=1
    try:
        with pytest.raises(DiskAdmissionError): trace.record('new')
        with pytest.raises(DiskAdmissionError): journal.append('gates',{'value':'new'})
        assert trace.path.read_bytes()==before_trace
        assert (journal.directory/'gates.jsonl').read_bytes()==before_journal
        assert budget.reserved_bytes==0
    finally: trace.close();journal.close()


def test_journal_rollback_cannot_extend_file_without_reservation(tmp_path):
    journal=Journal(tmp_path/'journal')
    try:
        end=journal.append('gates',{'value':'old'})
        with pytest.raises(ValueError,match='extend'):
            journal.rollback({'gates':end+1})
        assert (journal.directory/'gates.jsonl').stat().st_size==end
    finally: journal.close()
