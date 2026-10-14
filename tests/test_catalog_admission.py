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
    budget.quota_bytes=1
    committed=[(b'old',)]
    # The live ledger token keeps covering admitted growth; once cumulative
    # growth exhausts it, the next transaction is refused before BEGIN.
    for i in range(64):
        mutations=db.mutation_count
        try:
            with db: db.execute('INSERT INTO items VALUES(?)',(bytes([i%256])*60000,))
        except DiskAdmissionError: break
        committed.append((bytes([i%256])*60000,))
    else: raise AssertionError('catalog growth was never refused')
    assert not db.in_transaction
    assert db.mutation_count==mutations and budget.reserved_bytes==0
    assert db.execute('SELECT value FROM items').fetchall()==committed


def test_cursor_and_script_share_reservations_and_rollback(catalog):
    db,budget=catalog
    db.cursor().executescript('INSERT INTO items VALUES(1); INSERT INTO items VALUES(2);')
    with pytest.raises(RuntimeError,match='rollback'):
        with db:
            db.cursor().executemany('INSERT INTO items VALUES(?)',[(3,),(4,)])
            assert budget.reserved_bytes>0
            raise RuntimeError('rollback')
    assert db.execute('SELECT value FROM items ORDER BY value').fetchall()==[(1,),(2,)]
    assert budget.reserved_bytes==budget.lease_reserved_bytes and not db.admission_status()['active']


def test_generated_sql_payload_is_stopped_by_native_page_cap(catalog):
    db,budget=catalog
    with db: db.execute('INSERT INTO items VALUES(?)',(b'old',))
    before=db.path.stat().st_size
    with pytest.raises(sqlite3.DatabaseError):
        with db: db.execute('INSERT INTO items SELECT zeroblob(2000000)')
    assert db.path.stat().st_size<=before+db.growth_bytes
    assert db.execute('SELECT value FROM items').fetchall()==[(b'old',)]
    assert budget.reserved_bytes==budget.lease_reserved_bytes


@pytest.mark.parametrize('sql',[
    'PRAGMA journal_mode=WAL','PRAGMA writable_schema=ON',
    'PRAGMA synchronous=OFF','SELECT svsim_disk_guard_config(99999999,99999999,1)',
])
def test_caller_cannot_change_guard_or_durability(catalog,sql):
    db,budget=catalog
    with pytest.raises(CatalogAdmissionError): db.execute(sql)
    assert not db.admission_status()['active'] and budget.reserved_bytes==budget.lease_reserved_bytes


def test_backup_reserves_complete_destination_before_native_copy(catalog,tmp_path):
    db,budget=catalog
    source=sqlite3.connect(tmp_path/'source.sqlite')
    source.execute('CREATE TABLE items(value BLOB)')
    source.execute('INSERT INTO items VALUES(?)',(b'x'*400000,));source.commit()
    try: db.backup_from(source)
    finally: source.close()
    assert db.execute('SELECT length(value) FROM items').fetchone()[0]==400000
    assert budget.reserved_bytes==budget.lease_reserved_bytes and not db.admission_status()['active']


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
    assert budget.reserved_bytes==budget.lease_reserved_bytes


def test_open_failure_releases_startup_reservation(tmp_path):
    budget=DiskBudget(tmp_path/'ledger.sqlite',[tmp_path],quota_bytes=32*1024**2)
    try:
        for _ in range(2):
            with pytest.raises(sqlite3.OperationalError):
                CatalogConnection(tmp_path/'missing'/'catalog.sqlite',budget_getter=lambda:budget,require_native=True)
            assert budget.reserved_bytes==0
    finally: budget.close()


def test_trace_and_semantic_journal_refuse_growth_before_write(catalog,tmp_path):
    from svsim.disk_budget import APPEND_RESERVATION_BYTES
    _,budget=catalog
    trace=StreamingExecutionTrace(tmp_path/'trace.jsonl',profile_id='p',plan_id='p',disk_budget_getter=lambda:budget)
    journal=Journal(tmp_path/'journal',disk_budget_getter=lambda:budget)
    trace.record('old');journal.append('gates',{'value':'old'})
    before_trace=trace.path.stat().st_size;before_journal=(journal.directory/'gates.jsonl').stat().st_size
    budget.quota_bytes=1
    try:
        # Bytes inside the already admitted chunk remain admitted; the first
        # record beyond it is refused before any byte is written.
        for _ in range(APPEND_RESERVATION_BYTES):
            size=trace.path.stat().st_size
            try: trace.record('new',metadata={'pad':'x'*512})
            except DiskAdmissionError: break
        else: raise AssertionError('trace growth was never refused')
        assert trace.path.stat().st_size==size<=before_trace+APPEND_RESERVATION_BYTES
        for _ in range(APPEND_RESERVATION_BYTES):
            size=(journal.directory/'gates.jsonl').stat().st_size
            try: journal.append('gates',{'value':'x'*512})
            except DiskAdmissionError: break
        else: raise AssertionError('journal growth was never refused')
        assert (journal.directory/'gates.jsonl').stat().st_size==size<=before_journal+APPEND_RESERVATION_BYTES
        assert budget.reserved_bytes==budget.lease_reserved_bytes
    finally: trace.close();journal.close()


def test_journal_rollback_cannot_extend_file_without_reservation(tmp_path):
    journal=Journal(tmp_path/'journal')
    try:
        end=journal.append('gates',{'value':'old'})
        with pytest.raises(ValueError,match='extend'):
            journal.rollback({'gates':end+1})
        assert (journal.directory/'gates.jsonl').stat().st_size==end
    finally: journal.close()


def test_catalog_ledger_token_is_reused_until_cumulative_growth_needs_a_new_one(catalog):
    db,budget=catalog
    first=db.admission_status()['ledger_reservation_count']
    for i in range(50):
        with db: db.execute('INSERT INTO items VALUES(?)',(i,))
    assert db.admission_status()['reservation_count']>=50  # every transaction still has an envelope
    assert db.admission_status()['ledger_reservation_count']==first  # but no new ledger commit
    for i in range(30):
        with db: db.execute('INSERT INTO items VALUES(?)',(b'x'*60000,))
    assert db.admission_status()['ledger_reservation_count']>first
    live=budget.snapshot()['states']['WRITING']
    assert live['count']==1  # previous tokens were released when replaced
    db.close()
    assert budget.reserved_bytes==0
