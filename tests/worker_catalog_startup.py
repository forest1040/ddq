"""Real process death in each step of catalog creation and inode binding."""
import os
from pathlib import Path
import sys

from svsim.catalog_admission import CatalogConnection, get_sqlite_guard
from svsim.disk_budget import DiskBudget
import svsim.catalog_startup as startup

base,layout,point=Path(sys.argv[1]),sys.argv[2],sys.argv[3]
store=base/'store';(store/'metadata').mkdir(parents=True)
ledger=store/'metadata/disk-ledger.sqlite3' if layout=='private' else base/'ledger/ledger.sqlite3'
budget=DiskBudget(ledger,[store],persist_private_policy=layout=='private',shared_policy=layout=='shared')
(base/'ledger.txt').write_text(str(ledger))


def die():
    (base/'fault.txt').write_text(point)
    os._exit(17)

original_materialize,original_bind,original_rename=startup.materialize,budget.bind_catalog_startup,startup._exclusive_rename
original_finish=startup.finish_open

def materialize(lease):
    if point=='reserved':die()
    result=original_materialize(lease)
    if point=='pre-open':die()
    return result

def bind(key,**kwargs):
    if kwargs['phase']=='bound' and point=='staged':die()
    result=original_bind(key,**kwargs)
    if kwargs['phase']=='bound' and point=='bound':die()
    if kwargs['phase']=='opened' and point=='bound-open':die()
    return result

def rename(source,target):
    result=original_rename(source,target)
    if point=='published':die()
    return result

def finish(lease):
    if point=='opened':die()
    return original_finish(lease)

startup.materialize=materialize;budget.bind_catalog_startup=bind
startup._exclusive_rename=rename;startup.finish_open=finish
if point=='hot-journal':
    guard=get_sqlite_guard();original_connect=guard.connect
    def connect(*args,**kwargs):
        con=original_connect(*args,**kwargs)
        con.execute('PRAGMA synchronous=EXTRA')
        con.execute('BEGIN');con.execute('CREATE TABLE partial(value)')
        con.execute('INSERT INTO partial VALUES(?)',('x'*9000,))
        die()
    guard.connect=connect
catalog=CatalogConnection(store/'metadata/catalog.sqlite3',budget_getter=lambda:budget,require_native=True)
if point=='ready':die()
raise AssertionError('fault point not reached')
