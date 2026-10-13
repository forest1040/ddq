"""One managed-memory ledger across prefix and active suffix runtimes."""


def share_budget(parent, child):
    if child.cache.entries or getattr(child.store, 'resident', {}):
        raise ValueError('Budget transfer requires an empty newly forked cache')
    child.drain()
    amount = child.budget.used
    parent.cache.ensure(amount)
    parent.budget.reserve(amount)
    old = child.budget
    old.release(amount)
    child.budget = child.local.budget = child.cache.budget = parent.budget
    child.io_engine.budget = parent.budget
    if hasattr(child.store, 'budget'):
        child.store.budget = parent.budget
    child._shared_metadata_bytes = (amount-child.io_engine.metadata_bytes-
                                   child.runtime._measurement_budget)
    return child


def close_shared(child):
    budget = child.budget
    count = getattr(child, '_shared_metadata_bytes', 0)
    try:
        child.close()
    finally:
        if count:
            budget.release(count)
            child._shared_metadata_bytes = 0
