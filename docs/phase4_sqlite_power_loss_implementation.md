# P4-009: SQLite namespace durability and power-loss model

2026-10-16. Catalog, reservation ledger (including recovery writes), and volume coordinator now use `journal_mode=DELETE` with `synchronous=EXTRA`. The catalog proxy refuses callers weakening this setting. Ordinary state-store initialization uses the same setting.

SQLite's [synchronous documentation](https://www.sqlite.org/pragma.html#pragma_synchronous) distinguishes FULL from EXTRA: in DELETE mode, EXTRA also syncs the journal's directory after unlink. With FULL a transaction acknowledged just before a power failure can be rolled back when an unsynced journal deletion is lost. That is unsafe for an acknowledged increase published to a coordinator before a ledger commits, or for catalog rows subsequently referenced by durable checkpoint metadata. EXTRA protects this namespace transition subject to the filesystem/VFS honoring its sync operations. It does not guarantee safety against a device lying about successful sync.

## Model

The native budget VFS contains a bounded test-only durable-image model under `SVSIM_TEST_FAULTS`, the same explicit build flag used by existing deterministic fault tests. Production binaries expose neither model function. `svsim_test_power_arm(filter, image_directory)` starts from the initialized, durable database/journal pairs of the matching open participants. A successful native `xSync` saves that file's complete durable image; an `xDelete` with directory sync records durable deletion. Unacknowledged writes, truncates, and unsynced deletion do not update durable images. After child process death the parent restores those images in place before reopening SQLite, discarding volatile bytes and namespace changes. The control images themselves are the oracle and are outside the modeled/accounted dataset.

The model is limited to 32 files, 16 MiB per file. It uses deterministic cuts before/after SQLite journal sync, main sync, and journal delete. It models loss of all volatile changes, including resurrection of an unsynced journal. It is not a physical unplug experiment, a model of torn writes, arbitrary reordering of already synchronized writes, uninitialized database namespace creation, arbitrary external files, or dishonest hardware. Existing segment-loss tests cover segment bytes and publication order separately. Creation/startup identity and other writer lifecycle conditions remain separate acceptance obligations.

## Tests

`tests/test_sqlite_power_loss.py` uses real SQLite and the same-library native budget VFS:

- Eight catalog durability cuts recover exactly the old or new complete cut and pass integrity checks.
- A negative control acknowledges a FULL-mode commit, loses unsynced journal deletion, and demonstrates rollback. The EXTRA-mode counterpart retains the acknowledged commit. Thus successful process-kill recovery alone cannot satisfy this test.
- Eight coordinator/ledger cuts span coordinator increase, ledger increase, ledger decrease, and coordinator decrease. The durable coordinator claim is never below the live ledger's unmaterialized bytes. Writable reopen republishes the true pending value.
- A production build has no power-model functions.

Mac related regression: **88 passed** (`test_sqlite_power_loss`, `test_vfs_fault_points`, `test_catalog_admission`, `test_catalog_capability`, `test_disk_coordinator`, `test_volume_coordinator`), including real MPI. Linux buffered/O_DIRECT runs are recorded by the integration validation. Model images deliberately do not preserve a real machine's crash cache and are reported as a model, not physical power-loss evidence.

P4-009 remains IN_PROGRESS until all managed writers, recovery intents, lifecycle adapters, and required deployment scopes satisfy the acceptance matrix. `strict_capacity_guarantee` remains false.
