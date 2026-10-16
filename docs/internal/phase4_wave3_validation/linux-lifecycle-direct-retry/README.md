# O_DIRECT lifecycle retry: completed, then read-only acceptance recheck

Fresh retry, production core 9eb426f, validation revision ca01d57, predeclared stage budget 1150 s / global 1200 s. The actual 390.760-second stage completed with pytest exit 0: 72 passed and 1 non-applicable skip, 73 collected. No numerical/test failure.

The initial validation guard permitted only an inode-probe skip and therefore marked the stage failed. That original summary and JUnit are retained unchanged. The existing sub-page fault test writes 2048 bytes of an admitted 4096-byte slot, violating the O_DIRECT transfer-alignment contract. It is non-applicable to this direct backend, without implying that all aligned short I/O is impossible. Actual direct segment write/fsync/commit kills and physical ENOSPC provide separate failure coverage.

Validation commit 26ddac5 allows only this exact name/reason on backend direct, and rejects it for buffered or any other reason. The saved JUnit was rechecked read-only with expected cases=73; acceptance-recheck.json reports passed, 0 failures, 1 documented non-applicability and no unexpected skip. This recheck does not rerun tests or rewrite the original failed guard report. The first 240-second timeout remains in ../linux-lifecycle-initial.
