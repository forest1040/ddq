# PBS 265180: actual cross-host acceptance passed

Explicitly authorized retry on 2026-10-16 after the singleton MPI diagnostic was corrected. PBS placed four MPI ranks (two on each physical node CPUcluster009 / CPUcluster010). Exit_status=0, walltime=33 seconds. All eight cases passed: buffered/direct × run/mismatch/unsupported/recover. Every worker enforces --require-cross-host and reports hosts=2,ranks=4. This is actual execution, not a rendered job or single-host substitute.

The run oracle checks independent datasets and ledgers sharing each node-local coordinator, fork binding inheritance, state/expectation/parameter-shift numerics and collective joint-quota refusal before reservation mutation. Mismatch and unsupported cases assert collective refusal. Recovery checks offline checkpoint intent reconciliation, dry-run immutability, per-host claim republication, idempotence and numerical resume. Assertions and tolerances are in the pinned tests/mpi_volume_coordinator_runtime_worker.py.

The source overlay remains production 9eb426f, SHA256 9cdb8c95f8b6d801d79f58ec35d360d37ee5ac442b2e986f1213bd977481014e. The root-node manifest records all 270 files OK; pbsdsh bootstrap verifies the same inventory on every allocated node before entering the worker cases. Capacity logs cover both nodes before/after each case. Entire /var/tmp/ddq-20009 remained below 8 GiB and free space above 100 GiB. Job-local scratch/tmp cleanup and archive return both succeeded. Earlier node-local artifacts are preserved.

status.txt, cases.txt, eight worker logs, environment.log, source-check.txt, exit-summary.txt, cleanup.log, PBS stdout/stderr, qstat history and the exact submitted launcher are retained. An SSH known-host address warning occurred; the connection and all tests succeeded. The first failed attempt remains separately preserved in ../job-265179.

Scope: two hosts, four ranks, small numerical oracles, node-local filesystems. Shared-filesystem SQLite coordination and a physical hardware capacity or power-loss guarantee are not claimed; strict_capacity_guarantee remains false.
