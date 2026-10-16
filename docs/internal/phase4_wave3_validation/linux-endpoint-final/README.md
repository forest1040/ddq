# Final bounded endpoint/MPI Linux functionality

Fixed source 89e8ad2, immutable image sha256:3bee5df0c2960ab030ea1e9cb5528638929ab3cb6e45ee2ee073102f1d8cce14, 4 CPU / 8 GiB / network none / nonroot / dedicated scratch. The same immutable v3 context is used by the following performance campaign.

19 passed, 0 skipped, 26.74 seconds. Exact selection: tests/test_endpoint_prepare.py, tests/test_endpoint_identity_budget.py, tests/test_endpoint_heldout_report.py, tests/test_remaining_work_mpi.py. No slow-marker exclusion. The last file runs actual local MPI worker processes, including checkpoint payload shape consistency. Test logs and JUnit are retained; these functional timings are not a performance gain claim. The tests finished before the first v3 measurement container started.
