# Final actual MPI checkpoint failure matrix

Production core 9eb426f, validation revision ca01d57. Each backend runs the complete 30-cell checkpoint kill expansion and six physical ENOSPC expansions on two actual MPI ranks, with no slow-marker exclusion. Dedicated container-local 96 MiB tmpfs, nonroot, network none, 4 CPU / 8 GiB; each backend has a 1200-second deadline. Buffered completed in 220.961 s, direct in 139.191 s.

Each backend: 36 collected, 30 passed, six non-applicable skips. The skips are exactly decision/rank1 (five kill positions plus ENOSPC): only rank 0 writes the global decision. Exact node-name sets and exact skip reasons were checked. No MPI, native guard, missing FULL mount or unexpected skip was accepted. Each real process death/disk-fill is followed by a second MPI recovery run and the required old/new-cut numerical oracle. See retained JUnit, per-backend source manifests, logs, state/status and exit-status 0.

These are functional fault tests. They overlapped the independent lifecycle direct retry for part of execution on the 20-CPU / 32-GiB host, with separate datasets, namespaces and tmpfs mounts; no performance measurement ran concurrently. Timings are diagnostic only and cannot support a backend speed claim.
