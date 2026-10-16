# P4-009 cross-host coordinator acceptance

The reviewable job is [coordinator.pbs](coordinator.pbs). It runs the real runtime
worker on two distinct PBS nodes, two ranks on each node. Each of buffered and
O_DIRECT runs independent-dataset/fork/parameter-shift/joint admission, conflicting
coordinator refusal, unsupported-filesystem collective refusal, and offline
checkpoint recovery/claim republication/resume. The worker explicitly rejects a
single physical hostname when `--require-cross-host` is given.

Each separately authorized attempt uses one workq job, select=2:ncpus=4:mpiprocs=2:mem=8gb,
place=scatter, walltime 10 minutes. Upload only `src/`, `tests/`, `examples/` plus
SHA256 inventory as an overlay archive, the archive hash and this PBS script to
`lg:/var/tmp/ddq-20009/p4-acceptance-20261016/`. Code and tests are private repository
contents. PBS distributes them from the login node to each allocated node's same
local path; the existing verified runtime at
`/var/tmp/ddq-20009/phase4-dfdd4a403586` is reused, or its existing archive is copied
and checksum-verified when missing on a node. No dependency installation, mount,
admin command, or computation on the login node is requested.

Each node checks local ext4/xfs/btrfs, at least 100 GiB free, and at most 8 GiB in
`/var/tmp/ddq-20009` (including the runtime, source, scratch, old user artifacts).
Each MPI command is limited to 55 seconds; capacity is checked between commands.
Only this job's scratch/tmp are removed on completion. Logs and checksum results
are copied back to the login node, then downloaded to the repository evidence
folder. Dataset, code, and runtime never execute from shared `/home`. Earlier
job directories and environments are preserved.

The reviewable source bundle is prepared from production commit `9eb426f` after
its full fast suite passed (1317 passed). It has 270 tracked source/test/example
files, archive size 659201 bytes, SHA256
`9cdb8c95f8b6d801d79f58ec35d360d37ee5ac442b2e986f1213bd977481014e`.
The local archive is `/private/tmp/ddq-p4-crosshost-20261016/overlay.tar.gz`;
[planned-source.sha256](planned-source.sha256) lists every member digest.
The user explicitly authorized the upload and one PBS job on 2026-10-16. Job `265179.CPUcluster-PRISM` reached two distinct nodes, verified the overlay and capacity, then failed before any worker case during singleton MPI initialization (Exit_status 14). See [verbatim failure evidence](job-265179/README.md). The corrected launcher avoids importing MPI in singleton diagnostics and returns stage/status/logs through an EXIT trap. Its local fake PBS/SSH regression passed 8 cases. The user explicitly authorized one retry, `265180.CPUcluster-PRISM`, which passed all 8 actual worker cases on 2 physical hosts / 4 MPI ranks, Exit_status 0 and walltime 33 s. [All results, source checks and cleanup evidence](job-265180/README.md) are retained.
The later campaign-only incomplete-CI guard does not affect this worker. This
prepared bundle stays pinned to the tested production source above.
Exact SHA256 manifests are retained with results. The initial script passed `bash -n`; actual execution exposed the singleton MPI initialization problem above. The corrected script passed the local 8-case failure regression and all eight actual cross-host worker cases.
