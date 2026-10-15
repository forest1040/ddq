# P4-009 cross-host coordinator acceptance: prepared, not submitted

The reviewable job is [coordinator.pbs](coordinator.pbs). It runs the real runtime
worker on two distinct PBS nodes, two ranks on each node. Each of buffered and
O_DIRECT runs independent-dataset/fork/parameter-shift/joint admission, conflicting
coordinator refusal, unsupported-filesystem collective refusal, and offline
checkpoint recovery/claim republication/resume. The worker explicitly rejects a
single physical hostname when `--require-cross-host` is given.

Requested authorization: one new workq job, select=2:ncpus=4:mpiprocs=2:mem=8gb,
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

Source is packaged after the current fixes are committed; exact SHA256 manifests
are retained with results. No cross-host result is claimed until the actual job
passes. This prepared script has passed `bash -n` only.
