# P4 volume coordinator Linux follow-up

Date: 2026-10-15. This follow-up checks the normal `DistributedConfig.volume_coordinator` runtime added in `6dc37b5` on the existing `ent` Docker image. The requested Linux run is **blocked**: auto-review rejected exporting this worktree's source to the isolated `ent` context. The pre-existing context `/tmp/ddq-linux-dev-20261014-bnuv6j/context` predates the runtime test and does not contain `tests/test_volume_coordination_runtime.py` or its MPI worker. No source was copied to `ent` and no Linux result is claimed.

## Reproduction on ent

After the exact worktree source is present in the isolated Linux context through an approved transfer, run this from that context on `ent`:

```sh
DDQ_LINUX_IMAGE=ddq-linux:dev-20261014 \
  bash docker/linux/run_p4_coordinator_followup.sh \
  /tmp/ddq-p4-linux-followup-20261015-tmori
```

The runner uses a fixed container hostname, the existing image, the image's non-root user, `--network none`, 4 CPUs, 8 GiB RAM, the documented PID/shared-memory limits, read-only source mounts, and its own scratch/output paths. It runs coordinator tests first with `SVSIM_TEST_BACKEND=buffered`, then with `SVSIM_TEST_BACKEND=direct`. On Linux, the latter passes `backend='direct'` through `DistributedConfig` and `FileBlockStore`; a preflight opens a real file and asserts `StorageFile.backend_identity == 'linux-o-direct'`, then performs an aligned write, fsync, and readback. A fake environment value cannot satisfy this assertion. Each stage has finite 30-second preflight and 870-second pytest limits, for at most 30 minutes total.

Each run records toolchain/container metadata, a SHA-256 manifest for the mounted runtime/test sources and runner, preflight output, pytest logs, and JUnit XML in the supplied evidence directory. Buffered includes the existing disk-coordinator MPI cases; direct includes transport's real aligned O_DIRECT tests, runtime coordinator MPI cases, volume coordinator tests, and recovery tests. If `tests/test_volume_external_usage.py` is present, the runner includes and hashes it in both backend runs.

## Local fallback evidence

The requested Linux execution did not occur, and local macOS results do not substitute for it. From this worktree, the related local suite passed under the available Mac MPI runtime with both `SVSIM_TEST_BACKEND=buffered` (**94 passed, 54.94 seconds**) and `SVSIM_TEST_BACKEND=direct` (**94 passed, 46.52 seconds**). Full logs are `mac-buffered.txt` and `mac-direct.txt`. A first sandboxed attempt failed only where OpenMPI could not bind its loopback sockets (`Operation not permitted`); the same suite passed with the authorized elevated local run. On macOS `direct` means `F_NOCACHE`, not Linux O_DIRECT.

Local source hashes for the runtime integration are recorded in `source-hashes.txt`. The Linux run must regenerate hashes from the exact source mounted into the container before its result can be accepted. `docker/linux/run_p4_coordinator_followup.sh` passes `bash -n`.

## Transfer constraint

The attempted `rsync` from this private repository worktree to `ent:/tmp/ddq-p4-linux-followup-20261015-tmori/context/` was rejected by auto-review as unauthorized source egress. Do not infer approval from the available SSH connection. Approval for that exact source/destination pair is needed before transfer; no alternate transfer path was used.
