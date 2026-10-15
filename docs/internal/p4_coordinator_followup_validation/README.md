# P4 volume coordinator Linux follow-up

Date: 2026-10-15. This follow-up validates the normal `DistributedConfig.volume_coordinator` runtime added in `6dc37b5`, external-job growth monitoring, and the integrated sources on the existing `ent` Docker image. Linux **buffered: 105 passed in 82.51s**; **direct: 112 passed, 2 skipped in 83.83s**. The direct preflight asserted `linux-o-direct` and passed a real aligned write, fsync, and readback. Runtime cases include real MPI at 1/2 ranks. Phase 4 and P4-009 remain IN_PROGRESS.

The user explicitly approved transfer of `src/`, `tests/`, and `examples/` to the dedicated ent context. Auto-review initially refused transfer, then refused a wider payload containing Docker definitions and project metadata. Only the three approved source directories were transferred; image-provided project metadata and dependencies were used. Luna completed the runner/local evidence and began the buffered run; after Luna stopped due to exhausted credits, the parent completed direct validation and collected evidence. Logs, JUnit, environment details and hashes are in [linux/](linux/). All 244 files in the mounted source manifest match local integrated sources. No cluster jobs or image rebuilds were performed.

## Actual execution and evidence

Remote context: `/tmp/ddq-p4-linux-followup-20261015-tmori/context`; separate sibling `scratch/` and `output/`. Source mounts were only `src:/workspace/src:ro`, `tests:/workspace/tests:ro`, and `examples:/workspace/examples:ro`. Project configuration, README and lockfile were **not transferred or mounted**. Other limits, fixed hostname, non-root user, environment and pytest selections follow the runner below. Both backend runs included `tests/test_volume_external_usage.py`. Direct also included transport and transport-buffer tests; buffered included disk-coordinator tests. The direct skips are macOS-only transport cases; no coordinator runtime case was skipped.

The host was Linux x86_64 kernel 6.8.0-138-generic; Python 3.12.15, Open MPI 4.1.4, image `sha256:3bee5df0c2960ab030ea1e9cb5528638929ab3cb6e45ee2ee073102f1d8cce14`, image user `ddq:ddq`. See `linux/host-and-image.txt`, `linux/source-hashes.txt`, and the two `volume-coordination-*.txt`/`.xml` pairs. The integrated Mac fast suite is `integrated-fast.txt`: 1044 passed, 35 skipped, 83 deselected in 204.33s.

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

Before the Linux run, the related local suite passed under the available Mac MPI runtime with both `SVSIM_TEST_BACKEND=buffered` (**94 passed, 54.94 seconds**) and `SVSIM_TEST_BACKEND=direct` (**94 passed, 46.52 seconds**). Full logs are `mac-buffered.txt` and `mac-direct.txt`. These pre-integration local results do not substitute for Linux. A first sandboxed attempt failed only where OpenMPI could not bind its loopback sockets (`Operation not permitted`); the same suite passed with the authorized elevated local run. On macOS `direct` means `F_NOCACHE`, not Linux O_DIRECT.

Local source hashes for the runtime integration are recorded in `source-hashes.txt`. The Linux run must regenerate hashes from the exact source mounted into the container before its result can be accepted. `docker/linux/run_p4_coordinator_followup.sh` passes `bash -n`.

## Transfer constraint

Initial transfer was rejected as unauthorized source egress. After explicit user approval, a proposal including additional project metadata was also rejected. The final transfer contained only the explicitly approved `src/`, `tests/`, and `examples/`, and succeeded. No rejected payload was transferred through an alternate path. SSH connectivity alone was not treated as export permission.
