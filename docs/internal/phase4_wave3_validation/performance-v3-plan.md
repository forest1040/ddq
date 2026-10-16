# P4-002 performance v3 driver plan (2026-10-16)

`examples/p4_002_performance_driver_v3.py` is a host-side, single-case runner.
It was prepared without SSH or job execution. Run it on the Linux host after
the immutable `wave3-performance-context-v3/{src,tests,examples}` snapshot and
the selected container image are available:

```sh
python3 examples/p4_002_performance_driver_v3.py --list-cases
python3 examples/p4_002_performance_driver_v3.py --case overhead-buffered-p1
```

Each invocation runs exactly one stage/backend/rank case, under a 3600-second
hard limit by default. Cases can be started independently and rerun without
overwriting prior attempts. Each attempt records its source hashes, condition
manifest, driver log, partial JSONL/final artifacts, and status. A timeout or
nonzero process exit is recorded; partial rows remain available. Trial errors
and unknown ETA checks are preserved as data rather than removed.

The overhead cases keep the fixed paired design: `rx-ry-low` and `mixed`,
`mixed` at 20 layers, two warmups, seven measured pairs, seed `20261016`. Held-out
cases use the representative workloads `rx-ry-low`, `rx-ry-var`, and `mixed`
across S0/S1/S2, file/RAM, fractions 0.4/0.6/0.8, and one trial (54 checks per
backend/rank case). `upper`, `cz`, and `mixed-var` are explicitly listed as
omitted in each case manifest. This staged subset is not the full 108-check
matrix and cannot support claims about omitted workload classes.

The driver does not set an ETA accuracy or answer-rate acceptance threshold.
The separate result auditor reports all missing/error rows and forces partial
overhead artifacts to remain inconclusive. `backend_resolved_by_rank: [[]]`
remains unverified; it is not proof that a requested backend serviced I/O.


## Frozen execution, 2026-10-17

No v3 measurement ran against the earlier prepared context. That unused context was retained as `wave3-performance-context-v3-pre-endpoint-fix`; the fresh v3 context is production/validation revision `89e8ad2`, including the bounded endpoint-prelude fix, actual trace I/O evidence after timing, and exact-container cleanup. The image is pinned by immutable ID `sha256:3bee5df0c2960ab030ea1e9cb5528638929ab3cb6e45ee2ee073102f1d8cce14` via --image. All source hashes and the supplied revision are recorded per attempt.

The declared order is heldout-buffered-p1, heldout-buffered-p2, heldout-direct-p1, heldout-direct-p2, then overhead-buffered-p1, overhead-buffered-p2, overhead-direct-p1, overhead-direct-p2. The order was fixed before any v3 result. Each case uses fresh scratch and output; no trial data is shared across cases. Four Linux endpoint/MPI test files run before the first measurement. All performance cases run serially, after the separate Linux fault/lifecycle containers have exited. The Mac fast suite and subsequent Mac endpoint evaluation run on a different physical host. No TeX rebuild is planned during the final measurement.

Trial errors, unknown outcomes, command timeouts and failed cleanup remain reportable artifacts. An unsuccessful case does not cause its rows to be removed; the finite batch continues to the remaining predeclared cases.
