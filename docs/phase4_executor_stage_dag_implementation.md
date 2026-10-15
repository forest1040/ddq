# P4-002: bounded executor stage DAG

`executor_stages.py` is shared by the actual asynchronous and distributed
executor dispatch and the remaining-work extractor. `route_kind` preserves
physical-swap bypass, upper-only permutation, local Frame rule, local operation
shipping, MCX packed/full, joint exchange, and fused-local dispatch. The bounded
next window and epoch-aligned counted future attach an `ExecutorRoute` to each
executor `CountedTask`; classes retain source sets/counts and existing parent
DAG dependencies. No block labels, views, extents or cache entries are scanned.
Serialized stage nodes are compact six-element tuples: stage name, predecessor
names, resource, count lower, count upper, unknown reasons. Their Python
`NamedTuple` fields keep audit code readable while avoiding repeated JSON keys.
Counts use fixed-bit predicates and the rank ownership prefix, including upper
controls and the highest-bit-clear canonical XOR pair base. A 50-qubit synthetic
oracle checks the same bounded representation without expanding the block space.

Stage nodes express actual route dependencies and resources. Metadata-only
permutations and Frame rules have just metadata/commit nodes. Data routes carry
read, Frame resolution, main kernel, deferred write, metadata and commit nodes;
MPI nodes are present only for routes that can ship amplitude data. Packed MCX
has gather/packing and unpacking; same-owner pairs execute the partial kernel,
whereas remote pairs use exchange and scatter. Fusion records one fetch/state
pass with each original gate's kernel invocation, rather than charging one state
pass for every gate. S1 physical relocation and rotation-log overflow compaction
remain conditional. Unknown cache hits, zero extents, input placement, Frame/log
work and deferred writeback carry explicit count bounds and reasons. Physical
read/resolve bounds allow all global blocks to reside on one storage worker.

The pricing contract is `inclusive-executor-stages-v1`. A measured executor
`protocol_seconds` contains these stages, including conditional relocation or
compaction, once. It excludes publication/boundary calls, which remain separate
priced parents. The measured runtime condition and extractor condition both
carry the route and contract version; the existing cost adapter still requires
full condition/context equality, measured and validated evidence. An unpriced
parent leaves the total ETA null; a matching inclusive parent can cover its
conditional children, but children are never independently added to its cost.
Residual calls retain this contract inside their original class descriptor.
`executor_stage_coverage` explicitly distinguishes inclusion in a priced parent
from independently calibrated stages (currently zero). Expected/upper sums do
not credit overlap and `eligible_for_control` remains false.

`payload_byte_coverage` measures MPI amplitude **send** bytes, each direction
once, excluding metadata/control and storage traffic. Virtual rules/permutations
and operation shipping send zero amplitude bytes; operation shipping computes at
the storage worker even with several ranks. P=1 has zero MPI amplitude bytes.
Unknown multi-rank placement and optional compaction keep payload denominators
unknown, with finite conservative byte bounds on the route. Logical shard size
is never substituted for transferred payload.

`kernel_work_coverage` measures rank-local main-kernel amplitude visits,
excluding Frame and packing. It uses actual upper-control predicates, coordinator
ownership, fusion gate calls, and packed MCX's active amplitude count; it is not
a FLOP count or semantic gate count. S1 and possible log-compaction work remain
unknown. `storage_byte_coverage` separately measures physical amplitude storage
reads plus writes, excluding catalog/journals; cache, dirty writeback, and
publication uncertainty stay unknown even if MPI coverage is one with zero
bytes. Frame and packing uncertainty remains visible in stage descriptors.

Capture serialization includes stage plans and storage weights in its existing
metadata limit. Shared contract/scope text is not repeated per route. Counted
future classes account for their complete route size before admission; dropping
an over-budget future retains the semantic fallback with explicit unknowns.
Existing small-budget, private/in-flight and 32-source-window bounds still apply.

Validation includes direct runtime kernel/fetch call oracles for local,
controlled, joint, virtual, fused and MCX packed/full routes; recorded runtime
condition equality; window/future contract agreement; inclusive single charging
and missing/forged-contract rejection; separate storage/MPI coverage; conditional
S1/Frame/log states; and the existing private delta and metadata-limit suite.

Validation run (macOS, 2026-10-16): the final executor/ETA/phase2/IO/profile/
replan selection, including the two-rank S1 policy worker, passed **174 tests**.
After simplifying condition generation to call only the shared classifier
instead of constructing a second full DAG, the executor/ETA subset passed
**71 tests**. MPI local sockets required the authorized sandbox escalation.
The parent integration records the aggregate fast-suite, Linux and TeX results.
