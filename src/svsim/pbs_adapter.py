"""PBS resource validation and script rendering; intentionally no submission API."""
from __future__ import annotations

from dataclasses import asdict, dataclass
import json
import math
from pathlib import Path
import shlex
from typing import Sequence

from .development_runner import JobSpec


@dataclass(frozen=True)
class PBSNodeAllocation:
    """One node in an already granted allocation, as observed by the caller."""
    name: str
    assigned_cpus: int
    assigned_memory_bytes: int
    scratch_root: str
    scratch_available_bytes: int
    online: bool = True

    def __post_init__(self) -> None:
        if not self.name or min(self.assigned_cpus, self.assigned_memory_bytes, self.scratch_available_bytes) < 0:
            raise ValueError("invalid node allocation")


@dataclass(frozen=True)
class PBSAllocation:
    nodes: tuple[PBSNodeAllocation, ...]
    queue: str | None = None


@dataclass(frozen=True)
class RenderedPBSJob:
    script: str
    metadata: dict[str, object]


class PBSAdapter:
    """Validate a supplied allocation, then render a PBS script and metadata."""
    def render(self, spec: JobSpec, allocation: PBSAllocation) -> RenderedPBSJob:
        if not allocation.nodes:
            raise ValueError("allocation must contain at least one node")
        if any(not node.online for node in allocation.nodes):
            raise ValueError("allocation contains an offline node")
        if len({n.name for n in allocation.nodes}) != len(allocation.nodes):
            raise ValueError("allocation node names must be unique")
        if allocation.queue and not all(c.isalnum() or c in "_.-" for c in allocation.queue):
            raise ValueError("invalid PBS queue name")
        if any(not Path(n.scratch_root).is_absolute() for n in allocation.nodes):
            raise ValueError("node scratch roots must be absolute paths")
        forbidden = {"-host", "--host", "-hosts", "--hosts", "-H", "-machinefile",
                     "--machinefile", "-hostfile", "--hostfile", "--host-list"}
        if any(arg in forbidden or any(arg.startswith(option + "=") for option in forbidden)
               for arg in spec.launcher_argv):
            raise ValueError("PBS launcher must use the scheduler-provided allocation, not explicit hosts")

        # Deterministic rank packing makes the generated resource request auditable.
        remaining = spec.ranks
        chunks: list[tuple[PBSNodeAllocation, int]] = []
        for node in allocation.nodes:
            if remaining == 0:
                break
            cpus_per_rank = spec.threads
            capacity = node.assigned_cpus // cpus_per_rank
            host_ram = spec.host_ram_bytes or spec.rank_ram_bytes * min(spec.ranks, capacity)
            host_disk = spec.host_disk_bytes or spec.rank_disk_bytes * min(spec.ranks, capacity)
            if host_ram > node.assigned_memory_bytes:
                capacity = 0
            if host_disk > node.scratch_available_bytes:
                capacity = 0
            take = min(remaining, capacity)
            if take:
                chunks.append((node, take))
                remaining -= take
        if remaining:
            raise ValueError(f"allocation cannot fit {spec.ranks} ranks with requested memory/disk/threads")

        select = "+".join(
            f"1:ncpus={count * spec.threads}:mpiprocs={count}:mem={self._ceil_gib(spec.host_ram_bytes or spec.rank_ram_bytes * count)}gb"
            for _, count in chunks
        )
        wall = int(math.ceil(spec.walltime_seconds))
        h, rem = divmod(wall, 3600); m, s = divmod(rem, 60)
        lines = ["#!/bin/sh", f"#PBS -l select={select}", f"#PBS -l walltime={h:02d}:{m:02d}:{s:02d}"]
        if allocation.queue:
            lines.append(f"#PBS -q {allocation.queue}")
        command = " ".join(shlex.quote(x) for x in spec.command)
        if spec.cwd:
            cwd_line = f"cd {shlex.quote(spec.cwd)}"
        else:
            cwd_line = 'cd "$SVSIM_JOB_SCRATCH"'
        # A rendered select request does not bind a named node or its local
        # filesystem. Let PBS provide the runtime scratch location.
        lines.extend(["set -eu", 'export SVSIM_JOB_SCRATCH="${TMPDIR:?PBS must provide TMPDIR}"', cwd_line, command])
        metadata = {
            "revision": spec.revision, "lock_hash": spec.lock_hash, "test_case": spec.test_case,
            "ranks": spec.ranks, "threads": spec.threads, "walltime_seconds": spec.walltime_seconds,
            "command": list(spec.command), "queue": allocation.queue,
            "allocation_nodes": [asdict(node) for node in allocation.nodes],
            # PBS select requests resources but does not bind hostnames. This
            # packing is only an estimate used to validate supplied capacity.
            "placement_estimate": [{"node": node.name, "ranks": count} for node, count in chunks],
            "host_placement_guaranteed": False,
            "submission": "render-only; operator review and submission are external",
        }
        return RenderedPBSJob("\n".join(lines) + "\n", metadata)

    @staticmethod
    def _ceil_gib(byte_count: int) -> int:
        return max(1, math.ceil(byte_count / (1 << 30)))

    def write(self, rendered: RenderedPBSJob, script_path: str | Path,
              metadata_path: str | Path) -> None:
        """Write rendered artifacts only; this method never invokes PBS tools."""
        Path(script_path).write_text(rendered.script)
        Path(metadata_path).write_text(json.dumps(rendered.metadata, indent=2) + "\n")
