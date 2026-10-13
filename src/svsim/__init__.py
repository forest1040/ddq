"""Phase 0 semantic, deferred, blocked state-vector simulator reference."""
from .ir import GateOp, GateNormalizer, GlobalSequencer, NativeGateSelector, FallbackDecomposer
from .layout import QubitLayout
from .parameters import ParameterSymbol, Expr, ParameterBinding, ExactPi, expression, exact_pi_coefficient
from .unitary import UnitaryValidator, MatrixAssetRegistry
from .runtime import DeferredSimulator, PerBlockLog, JointTaskDAG, BlockView, BlockVersion, FrameGroupRegistry
from .observables import PauliTerm, ObservableIR, StateHandle, BlockPauliInner, GradientPlanTemplate, adjoint_gradient
from .transport import StorageFile, StorageCapabilities, open_storage
from .storage import StorageConfig, BlockRef, FileBlockStore, StoredBlockView
from .cache import MemoryBudget, BufferLease, BlockCache
from .checkpoint import SnapshotManifest, StoredStateHandle
from .stored_runtime import StoredSimulator
from .observable_plan import ObservablePlan
from .lowering import StructuredExactDecomposer
from .telemetry import StreamingExecutionTrace, ObservePerformanceGuard
from .clifford import CliffordTableau
from .frame import FrameAsset
from .phase import PhaseValue
from .pauli_rotation import SignedPauli, apply_pauli_rotation, apply_pauli_pair
from .permutation import PermutationEdge
from .frame_planner import FramePlanDecision
from .frame_benchmark import FrameCalibration, calibrate_frames
from .distributed import DistributedConfig, DistributedSimulator
from .distributed_plan import MPIProfile
from .async_scheduler import AsyncPolicy, AsyncDistributedSimulator
from .parameter_batch import ParameterBatchExecutor
from .runtime_control import ReplanEvidence
from .development_runner import JobSpec, LocalDevelopmentRunner
