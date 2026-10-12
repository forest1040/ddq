"""Phase 0 semantic, deferred, blocked state-vector simulator reference."""
from .ir import GateOp, GateNormalizer, GlobalSequencer, NativeGateSelector, FallbackDecomposer
from .layout import QubitLayout
from .parameters import ParameterSymbol, Expr, ParameterBinding, expression
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
