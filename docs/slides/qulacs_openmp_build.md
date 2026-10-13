# Qulacs 0.6.14 OpenMP build on macOS arm64

This records the isolated local build used for the Qulacs RAM and kernel measurements. It uses the upstream [Qulacs v0.6.14 source](https://github.com/qulacs/qulacs/tree/v0.6.14), pinned to commit `4f6de32cca807ec4cabf1107e3806786cb953c56` (source archive SHA256 `8d1b9d77375f8c4e2b6bde1de7f195c875b75e6bdd9a44012dad94e176211b40`). The build used the existing AppleClang 21, Homebrew `libomp` and Boost 1.92.0; it did not install Homebrew packages or change repository dependencies.

The commands below create all build files and the Python environment under `/private/tmp/ddq-qulacs-omp`. They use the repository Python as the interpreter source, but do not modify the repository `.venv`.

```sh
mkdir -p /private/tmp/ddq-qulacs-omp
git clone --depth 1 --branch v0.6.14 \
  https://github.com/qulacs/qulacs.git \
  /private/tmp/ddq-qulacs-omp/source

uv venv --python /Users/tmori/work3/ddq/.venv/bin/python \
  /private/tmp/ddq-qulacs-omp/venv
UV_CACHE_DIR=/private/tmp/ddq-qulacs-omp-cache \
  uv pip install --python /private/tmp/ddq-qulacs-omp/venv/bin/python \
  setuptools wheel setuptools-scm numpy scipy pybind11 cmake==3.31.6
```

Qulacs v0.6.14 calls `add_subdirectory` on the fetched pybind11 source without a binary directory. CMake 3.31 rejects that call for this out-of-tree FetchContent source. The successful build used this one-line compatibility patch in the temporary source checkout:

```sh
VIRTUAL_ENV=/private/tmp/ddq-qulacs-omp/venv \
UV_CACHE_DIR=/private/tmp/ddq-qulacs-omp-cache \
uv run --active --no-project python - <<'PY'
from pathlib import Path
p = Path('/private/tmp/ddq-qulacs-omp/source/CMakeLists.txt')
s = p.read_text()
old = 'add_subdirectory(${pybind11_fetch_SOURCE_DIR})'
new = 'add_subdirectory(${pybind11_fetch_SOURCE_DIR} ${pybind11_fetch_BINARY_DIR})'
assert old in s
p.write_text(s.replace(old, new))
PY
```

The compiler and OpenMP flags below explicitly select the existing `libomp`; upstream defaults OpenMP off for AppleClang. The source CMake file appends the OpenMP flags to both C and C++ flags. The target is built with at most two jobs.

```sh
/private/tmp/ddq-qulacs-omp/venv/bin/cmake \
  -S /private/tmp/ddq-qulacs-omp/source \
  -B /private/tmp/ddq-qulacs-omp/build \
  -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_C_COMPILER=/usr/bin/clang \
  -DCMAKE_CXX_COMPILER=/usr/bin/clang++ \
  -DPYTHON_EXECUTABLE=/private/tmp/ddq-qulacs-omp/venv/bin/python \
  -DPython_EXECUTABLE=/private/tmp/ddq-qulacs-omp/venv/bin/python \
  -DUSE_PYTHON=Yes -DUSE_OMP=Yes -DUSE_MPI=No -DUSE_TEST=No \
  -DOpenMP_C_FLAGS='-Xpreprocessor -fopenmp -I/opt/homebrew/opt/libomp/include -L/opt/homebrew/opt/libomp/lib -lomp' \
  -DOpenMP_CXX_FLAGS='-Xpreprocessor -fopenmp -I/opt/homebrew/opt/libomp/include -L/opt/homebrew/opt/libomp/lib -lomp' \
  -DOpenMP_C_LIB_NAMES=omp -DOpenMP_CXX_LIB_NAMES=omp \
  -DOpenMP_omp_LIBRARY=/opt/homebrew/opt/libomp/lib/libomp.dylib

/private/tmp/ddq-qulacs-omp/venv/bin/cmake \
  --build /private/tmp/ddq-qulacs-omp/build \
  --target python --parallel 2
```

The compiled extension and Python package were placed in the temporary environment's site-packages directory so the build did not invoke `setup.py`'s machine-wide default build parallelism:

```sh
VIRTUAL_ENV=/private/tmp/ddq-qulacs-omp/venv \
UV_CACHE_DIR=/private/tmp/ddq-qulacs-omp-cache \
uv run --active --no-project python - <<'PY'
from pathlib import Path
import shutil

site = Path('/private/tmp/ddq-qulacs-omp/venv/lib/python3.12/site-packages')
source = Path('/private/tmp/ddq-qulacs-omp/source/pysrc/qulacs')
shutil.copytree(source, site / 'qulacs', dirs_exist_ok=True,
                ignore=shutil.ignore_patterns('__pycache__', '*.pyc'))
shutil.copy2(
    '/private/tmp/ddq-qulacs-omp/build/python/qulacs_core.cpython-312-darwin.so',
    site / 'qulacs_core.cpython-312-darwin.so')
(site / 'qulacs' / '_version.py').write_text(
    '__version__ = "0.6.14"\n__version_tuple__ = (0, 6, 14)\n')
PY

VIRTUAL_ENV=/private/tmp/ddq-qulacs-omp/venv \
UV_CACHE_DIR=/private/tmp/ddq-qulacs-omp-cache \
uv run --active --no-project python -c \
  'import qulacs, qulacs_core; print(qulacs.__version__, qulacs.__file__, qulacs_core.__file__)'
otool -L /private/tmp/ddq-qulacs-omp/venv/lib/python3.12/site-packages/qulacs_core.cpython-312-darwin.so
shasum -a 256 /private/tmp/ddq-qulacs-omp/venv/lib/python3.12/site-packages/qulacs_core.cpython-312-darwin.so
```

The resulting extension SHA256 was `4153e0b13c25214fe2904cf715077706f8ba784da8be932d09d6c22232e3b2d3`. `otool -L` showed a direct dependency on `/opt/homebrew/opt/libomp/lib/libomp.dylib`.

## Small OpenMP capability check

With a 20-qubit state, H and PauliRotation were each warmed up once and then timed four times in a fresh process with `OMP_DYNAMIC=FALSE`. The table gives median process CPU time divided by median wall time; this is a small runtime capability check, not a QPE performance estimate.

| OMP threads | H CPU/wall | PauliRotation CPU/wall |
|---:|---:|---:|
| 1 | 1.01 | 1.00 |
| 8 | 5.31 | 7.50 |

The corresponding median wall times were 0.450 ms / 2.376 ms at one thread and 0.145 ms / 0.325 ms at eight threads for H / PauliRotation. The earlier prebuilt wheel did not list `libomp.dylib` in `otool -L`; its eight-thread run had CPU/wall near 1.0. That wheel comparison and this check establish the presence and use of the OpenMP runtime for these calls only.

The capability check can be repeated with:

```sh
for nthreads in 1 8; do
  OMP_NUM_THREADS=$nthreads OMP_DYNAMIC=FALSE \
    QULACS_NUM_THREADS=$nthreads \
    VIRTUAL_ENV=/private/tmp/ddq-qulacs-omp/venv \
    UV_CACHE_DIR=/private/tmp/ddq-qulacs-omp-cache \
    uv run --active --no-project python - <<'PY'
import os, statistics, time
from qulacs import QuantumState
from qulacs.gate import H, PauliRotation

state = QuantumState(20)
state.set_computational_basis(0)
gates = (H(0), PauliRotation([0, 1, 2, 3], [1, 2, 3, 1], 0.123))
for gate in gates:
    gate.update_quantum_state(state)
    samples = []
    for _ in range(4):
        cpu, wall = time.process_time(), time.perf_counter()
        gate.update_quantum_state(state)
        samples.append((time.perf_counter() - wall, time.process_time() - cpu))
    wall = statistics.median(x[0] for x in samples)
    cpu = statistics.median(x[1] for x in samples)
    print(os.environ['OMP_NUM_THREADS'], gate.get_name(),
          f'wall={wall:.6f}', f'cpu={cpu:.6f}', f'cpu_wall={cpu/wall:.2f}')
PY
done
```
