# Linux x86_64 development image; override either digest deliberately when updating.
ARG PYTHON_IMAGE=python:3.12-slim-bookworm@sha256:34386ef0cb081344d7ec1c103ba398e6e9f64e9ab3a1509accc92a4e24a07258
ARG UV_IMAGE=ghcr.io/astral-sh/uv:0.12.23-debian-slim@sha256:cb8750e12902fdaca60c1e48a796d19c02e6c2037ca801f322689fa97f0a44f2

FROM ${UV_IMAGE} AS uv-bin

FROM ${PYTHON_IMAGE} AS development

COPY --from=uv-bin /usr/local/bin/uv /usr/local/bin/uvx /usr/local/bin/

ENV DEBIAN_FRONTEND=noninteractive \
    PATH="/workspace/.venv/bin:/usr/local/bin:${PATH}" \
    UV_PROJECT_ENVIRONMENT=/workspace/.venv \
    UV_PYTHON_DOWNLOADS=0 \
    UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1 \
    MPI4PY_BUILD_MPICC=mpicc \
    MPI4PY_BUILD_MPIABI=0 \
    OMP_NUM_THREADS=1 \
    OPENBLAS_NUM_THREADS=1 \
    MKL_NUM_THREADS=1 \
    NUMEXPR_NUM_THREADS=1 \
    VECLIB_MAXIMUM_THREADS=1 \
    TMPDIR=/scratch

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       build-essential \
       ca-certificates \
       libopenmpi-dev \
       libsqlite3-dev \
       liburing-dev \
       openmpi-bin \
       pkg-config \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /workspace
RUN mkdir -p /scratch /output
COPY pyproject.toml uv.lock README.md ./
COPY src/ ./src/
COPY tests/ ./tests/
COPY examples/ ./examples/

# Build mpi4py from source using the same system OpenMPI that mpirun loads.
RUN --mount=type=cache,target=/root/.cache/uv \
    UV_OFFLINE=0 uv sync --frozen --extra mpi --group dev --no-binary-package mpi4py \
    && python -c 'from mpi4py import MPI; print(MPI.Get_library_version())' \
    && mpirun --version

ARG DEV_UID=1000
ARG DEV_GID=1000
RUN groupadd --gid ${DEV_GID} ddq \
    && useradd --uid ${DEV_UID} --gid ddq --create-home --shell /bin/bash ddq \
    && mkdir -p /scratch /output \
    && chown ddq:ddq /scratch /output

ENV UV_OFFLINE=1 \
    HOME=/home/ddq \
    PYTHONDONTWRITEBYTECODE=1

USER ddq:ddq
WORKDIR /workspace
CMD ["python", "--version"]
