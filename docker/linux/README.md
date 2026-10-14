# Linux x86_64 開発コンテナ

Debian 系 Linux 向けの単一ホスト開発環境です。CPython 3.12 と uv は digest で固定し、OpenMPI、`liburing-dev`、SQLite 開発ヘッダー、C コンパイラーをイメージ内に用意します。`mpi4py` は lock 済み source distribution から `mpicc` でビルドし、同じイメージの OpenMPI にリンクします。ホスト側 MPI ライブラリはコンテナへ bind mount しないでください。

ビルドコンテキストは `.dockerignore` の allow-list で制限し、`src/`、`tests/`、`examples/`、`pyproject.toml`、`uv.lock` など必要なファイルだけを含めます。`.git`、`docs/`、内部計画資料、キャッシュ、仮想環境は含めません。Python と uv の base image は digest 固定です。Debian パッケージの版は apt リポジトリに追随し、apt snapshot は固定していません。

## ビルド

リポジトリのルートで実行します。ビルド時は apt と lock 済み Python パッケージを取得するためネットワークが必要です。UID/GID をビルド時に合わせ、実行時はイメージの non-root ユーザーを使います。

```sh
docker build --platform linux/amd64 \
  --build-arg DEV_UID="$(id -u)" \
  --build-arg DEV_GID="$(id -g)" \
  -f docker/linux/Dockerfile \
  -t ddq-linux:dev .
```

イメージ内の Python、MPI、mpi4py を確認します。

```sh
docker run --rm --network none ddq-linux:dev \
  sh -c 'python --version; uv --version; mpicc --showme:version; mpirun --version; python -c "from mpi4py import MPI; print(MPI.Get_library_version())"'
```

## 通常のテスト

scratch と出力先はホストのローカル ext4 など、コンテナ実行ホスト上のローカルファイルシステムに用意してください。NFS などの共有ファイルシステムは測定・I/O 検証の scratch に使いません。`/scratch` と `/output` だけを writable bind mount にし、コードは個別に read-only mount します。

```sh
ROOT_DIR="$PWD"
SCRATCH_DIR="$HOME/ddq-linux-scratch"
OUTPUT_DIR="$HOME/ddq-linux-output"
mkdir -p "$SCRATCH_DIR" "$OUTPUT_DIR"

docker run --rm --network none \
  --cpus=4 --memory=8g --pids-limit=512 --shm-size=256m \
  -v "$ROOT_DIR/src:/workspace/src:ro" \
  -v "$ROOT_DIR/tests:/workspace/tests:ro" \
  -v "$ROOT_DIR/examples:/workspace/examples:ro" \
  -v "$SCRATCH_DIR:/scratch" \
  -v "$OUTPUT_DIR:/output" \
  -e UV_CACHE_DIR=/scratch/uv-cache \
  -e DDQ_MPI_TEST_TIMEOUT=600 \
  -e TMPDIR=/scratch \
  -e OMP_NUM_THREADS=1 -e OPENBLAS_NUM_THREADS=1 \
  -e MKL_NUM_THREADS=1 -e NUMEXPR_NUM_THREADS=1 \
  ddq-linux:dev \
  uv run --offline --no-sync pytest -p no:cacheprovider
```

Image の既定ユーザーは build 時に指定した UID/GID の non-root user です。`HOME` を上書きせず、`UV_CACHE_DIR` を scratch volume 内に置きます。`uv --offline --no-sync` は実行中の依存取得・同期を行いません。`--network none` も併用し、runtime のネットワークアクセスを切ります。BLAS などの thread pool は各 1 thread に制限します。

イメージ全体を `/workspace` に mount すると `/workspace/.venv` が隠れるため、その方法は使いません。別 worktree のコードを試す場合も、上記のように worktree 内の `src/`、`tests/`、`examples/` を個別に read-only mount してください。scratch と出力先は専用ディレクトリを使います。

## io_uring の opt-in 検証

通常の pytest は native io_uring を初期化しません。Docker の標準 seccomp profile のまま通常テストを実行してください。native backend の確認時だけ `DDQ_NATIVE_URING=1` と `docker/linux/seccomp-uring.json` を指定します。この profile は固定した Moby default profile に `io_uring_setup`、`io_uring_enter`、`io_uring_register` の 3 syscall の許可だけを加えています。出典と変更ハッシュは隣接する `seccomp-provenance.json`、ライセンスは `LICENSE.moby-profiles` に記録しています。

```sh
ROOT_DIR="$PWD"
SCRATCH_DIR="$HOME/ddq-linux-scratch"
OUTPUT_DIR="$HOME/ddq-linux-output"
mkdir -p "$SCRATCH_DIR" "$OUTPUT_DIR"

docker run --rm --network none \
  --cpus=4 --memory=8g --pids-limit=512 --shm-size=256m \
  --security-opt "seccomp=$ROOT_DIR/docker/linux/seccomp-uring.json" \
  -v "$ROOT_DIR/src:/workspace/src:ro" \
  -v "$ROOT_DIR/tests:/workspace/tests:ro" \
  -v "$ROOT_DIR/examples:/workspace/examples:ro" \
  -v "$SCRATCH_DIR:/scratch" \
  -v "$OUTPUT_DIR:/output" \
  -e UV_CACHE_DIR=/scratch/uv-cache -e TMPDIR=/scratch \
  -e DDQ_NATIVE_URING=1 \
  -e OMP_NUM_THREADS=1 -e OPENBLAS_NUM_THREADS=1 \
  -e MKL_NUM_THREADS=1 -e NUMEXPR_NUM_THREADS=1 \
  ddq-linux:dev \
  uv run --offline --no-sync pytest -p no:cacheprovider \
  tests/test_native_uring_linux.py tests/test_native_uring_mpi_linux.py
```

単独の capability probe は次のように実行できます。

```sh
mkdir -p "$HOME/ddq-linux-scratch"

docker run --rm --network none \
  --cpus=4 --memory=8g --pids-limit=512 --shm-size=256m \
  --security-opt "seccomp=$PWD/docker/linux/seccomp-uring.json" \
  -e DDQ_NATIVE_URING=1 \
  -e UV_CACHE_DIR=/scratch/uv-cache -e TMPDIR=/scratch \
  -e OMP_NUM_THREADS=1 -e OPENBLAS_NUM_THREADS=1 \
  -e MKL_NUM_THREADS=1 -e NUMEXPR_NUM_THREADS=1 \
  -v "$HOME/ddq-linux-scratch:/scratch" \
  ddq-linux:dev \
  uv run --offline --no-sync python examples/linux_native_io_probe.py --probe
```

custom profile は opt-in の検証だけに使います。`seccomp=unconfined`、`--privileged`、ホスト sysctl の変更は不要で、使用しないでください。`liburing-dev` はコンテナ内のビルドヘッダーを提供しますが、ホスト kernel や上位 container policy の制限は変更しません。ホストが `io_uring` を無効化している場合、限定 seccomp profile でも有効にはできません。

## MPI の単一ホスト確認

MPI はこのイメージ内の OpenMPI で構成しています。1、2、4 rank を同じホスト上で確認できます。Docker network namespace 内で rank を起動するため、これは単一ホスト確認です。複数ホストや cluster の通信・性能・運用検証を代替しません。

```sh
for ranks in 1 2 4; do
  docker run --rm --network none \
    --cpus=4 --memory=8g --pids-limit=512 --shm-size=256m \
    -e OMP_NUM_THREADS=1 -e OPENBLAS_NUM_THREADS=1 \
    -e MKL_NUM_THREADS=1 -e NUMEXPR_NUM_THREADS=1 \
    ddq-linux:dev mpirun -n "$ranks" python -c \
    'from mpi4py import MPI; c=MPI.COMM_WORLD; print(c.rank, c.size, MPI.Get_library_version().splitlines()[0])'
done
```

イメージの既定ユーザーは non-root のため、MPI の root 実行許可は必要ありません。ホストへの apt install や sudo 操作も必要ありません。

## 証跡を保存する一括検証

Linux ホストで上記のイメージをビルドした後、次を実行できます。保存先はローカルファイルシステムを選んでください。

```sh
DDQ_LINUX_IMAGE=ddq-linux:dev bash docker/linux/run_validation.sh /tmp
```

専用の一時ディレクトリに toolchain、source hash、capability probe、pytest のログと JUnit XML を保存します。通常テストは既定 seccomp、native の12ケースは限定 profile を使います。native MPI は buffered／O_DIRECT と1／2／4 rankを組み合わせ、`MPI_THREAD_SINGLE`、状態・期待値・勾配、fork、checkpoint／resumeを確認します。probe と test には時間上限があり、終了時はこの実行専用のコンテナだけを片付けます。scratch と証跡は保存され、手動削除できます。

一括scriptの上限はprobe 60秒、native suite 600秒、通常回帰7200秒です。初回の分割回帰では成功ケースの合計が約3873秒だったため、一括実行は余裕のある有限上限にしています。Phase 3／4のMPI testには`DDQ_MPI_TEST_TIMEOUT=600`を渡します。Macの短い既定120秒と区別し、回路・精度・RAM／SSD予算は変更しません。native MPI各ケースの上限は180秒です。

既定は全suiteです。既存のnative成功証跡を維持してfixture等の修正後だけ通常回帰を再実行する場合は、`DDQ_LINUX_VALIDATION_STAGE=regression`を併用できます。`native`指定ではnative suiteだけを実行します。どちらもtoolchainと2種類のprobe、対象source hashを保存します。

既定 seccomp での probe が exit 2 を返す場合は capability 拒否として別記録し、限定 profile の成功を代用しません。明示 native テストでは初期化不能を失敗とし、worker への fallback や skip にしません。通常テストでは native ケースを skip します。証跡はホスト情報と絶対パスを含むため、外部共有前に匿名化してください。

Linux x86_64 と io_uring 対応 kernel を対象とします。ホスト側の制限は `uname -m`、`uname -r`、`/proc/sys/kernel/io_uring_disabled` で確認できます。イメージやテスト結果だけでは、実ホストの kernel、Docker seccomp 設定、filesystem、MPI の実測結果を保証しません。

構築方法は[uv公式Docker手順](https://docs.astral.sh/uv/guides/integration/docker/)、[mpi4py公式build手順](https://mpi4py.readthedocs.io/en/stable/install.html)に沿う。seccompによる制限は[Docker公式資料](https://docs.docker.com/engine/security/seccomp/)を参照。今回の実行範囲と結果は[Linux開発資料](../../docs/linux_development.md)に記載する。
