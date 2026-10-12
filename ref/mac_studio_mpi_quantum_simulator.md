# Mac Studio単体でMPI対応の量子回路シミュレータを開発する

**対象環境:** Mac Studio M3 Ultra / 512 GB Unified Memory / macOS\
**推奨構成:** Open MPI + mpi4py + NumPy + uv\
**目的:** Mac Studio 1台で複数MPI rankを動かし、将来のマルチノード環境へ移植しやすい、メモリ＋SSD型の大規模状態ベクトルシミュレータを開発する。

## 1. 結論

Mac Studio単体での開発には、**Open MPI + mpi4py + NumPy** を推奨する。

- 1台のMacで複数のMPIプロセス（rank）を起動し、分散環境と同じコード構造を検証できる。
- まずは4〜8 rankで実装・デバッグし、rank間の明示的なデータ交換を組み込む。
- **MPI rankと状態ベクトルのblockを分離**し、各rankが複数blockを管理する構造とする。
- ブロックをRAMとSSDへ階層的に配置し、DAG（演算ログ）の遅延評価により通信・I/Oを最適化する。
- 単一Mac上のrank間通信は高速なので、将来のマルチノード通信を評価する際は帯域・レイテンシを別途モデル化する。

## 2. 推奨アーキテクチャ

```text
               Mac Studio M3 Ultra (512GB Unified Memory)
+----------------------------------------------------------------+
|                         Global Scheduler                       |
|   DAG / lazy evaluation / block placement / communication plan |
+-------------------------------+--------------------------------+
                                |
                       Open MPI / mpi4py
                                |
                 +--------------+--------------+
                 |              |              |
              Rank 0         Rank 1         Rank 2 ...
            +---------+     +---------+     +---------+
            | Compute |     | Compute |     | Compute |
            | Cache   |     | Cache   |     | Cache   |
            +---------+     +---------+     +---------+
                 |              |              |
                 +--------------+--------------+
                                |
                  macOS Unified Memory (RAM)
                                |
                        Block Storage I/O
                                |
                SSD / NVMe (rank-owned block files)
```

**設計上の重要点:** Mac単体でもrank間通信を明示的に実行し、ローカルメモリ内演算とrank間演算を区別する。Open MPIは同一ホスト上の通信で共有メモリベースの転送を利用できるが、ネットワーク越しの通信特性まで再現するわけではない。

## 3. インストール手順（Homebrew + uv）

```bash
# Open MPI
brew install open-mpi

# 実行環境の確認
which mpiexec
which mpicc
mpiexec --version
mpicc --version

# Pythonプロジェクト作成
uv init mpi-quantum-sim
cd mpi-quantum-sim
uv add mpi4py numpy

# mpi4pyから読み込まれているMPI実装を確認
uv run python -c 'from mpi4py import MPI; print(MPI.Get_library_version())'
```

最初はMPIライブラリを**1系統（HomebrewのOpen MPI）に統一**する。別のMPI実装を混在させると、ビルドや実行時に不整合が発生することがある。

4 rankの起動テスト:

```bash
uv run mpiexec -n 4 python -c \
  'from mpi4py import MPI; c=MPI.COMM_WORLD; print("rank", c.rank, "of", c.size)'
```

`rank 0`〜`rank 3`が順不同で表示されればよい。環境によってはMPI起動下の`python`が意図する仮想環境を参照しているか追加確認する。

## 4. 最小のMPI分散状態ベクトル実装

8量子ビットの状態ベクトルを4 rankに分割する。各rankは状態ベクトルの連続領域を持ち、rankを跨ぐHadamardゲートを `Sendrecv` により実行する。

ファイル名: `mpi_hadamard.py`

```python
from mpi4py import MPI
import numpy as np

comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

n_qubits = 8

# rank数は2以上かつ2の累乗
assert size >= 2 and (size & (size - 1)) == 0
assert size <= (1 << (n_qubits - 1))

rank_bits = size.bit_length() - 1
local_bits = n_qubits - rank_bits
local_size = 1 << local_bits

# rankごとに連続する振幅領域を保持
state = np.zeros(local_size, dtype=np.complex128)

# 初期状態 |00000000>
if rank == 0:
    state[0] = 1.0

# rankを跨ぐ量子ビットにHadamardゲートを適用
# rankの最下位ビットが対応する量子ビットを選ぶ
target = local_bits
partner = rank ^ (1 << (target - local_bits))

remote = np.empty_like(state)

comm.Sendrecv(
    sendbuf=state,
    dest=partner,
    recvbuf=remote,
    source=partner,
)

# Hadamard: [a, b] -> [(a+b)/sqrt(2), (a-b)/sqrt(2)]
# rankの対応ビットが0なら上半分、1なら下半分を計算
if ((rank >> (target - local_bits)) & 1) == 0:
    state[:] = (state + remote) / np.sqrt(2)
else:
    state[:] = (remote - state) / np.sqrt(2)

local_norm = float(np.vdot(state, state).real)
global_norm = comm.allreduce(local_norm, op=MPI.SUM)

if rank == 0:
    print(f"Norm = {global_norm:.12f}")
    print(f"Hadamard target = q{target}")
```

実行:

```bash
uv run mpiexec -n 4 python mpi_hadamard.py
```

期待される出力:

```text
Norm = 1.000000000000
Hadamard target = q6
```

**補足:** このサンプルはrank間でローカル配列全体を交換するため、サイズが大きくなると通信バッファが重い。実用実装では固定サイズの**タイル単位の交換**、複数通信のパイプライン化、再配置の削減を導入する。また、サンプルのHadamard対象はrank配置によって決まる。一般のゲート演算はターゲット量子ビットとrank/block配置に応じて演算方式を選択する。

## 5. rankとblockは分離する

**1 rank = 1 block** にはしない。1 rankが複数のblockを保持し、blockはグローバルなIDで識別する。

| 構成要素 | 役割 |
|---|---|
| MPI rank | blockを保持・計算する実行ワーカー |
| Global Block ID | 状態ベクトル全体でのblock識別子 |
| Block Cache | RAM上でのblockキャッシュと置換 |
| Block Storage | SSD上のblockファイルと非同期I/O |
| Scheduler | DAG、演算順序、通信、I/Oの最適化 |
| Communication Backend | rank間転送・将来の共有メモリ最適化 |

### 演算の分類

| 場面 | 処理方式 |
|---|---|
| 同一block内 | ローカルメモリ演算 |
| 同一rankの異なるblock | MPI不要のローカルブロック操作 |
| 異なるrankのRAM上 | MPIによる振幅/タイル交換 |
| SSD上のblockが必要 | SSDから読み込み、演算後に必要に応じ書き戻し |
| 複数rankに跨るゲート | 通信、再配置、または演算計画の変更 |

ゲート列からDAGを生成し、blockごとの演算を可能な限りまとめれば、SSDアクセスとrank間通信を削減できる。

## 6. macOSでの通信とSSD I/O

### 6.1 MPI通信

- まずは `Sendrecv`、`Isend`、`Irecv` などの標準MPI APIで実装する。
- mpi4pyの大文字で始まるバッファ通信APIはNumPy配列を直接送受信でき、通常のPythonオブジェクトのpickleシリアライズを回避できる。
- 必要に応じて `MPI.Win.Allocate_shared()` による明示的な共有メモリも検討できるが、同期と競合制御が必要になる。
- 実ノード間ネットワークの帯域・遅延はMac単体から直接は推定できないので、別途ベンチマークやシミュレーションを行う。

### 6.2 SSD I/O

初期実装では、rankごとに専用ディレクトリを持ち、POSIX I/Oでblockを読み書きする方式が簡単である。

```text
storage/
  rank_000/
    block_000000.bin
    block_000004.bin
  rank_001/
    block_000001.bin
    block_000005.bin
  rank_002/
    block_000002.bin
    block_000006.bin
  rank_003/
    block_000003.bin
    block_000007.bin
```

Linuxでは`O_DIRECT`、macOSでは`fcntl(fd, F_NOCACHE, 1)`でOSのページキャッシュを回避または抑制する構成を検討できる。ただし、**`F_NOCACHE`は`O_DIRECT`と完全に等価ではない**。性能はblockサイズ・ファイルシステム・実際のI/Oパターンで比較する。

MPI-IO（`MPI.File`）は別の選択肢だが、macOSで`F_NOCACHE`相当の制御が自動的に保証されるものではない。最初は独自のblock storage抽象化で制御する方が扱いやすい。

## 7. 512 GB RAMでの状態ベクトル容量

完全な状態ベクトルに必要な容量（`2^n`個の複素振幅）:

| 量子ビット数 | complex64（8 bytes/振幅） | complex128（16 bytes/振幅） |
|---:|---:|---:|
| 32 | 32 GiB | 64 GiB |
| 34 | 128 GiB | 256 GiB |
| 35 | 256 GiB | 512 GiB |
| 36 | 512 GiB | 1 TiB |
| 40 | 8 TiB | 16 TiB |

注意点:

- これらは**状態ベクトル本体のみ**の理論容量であり、MPI通信バッファ、OS、スケジューラ、I/Oキャッシュなどは含まない。
- 512 GBという製品容量と512 GiBは厳密には異なる。35 qubit・complex128は状態ベクトル本体だけで512 GiB必要なため、512 GBモデルのRAMには収まらない。
- 35 qubit・complex64は256 GiBなので、追加のメモリ消費を考慮したうえでRAM内実行の候補になる。
- 40 qubitではSSD等の外部記憶階層を前提とした構成が必要になる。

## 8. MPI rank数の目安

| rank数 | 主な用途 |
|---:|---|
| 1 | 単一プロセス基準性能 |
| 2 | 最小のrank間通信試験 |
| 4 | 初期開発・デバッグ |
| 8 | 分散block演算の評価 |
| 16 | 通信・I/O並列度と競合の評価 |
| それ以上 | 実測で改善を確認できる場合に検討 |

rank数を増やしても、Mac Studioのメモリ帯域・SSD帯域は共有される。**rank数の増加が速度向上を保証するわけではない**。rank数、blockサイズ、タイルサイズ、同時I/O数を組み合わせて比較する。

## 9. 推奨開発ロードマップ

### Phase 1: MPI Reference Simulator

- Open MPI + mpi4py + NumPyで動作確認。
- 1、2、4、8 rankで状態ベクトルを分散。
- 1量子ビットゲート、2量子ビットゲート、rank間交換を実装。
- 小サイズの厳密シミュレータと振幅・ノルムを照合。

### Phase 2: Block Storage

- Global Block IDとrankへの配置を分離。
- 1 rankで複数blockを管理。
- RAMのLRU等のblock cacheとSSDへの退避・復帰を実装。
- macOS `F_NOCACHE`あり/なしでベンチマーク。

### Phase 3: DAG / 遅延評価スケジューラ

- ゲート操作を直ちに実行せずDAGへ記録。
- block局所性、通信、SSDアクセス、計算コストを考慮してスケジューリング。
- 同一blockに対する複数演算の融合、先読み、パイプライン化を検討。

### Phase 4: 将来のマルチノード実行計画の評価

通信バックエンドを抽象化する。

```text
CommunicationBackend
  ├── LocalBackend         # 単一プロセス
  ├── MPIBackend           # MPI通信
  ├── SharedMemoryBackend  # 同一ノード高速化（必要になれば）
  └── SimulatedBackend     # 仮想ネットワーク
```

`SimulatedBackend` は、帯域・レイテンシ・転送単位を仮定した仮想通信時間の見積もりに使う。**Mac Studio上の実測値をマルチノード環境の性能として直接流用しない**。

## 10. 最終的な推奨

**まずOpen MPI + mpi4pyで4〜8 rankのReference MPI Simulatorを動作させ、rank/block分離、RAMキャッシュ、SSD I/O、DAGスケジューラを順に実装する。**

最初からMPI通信そのものを自作したり、macOS専用の共有メモリ最適化へ踏み込んだりする必要はない。優先すべき課題は、**ブロックの局所性を最大化し、通信とSSD I/Oの総量を減らす実行計画**である。
