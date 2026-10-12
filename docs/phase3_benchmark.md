# Phase 3 実測比較

2026-10-13。同一 Mac 上の Open MPI 5.0.11、mpi4py 4.1.2、Python 3.12.13、NumPy 2.5.4、uv 環境で測定した。[生の JSON](phase3_benchmark.json)に rank 別の予算、send／recv、metadata、storage counters と環境を保存した。各条件は一回の実行であり、CPU／SSD の背景負荷を隔離していない。統計的な速度差や multi-node scaling の評価には使わない。

## MPI＋SSD と RAM 上限

回路は 16Q、b=8、`RY(0, .37)`、q=1..15 の H、lower controls `(0=1,1=0)`／upper target 15 の MCX。全状態は 1 MiB、rank ごとの managed RAM は 256 KiB、metadata reserve は 64 KiB、tile は 16 KiB。file／buffered backend を使用した。

`execute` は materialize の elapsed MAX。end-to-end は simulator 作成、回路登録、materialize、Z0 の期待値、checkpoint、close、resume、再度の期待値を含む。勾配と全状態 Gather はこの容量実験では実行していない。

| rank | MCX | execute 秒 | 全体 秒 | send payload 合計 | SSD read 合計 | SSD write 合計 | rank managed peak |
|---:|---|---:|---:|---:|---:|---:|---:|
| 1 | packed | 7.055 | 7.375 | 0 | 9 MiB | 9 MiB | 256 KiB |
| 2 | packed | 7.230 | 7.582 | 1.25 MiB | 9 MiB | 9 MiB | 256 KiB |
| 4 | packed | 11.193 | 11.764 | 2.25 MiB | 9 MiB | 9 MiB | 256 KiB |
| 4 | full | 11.294 | 11.863 | 3 MiB | 9 MiB | 9 MiB | 256 KiB |

すべて `Z0 = 0.9323273456060321` で、analytic `cos(.37)` に一致し、checkpoint 再開後も一致した。全体の振幅を root RAM に集めずに実行した。managed peak は process RSS の測定値ではなく、aligned buffer／cache／metadata の予算 accounting。

4 rank では packed により全回路の send payload が 3 MiB → 2.25 MiB に減る。MCX 単独の remote payload は 1 MiB → 256 KiB で、lower control 2 個の active 率 1/4 に対応する。upper H 等の共通通信を含めた全回路の削減率は 25%。SSD は full Block 読み書きのため、この例では減らない。send と recv を合計して通信量を二重計数しない。

rank を増やしても今回の直列 coordinator／metadata collective runtime は高速化しなかった。Block 単位の SQLite／rule lookup、同期 vote、Frame 解消が支配的で、packed の byte 削減だけで end-to-end の大きな改善を主張できない。

再現コマンド：

```sh
uv sync --locked --extra mpi
mpiexec -n 4 .venv/bin/python -m mpi4py examples/phase3.py \
  --directory /tmp/phase3-packed --num-qubits 16 --block-bits 8 \
  --budget-kib 256 --exchange packed --output /tmp/phase3-packed.json
mpiexec -n 4 .venv/bin/python -m mpi4py examples/phase3.py \
  --directory /tmp/phase3-full --num-qubits 16 --block-bits 8 \
  --budget-kib 256 --exchange full --output /tmp/phase3-full.json
```

各保存先は新しく用意する。1/2 rank は `-n` と保存先を変更する。

## macOS F_NOCACHE と随伴

8Q／b=2、file／direct、1 rank の同じ種類の回路で、Z0 が `0.9323273456060333`、symbolic theta の随伴微分が `-0.36161543196496143` となった。`cos(.37)`／`-sin(.37)` と一致する。全状態 Gather は行っていない。実行約 0.941 秒、随伴約 2.109 秒、checkpoint／restart を含む全体約 3.170 秒。測定時の metadata reserve は 32 KiB、managed peak は 512 KiB。デモの現在の default reserve は 64 KiB。

```sh
mpiexec -n 1 .venv/bin/python -m mpi4py examples/phase3.py \
  --directory /tmp/phase3-direct --gradient --backend direct
```

Linux O_DIRECT は共通 backend の既存試験を継続するが、今回の host は macOS のため Linux 専用の 2 tests は skip。

## 通信量・S1・slow rank の検証

real MPI integration の専用ケースは n=8／b=6 とし、0〜6 個の lower control と mixed open/closed values を試験する。identity input の一つの remote pair について、k>=1 の send payload 合計が `2 * 16 * 2**(6-k)` に一致することを 2/4 rank で確認する。k=0 の upper-only MCX は virtual permutation のため 0 bytes。P=1 の local pair も 0 bytes。各回路は random complex input の eager reference と global phase を合わせず照合する。

自動 S1 は明示した synthetic profile を用いて profitable branch の state と finite-difference gradient を確認した。これを実測による S1 速度改善とは扱わない。実 calibration は directed peer のサイズ別・tile／backend／host／MPI 条件付き測定と、その public API の試験を行った。

slow rank は 2 rank の rank 1 に 5 ms の stage-entry delay を注入し、同条件の 24 回の transfer observation で DEGRADED を検出する。root、計画、交換 policy を変更しない observe-only の動作を確認した。

最終検証コマンド `uv run --locked --extra mpi pytest -q` は **300 passed、2 skipped**。file／RAM、1/2/4 rank、期待値／随伴、prefix／COW、checkpoint failure／restart、v1/v2 migration、GC、40/50Q metadata-only と baseline regression を含む。
