# Phase 1 ベンチマーク

2026-10-12 に同じ初期状態・回路を用いて測定した小規模結果。速度差はこの条件での単回実測で、繰り返し分布や大規模実機性能は未評価。

## 条件

- 環境：Darwin 25.6.0 / arm64、Python 3.12.13、NumPy 2.5.4。
- mmap page：16384 bytes。managed peak は page 単位の allocation と metadata reserve を含む。
- 14 qubit / complex128：状態 256 KiB、RAM 予算 128 KiB、Block 6 bit / 1 KiB、buffered I/O、alignment 64 bytes。
- 初期状態 seed 20261012、任意 3Q/4Q seed 7003/7004。Dense target は Block-local。
- cold-cache は保証しない。calibration は専用 scratch、warmup 1、成功 sample 2、同期 concurrency 1。

```sh
uv run python examples/phase1_benchmark.py \
  --num-qubits 14 --block-bits 6 --ram-budget 131072 \
  --output /tmp/svsim-phase1-benchmark/report.json
```

JSON report と scenario ごとの JSONL trace が出力される。`read/write` は初期 import、実行、snapshot、export を含む振幅 slot の実転送量。catalog/journal の filesystem byte は含まない。実行時間は materialize の wall-clock。別途 report の end-to-end 時間には初期 import、snapshot、export、eager oracle・誤差評価も含む。

## 結果

| ケース | 実行秒 | read MiB | write MiB | managed peak KiB | 最大振幅誤差 |
|---|---:|---:|---:|---:|---:|
| named-eager | 2.3510 | 2.000 | 2.000 | 128 | 3.5e-18 |
| named-batching | 1.1702 | 1.000 | 1.000 | 128 | 3.5e-18 |
| named-deferred-no-frame | 2.5368 | 1.000 | 1.000 | 128 | 3.5e-18 |
| named-per-block-frame | 2.7777 | 1.000 | 1.000 | 128 | 3.5e-18 |
| named-auto | 2.4891 | 1.000 | 1.000 | 128 | 3.5e-18 |
| named-fallback | 6.2997 | 1.750 | 1.750 | 128 | 1.81e-17 |
| dense-3q-auto | 0.3396 | 0.500 | 0.500 | 128 | 0 |
| dense-3q-structured-exact | 0.6533 | 0.500 | 0.500 | 128 | 1.4e-17 |
| dense-4q-auto | 0.4449 | 0.500 | 0.500 | 128 | 0 |
| dense-4q-structured-exact | 1.5323 | 0.500 | 0.500 | 128 | 3.47e-17 |

全 10 ケースで参照精度・managed RAM の上限を満たし、materialization＋snapshot の実測 read/write bytes は AccessPlan の保守的な上下限内だった。

局所 epoch の batching は、この回路では gate ごとの eager と比べて read/write を半減した。Frame mode と batching の転送量は同じで、Frame 管理が常に高速化する結果ではない。named auto/fallback の分解追加費用、3Q/4Q structured-exact の CPU 費用も確認できる。Dense の native と分解は同じ局所 epoch 内で処理されるため、この条件での転送量は同じ。

compute event の時間は内部の I/O・Frame を含む task 時間で、IO/frame の event 時間と足して総時間を計算しない。RSS は process 全体の high-water mark で、scenario ごとの独立した最大値ではない。繰り返し試験や性能主張には別プロセス・複数回の測定が必要。

## macOS F_NOCACHE と prefix COW

次の実行例も macOS の実ファイルで確認した。状態 1 MiB に対し managed RAM 256 KiB。

```sh
uv run python examples/phase1.py \
  --storage /tmp/svsim-phase1-demo --backend direct
```

| Binding | 期待値 ZZ | suffix managed peak | suffix read | suffix write |
|---|---:|---:|---:|---:|
| theta=0.3 | 0.955336489126 | 256 KiB | 2 MiB | 1 MiB |
| theta=0.7 | 0.764842187284 | 256 KiB | 2 MiB | 1 MiB |

prefix fork 時の payload copy は 0。各 suffix は自分の COW root と新規 extent を持ち、snapshot と streaming expectation を完了した。suffix のゲートが全 Block に作用するため suffix write は状態 1 本分で、COW が必ず suffix の書込量を減らすとは主張しない。

Linux 実 O_DIRECT はこの macOS 実測に含まない。Linux 向け整列契約のテストと実 O_DIRECT の platform test を別途用意している。
