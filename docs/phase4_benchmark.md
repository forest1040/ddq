# Phase 4：Mac上の同期・非同期・S2比較

2026-10-13。Mac Studio、Python 3.12.13、NumPy 2.5.4、mpi4py 4.1.2、Open MPI 5.0.11。uvのlock環境と`LocalDevelopmentRunner`を使い、同じMac上で1/2/4 rankを実行した。[生JSON](phase4_benchmark.json)、[計測script](../examples/phase4_benchmark.py)、[同一回路worker](../examples/phase4_benchmark_worker.py)を保存した。クラスタは使用していない。

各条件はwarmup 1回＋測定3回。中央値と最小・最大をJSONに記録した。CPU／SSDの背景負荷を隔離した測定ではなく、同時期に回帰テストも実施している。小回路に対するこの結果から、実ネットワークのscaling、大規模量子化学計算、一般的な速度向上を推定しない。

## 回路と容量

8Q、b=4、complex128状態4 KiB、rank当たりmanaged RAM 256 KiB、metadata reserve 64 KiB、buffered file backend。振幅はowner別ローカルdirectoryに保存する。全条件で`frame_policy='materialize'`を明示し、local rotationを物理実行する。回路は`H(7)`と、`RY(0,.37/6)`／`RZ(1,.12)`を6組。期待値は`Z0=cos(.37)`。checkpoint／resume後の期待値も照合する。

Macのmmap pageは16 KiB、disk slotは4 KiBであり、全状態4 KiBと実際のcache／保存容量は異なる。`execute`はmaterializeのrank elapsed MAX。read/writeは期待値とcheckpointを含む最初のruntimeの合計で、resumeのreadは別runtimeのため含まない。全体時間はJSONの`end_to_end_seconds`に記録する。

## 結果

| rank | 実行器 | 方針 | execute中央値 秒 | SSD read合計 KiB | SSD write合計 KiB |
|---:|---|---|---:|---:|---:|
| 1 | 同期 | S0 | 0.5117 | 832 | 832 |
| 1 | 非同期 | S0 | 0.5827 | 832 | 832 |
| 1 | 非同期 | S2 | 0.0909 | 128 | 128 |
| 2 | 同期 | S0 | 0.6626 | 824 | 832 |
| 2 | 非同期 | S0 | 0.6966 | 824 | 832 |
| 2 | 非同期 | S2 | 0.1100 | 120 | 128 |
| 4 | 同期 | S0 | 1.2332 | 0 | 832 |
| 4 | 非同期 | S0 | 1.3029 | 16 | 832 |
| 4 | 非同期 | S2 | 0.2616 | 32 | 128 |

S2のlocal fusionにより12個のlocal rotationを一つのblock passにまとめ、全条件で書込みが832→128 KiB（約84.6%減）となった。同期S0に対するexecute中央値の比は1 rankで5.63倍、2 rankで6.03倍、4 rankで4.71倍。この回路ではfusionによるpass数削減が主な改善で、gate reorderだけの効果を独立測定した値ではない。

非同期S0は同期S0より遅かった。小さいslot、SQLite・collective・request管理の費用があり、nonblocking APIに変えただけで速くなるとは判断しない。4 rankの同期readが0なのはcacheに残るためで、物理SSD性能が無限大になった意味ではない。非同期の先読み・書き戻しでcache挙動が変わり、read合計は同期と一致するとは限らない。

最大managed peakは全条件で256 KiB以内。process RSS、MPI内部buffer、Python object、OS cacheの全体量を示す値ではない。

## 再現

```sh
uv run --locked --extra mpi python examples/phase4_benchmark.py \
  --repetitions 3 --backend buffered --output /private/tmp/ddq-phase4-benchmark.json
```

Macのlocal queueからMPIを起動し、各jobは新しいscratch directoryを使用する。scriptにSSH、PBS投入、クラスタ起動は含まない。

状態がrank予算を超えるケースは別の実MPI回帰テストで検証した。b=6、P=1/2/4に応じてn=14/15/16、各rankの状態256 KiBに対してmanaged RAMは192 KiB。全状態Gatherを使わず、4つの解析的期待値と正規化、checkpointを確認している。上の小回路の時間をこの容量ケースの時間として扱わない。
