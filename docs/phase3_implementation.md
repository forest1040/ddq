# Phase 3 実装と検証

2026-10-13。[設計書](phase3_design.md)と[実測比較](phase3_benchmark.md)に対応する同期 MPI runtime を実装した。

## 実行環境

Python は uv の `.venv` を使用する。MPI は optional extra であり、通常の `import svsim` は mpi4py の import／MPI 初期化を行わない。system MPI は別途必要。

```sh
brew install open-mpi                         # macOS の例
uv sync --locked --extra mpi
uv run --locked --extra mpi pytest -q
mpiexec -n 4 .venv/bin/python -m mpi4py examples/phase3.py \
  --directory /tmp/svsim-phase3 --gradient --backend direct
```

保存先は新しいディレクトリを指定する。`direct` は既存の Linux O_DIRECT／macOS F_NOCACHE 抽象化を利用する。CLI の `python -m mpi4py` は、未処理例外時に peer が finalize 待ちを続けることを避けるために使う。

## API

```python
from mpi4py import MPI
from svsim import DistributedConfig, DistributedSimulator
from svsim import ParameterSymbol, ObservableIR, PauliTerm

config = DistributedConfig('/tmp/svsim-job', block_bits=4,
    ram_budget_bytes_per_rank=1024**2, backend='direct', mcx_exchange='auto')
theta = ParameterSymbol('theta', parameter_id='theta')
with DistributedSimulator(8, config, comm=MPI.COMM_WORLD) as sim:
    sim.calibrate_network(repetitions=3)
    sim.gate('H', (0,))
    sim.gate('CX', (0, 7))
    sim.gate('RY', (0,), angle=theta)
    binding = sim.bind({theta: .37})
    prefix = sim.commit_prefix((theta,), binding=binding)
    with sim.fork(prefix, binding=binding) as run:
        observable = ObservableIR((PauliTerm(0, 1),))
        value = run.expectation(observable)
        gradient = run.gradient(observable, parameters=(theta,))
        snapshot = run.commit_snapshot()
```

全 rank が同じ順序で collective API を呼ぶ。symbol は安定した `parameter_id` を指定する。入力回路、Binding、通信 round、root、layout、placement の不一致は collective error にする。rank 数は 2 の冪かつ Block 数以下。1/2/4 rank を実検証した。

`state(allow_gather=True)` は小規模 debug 用で、rank 0 の managed RAM へ全状態と作業領域が収まる場合のみ実行する。通常の期待値、勾配、checkpoint、`export_state(path)` は全状態 Gather を行わない。export は rank 別 physical-shard 順の complex128 ファイルと logical index へ戻す layout manifest を出す。

## 所有権と記憶階層

`OwnershipMap` は global Block ID を連続した rank shard の range に割り当てる。metadata owner と amplitude extent owner を分け、virtual X/CX/SWAP や Operation shipping 後も正しい元 extent を取得する。root の共有 UUID は全 rank で一致するが、override 行と allocator は rank-local SQLite に置く。共有 catalog の複数 writer は作らない。

`amplitude_store='file'` は rank ごとの append-only 集約 segment、`'ram'` は同じ immutable version API を使う予算付き MemoryBlockStore。Block 数に比例する Python 配列を持たず、Frame／rotation／permutation の共有規則を保存する。file backend の 40/50Q・b=24 metadata-only 回路も振幅ファイルを作らない。

managed budget は既存 cache、full Block 作業領域、aligned buffer、packed tile と RAM resident extent を含む。初期 host admission は host ごとに rank budget を合算し、検出した空き RAM から MPI/OS 用に max(256 MiB, 5%) を残す。これは process RSS／MPI 内部 buffer／OS cache の厳密上限ではない。集約 segment の容量 admission は `storage_budget_bytes_per_rank` で指定できる。

RAM の段階更新は rollback root、現在の root、保存済み root、随伴の両 stream を生存参照として扱う。不要な resident version を解放し、checkpoint では全 resident extent を segment に書き fsync してから解放する。GC は参照閉包を保守的に保存し、file の再回収は未参照の segment 全体に限定する。prefix の外部 lease がある間は GC を拒否する。

## MPI／ゲート実行

振幅は型付き buffer の Sendrecv、metadata は長さ制限付き canonical JSON Bcast。control/data communicator を分離し、一つずつ確定した round を実行する。呼出しは main thread に限定し、library は MPI.Finalize を呼ばない。source/destination、root／version／表現／layout を照合し、chunk と固定 channel を使う。

upper Joint、最大 4Q の Dense と多 upper-X の Pauli 回転を bounded coordinator で計算し、出力を COW で公開する。全 rank の prepare／completion vote 後に frontier を進め、通常の admission／I/O 失敗は元の cut へ戻す。local な非 metadata gate は入力 extent owner で計算する Operation shipping を使う。Frame asset は hash と tableau format を検証して配送する。振幅取得時には Frame、位相、base log を source 側で解消した `logical-identity` 表現を使う。

MCX の packed 経路は lower control が成立する昇順 offset を tile へ詰め、旧 input を保持したまま相手の値を scatter する。inactive offset を保持する。`packed`／`full` は明示指定でき、`auto` は block／tile／backend／host／MPI 条件の一致する profile がある場合に packing＋通信費用を比較する。未知条件は full。SSD の読み書きは full Block のまま。

S0 は元の layout で実行する。S1 は元ゲート順を保ち、global bit を local bit へ移す physical SWAP の回収を保守的な費用下界で判定する。`swap_policy='S1'` と条件の一致する `network_profile`、測定した `storage_bandwidth` が必要。profile 不在では S0。全 bit 階層の明示交換と、自動 S1 の state／gradient 一致を検証した。現在の routing は full Block を使い、半 Block の理想転送量を費用として仮定しない。

## 期待値と随伴

期待値は upper-X mask ごとに partner fetch を共有し、source-owned 複素部分和を complex Allreduce する。Y の位相と upper-Z、layout、open/closed projector を同じ Pauli 内積 kernel に通す。

随伴は `lambda=O psi` を Block 単位で作り、forward と adjoint の二つの root lineage を storage 上で逆再生する。元の GateOp／Expr occurrence ごとに `Im<lambda|QP|psi>` と chain rule を加算し、最後に parameter 順の vector Allreduce を行う。physical layout の inverse を両 stream に適用し、呼出し後の root／frontier／Binding／layout を復元する。全中間 state を RAM に保存しない。

## checkpoint と互換性

rank-local payload／catalog／journal／asset を flush して prepare manifest を保存する。全 rank 成功後に rank 0 が checksum 付き global manifest を atomic replace／directory fsync する。この global manifest が唯一の durable commit。prepare のみの cut は再開対象にしない。rank prepare の集合、root／frontier、circuit／asset／extent checksum を検査する。

global checkpoint は schema/semantics v3。rank SQLite は既存 v2 catalog へ distributed_roots／extent_owners／distributed_settings を追加する方式で、SQLite user_version は互換性のため 2 を継続する。prefix fork は metadata のみをコピーし、元 extent を read-only reader で参照する。固定 parameter／exact witness、frontier／layout／placement を照合し、suffix だけを COW で更新する。

Phase 1/2 の snapshot は、空の P=1 target の `migrate_stored_snapshot(source_storage_config, snapshot)` へ streaming 移行できる。元 catalog と extent は read-only、legacy Frame 適応は一時 metadata backup 上で行う。symbolic tape、Binding、layout と inverse history を保存し、source catalog が変更されないことを試験した。同一 n/b が必要で、rank 数変更は offline repartition が必要。

## 校正と検証範囲

`calibrate_network()` は全 directed peer、Block／packet size、tile／backend／host／MPI 条件を保存する。Sendrecv 経路全体の elapsed MAX と packing 費用を測るため、raw NIC 帯域とは異なる。成功した転送だけを peer／packet 条件へ対応付け、ObservePerformanceGuard の連続 window と hysteresis で slow rank を報告する。plan の自動切替はしない。

実 MPI tests は 1/2/4 rank の file／RAM、random complex input、mixed open control、Dense、Pauli、virtual permutation、physical SWAP、期待値、随伴有限差分、prefix、checkpoint／再開／GC を検証する。通信量は全 rank の send payload の和で数え、recv と二重加算しない。2 rank の実 delay 注入で observe-only の劣化検出を確認した。checkpoint prepare／global commit と fork backup／lease、migration の publication 失敗も試験する。Linux 専用 I/O tests は macOS では skip。

初期実装は同期 correctness runtime。coordinator と collective metadata lookup の直列費用が大きい。common-frame ALIGN による Joint 転送削減、同時 SSD＋MPI／reduction を含む総合 profile、GC の live segment compaction、非同期 pipeline と自動再計画は今後の性能改善対象。今回の実機検証は同一 Mac 上であり、multi-node 性能や rank 脱落後の communicator 継続を検証したものではない。
