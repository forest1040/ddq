# Phase 4 実装：Mac 上の非同期実行と node-local 保存

- 作成日：2026-10-13
- 対応設計：[Phase 4 設計書](phase4_design.md)
- 状態：主要APIを実装し、Macと許可後のPBSクラスタで単一ノード・実2ノードの2／4 rankを検証した。設計全体の拡張項目・native io_uringには以下の未実装・未検証境界が残る。
- 実行条件：Pythonは `uv` のlock環境。Mac Studioのテスト後に利用者の許可を得て、PBS各ノード4 CPU・8 GiB、各job 10分以内、local disk各8 GiB以下でクラスタ検証した。

設計全体の完了条件は未達であり、残件・優先度・完了証跡は[Phase 4 backlog](phase4_backlog.md)で管理する。

2026-10-14にP4-009／001の[容量予約・runtime計測の基盤](phase4_foundation_implementation.md)を追加した。Macで検証し、両backlogはIN_PROGRESS。今回の変更はクラスタでは未実行。

同日にP4-002／004／006／007の[先行実装](phase4_remaining_implementation.md)を追加した。finite remaining capture／診断ETA、bounded policy候補、P=1 tile、cache identity／費用fallbackが対象。4項目はIN_PROGRESSで、細粒度cost、自動safe切替、P>1 tile、cache共有／公平性と性能評価は残る。

P4-003／005／008の[注入・paired report・明示勾配校正](phase4_validation_implementation.md)も追加した。injected completionはproduction costから除外する。actual三方式の数値とunknown時のadjoint fallbackを検証し、positive採用／VERIFY、包括admissionと最終性能matrixは残す。

## 1. 実装範囲

`AsyncDistributedSimulator` は Phase 3 の正確な block kernel、Frame、parameter Binding、MPI owner routing を引き継ぐ。物理 I/O の worker と main thread の MPI progress を追加し、未公開 epoch 内の reordered execution と公開 prefix cut を分ける。CPU kernel 全体を別 thread へ移す方式ではなく、既存の gate kernel を main thread から実行しながら I/O・通信を進める。

| Module | 現在の実装 |
|---|---|
| `async_io.py` | 有限 request credit、positional read/write/flush、completion-owned pin、main-thread completion callback、worker／cooperative engine |
| `uring_backend.py`／`native/uring_backend.c` | optional Linux liburing adapter、明示 capability probe、CQE completion と buffer 管理 |
| `mpi_progress.py`／`async_exchange.py` | 有限 typed MPI request、複数partnerの同時転送、slot generation、main-thread progress、completionとbuffer寿命の管理 |
| `async_store_io.py` | 発行前の extent／disk 予約、成功 completion 後の checksum／catalog 更新、descriptor 寿命の管理 |
| `async_cache.py` | bounded prefetch、dirty writeback、I/O 中の pin、validated cost hint と LRU fallback |
| `execution_tasks.py` | immutable な実行 step と epoch coverage、管理情報の予約、失敗 epoch の非公開化 |
| `async_scheduler.py` | collective な epoch 実行、drain、S0/S1/S2 選択、snapshot、勾配実行への接続 |
| `s2_planner.py`／`fused_swap.py` | 保守的な依存解析、local gate fusion、最大 2 組の ordered physical bit swap を一つの state pass に合成 |
| `runtime_control.py`／`runtime_cost.py` | matched-condition health guard、版付き policy、候補 dry-run、有限 DAG cost／ETA、LIVE_TUNE／FRONTIER_REPLAN |
| `parameter_batch.py`／`batch_resources.py` | immutable prefix fork、同一 rank group の suffix 交互実行、共通 RAM budget、lease 解放 |
| `gradient_scheduler.py`／`phase4_gradient.py` | adjoint／recompute／parameter-shift の適用条件・cost 判定と実行 adapter |
| `storage_placement.py`／`distributed_checkpoint.py`／`checkpoint_payload.py` | 物理 owner 配置、v4 local prepare、分割 metadata 通信、coordinator の COMMIT、local decision copy、collective resume |
| `disk_budget.py` | host/device の共通 SQLite reservation ledger、実空き・実ファイルサイズの検査、quota／最低空き／drain 余裕 |
| `development_runner.py`／`pbs_adapter.py` | Mac の有限 local job queue、タイムアウト・取消・report、PBS resource 検査と script の生成 |

## 2. 非同期 engine と資源管理

`WorkerIOEngine` の worker は file descriptor に対する I/O のみを行う。MPI、SQLite、catalog 更新、policy 変更を worker callback 内で行わない。結果の検査と catalog の確定は、engine の `poll()` を呼ぶ main thread が行う。

`BufferLease` は owner と completion pin を分ける。owner が `close()` を要求しても request pin が残っている間は backing storage と RAM credit を保持し、最後の completion で解放する。I/O request 数が上限に達した場合は `BackpressureError` を返す。発行済み request を drain してから buffer・descriptor を閉じる。

MPI が `THREAD_SINGLE` の場合、`io_engine='auto'` は main thread の cooperative engine を選ぶ。worker を明示指定した場合は拒否する。macOS の `backend='direct'` は既存の `F_NOCACHE` backend を使う。Linuxの `O_DIRECT` backendも既存実装を引き継ぎ、PBSの単一ノード・実2ノードでworker I/Oを検証した。

**native io_uringは2026-10-14にLinux Dockerで実kernel検証へ着手した。** [環境と検証範囲](linux_development.md)、[内部記録](internal/ent_linux_validation/README.md)を参照する。`io_engine='io_uring'` の明示指定時だけ、Linux、C compiler、liburing header／library、kernel の ring 初期化を確認する。対応しない条件では `NotImplementedError` となり、worker へ黙って切り替えない。`auto` の基準は引き続き worker／cooperative である。native ring は main thread で progress し、kernel submission の致命的失敗で request の到達状況を証明できない場合は ring を停止し、未完了 buffer の pin を process 終了まで保持する。従来のPBSクラスタにはliburing header／libraryを新規導入せず、nativeの全fault／durability matrixはP4-010へ残す。

`DistributedConfig` に追加した設定は次のとおり。

| 設定 | 意味 |
|---|---|
| `storage_scope` | `shared` は Phase 3 互換。`node-local` は owner 配置を通信で合意する。Async runtime は node-local を使用する |
| `disk_quota_bytes` | 同一 dataset／host／device の保存容量上限。未指定の場合も実空き検査を行う |
| `min_free_bytes` | 他 job と OS に残す最低空き容量 |
| `drain_reserve_bytes` | 通常の新規書込みには与えず、checkpoint／drain で利用する余裕 |

同じdatasetを初期化する同一host/deviceのrankはledgerを共有する。prefix／foreign local rootも実使用量へ登録するが、独立ledger間の未物理化予約の完全な調停はP4-009に残る。segment、metadata、journal、asset、manifest、途中fileを集計し、重なる親子directoryを二重計上しない。書込み前の予約はSQLite transactionで直列化し、外部jobの消費はstatvfsの空きへ反映する。

segment、RAM storeのflush_to_file、asset、journal、trace、checkpoint payload／decisionへadmissionを接続した。2026-10-14からcatalog open／SQL transaction前の予約と同SQLiteのVFSを使用し、ledger自身は固定metadata poolで管理する。全writerのfault／回収証跡は残るためstrict_capacity_guaranteeはfalse。OS cache、MPI library、Python object全体の追跡も対象外で、host/job割当に余裕を残す。詳しくは[基盤実装](phase4_foundation_implementation.md)。

予約中に process が失われた場合、その disk reservation は永続 ledger に残る。自動的な credit 復活は行わず、安全側で停止する。異常終了後の ledger／残骸整理と容量確認は、再開運用の確認項目である。

## 3. Epoch、S2、physical swap

epoch は最大 `lookahead_gates` の source occurrence を持つ。実行順を変えても source circuit 自体を書き換えず、actual execution history に source ID と physical gate／layout を記録する。coverage が完成し、I/O と MPI を drain した後で frontier を公開する。失敗時は epoch 開始時の root、frontier、layout、placement と history 長へ戻し、途中 cut を snapshot として公開しない。RAM reclamation は rollback root と workspace root も保持する。

S2 は bounded window と beam を使い、証明できる独立／diagonal な候補を扱う。unknown な依存は元の順序を保持する。local fusion は対応 kernel を使って一度の block pass 内に gate をまとめる。任意の dense gate fusion や、近似的な commuting 判定は行わない。

`fused_index_bit_swap(pairs)` は logical SWAP と異なる physical index transition である。ordered pair list を合成して destination block を一度作り、layout history を保存する。最大 2 組の pair と full block working-set が対象で、重なる bit pair の順序も保持する。予算より小さな destination tile を使う一般的な aligned streaming route は未実装であり、合法な full-block route を admission できない条件を無理に実行しない。

## 4. Node-local checkpoint と再開

`StoragePlacement` は store ID、owner rank、host、volume ID、owner-local root と版を持つ。共通 config の合意から rank 固有の directory を除き、配置集合を合意する。同一 Mac 上でも rank ごとに異なる root を使える。foreign store の reader は、自分の owner entry から自分の shard path だけを解決する。既知の NFS/CIFS/SMB/SSHFS/Lustre/GPFS path は node-local placement として拒否する。

v4 commit は次の順序で行う。

1. 一致 cut を drain し、各 owner が state、catalog、journal、asset と prepare を flush／fsync する。
2. prepare の hash を交換する。canonical metadata を bounded typed MPI chunk に分けて配送し、各 owner が local payload file を fsync する。
3. rank 0 が payload の SHA-256／size と全 prepare hash を含む小さな COMMIT decision を、local disk へ atomic replace／directory fsync する。
4. decision を通信で配送し、各 rank が local decision copy を fsync して acknowledgment する。

coordinator の durable decision が authoritative commit point である。その後の copy 配送／保存失敗では旧 cut に戻らず、runtime を停止し、再開時に照合する。`_checkpoint_decision_durable` により runtime policy の rollback も commit 前後を区別する。payload が一つの control message より大きくても、転送時に同サイズの巨大 byte buffer を作らない。semantic metadata の全 object 自体は現在の runtime 内に保持しており、完全な disk-indexed gate tape へ置き換えたわけではない。

resume は communicator を先に作り、coordinator が選択した snapshot の ID と dimension を通知する。その後、owner-local catalog を開き、metadata payload を通信で照合する。各 rank は自分の prepare、journal cursor、asset、decision copy、host／volume／root を検査して collective vote する。正常な既存 payload は再書込みしない。失われた decision copy は coordinator の authoritative decision から修復できる。

最新の v4 decision／payload が壊れている場合、古い cut を黙って選び直さない。prepare だけの候補、未完了 request、FD、worker を復旧対象としない。owner volume の喪失を decision copy だけで復旧する機能、coordinator の自動交代、rank 数変更、shard replica は未実装である。

fork は各 owner の catalog を複製し、immutable amplitude extent を source store へ参照する。fork の lease 作成は collective に合意し、owner-local file に保存する。GC は全 rank の lease／pin と committed root を照合して、自分の local extent のみを回収する。一 rank だけに lease が残っていても GC を拒否する。共有 filesystem の mirror は必須ではない。

### v3 互換の境界

v3 checkpoint が **既存の local owner volume 上にあり、同じ P/n/b と owner root で読める場合**、node-local runtime が legacy registry を owner descriptor へ変換できる。state を保持したまま次の commit で v4 になる。registered prefix store も local volume として検査する。

共有 filesystem 上の v3 dataset を別々の local disk へコピーする adapter は未実装である。計算時に shared prefix path を参照し続ける互換経路も提供しない。この場合は必要な owner shard／catalog／journal／asset の明示的な offline コピー、checksum、容量、配置の確認が前提となる。同様に、別 host／別 volume への runtime migration は自動実行しない。

## 5. Parameter batch と勾配

`ParameterBatchExecutor(template, prefix)` は同一 communicator の Binding suffix を round-robin に進める。active job 数と lookahead が有界で、template と子 runtime は `MemoryBudget` を共有する。disk quota は dataset／host／device ledger で合算する。immutable prefix と Binding ごとの COW 出力は分離し、終了時に cache／worker／lease の資源を戻す。複数 rank group の並列 batch と、別 Binding の logical amplitude を自動的に coalesce する cache は対象外である。

勾配の公開 API は `gradient(..., method='adjoint'|'recompute'|'parameter-shift'|'auto')`。adjoint は actual execution history を逆順にたどり、fused layout transition も扱う。recompute は initial cut から必要な forward state を再計算する。parameter-shift は source occurrence ごとに shift し、Expr の chain factor を適用する。対応する Pauli rotation と controlled rotation の shift rule を検査する。対応しない gate を便宜的に 2-point rule へ落とさない。

`GradientScheduler` は明示された測定 cost／confidence interval と適用条件を比較する。測定が不足する場合、未測定の高速化を根拠に自動採用しない。cost の自動校正を含む大規模運用の性能評価は今後の検証項目である。

## 6. Runtime control と観測の境界

操作は `sim.runtime` を介して collective に呼ぶ。

- `configure_guard(profile)`／`observe(sample)`：matched-condition の既存 health guard を利用する。
- `record_cost(CostSample(...))`／`estimate_eta(tasks)`：明示した測定条件と有限 DAG の cost を登録・推定する。
- `update_eta(endpoint_frontier=..., collective=...)`：有限window／counted suffix／requestを自動captureし、3 viewの診断ETAを更新する。
- `search_candidates(collective=True)`：identityを含む有限policy候補を生成し、全rankのcapability拒否を集約する。生成候補の自動採用は未接続。
- `request_replan(updates, evidence=..., apply=False)`：active state を変更せず candidate を生成する。
- `apply_candidate(id)`：cut、plan／policy 版、有効期限、測定条件と利益下界を検査する。
- `apply_policy(updates, classification=...)`：drain と全 rank vote を経て policy を適用する。FRONTIER_REPLAN は cut と policy を同じ durable commit に保存する。

既定の `mode='observe'` は candidate を自動適用しない。`recommend` は既存の手動evidence付きeligible candidateの明示適用に使う。`auto` はDEGRADED時にbounded候補探索を行うが、校正済みtransition費用の自動供給が無いため生成候補を採用しない。DEGRADEDだけを根拠とするprefetch削減は廃止し、未実証の改善をVERIFY成功とはしない。

I/O completionにはqueue／service／whole wallを記録する。既存`RuntimeCostModel`は明示CostTask DAG、新`RuntimeETA`は自動captureのcounted DAGを扱い、未検証overlapはserial expected／upperとする。総ETAは必要costが揃ったときだけ数値で、通常の未校正profileではnullとなる。現endpointはgates-only。細粒度queue／fsync／MPI／kernelのlowering、mixed-load校正、migration費用と運用評価は残る。cacheは最大64 hintでvalidated／同unit／freshnessを検査し、unknown・staleではLRUへ戻る。raw／resolved identityは追加したが、resolved bufferのBinding共有や公平性は未有効。詳細は[先行実装資料](phase4_remaining_implementation.md)。

## 7. Mac の開発・実行

全 rank が public collective API を同じ順で呼ぶ。次の例は各 rank に異なる local root を与える。新規 run directory を毎回生成し、既存 dataset を上書きしない。

```python
# /private/tmp/phase4_demo.py
from pathlib import Path
from uuid import uuid4
from mpi4py import MPI
from svsim import (
    AsyncDistributedSimulator, AsyncPolicy, DistributedConfig,
    ObservableIR, PauliTerm, ParameterSymbol,
)

comm = MPI.COMM_WORLD
rank = comm.Get_rank()
run = comm.bcast(uuid4().hex if rank == 0 else None, root=0)
root = Path('/private/tmp') / f'ddq-phase4-{run}' / f'owner-{rank}'
config = DistributedConfig(
    str(root), block_bits=2, backend='direct',
    ram_budget_bytes_per_rank=8 * 1024**2,
    metadata_reserve_bytes=256 * 1024,
    tile_bytes=4096, storage_scope='node-local',
    disk_quota_bytes=64 * 1024**2,
    min_free_bytes=16 * 1024**2,
    drain_reserve_bytes=4 * 1024**2,
)
policy = AsyncPolicy(mode='observe', swap_policy='S2',
                     max_io_requests=2, max_mpi_requests=4)
theta = ParameterSymbol('theta', parameter_id='demo-theta')
observable = ObservableIR((PauliTerm(0, 1 << 5),))
with AsyncDistributedSimulator(6, config, comm=comm, policy=policy) as sim:
    sim.gate('H', (0,))
    sim.gate('RY', (5,), angle=theta)
    binding = sim.bind({theta.parameter_id: 0.37})
    sim.materialize(binding=binding)
    value = sim.expectation(observable)
    derivative = sim.gradient(observable, parameters=[theta], method='adjoint')
    snapshot = sim.commit_snapshot()
    if rank == 0:
        print(value, derivative[theta.parameter_id], snapshot['snapshot_id'])
```

```bash
uv sync --locked --extra mpi
uv run --locked --extra mpi mpiexec -n 4 python /private/tmp/phase4_demo.py
uv run --locked --extra mpi pytest -q
```

`LocalDevelopmentRunner` は simulator 内の scheduler と別の単一 host job queue である。`JobSpec` に revision、lock hash、argv、rank 数、RAM／disk、walltime、test case を渡す。argv を shell へ渡さず、host の申告予算を合算して FIFO admission する。timeout／cancel は process group に適用し、stdout／stderr と JSON report を保存する。MPI launcher と環境は `JobSpec` の明示 argv／env に含める。runner の申告資源上限は OS の memory sandbox ではない。

`PBSAdapter` は supplied allocation の CPU／memory／scratch と offline 状態を検査し、script と metadata を生成するだけである。`qsub`、`ssh`、node 起動の API は含まない。site固有の運用は[内部クラスタ計画](internal/lab_cluster_execution_plan.md)に従う。実行履歴は[内部検証記録](internal/phase4_cluster_validation.md)に保存した。

## 8. 検証と残る完了条件

Mac 検証は、unit test と同一 Mac 上の real MPI 1／2／4 rank を組み合わせる。対象は worker／cooperative I/O、completion pin、MPI buffer と slot の寿命、S0/S2 と fusion、ordered fused swap、exact phase／controlled gate／Frame、prefix／batch、三つの勾配方式、RAM より大きい state の block 実行、buffered／F_NOCACHE、local job queue の timeout／cancel と report、PBS rendering である。NumPy oracle と比較し、managed peak と request 上限も検査する。

lifecycle の real MPI test は rank ごとに異なる root を使い、peer の manifest path を開く経路を拒否する。control packet より大きな Unicode payload、remote lease による GC 拒否、COMMIT 後の copy failure と停止／復旧、missing copy 修復、local prepare の破損、最新 payload 破損時の旧 cut 選択拒否、RAM checkpoint の容量不足と retry、v3 local-volume upgrade を検証する。最新の全テスト件数・実測時間は最終テスト report に記録する。実装文書のサンプルは Mac の 4 rank／F_NOCACHE で実行し、`cos(0.37)` と `-sin(0.37)` に一致する期待値・勾配と v4 snapshot の保存を確認した。

許可後のクラスタ検証では真の別host／別root、Linux worker I/O／O_DIRECT、PSM2／OFI／Ethernet、2／4 rankのS0/S2、batchと三方式の勾配、v4 checkpoint／resume／prefix／GCとfault検証が成功した。local filesystemの空き100 GiB以上を維持した。native io_uringの全fault／durability matrix、共有filesystemからのoffline dataset移行、一般的なtile migration、性能guard／ETAの運用拡張、保存媒体喪失からの復旧は未検証・未実装境界として残る。詳細は[内部検証記録](internal/phase4_cluster_validation.md)。

### Mac最終検証記録

`uv run --locked --extra mpi pytest -q` は **378 passed、2 skipped、98.98秒**。skipはLinux O_DIRECT専用の2件で、Macでの成功として数えない。全体試験後、実行器内で複数のpartner転送が同時に進むことを2/4 rankで検査するoracleを追加し、`tests/test_phase4_mpi.py tests/test_pbs_adapter.py` の **21件が30.77秒で成功**した。source IR不変、complex128の位相を合わせない数値一致と、管理RAMの予算内動作を確認した。

### Linux・実2ノード検証記録

PBS単一ノードの選択unit testは **28 passed、13.54秒**、1／2／4 rankのO_DIRECTとv4 lifecycleが成功した。実2ノードでは2／4 rankそれぞれbuffered／O_DIRECT × S0／S2の8ケース、複数partner同時転送、v4 lifecycle、16 qubitのbounded state（1 rank状態256 KiB、managed budget192 KiB）が成功した。専用oracleはPSM2の2／4 rank、OFI/PSM2の4 rank、Ethernet/TCPの4 rankで期待値と三方式の勾配、batch、再開の一致を確認した。v3の既存treeからのupgrade試験は同一host／volumeに限定し、修正後のMac lifecycle回帰 **9 passed、2.76秒** を確認した。cross-hostのv4検証は省略していない。専用oracleは小規模correctness用であり、量子化学の性能推定や混合負荷の自動校正を実施したものではない。

`examples/phase4.py` の4 rank／F_NOCACHE／adjoint例も成功し、期待値0.9323273456060345、勾配-0.36161543196496193、v4 checkpointからの再開を確認した。[実測比較](phase4_benchmark.md)にlocal schedulerによるwarmup＋3反復の同期／非同期／S2比較を記録する。
