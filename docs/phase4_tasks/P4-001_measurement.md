# P4-001：条件一致の校正とruntime completion計測

状態：DONE（2026-10-15、利用者の判断）。完了証跡は[evidence bundle](../internal/p4_001_evidence/README.md)、runtime校正のheld-out envelopeと引継ぎ事項は[cost registry資料](../phase4_cost_registry_implementation.md)にある。以下は着手時（2026-10-14）の記録である。[基盤実装](../phase4_foundation_implementation.md)でcompletion／adapter／有限runtime campaignを追加した。全受入条件は維持する。共通契約は[全体設計](../phase4_remaining_design.md)。Phase 5のprofile codec・condition・有限campaignを拡張し、P4-002/003/004/007/008へ実測costを供給する。

[追加実装](../phase4_safety_monitoring_implementation.md)でcollectorのon/offを同じRAM poolで対照し、actual traceからresolved backendを取得する。全monitor費用と全stage registry／自動接続の完了条件は維持する。

## 設計時点（6aa8581）の現状と差分

`RuntimeControl.io_completion()`は成功read/writeをlegacy BenchmarkSampleへ変換する。`MPIProgressEngine.wait()`はcontrol/ack込みwhole-transferを観測する。Phase 5 campaignはRAM/kernel/storageと直列mixedを測る。compute、queue/progress、Frame、packing、metadata/commitの共通completion record、実executor構成での混合校正、ProfileDocumentからruntimeへの厳密adapterを追加する。

変更対象：[runtime_control.py](../../src/svsim/runtime_control.py)、[async_scheduler.py](../../src/svsim/async_scheduler.py)、[async_io.py](../../src/svsim/async_io.py)、[mpi_progress.py](../../src/svsim/mpi_progress.py)、[mpi_health.py](../../src/svsim/mpi_health.py)、[benchmark_campaign.py](../../src/svsim/benchmark_campaign.py)。新規予定：`runtime_measurement.py`、`runtime_profile_adapter.py`、`runtime_calibration.py`。

## 入出力・条件

`CompletionObservation`はcontext/condition、parent/group ID、stage、units、quantity、queue/service/protocol wall、CPU、payload/storage bytes、success、validation、evidence/scope、warmupフラグ、raw参照を持つ。unit callは単一実operation。group sampleは含まれるchildを明示し、task側からchildを再加算できないよう消費集合を返す。

| 系統 | 必須condition |
|---|---|
| kernel/Frame | backend/build/thread、n/b、dtype、target/control/Pauli/angle class、Frame/log class、materialization経路、fused構造 |
| storage | device/volume、resolved backend、alignment、payload/slot、working set/cache class、read/write/fsync、engine/queue/depth、混合負荷class |
| MPI | source/destination host class、library/thread level、payload/tile/packing、peer/concurrency、scope、controlを含む測定区間 |
| metadata/lifecycle | catalog規模class、transaction/row/payload、root depth、prepare/decision/ack、durability境界 |

宣言したqueue上限と実際の同時稼働を分ける。cache stateやFrame形状が不明ならunknown条件とし、別classのbaselineへ強制一致させない。kernel angle0.37の現cellをExpr/ExactPiや別Frameへ広げる場合は新cellを測る。

## 収集アルゴリズム

1. main threadがsubmit eventとstable event IDを登録。I/O workerはlocal start/endだけを記録し、MPI/SQLを呼ばない。
2. completion callback終了とchecksum/kernel検証の成功をmain loopで確認して初めて成功sampleにする。cancel/short/error/terminal payloadは別failure eventへ保存する。
3. queue wait、service、whole protocol、epoch end-to-endは別metric。poll待ち時間を複数requestへ重複配賦しない。clockは同hostのmonotonic durationのみ。
4. conditionごとのrolling ringは64、active conditionは256。rawをlocal diskの上限付きchunkへ送り、flush自体のdisk予約とmonitor費用も記録する。
5. 合意済みepoch境界でhost/rank summaryをchunk交換する。wallはrank MAX、不均衡とpeer別分布を残し、遅いrankの情報を平均で消さない。

ProfileDocumentの不変baselineとrolling observedを別に保持する。条件・構成fingerprintを全項目比較し、runtime選択用cellは校正と数値検証が成功したものだけ。legacy aggregateはloss付きreferenceで、欠けるpeer/build/cache条件を埋めた実測と扱わない。

## 校正とquality

`CalibrationSpec`に有限case、seed、scratch、RAM/disk/wall、warmup/sample、engine/policy、thread、scopeを指定する。通常warmup1以上・measured5以上、small kernel7。順序はseed shuffle、A/Bは交互paired trial。採用できるactual executorだけを実行し、whole-group wallと各stageを併記する。caseごとの初期state/cache準備・検証区間を固定する。

まずcompute＋storage、compute＋MPI、storage＋MPIを実schedulerで測る。worker/cooperative、queue depth、same-host/cross-hostを別cellにする。mixedが未測定ならserial referenceかunknownで、重なりを推測しない。large Pは有限peer classの代表だけを測り、未測定peerをunknownとする。

statistical qualityはcorrectnessの`validated`とは別。sample extremaはdiagnostic、mean confidence intervalは推定手法付き、candidate用conditional envelopeはheld-out coverage付きとする。自動adapterは必要quality/freshnessを検査し、現CostSampleの下上界へmin/maxを無条件に割り当てない。

injected試験は専用`runtime-calibration` kind/scopeで保存する。現hardware-profileのsame-host/cross-host制約を迂回して偽装しない。productionはinjected/external/assumedを採用しない。評価専用registryのscope許可はcase manifestに明示する。

## API・失敗

予定API：`collector.submit/complete()`、`collector.snapshot()`、`calibrate_runtime(spec)`、`adapter.match(context, condition, required_quality)`。既存手動record_cost/observe APIは維持する。新svbench suiteは明示commandで実行し、estimate/probe/通常importから校正を始めない。

予算不足はcase未実施、deadlineはomitted、検証失敗はfailedとしてrawへ残す。新profileの公開は全validator成功後。profile保存に失敗してもbaselineを書き換えない。monitor budget超過は収集頻度を落としてLOSS/partialを記録し、自動切替の根拠を追加しない。

## 受入と実装分割

| 段階 | 証跡 |
|---|---|
| 001A completion/adapter | fake時刻・検証後成功、重複event除去、条件不一致、legacy loss、数値oracle、raw→summary再現 |
| 001B Mac actual mixed | uv/P=1/2/4、worker/cooperative、buffered/F_NOCACHE、queue classごとのwarmup/反復、overhead/variation |
| 001C 統合 | ETA/Guard/cache/gradientが同じregistryを使う。injectedをproductionへ流用しない |
| Linux/別host | 許可後O_DIRECT、actual peer/library条件、rank MAX、failure/cancelを成功sampleに混ぜない |

raw JSON、immutable profile、reproduction manifest、条件別median/tail/variation、monitor自体を含む比較reportが揃ってDONE。Mac直列mixedだけでは実scheduler overlapとcross-hostを完了扱いにしない。
