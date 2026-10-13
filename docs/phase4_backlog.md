# Phase 4 backlog

更新日：2026-10-13。**Phase 4は未完了**。主要APIとMac／Linux・実2ノードのcorrectness検証は実装・実施済みだが、適応制御と性能評価を含む設計の完了条件には残件がある。

基準は[開発計画](../plan/distributed_quantum_circuit_simulator_development.md#phase-4非同期階層スケジューラ)と[設計書 §15](phase4_design.md#15-実装順と完了条件)。現在の挙動・制限は[実装資料](phase4_implementation.md)、既存の性能測定は[Mac比較](phase4_benchmark.md)に記載する。site固有の情報はこのbacklogに書かず、`docs/internal/`で管理する。

## 管理方法

[Phase 5設計書](phase5_design.md)で共通profile・校正・静的推定を先行整備し、P4-001／002／003／004／005／007／008へ接続する。容量拡大前にP4-009、必要な転置経路にP4-006を解消する。設計書の作成だけでは本backlogの状態を変更しない。

[Phase 5基盤実装](phase5_implementation.md)で不変profile、完全一致cell、Macの有限mixed計測、容量referenceとstate-free解析を整備した。P4-001のruntime計測とMPI／Frame／metadataの共通化、P4-002以降への接続は引き続きOPEN。全項目の完了状態は変更していない。

[残件の全体設計](phase4_remaining_design.md)に共通契約・実装順・milestoneを定義し、各IDの個別設計を作成した。下表のIDから変更箇所、API案、アルゴリズム、失敗処理、受入条件を参照できる。設計済みであり、実装のOPEN/BLOCKEDは維持する。P4-009とP4-001を先行し、ETA→候補探索→tile/cache/gradient→適応/最終比較の順に統合する。

状態は `OPEN`、`IN_PROGRESS`、`BLOCKED`、`DONE`。優先度P0は完了判定の中心となる課題、P1は設計への適合・評価の補完、P2はoptional実機検証または移行対応。優先度だけを理由に完了条件から除外しない。

着手時に担当・関連commitを追記し、完了時はテスト／reportへのリンクと、測定条件・結果を記録する。scopeを縮小する場合は設計書と開発計画に反映し、項目を黙ってDONEにしない。以下はすべて担当未定。

## 一覧

| ID | 優先度 | 状態 | 残件 | 主な依存 |
|---|---|---|---|---|
| [P4-001](phase4_tasks/P4-001_measurement.md) | P0 | OPEN | 条件一致の混合負荷校正とruntime計測 | Phase 5基盤 |
| [P4-002](phase4_tasks/P4-002_remaining_eta.md) | P0 | OPEN | 残DAGからの自動ETA生成 | P4-001 |
| [P4-003](phase4_tasks/P4-003_adaptive_validation.md) | P0 | OPEN | 性能低下から改善確認までの適応シナリオ | P4-001、002、004、009 |
| [P4-004](phase4_tasks/P4-004_replanning.md) | P0 | OPEN | 費用に基づく再計画候補の探索と運用制御 | P4-001、002、009。tile採用時006 |
| [P4-005](phase4_tasks/P4-005_performance_evaluation.md) | P0 | OPEN | 完了条件に対応する性能比較・trace report | P4-001〜004、006〜009 |
| [P4-006](phase4_tasks/P4-006_tile_transpose.md) | P1 | OPEN | 作業予算内のaligned tile転置 | P4-009 |
| [P4-007](phase4_tasks/P4-007_cache_policy.md) | P1 | OPEN | Frame・Binding共有を含むfuture-aware cache費用 | P4-001、002、009 |
| [P4-008](phase4_tasks/P4-008_gradient_selection.md) | P1 | OPEN | 勾配方式の自動校正と安全な選択評価 | P4-001、004、009 |
| [P4-009](phase4_tasks/P4-009_disk_admission.md) | P1 | OPEN | SQLite書込み前のdisk admission | Phase 5基盤 |
| [P4-010](phase4_tasks/P4-010_native_uring.md) | P2 | BLOCKED | native io_uringのLinux実機検証 | 利用可能なliburing／kernel環境 |
| [P4-011](phase4_tasks/P4-011_offline_migration.md) | P2 | OPEN | v3共有treeのoffline node-local移行 | P4-009、対応範囲の確定 |

## 完了条件と作業内容

### P4-001：条件一致の混合負荷校正とruntime計測

現在はcallerからcost／health sampleを登録でき、I/O時間も記録する。自動mixed-load校正は未実装。

- read/write/fsync、MPI、compute、queue/progress、Frame/packingを実際のtask completionから有界に収集する。
- block／payload形状、backend、配置、同時実行条件を識別し、条件不一致のsampleをbaselineと比較しない。
- 単独と混合負荷をwarmup・反復付きで測り、未測定overlapを加速として見込まない。
- 完了証跡：再現script、条件付きprofile、測定／監視overhead、中央値・tail・変動を含むreport。

### P4-002：残DAGからの自動ETA生成

現在の `estimate_eta(tasks)` は明示した有限DAGを扱う。runtimeからの自動抽出は未実装。

- 未実行gate、layout transition、I/O、MPI、metadata/commitを残DAGに反映する。抽出・評価用メモリも有界にする。
- baseline／observed／adapted ETAを、profileとplanの版・切替費用・不確実性とともに更新する。
- 未知baselineでは自動切替を正当化せず、remaining workの減少と実測完了時刻を照合する。
- 完了証跡：依存／resource競合を持つoracle、実runtimeでのETA誤差report。

### P4-003：性能低下から改善確認までの適応シナリオ

Guardとpolicy変更API、prefetch credit削減は実装済み。設計が要求する一貫した改善実証が残る。

- 固定計画と同じ時刻・量の劣化をアプリ内に注入し、SSD／MPI／computeを個別に検証する。他jobに影響する負荷は使わない。
- 持続低下→検出→LIVE_TUNE→必要なFRONTIER_REPLAN→VERIFYINGをtraceで追跡する。
- positive caseで監視・探索・移行込みの実利益を確認する。短い残回路、利益下界なし、未知baseline、瞬間低下、復旧・再悪化では無益な切替を抑える。
- observe／recommend／auto、手動操作、dry-run、cooldownと失敗時のdurable cutを検査する。
- 完了証跡：決定的な注入試験とreal MPI試験、検出遅延・誤検出・切替回数・予測／実利益のreport。

### P4-004：費用に基づく再計画候補の探索と運用制御

現在は明示したpolicy候補とevidenceを検査・適用できる。配置変更の自動探索は未実装。

- 合法な取得経路・layout／swap・queue設定から有限候補を生成し、Frame整合、COW、容量、通信、移行・再校正費用を含めて比較する。
- 利益下界・残回路長・測定条件を満たす候補だけを選び、探索時間・候補数・再計画頻度に上限を設ける。
- safe frontierで全rank合意・drain・commitし、失敗時はauthoritative cutに従う。
- 完了証跡：候補採用／拒否理由、commit前後fault、replan後の状態・期待値・勾配一致。

### P4-005：完了条件に対応する性能比較・trace report

既存Mac比較は小回路の同期S0／非同期S0／非同期S2で、主な改善はlocal fusion。クラスタ測定は小規模correctness用であり、完全な性能比較ではない。

- 同じsource／initial／Binding、n/b/P、managed budget、backend、精度でS0/S1/S2、sequential/fused、同期/非同期、fixed/observe/LIVE_TUNE/replanを比較する。
- rank budgetを超える状態のMPI＋local disk、batch、三方式の勾配を含める。warmup・反復・median/tail・variationを保存する。
- MPI send payload削減、swap追加read/write、Frame整合、COW/pin peak、checkpoint、監視／探索／移行を分解し、end-to-endの正味差を説明する。
- 改善しない条件も保存する。same-hostとcross-hostを区別する。
- 完了証跡：再現script、raw JSON、数値一致と資源上限を確認した比較report。

### P4-006：作業予算内のaligned tile転置

現在は最大2組のordered physical swapをfull blockで処理する。

- full output blockが予算に収まらない場合にaligned destination tileへ転置する経路を実装する。
- overlapping pairの順序、bit階層、O_DIRECT alignment、Frame解消、COW容量、各output範囲の一意な書込みを保証する。
- 完了証跡：単独swap列との数値一致、adjoint/inverse、予算・容量不足時の拒否、単独／融合passの比較。

### P4-007：Frame・Binding共有を含むfuture-aware cache費用

現在は最大64個のfuture-version hintを使い、hintなしではLRUへ戻る。

- bounded lookaheadで取得／再取得、Frame materialization、Binding間の合法なprefix共有を費用評価する。
- stale hint、未知cost、reader/writer競合では安全なfallbackを維持する。logical amplitudeとBindingを混同しない。
- 完了証跡：LRUとの同条件比較、quota公平性、余分な先読み／I/Oとmanaged peakのtrace。

### P4-008：勾配方式の自動校正と安全な選択評価

三方式の数値一致と明示costによる選択は検証済み。実測costの自動校正と運用評価が残る。

- forward/reverse取得、checkpoint/restore、再計算、shift jobのI/O・通信・容量を同条件で校正する。
- shared Expr、controlled generator、actual layout/historyを保持し、合法な候補だけを比較する。
- 適応／replan後にも勾配が一致すること、未知costで保守的選択になることを検査する。
- 完了証跡：固定methodとautoの費用・精度・peak比較。数値だけで自動選択の性能を合格扱いにしない。

### P4-009：SQLite書込み前のdisk admission

現在はcatalog page growthを次のadmissionで実使用量へ反映する。SQL statementごとの事前予約はない。

- catalog／journalの成長をtransaction前に予約し、並行rankのdataset／host／device総予算に含める。
- commit/rollback後の予約精算と、drain用余裕の保護、容量不足時の一貫した失敗を検証する。
- 完了証跡：quota境界・並行書込み・rollback・異常終了後のreservationを含むfault試験。

### P4-010：native io_uringのLinux実機検証

adapterとMacのfake CQE試験は実装済み。検証サイトにはliburing header/libraryが無いためBLOCKED。新規導入は今回の許可範囲に含めなかった。

- 利用可能な環境でworkerとの数値一致、alignment、short I/O、CQE error、cancelと元requestの終端、fsync、buffer lifetimeを検証する。
- capability拒否とnative成功を区別して記録する。mockをnative実測に数えない。
- optional項目のため、環境で利用不可と明記した場合はworker I/OによるLinux必須検証で完了判定できる。

### P4-011：v3共有treeのoffline node-local移行

現在のupgradeは同一host／既存local owner volumeのみ。別volumeへのコピーは対応しない。

- 対応範囲を設計書と照合し、旧owner shard／catalog／journal／asset／prefix参照のoffline移行手順またはadapterを整備する。
- checksum、容量、配置、移行先でのcollective resume、失敗時の再実行を検証する。計算時の共有tree依存を持ち込まない。
- 完了証跡：旧形式からの数値一致とv4 commit。範囲を制限する場合は設計と完了条件に明示して解決する。

## Phase 4の完了判定

P4-001〜009の完了証跡を揃え、P4-011の対応範囲を解決する。P4-010はoptionalとして実機成功または環境制約を明記する。既存のMac／実2ノードcorrectnessを維持し、設計書§15のpositive／negative適応シナリオと正味性能比較が満たされた時点で完了とする。今後のクラスタ利用は既存の許可・資源上限を確認し、範囲を拡大する場合は先に許可を得る。

node/rank loss後の継続、shard replication、rank数変更は設計上の対象外。大規模量子化学の資源推定と製品CLI整備はPhase 5で扱う。このbacklogだけで対象範囲を拡大しない。
