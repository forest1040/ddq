# Phase 4 backlog

更新日：2026-10-14。**Phase 4は未完了**。主要APIとMac／Linux・実2ノードのcorrectness検証は実装・実施済みだが、適応制御と性能評価を含む設計の完了条件には残件がある。

基準は[開発計画](../plan/distributed_quantum_circuit_simulator_development.md#phase-4非同期階層スケジューラ)と[設計書 §15](phase4_design.md#15-実装順と完了条件)。現在の挙動・制限は[実装資料](phase4_implementation.md)、既存の性能測定は[Mac比較](phase4_benchmark.md)に記載する。site固有の情報はこのbacklogに書かず、`docs/internal/`で管理する。

## 管理方法

[Phase 5設計書](phase5_design.md)で共通profile・校正・静的推定を先行整備し、P4-001／002／003／004／005／007／008へ接続する。容量拡大前にP4-009、必要な転置経路にP4-006を解消する。設計書の作成だけでは本backlogの状態を変更しない。

[Phase 5基盤実装](phase5_implementation.md)で不変profile、完全一致cell、Macの有限mixed計測、容量referenceとstate-free解析を整備した。P4-001のruntime計測は基盤に着手し、Frame／metadataの共通化とP4-002以降への接続は残る。完了状態は変更していない。

[残件の全体設計](phase4_remaining_design.md)に共通契約・実装順・milestoneを定義し、各IDの個別設計を作成した。下表のIDから変更箇所、API、アルゴリズム、失敗処理、受入条件を参照できる。設計書の作成だけでは状態を変更せず、実装の進捗を下表で管理する。P4-009とP4-001を先行し、ETA→候補探索→tile/cache/gradient→適応/最終比較の順に統合する。

状態は `OPEN`、`IN_PROGRESS`、`BLOCKED`、`DONE`。優先度P0は完了判定の中心となる課題、P1は設計への適合・評価の補完、P2はoptional実機検証または移行対応。優先度だけを理由に完了条件から除外しない。

P4-001／009の関連commit件名：`Implement Phase 4 disk admission and runtime measurement foundations`。Mac回帰469件成功、24構成・336 trialの検証証跡を同変更に含める。

着手時に担当・関連commitを追記し、完了時はテスト／reportへのリンクと、測定条件・結果を記録する。scopeを縮小する場合は設計書と開発計画に反映し、項目を黙ってDONEにしない。P4-001〜010の先行実装はCodexとLunaが担当し、P4-011は担当未定。

2026-10-14に[容量予約・runtime計測の基盤](phase4_foundation_implementation.md)を実装した。catalogとledgerの同SQLite VFS、writerへの予約接続、validated completion、厳密profile adapter、有限actual-runtime campaignを追加した。Macの検証と残る受入条件は[検証資料](internal/phase4_foundation_validation/README.md)へ記録する。両項目はIN_PROGRESSで、全writerのfault／回収証跡、profile registryの全stage・自動接続、監視overhead、Linux／cross-host校正は残る。M1とPhase 4の完了は宣言しない。

## 一覧

2026-10-14に[writer予約・永続化境界](phase4_writer_durability_implementation.md)を補強した。checkpoint JSON、traceのmkdir／open、asset／snapshot sidecarを変更前にadmitし、atomic writerのdirectory fsync／cleanupまで予約を保持する。flush後の物理化、cleanup不明時のtoken保持、trace巻戻し失敗後の追記拒否を追加した。SQLite build／dispatch capabilityを保存し、識別不一致のsoft fallbackを拒否する。sidecarの実process crashと関連Mac／Linux回帰を検証した。全writerのcut結合／fault、temporary branch GC、host調停は残り、P4-009はIN_PROGRESSを維持する。予約・物理化・精算を含む監視／metadata費用はP4-001／005へ接続する。

2026-10-15に[catalog commitのgate単位集約](phase4_catalog_batch_implementation.md)を実装した（[設計](phase4_catalog_batch_design.md)は利用者確認済み）。snapshotからの再開という契約は変えず、gate stageごとに1つのdurable transactionへまとめた。rollback時のcache・RAM resident・allocator・extentアドレスの整合を保ち、実processのbatch内killからのresumeを試験した。ent Dockerではcatalog commitが858回から69回、executeが2.74秒から0.39秒になった。runtime ETAはLinuxの1／2 rank RAMと2 rank fileで、median 6.8％／18.5％／22.5％、coverageは全て100％になった。Mac・Linux全回帰はともに828 passed。hot journal規模の専用試験などは残り、P4-009はIN_PROGRESSを維持する。

2026-10-14に[Phase 5時間モデルのruntime ETA view](phase4_time_model_eta_implementation.md)を追加した。async executorでの校正、残作業snapshotからの正確なgate／pass feature、gates-only execute予測、独立した`time-model` view、直前sync probeでの評価を実装した。実runtimeのheld-out（frontier 0／50%から完了まで）で、median ｜相対誤差｜はMac 5.7%、Linux 3.2%（coverage 100%／87.5%）。続いてrank bitの`remote` classを加え、2／4 rankとfile storeへ広げた。Macは2 rank RAM 4.5%、1／2 rank file 1.2%／4.3%。Linuxは2 rank RAM 31%、4 rank RAM 11%、1 rank file 5.3%、2 rank file 33%（coverage 52.5%）で、multi-rankのLinuxは装置状態の切替に追従できず未達がある。multi-node、S1／S2、in-flight残量、end-to-end endpointは残る。P4-001／002はIN_PROGRESSを維持する。

2026-10-14に[ledger commitの集約](phase4_ledger_lease_implementation.md)を追加した。trace／journal追記は64 KiBのchunk token、catalogは累積成長を追跡するtokenの再利用に変え、全byteを書込み前にlive tokenで覆う契約を維持した。ent Dockerの計測では、ledger commitが2,452回から16回、executeが約3倍速くなった。Mac全回帰821件とLinux回帰812件は成功した。書込み途中のkill／電源断faultとnative matrixでの再確認は残り、P4-009はIN_PROGRESSを維持する。

2026-10-14に[Linux Docker開発環境](linux_development.md)を追加した。利用者が許可したLinuxサーバ内だけにuv・OpenMPI・liburingを用意し、既定seccompのcapability拒否と限定profileのnative成功を別記録した。実kernelのO_DIRECT・CQE・cancelと、1／2／4 rankの明示nativeテストを追加する。P4-010は環境不足のBLOCKEDを解消してIN_PROGRESSへ変更し、全fault／durability／worker対照性能matrixは残す。

2026-10-14に[同volume共有disk admissionとforeground RAM](phase4_disk_coordination_implementation.md)を追加した。独立datasetの明示共通ledger、forkのbootstrap前継承、永続policy照合、登録rootの最小祖先集合と合算容量検査を実装した。cacheの任意metadataは最低作業領域を圧迫する際に精算する。Macの独立dataset／fork／勾配とRAMを超えるstateを含む関連回帰107件を確認した。P4-009／007はIN_PROGRESSを維持し、temporary branchの安全なGC、全writerのfault／cut結合、Linuxを含む全matrix／別hostは残す。

2026-10-14に[回収・cache課金・勾配資源検証・監視費用](phase4_safety_monitoring_implementation.md)を補強した。明示disk回収の二重適用拒否、共有RAMへのhint／trace課金、held-out resource upperの再検証、completion collectorのon/off対照とresolved backend取得を追加した。P4-001／007／008／009はIN_PROGRESSを維持する。collector以外を含む全monitor費用、全future計画の自動接続、認証済みauto性能、全writerのfault／host調停は残る。

2026-10-14に[checkpoint reservation recovery設計](phase4_checkpoint_recovery_design.md)と[実装資料](phase4_checkpoint_recovery_implementation.md)を追加した。全writer終了後の明示offline/node-local operationとして、最新committed v4 sidecar intentをmetadata/cut/inventoryと照合する。dry runを含むexisting-only recovery openではschema migration/create、root登録、shared policy変更を行わず、reservation状態の精算とrecovery receipt保存はapply時のみ行う。partial temporaryは保持し、decision-copy欠損時は観測byteを精算して通常resumeがcopyを再作成する。適用中断ではtokenごとの部分成功があり得る。振幅/全physical closure、unlinked/uncommitted token、GC、全writer、外部job/別ledger、cross-host、電源断は対象外。検証記録は[内部validation資料](internal/phase4_checkpoint_recovery_validation/README.md)に置く。これはP4-009の限定的な進捗であり、P4-009とPhase 4をIN_PROGRESSのまま保つ。

同日、[recovery-only metadata pool設計](phase4_recovery_pool_design.md)と[実装資料](phase4_recovery_pool_implementation.md)を追加した。existing-only openでschema/policy/root/volume/native VFSを照合し、migration/create/root登録/policy変更を禁止する。logical quota/pending/drainを回収阻害から除外し、min_freeと未割当pool・page/journal capは維持する。pool/full・physical shortage・実I/O failureは拒否し、actual hardware ENOSPCや電源断耐久性、cross-host、全writerは未受入である。検証記録は[内部validation資料](internal/phase4_recovery_pool_validation/README.md)を参照する。P4-009とPhase 4は未完了のまま。

2026-10-14に[P4-003／005／008の先行実装](phase4_validation_implementation.md)を追加した。注入とsafe-refusal、paired report、明示勾配校正とstrict adapterをMacで検証し、3項目をIN_PROGRESSとする。関連commit件名：`Implement Phase 4 adaptive evaluation, paired reports and gradient calibration`。実costによるpositive採用／VERIFY、全比較matrix、包括admissionと認証済みauto性能は残り、Phase 4の完了を宣言しない。

2026-10-14に[P4-002／004／006／007の先行実装](phase4_remaining_implementation.md)を追加した。Mac内の実MPIでsnapshotと候補合意を確認し、P=1 tile route、cache identity／unknown fallbackを検証した。4項目をIN_PROGRESSへ変更する。関連commit件名：`Implement Phase 4 remaining-work ETA, proposals, tile and cache foundations`。細粒度cost、自動safe切替、P>1 tile、cache共有／公平性／RAM課金と性能評価は残す。M2／M3の完了条件は維持する。

| ID | 優先度 | 状態 | 残件 | 主な依存 |
|---|---|---|---|---|
| [P4-001](phase4_tasks/P4-001_measurement.md) | P0 | IN_PROGRESS | 条件一致の混合負荷校正とruntime計測 | Phase 5基盤 |
| [P4-002](phase4_tasks/P4-002_remaining_eta.md) | P0 | IN_PROGRESS | 残DAGからの自動ETA生成 | P4-001 |
| [P4-003](phase4_tasks/P4-003_adaptive_validation.md) | P0 | IN_PROGRESS | 性能低下から改善確認までの適応シナリオ | P4-001、002、004、009 |
| [P4-004](phase4_tasks/P4-004_replanning.md) | P0 | IN_PROGRESS | 費用に基づく再計画候補の探索と運用制御 | P4-001、002、009。tile採用時006 |
| [P4-005](phase4_tasks/P4-005_performance_evaluation.md) | P0 | IN_PROGRESS | 完了条件に対応する性能比較・trace report | P4-001〜004、006〜009 |
| [P4-006](phase4_tasks/P4-006_tile_transpose.md) | P1 | IN_PROGRESS | 作業予算内のaligned tile転置 | P4-009 |
| [P4-007](phase4_tasks/P4-007_cache_policy.md) | P1 | IN_PROGRESS | Frame・Binding共有を含むfuture-aware cache費用 | P4-001、002、009 |
| [P4-008](phase4_tasks/P4-008_gradient_selection.md) | P1 | IN_PROGRESS | 勾配方式の自動校正と安全な選択評価 | P4-001、004、009 |
| [P4-009](phase4_tasks/P4-009_disk_admission.md) | P1 | IN_PROGRESS | SQLite書込み前のdisk admission | Phase 5基盤 |
| [P4-010](phase4_tasks/P4-010_native_uring.md) | P2 | IN_PROGRESS | native io_uringのLinux実機検証 | Linux Docker環境、全fault／durability matrix |
| [P4-011](phase4_tasks/P4-011_offline_migration.md) | P2 | OPEN | v3共有treeのoffline node-local移行 | P4-009、対応範囲の確定 |

## 完了条件と作業内容

### P4-001：条件一致の混合負荷校正とruntime計測

completion collector、厳密profile adapter、実executorの有限mixed campaign、同じpoolを予約したcollector on/off対照とtrace由来のresolved backend取得を実装した。全stageのcondition registry、自動cost接続、全monitorのoverheadとcross-host校正は残る。

- read/write/fsync、MPI、compute、queue/progress、Frame/packingを実際のtask completionから有界に収集する。
- block／payload形状、backend、配置、同時実行条件を識別し、条件不一致のsampleをbaselineと比較しない。
- 単独と混合負荷をwarmup・反復付きで測り、未測定overlapを加速として見込まない。
- 完了証跡：再現script、条件付きprofile、測定／監視overhead、中央値・tail・変動を含むreport。

### P4-002：残DAGからの自動ETA生成

window／counted suffix／in-flightの自動captureと3 viewの診断ETAを実装した。細粒度stage、profile充足、payload coverage、real held-out誤差とoverheadは残る。

- 未実行gate、layout transition、I/O、MPI、metadata/commitを残DAGに反映する。抽出・評価用メモリも有界にする。
- baseline／observed／adapted ETAを、profileとplanの版・切替費用・不確実性とともに更新する。
- 未知baselineでは自動切替を正当化せず、remaining workの減少と実測完了時刻を照合する。
- 完了証跡：依存／resource競合を持つoracle、実runtimeでのETA誤差report。

### P4-003：性能低下から改善確認までの適応シナリオ

bounded注入driverとactual compute hook、専用Guardとsafe-refusalを実装した。injected資料をproduction校正から除外する。actual storage／MPI、real cost採用／VERIFY、正味改善と別hostは残る。

Guardとpolicy変更APIは実装済み。DEGRADEDだけを根拠にしたautoのprefetch削減は廃止し、未校正なら候補探索までに留める。設計が要求する一貫した改善実証が残る。

- 固定計画と同じ時刻・量の劣化をアプリ内に注入し、SSD／MPI／computeを個別に検証する。他jobに影響する負荷は使わない。
- 持続低下→検出→LIVE_TUNE→必要なFRONTIER_REPLAN→VERIFYINGをtraceで追跡する。
- positive caseで監視・探索・移行込みの実利益を確認する。短い残回路、利益下界なし、未知baseline、瞬間低下、復旧・再悪化では無益な切替を抑える。
- observe／recommend／auto、手動操作、dry-run、cooldownと失敗時のdurable cutを検査する。
- 完了証跡：決定的な注入試験とreal MPI試験、検出遅延・誤検出・切替回数・予測／実利益のreport。

### P4-004：費用に基づく再計画候補の探索と運用制御

identityと有限policy候補、全rank capability集約、全費用を一回加算するoracleを実装した。生成候補は未qualifiedで採用不可。実costへの自動接続、配置候補、durable safe切替、VERIFYは残る。

- 合法な取得経路・layout／swap・queue設定から有限候補を生成し、Frame整合、COW、容量、通信、移行・再校正費用を含めて比較する。
- 利益下界・残回路長・測定条件を満たす候補だけを選び、探索時間・候補数・再計画頻度に上限を設ける。
- safe frontierで全rank合意・drain・commitし、失敗時はauthoritative cutに従う。
- 完了証跡：候補採用／拒否理由、commit前後fault、replan後の状態・期待値・勾配一致。

### P4-005：完了条件に対応する性能比較・trace report

manifest／raw測定hash／rank MAX／resource検証、全metricのpaired統計と有限Mac runnerを実装した。large state／tile／cache／control／lifecycle／別hostの最終matrixは残る。

既存Mac比較は小回路の同期S0／非同期S0／非同期S2で、主な改善はlocal fusion。クラスタ測定は小規模correctness用であり、完全な性能比較ではない。

- 同じsource／initial／Binding、n/b/P、managed budget、backend、精度でS0/S1/S2、sequential/fused、同期/非同期、fixed/observe/LIVE_TUNE/replanを比較する。
- rank budgetを超える状態のMPI＋local disk、batch、三方式の勾配を含める。warmup・反復・median/tail・variationを保存する。
- MPI send payload削減、swap追加read/write、Frame整合、COW/pin peak、checkpoint、監視／探索／移行を分解し、end-to-endの正味差を説明する。
- 改善しない条件も保存する。same-hostとcross-hostを区別する。
- 完了証跡：再現script、raw JSON、数値一致と資源上限を確認した比較report。

### P4-006：作業予算内のaligned tile転置

P=1／local FileBlockStore／identity Frame＋empty logのaligned tile routeを実装した。source page＋destination tileだけで実行し、full-block不可能なcaseも検証した。P>1、一般Frame、durable切替fault、勾配と性能比較は残る。

- full output blockが予算に収まらない場合にaligned destination tileへ転置する経路を実装する。
- overlapping pairの順序、bit階層、O_DIRECT alignment、Frame解消、COW容量、各output範囲の一意な書込みを保証する。
- 完了証跡：単独swap列との数値一致、adjoint/inverse、予算・容量不足時の拒否、単独／融合passの比較。

### P4-007：Frame・Binding共有を含むfuture-aware cache費用

raw／resolved identityとbounded cost hintを追加した。validated・同unit・freshnessが揃う場合のみ費用を使い、それ以外はLRUへ戻る。hint／trace／counterは共有RAMへ課金し、stale／close時に精算する。既知のfuture sequenceを有界summaryへ変換できるが、全future計画の自動抽出／actual cost、Binding共有／公平性とpaired実測は残る。

- bounded lookaheadで取得／再取得、Frame materialization、Binding間の合法なprefix共有を費用評価する。
- stale hint、未知cost、reader/writer競合では安全なfallbackを維持する。logical amplitudeとBindingを混同しない。
- 完了証跡：LRUとの同条件比較、quota公平性、余分な先読み／I/Oとmanaged peakのtrace。

### P4-008：勾配方式の自動校正と安全な選択評価

明示actual calibration、strict envelope adapter、runtime attachmentと未認証fallbackを実装した。明示resource upperが全held-out測定peakを覆うかをcalibrator／adapter双方で再検証する。認証済みauto性能、包括admission、generator／Frame／replan／別hostは残る。

三方式の数値一致と明示costによる選択は検証済み。実測costの自動校正と運用評価が残る。

- forward/reverse取得、checkpoint/restore、再計算、shift jobのI/O・通信・容量を同条件で校正する。
- shared Expr、controlled generator、actual layout/historyを保持し、合法な候補だけを比較する。
- 適応／replan後にも勾配が一致すること、未知costで保守的選択になることを検査する。
- 完了証跡：固定methodとautoの費用・精度・peak比較。数値だけで自動選択の性能を合格扱いにしない。

### P4-009：SQLite書込み前のdisk admission

catalog open／transaction前の予約と同SQLite VFS、ledger pool、extent・journal・trace・assetへの接続を実装した。実子processの異常終了後に予約とpartial fileを保持し、明示回収の二重適用と既記録byteの減算を拒否する。checkpoint sidecarについては最新committed v4 cutに結び付けた限定的なoffline回収を追加した。全writerのfault／recovery、unlinked/uncommitted reservation、temporary branch GC、異なるledger／外部jobを含むhost調停、Linuxを含む全matrixは残る。Linuxの一部機能検証は済んでいる。

- catalog／journalの成長をtransaction前に予約し、並行rankのdataset／host／device総予算に含める。
- commit/rollback後の予約精算と、drain用余裕の保護、容量不足時の一貫した失敗を検証する。
- 完了証跡：quota境界・並行書込み・rollback・異常終了後のreservationを含むfault試験。

### P4-010：native io_uringのLinux実機検証

adapterとMacのfake CQE試験を維持し、許可された別LinuxサーバにDocker開発環境と明示native検証を追加した。[記録](internal/ent_linux_validation/README.md)にcapability拒否／成功、toolchain、source hashと実テスト結果を保存する。従来の研究室クラスタへは新規導入していない。初期native correctnessを全受入matrixの完了とはしない。

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
