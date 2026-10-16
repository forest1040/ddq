# Phase 4 引き継ぎ書（2026-10-15）

## 継続wave 3（2026-10-16、実装・検証継続中）

3つの専用worktreeを並列使用し、Lunaへwork coverage・overhead runner、続いてendpoint認証を委譲した。利用者の指示はP4-009とP4-002がDONEになるまで継続であり、以下の追加実装だけで完了とはしない。

- **P4-009：durability。** SQLite DELETE journalをEXTRAへ統一し、成功したxSync/xDeleteだけをdurable imageへ反映する試験用電源断模型を追加した。FULLでacknowledged commitが戻るnegativeとEXTRAの保持を含む。[実装と模型の範囲](phase4_sqlite_power_loss_implementation.md)。
- **P4-009：起動と回収。** catalog作成前のgeneration intent、空fileのfsync、inode/birthtimeのCAS束縛、exclusive rename、open後の通常ownerへの移行を追加。[catalog起動復旧](phase4_catalog_startup_recovery_implementation.md)。payload/chunkは[checkpoint metadata回収](phase4_checkpoint_metadata_recovery_implementation.md)。保存済private policyが欠けた旧ledgerは推測でbindせず拒否する。
- **P4-009：writer。** [state export](phase4_state_export_admission_implementation.md)をstreaming・予約・atomic publish・intent付き回収へ接続。export rootと追加dataset rootをvolume coordinatorにも登録し、pending減少後の実byteを課金に残す。Journalはopen/repairより先に予約、trace/journal/migration namespaceとpurpose限定fork lease JSONはowner-death/inventory検証後に回収する。[現行writer監査](phase4_writer_audit.md)。
- **P4-002：残work。** [実executorと共通のstage DAG](phase4_executor_stage_dag_implementation.md)をwindow/counted futureへ接続し、親inclusive時間は一度だけ課金する。件数、MPI amplitude送信payload、main kernel、physical storageのcoverageを区別し、不明な分母はnullと理由を返す。
- **検証。** 初回統合高速は1198 passed/37 skipped/83 deselected、6 failed。能力検査前のstartup予約、管理root外のmigration clone、走査方式変更に伴う消失journal回帰を検出し修正した。該当6件を含む選択は40 passed。後続のexport全rank精算競合、shared shard readerの互換、追加diagnosticに対する旧assertも修正し、最終固定sourceの高速は **1317 passed/38 skipped/83 deselected（280.88秒）**。Linux初回buffered305 passed、O_DIRECT304 passed＋同export競合1件を記録。修正版追加選択はbuffered83 passed。監視overhead/held-outと最終Linux追加回帰は継続中。[実行証跡](internal/phase4_wave3_validation/README.md)に初期失敗も保存する。
- **残り。** Linux/multi-rank overheadとheld-out再評価、Linuxでの最終追加回帰、実別host調停。bootstrapの変更前pool admission・kill/retry課金、source-bound migration clone回収、store-private coordinator、checkpoint work shape、GC/sidecar/appendのinventory強制は実装済み。実別hostの[PBS計画](internal/phase4_crosshost_acceptance/README.md)は準備済み、未投入。P4-002/009はIN_PROGRESSを維持する。

## 継続wave 2（同日）

3つの専用worktreeでcatalog帰属・移行判断・比較matrixを並列実装した。比較matrix担当はLunaへ委譲した。

- **P4-009。** catalog tokenをcanonical path／device／inode／birthtimeへ結び付け、DB／journalのpath・identity・観測byteを回収計画とreceiptへ保存する。旧token、birthtime取得不能、startup identity未確定は自動回収を拒否する。DB自身のsymlinkは拒否し、root外の親pathにあるsymlinkは正規化して既存API互換を維持する。[契約・制限](phase4_uncommitted_recovery_implementation.md#catalog予約のfile単位帰属追加)を参照。
- **P4-003／004。** baselineのheld-out coverage／drift／class一致を確認し、不安定・不足ならunknown、現在の証拠が無効なら古いestimateも失効する。予定遅延0の注入ticketのbookkeepingを注入時間に数える不具合も修正した。Mac MPI 2 rankのsustainedは切替・VERIFY成功、missing-baselineは切替0だった（各1組のsmoke）。[実装と残件](phase4_layout_migration_implementation.md#baselineの品質による安全な拒否2026-10-15継続wave-2)を参照。Linux O_DIRECT実campaignでの保守性回復は未認定。
- **P4-005。** 8 groupの比較matrixで不足・失敗・unsupported・予算不足を残す。2 variant、warmup 2回、全planned trial、7組のpaired trialを要求し、大状態MPI/fileとcross-host Linuxは別slotにする。[代表coverage validator](phase4_comparison_matrix.md)であり、全axisの受入判定はnot-assessed。性能比較の全実測は未完了。
- **状態。** P4-003／004／005／009とPhase 4全体はIN_PROGRESSを維持する。
- **最終検証。** 高速版1081 passed／36 skipped／83 deselected、Linux buffered／O_DIRECT各196 passed／1 deselected。実statx birthtime、別volumeでの変更前拒否、O_DIRECT実write／fsync／readbackも成功した。[初期failureと最終証跡](internal/phase4_wave2_validation/README.md)を参照。利用者の追加指示により、以後はP4-009とP4-002をDONEにするまで継続する。

## 引き継ぎ後の追記（同日）

専用worktreeを用いた3並列タスクで、外部jobの宣言超過検出、短いrunのETA認証再確認、Linux coordinator再検証を実施した。Linux担当はLunaへ委譲し、担当停止後は親エージェントが実検証を引き継いだ。以下の本文は再起動前の履歴として残す。

- **P4-009。** 専用rootのnet file増分を観測し、宣言超過と観測不能を次のadmission前に拒否する。観測済み増分をpendingから控除し、二重計上を避ける。rootなし、root外write、観測間の一時超過は保証しない。`strict_capacity_guarantee`はfalse。[実装資料](phase4_disk_coordination_implementation.md#外部jobの宣言超過の検出2026-10-15追加)に契約と残件を記録した。
- **P4-002。** 角度pooling後のサンプルを実submit順へ修正し、途中のslowdownを誤認証する不具合を直した。burn-in／held-outの条件は維持。48 checkは30回答、median誤差8.6％、coverage 83.3％。短いrunの未回答は今回、件数不足だけではなくcoverage不良／driftだった。回答率改善の比較実証とLinux／multi-rank overheadは残る。[再評価](phase4_exact_future_eta_implementation.md#短いrunの認証再確認2026-10-15追加)を参照。
- **高速テスト。** 統合版の`uv run pytest -m "not slow" -q`は1044 passed、35 skipped、83 deselected（204.33秒）。[ログ](internal/p4_coordinator_followup_validation/integrated-fast.txt)を保存した。
- **TeX。** Phase 4の進捗と制限を追記し、LuaLaTeXで38ページのPDFへビルドした。
- **Linux再検証。** 通常runtime、外部job監視、実MPI 1／2 rank、回収はbuffered 105 passed、O_DIRECT 112 passed／2 skipped（Mac専用transport試験）だった。実O_DIRECTのwrite／fsync／readbackも成功。244 mounted sourceのhashは統合版と一致した。本文にある通常runtime接続のLinux未確認は、この範囲で解消した。
- **完了状態。** P4-002／009はIN_PROGRESSを維持。layout migration、全writer fault／回収、file単位のcatalog予約帰属、電源断model、複数host調停、最終比較matrixと自動運用の残件は継続する。本文の「未確認」のLinux結果は[今回の検証記録](internal/p4_coordinator_followup_validation/README.md)を参照する。

サーバ再起動前の時点で、mainの状態と進行中の作業、次の作業をまとめる。backlogの状態の正は[phase4_backlog.md](phase4_backlog.md)にある。

## mainの状態

- **commit。** 最新は、この引き継ぎ書のcommitである。その直前が`6dc37b5`（volume coordinatorの通常runtime接続）である。未pushである。
- **高速テスト。** `uv run pytest -m "not slow" -q`は1027 passed、35 skippedだった（Mac）。
- **Phase 4のmilestone。** M1（P4-009＋001）はP4-009が未完了なので、未達である。M2（P4-002＋004）以降も、各項目がIN_PROGRESSである。
- **状態がDONEの項目。** P4-001だけである。

## 今日（2026-10-15）の作業

### P4-002：残workからのETA（IN_PROGRESS）
資料は[phase4_exact_future_eta_implementation.md](phase4_exact_future_eta_implementation.md)にある。
- **実行形状。** 残りepochの実行形状（S0／S2、multi-rankのS1判断の再現を含む）を正確に数え、runのrolling実測で残り全体を価格付けする。
- **private epoch。** 実行中の呼出しを、実測の残量で価格付けする。
- **class。** 角度だけが違うclassは、held-out認証付きで角度poolingする。
- **監視費用。** runが長くなっても監視費用が増えないようにした（collectorのcopy削除、走査の増分化）。
- **endpoint。** `set_eta_endpoint`で、期待値、gradient、durable checkpointまでETAを延ばせる。
- **held-out評価。** 答えたcheckのmedian誤差は3.7〜8.1％だった。修正後のMac再測定では、答える割合が50％、median 6.6％、coverage 94％だった。
- **残り。** 短いrunでのclass認証（答える割合）、Linuxとmulti-rankの監視overheadの判定。

### P4-004／003：layout migration（IN_PROGRESS）
資料は[設計](phase4_layout_migration_design.md)と[実装](phase4_layout_migration_implementation.md)にある。
- **方式（利用者判断：案A）。** 事前校正したexecutor class profileで、移行後のclassとswapを価格付けする。
- **成果。** MPI劣化の下で、rank bitをlocal bitへ移すpositiveを確認した（Mac・Linux、paired CI95上限<0）。
- **損益分岐ルール（利用者判断：案A）。** 払済みの超過が、一回きりの費用に達してから切り替える。
- **残り。** 検出窓と同じ長さのtransientでも、移行費用が小さいと切り替える。LinuxのVERIFY失敗（装置状態の切替）、O_DIRECTでの非保守的な予測、productionでの劣化検出経路も残る。
- **注意。** profileのfingerprintは、host名・code revision・volumeに結び付く。code変更後は再校正が必要である。Dockerでは`--hostname`を固定する。

### checkpoint（利用者判断：案1）
資料は[phase4_checkpoint_recovery_implementation.md](phase4_checkpoint_recovery_implementation.md)の末尾にある。
- **塊の保存。** v4 payloadの`gates`／`history`を、内容のhashで名付けたwrite-onceの塊（512件）に分けた（payload形式2）。
- **効果。** 40回目のcheckpointは742 msから113 msになった。残りの増加はCPUだけである。
- **互換性。** 形式1も読める。

### P4-009：disk admission（IN_PROGRESS）
資料は[fault matrix](phase4_disk_fault_matrix_implementation.md)、[未commit予約の回収](phase4_uncommitted_recovery_implementation.md)、[調停](phase4_disk_coordination_implementation.md)にある。
- **fault試験。**
  - quota境界の1 byte／page掃引
  - 実processのkill matrix（catalog、ledger、sidecar、segment write／fsync／公開）
  - 実ENOSPC（tmpfs）
  - O_DIRECT matrix
  - 2 rankのcheckpoint prepare／decision kill
  - SQLiteのfsync内部への決定的な故障点（試験用buildだけ）
- **回収とGC。**
  - 未commit予約のoffline回収（排他lockで所有者の死亡を証明、dry run、計画digest、CAS＋receipt）
  - temporary GC、dead branch GC
  - store専用ledgerの回収とpolicyの保存
- **修正した不具合。**
  - extentをsegmentのfsyncより前に公開していた（電源断時の順序）
  - 未commit snapshotのintent付きtokenが回収できなかった
  - Linuxでinodeが再利用され、lease directoryの置換を見逃した
- **調停（volume coordinator）。** 同じvolumeの独立したledger同士と外部jobの予約を、1つの上限で調停する。共有filesystemは拒否する。
- **通常runtimeへの接続（`6dc37b5`）。** `DistributedConfig.volume_coordinator`（opt-in、node-localの絶対path）で使える。
  - rank間では、使うかどうかを合意する。fork、prefix fork、parameter-shiftの子は、同じcoordinatorを継承する。
  - 再開では、coordinatorの欠落・別path・新たな束縛を、全rankでそろって拒否する。
  - offline回収（未commit予約、checkpoint）の後で、coordinatorのclaimを真の値へ再公開する。
  - 試験は16件（実mpiexecの1／2 rankを含む）で、Macで成功した。
  - **未確認。** このcommitのLinux（ent）での結果は、subagentの報告前に取り込んだため確認していない。再起動後に、`tests/test_volume_coordination_runtime.py`ほかをentのbuffered／O_DIRECTで実行して確かめる。
- **残り。**
  - 外部jobが宣言を超えて書いた場合の検出
  - 全writerのfault／回収
  - catalogの予約をfile単位で帰属させること
  - 電源断のmodel（SQLite journal、coordinator DB）
  - 複数hostの調停（現在は拒否）
  - `strict_capacity_guarantee`は立てない

## 未決・相談事項

- P4-004の移行では、transientでも切り替える場合がある。より長い持続を要求するかは未決である（現状は損益分岐ルールで、損失の上限だけを保証する）。
- checkpointで残っているO(N)のCPU（全gateの`gate_data`変換と、全体hashによる合意）を削るかどうか。

## 次の候補

1. P4-009の残り（上記）
2. P4-002：短いrunでの認証
3. P4-005：最終比較のmatrix
4. P4-006／007／008：tile、cache、勾配の自動運用

## 作業環境の注意

- **Linux検証。** `ssh ent`のDockerで行う（image `ddq-linux:dev-20261014`、`docs/linux_development.md`）。profileを使う場合は、`docker run --hostname <固定名>`を指定する。subagentは`~/ddq-p4009*`、`~/ddq-p4002`、`~/ddq-p4004`へsyncした。不要なら削除してよい。
- **cluster。** 研究室cluster（`ss lg`）は、今日は使っていない。
- **subagentのworktree。** `.claude/worktrees/agent-*`に残っている。内容はすべてmainへ取り込み済みなので、削除してよい（`git worktree remove`）。
- **sonnetの利用上限。** 一度、上限に達した（14:10 JSTに解除）。
- **再現script。**
  - `examples/p4_002_observed_eta.py`（observed ETAのheld-out評価）
  - `examples/p4_004_layout_migration.py`（executor profileの校正）
  - `examples/phase4_remaining_campaign.py --executor-profile`
  - `examples/p4_009_segment_sync_benchmark.py`（fsync順序の費用）
