# Phase 4 引き継ぎ書（2026-10-15）

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
