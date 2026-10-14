# P4-001／002：Phase 5時間モデルによるruntime ETA view

更新日：2026-10-14。[Phase 5の時間モデル](phase5_implementation.md)（作業量×装置sync latencyの非負線形モデル）を、Phase 4 async runtimeの残作業ETAへ接続した。P4-001／002とPhase 4はIN_PROGRESSのまま変えない。

## 構成

| 部品 | 内容 |
|---|---|
| `svsim benchmark --executor async` | Phase 4の`AsyncDistributedSimulator`（既定`AsyncPolicy`）で校正runを実行する。条件classは`mode=distributed-async`と`executor_policy`（policy内容のhash）で、sync executorとは混ぜない。runtime profileの慣例に合わせ、`--threads`は`io_workers`（既定2）と一致させる |
| `TimeModelDocument.predict_remaining` | gates-only endpointのため、execute phaseだけを定数項なしで評価する。setup／compile／flush／cleanupと実行の切片は残作業に含めない。初期状態はsetupにしか効かないので、条件照合から外す |
| `runtime_time_model_eta` | 残作業snapshotをfeatureへ変換する。windowの各executor groupの物理`operations`とtailのsemantic class×countを、gate・pass・`passes:<gate>:<local|joint>`へ正確に数える。publicationは除く |
| `RuntimeControl.attach_time_model` | `update_eta`に独立した`time-model` viewを追加する。sourceは`time-model:<model_id>`。rank MAXは既存のallgatherで取る。`set_time_model_sync_latency`で、直前に測ったsync latencyを与えられる |
| `svsim benchmark --eta-model`、`svsim eta-report` | 実行をfrontier 0と50%で分ける。各点で、ETA（校正median sync版と直前probe版）と、そこから完了までの実execute時間（rank MAX）を記録・集計する。ETA captureとprobeは時間外 |

**unknownになる場合。** 次のいずれかを含むとtotalはunknownで、partial値だけを出す：未完了scan、stale、private epoch、in-flight request、class上限超過、post-SWAP／S1 layout、未bind parameter、monitor予算不足、注入中。tail側の`FUTURE_EXECUTOR_SHAPE_UNKNOWN`はgate単位の校正費用に吸収するので、阻害理由にしない。前提はview内の`assumptions`に明記した：tailはcurrent layout（S0）、gate単位の費用は校正どおり、監視とpublicationは校正したexecute phaseに含まれる。

**既存viewとの関係。** baseline／observed／adaptedのper-task CostSample view、RuntimeCostModelの意味、`remaining_seconds`（observed）、適応制御の判断には触れない。time-model viewは`eligible_for_control=False`の診断値で、自動切替の根拠にしない。fingerprintは、検証済みprofileがあればその値、なければruntime directoryのprobeで照合する。不一致は`TIME_MODEL_CONDITION_MISMATCH`のunknownとして返す。

## 検証

| 環境 | 校正 | held-out ETA check | median ｜相対誤差｜ | p95 | worst | interval coverage |
|---|---|---|---|---|---|---|
| Mac（1 rank、b=4、RAM store、async） | 52件×3回（不安定除外0） | 4 workload×5回×2点＝40 | 5.7% | 11.0% | 28.8% | 100% |
| ent Docker（Linux ext4、直前probe） | 52件×3回（不安定除外7） | 40 | 3.2% | 37.5% | 80.1% | 87.5% |
| 同上、校正median sync | 同上 | 40 | 25.3% | — | 71.0% | — |
| 同上、安定filter導入前（[v1](internal/phase5_timing_validation/eta-linux-v1/)、直前probe） | 52件×3回 | 40 | 19.0% | 64.9% | 133.5% | 47.5% |

> 2026-10-15追記：[catalog commitの集約](phase4_catalog_batch_implementation.md)後、Linuxの再評価では1／2 rank RAMと2 rank fileのmedianが6.8％／18.5％／22.5％、coverageが全て100％になった（下表は集約前の値）。

> 2026-10-15追記：研究室クラスタの2 node（009＋010、PSM2、1 rank／node、file store、O_DIRECT）で、cross-hostの校正とruntime ETAを測った。median ｜相対誤差｜ 7.8%、worst 10.3%、coverage 95.8%（[内部記録](internal/phase5_crosshost/README.md)）。benchmarkは複数hostに対応した：node数をhost名から数え、rank 0が入力を読んで全rankへ配り、fingerprintはrank 0で照合し、hostごとのleaderが自分のvolumeを走査・削除する。

### rank数とstorageのmatrix

rank bitを対象とするgateは、MPI交換を伴う`remote` route classとして別に校正する（`calibration-suite --ranks`）。multi-rankでは全rankが1つのdatasetを共有し、disk計上とcleanupはrank 0が行う。手順は[run_matrix.sh](internal/phase5_timing_validation/run_matrix.sh)、証跡は[Mac](internal/phase5_timing_validation/matrix-mac/)と[Linux](internal/phase5_timing_validation/matrix-linux/)。各条件40 checkで、Linuxは直前probeで予測した。

| 環境 | rank／storage | 不安定除外 | median | p95 | worst | coverage |
|---|---|---|---|---|---|---|
| Mac | 2／RAM | 0 | 4.5% | 19.2% | 32.3% | 97.5% |
| Mac | 4／RAM | — | 未実施 | — | — | — |
| Mac | 1／file | 0 | 1.2% | 7.9% | 10.8% | 100% |
| Mac | 2／file | 0 | 4.3% | 8.3% | 14.2% | 100% |
| Linux | 2／RAM | 13 | 31.3% | 56.7% | 185% | 100% |
| Linux | 4／RAM | 22 | 11.1% | 70.8% | 158% | 100% |
| Linux | 1／file | 6 | 5.3% | 56.2% | 66.5% | 90% |
| Linux | 2／file | 11 | 32.8% | 57.0% | 235% | 52.5% |

Mac 4 rankの校正は、同一host上の4 process×（main＋io worker 2）でMPI progressのbusy pollがCPUを奪い合った。40分経っても4 roundの2 round目で、wall上限を超えたため停止し、未実施とした。Linuxのmulti-rankでは、複数rankの同時fsyncで装置状態の切替が増えた（不安定除外が増加）。直前probeが示す状態と実行中の状態がずれ、速い状態で+30%（2 rank RAM）、遅い状態で約-21%の偏りが残る。2 rank／fileは30%目標を満たさない。1 rankのRAM／fileと4 rank RAMのmedianは目標内だが、p95は大きい。multi-rankのLinuxでは、check時点の1回のprobeでは実行中の状態を代表できない。実行中の連続sync観測とETAの逐次更新、またはcatalog commitそのものの集約が必要である。

held-outは、校正suite外の混合circuit 4種である（7〜8Q、8〜40 gate。40 gateは校正範囲外でextrapolated）。observedは、check時点のfrontierから完了までの実execute wallのrank MAXである。証跡は[Mac](internal/phase5_timing_validation/eta-mac/)と[Linux](internal/phase5_timing_validation/eta-linux/)、手順は[flow_eta.sh](internal/phase5_timing_validation/flow_eta.sh)。新規testは次を確認する：残作業featureの正確な計数と阻害理由、`predict_remaining`の切片除外と条件照合、実async runtimeでのview値・rank MAX・frontier進行後の減少、fingerprint不一致のunknown、精度集計。Mac全回帰は825 passed／14 skipped。Linuxでは関連14件がcontainer内で成功した。

Linuxの装置は、fsyncが約0.8 msの速い状態と約5 msの遅い状態を切り替える（[Phase 5検証記録](internal/phase5_timing_validation/README.md)）。runtime ETAでは、checkの直前に16回のfsync probeで状態を測り（`set_time_model_sync_latency`）、その値で評価するのが主予測である。校正median syncで評価すると誤差は25%に留まる。

最初の評価（v1）では、速い状態で15〜35%過大予測し、実測がlowerを下回るcaseが多かった（coverage 47.5%）。開始probeでは速かったのにtrial途中で遅くなった校正trialが、遅さを基底係数へ押し込んでいたためである。各trialのflush後にもprobeを取り、前後の比が2倍以内の安定trialだけで適合するよう変えた（`stability_rule`、除外数を記録）。その結果、状態別の中央値偏りは速い状態-1.0%、遅い状態-1.4%になった。coverageは遅い状態100%、速い状態78%である。残るworst（80%、extrapolatedの40 gate case）とp95は、check後の状態切替と範囲外外挿による。

## 残件

multi-node、multi-rank Linuxでの状態切替に追従する逐次ETA、Mac 4 rank、S1／S2・fusion・post-SWAP layoutのtail、in-flight残量、checkpoint／observable／gradientを含むend-to-end endpoint、monitor overheadの受入、適応候補（P4-003／004）との接続が残る。P4-002の受入（実runtimeのheld-outで30%以内）は、この条件classでの証跡に限られる。
