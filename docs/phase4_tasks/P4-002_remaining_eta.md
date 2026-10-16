# P4-002：runtime残workからの自動ETA

状態：IN_PROGRESS（2026-10-14）。依存：P4-001。共通契約は[全体設計](../phase4_remaining_design.md)。実行中に未完了gate、actual layout、request、metadata/commitを集計し、baseline/observed/adapted ETAを自動更新する。

window／counted suffix／in-flight capture、private STAGED delta、strict-profile／diagnostic view、epoch／local completion更新を先行実装した。[実装範囲と検証](../phase4_remaining_implementation.md#残workとetap4-002)。現endpointはgates-only、suffix executor／private residualはunknown。細粒度stageとpayload coverage、held-out誤差／overheadの受入は残る。

[残りepochの正確な実行形状と実測費用によるETA](../phase4_exact_future_eta_implementation.md)（2026-10-15）で、S0／S2の残りの実行形状を正確に数えてobserved viewで価格付けし、private epochの実行中の呼出しをresidualで扱い、held-out評価を行った。答えたcheckのmedianは目標内である。続いて、end-to-end endpoint（期待値／gradient／checkpoint）と、multi-rankのS1判断の再現を追加した。短いrunの未回答理由はruntime ETAへ反映するようにしたが、回答率の改善は未実証である。checkpoint payload v2は512件単位のchunk再利用を行い、更新conditionにpayload sizeとincrementを保持する。同サイズの小さなend-to-end checkpointは14 sampleからinterval回答したが、可変size checkpointのgrowthはsizeごとのheld-out評価が残る。

## 現状・変更箇所

[runtime_cost.py](../../src/svsim/runtime_cost.py)はcallerの有限CostTask DAGを維持する。[execution_tasks.py](../../src/svsim/execution_tasks.py)のEpochLedgerはwindow coverage用。[async_scheduler.py](../../src/svsim/async_scheduler.py)から新規`remaining_work.py`／`runtime_eta.py`へ接続した。実I/O/MPI/computeの細粒度DAGはまだ生成しない。

## 入出力

`RemainingWork`にcontext hash、published cut、windowのsource/actual-step対応、in-flight状態、counted tail、layout/Frame/Binding/barrier、残payload/storage/control、unknownを保持する。`ETASnapshot`はsnapshot IDと採用profile set、3種類のcost view、lower/expected/conditional upper、partial component、coverage、model/quality、抽出費用を返す。

現行explicit DAG APIは維持する。新modelはcounted groupを扱うv2 adapterとし、従来RuntimeCostModelの数値意味を黙って変更しない。

## 抽出・集計

1. published frontierでn/b/P、root、layout/history、Binding/witness、plan/policy版をfreezeする。private epoch中は最後のsnapshotにin-flight deltaだけを追加し、公開済みfrontierを進めない。
2. 次window≤32の実executor経路からread、Frame resolve、packing、MPI、kernel、write、metadata、commitの依存を生成する。fused stepは元source集合を持つ。
3. suffixはsource scanをchunkで進め、operation/bit階層/mask/Frame/barrierのcounted group≤256へ集計する。全Block、全peer、巨大repeatを列挙しない。初回scanもCPU/metadata budget内でpauseできる。
4. future S1/S2/layout/Frameが一意に決まらなければbranch/boundsまたはunknownとする。現在のlayoutを末尾まで固定した予測はそのassumptionを持つ別view。semantic countだけを正確なtask countへ置換しない。
5. 実completionから残countを減らす。STAGEDでも未commitなら必要なvalidate/publish/flushは残す。retry/failed/dropped speculation、checkpoint/recomputeを通常進捗に数えない。

Block ownershipはrange/countを使い、sourceと実行ordinalを区別する。end-to-end対象にobservable/gradient/cleanupがある場合は明示endpointを付け、その費用を除外したgate-only ETAと区別する。

## In-flightとcost

requestの完了済みchunkは除外できる。未完了時間は単に`max(0, mean-elapsed)`へしない。elapsedを超えた条件付きresidualの校正があればそのenvelope、無ければunknownとする。未完了requestがあるのにremaining=0にはしない。

各snapshotに対してbaseline cost、同conditionのobserved cost、候補のadapted costを評価する。mixed groupは構成費用を一回置換し、CPU/main-thread/device/NIC等の共有resource制約を保持する。secondsとして測ったcostを帯域で再除算しない。未検証並列性はserial expected/upper、lowerは根拠のあるcritical/resource boundだけ。

総ETAは必要costが揃った場合のみ数値。unknown critical stageがあればnull＋reason。件数だけでなくpayload/kernel work/critical chainのcoverageを返す。model intervalは条件付き予測で、未来の厳密完了時刻やend-to-end95%保証とはしない。

## 更新・制御

`RemainingWorkExtractor.capture(sim)`、`RuntimeETA.evaluate(snapshot, cost_view)`、`RuntimeControl.update_eta()`を追加予定。更新はepoch境界＋一定時間のlocal progress通知を初期値とし、collective summaryは合意済み順序のみ。profile/policy変更、checkpoint、gradient method変更でsnapshotを無効化・再構築する。

suffix summaryが古ければstaleと表示して自動gainへ使わない。観測が新baselineの状態へ変わった場合は同snapshotで再評価する。baseline viewを新observedで上書きしない。抽出と再評価の時間はmonitor/planning費用へ含める。

## 受入・完了

| case | 判定 |
|---|---|
| 小DAG oracle | explicit DAGとcounted summaryのdependency/resource/countが一致 |
| virtual/fused/S2 | actual source coverageとlayout/history、Frame/logを反映し、単純gate countと区別 |
| in-flight/retry | 重複除去、未commit費用の残存、時間超過requestで偽のremaining=0がない |
| unknown/stale/budget | 総ETA null、抽出打切り・不足・version違いを可視化、自動切替しない |
| 大きいcount | 全Block/occurrence/peer生成禁止、metadata≤64 MiB、summary上限検査 |
| real runtime | held-out回路のsnapshot時刻から実完了までを照合、rank MAX/短区間もreport |

実装は002A window capture、002B suffix/in-flight、002C profile viewsとheld-out評価に分ける。初期誤差目標median30%以内、p95/worst/絶対誤差とmonitor overheadも保存する。達成しないcaseは残して原因・不足を示す。手動CostTaskの成功だけではDONEにしない。

## 受入表の証跡対応（2026-10-16）

| case | 主な実行証跡 | 現状 |
|---|---|---|
| 小DAG oracle | [`test_counted_dag_matches_explicit_dag_oracle_for_dependencies_resources_and_counts`](../../tests/test_runtime_eta.py)、`test_dag_lower_bound_and_serial_expected_upper_match_oracle` | counted nodeを明示taskへ展開した小DAGと旧explicit modelのlower／expected／upper、resource demand、dependency、countを比較する回帰を追加。 |
| virtual/fused/S2 | [`test_executor_stages.py`](../../tests/test_executor_stages.py)、[`test_s2_group_preserves_each_source_id_once`](../../tests/test_remaining_work.py)、[`test_future_shape_matches_scheduler_calls_and_epochs`](../../tests/test_remaining_work.py) | 実dispatchとstage route、fused source ID、S2未来call数を照合。virtual route、conditional Frame/log、unknown routeはstage-oracle testsで個別検証。 |
| in-flight/retry | [`test_private_staged_delta_keeps_published_cut_and_marks_unknown`](../../tests/test_remaining_work.py)、[`test_call_residual_prices_the_remainder_of_a_certified_class`](../../tests/test_remaining_work.py)、[`test_duplicate_or_excessive_inflight_requests_are_rejected`](../../tests/test_runtime_eta.py)、[`test_duplicate_unknown_hole_and_failure_never_publish`](../../tests/test_execution_tasks.py) | private/STAGED重複除去、校正済みresidualと超過unknown、重複request拒否、失敗epochの未公開化を検証。 |
| unknown/stale/budget | [`test_remaining_work.py`](../../tests/test_remaining_work.py)、[`test_partial_snapshots_never_publish_total_eta`](../../tests/test_runtime_eta.py)、[`test_unknown_suffix_and_huge_count_never_claim_unjustified_eta`](../../tests/test_remaining_work.py) | scan不足、stale、profile version違い、byte予算超過、構造unknownでtotal ETAがnullとなり、reasonとpartial coverageを保持する。 |
| 大きいcount | [`test_huge_count_with_finite_weighted_cost_remains_compact`](../../tests/test_remaining_work.py)、[`test_huge_block_count_uses_algebra_and_remote_payload_stays_unknown`](../../tests/test_executor_stages.py)、[`test_capture_bounds_window_scan_groups_and_metadata`](../../tests/test_remaining_work.py) | huge countは加重算術、block数は代数式、captureは有限group／byte budgetを使う。全occurrence／peer列挙や64 MiB到達の単一専用試験ではなく、既定のより小さい上限でoverflow・打切りを検証する。 |
| real runtime | [48-check held-out](../internal/p4_002_observed_eta/chronological-holdout-20261015/mac-48-checks.json)、[checkpoint growth/same-size smoke](../phase4_exact_future_eta_implementation.md#checkpoint-payload-v2と成長費用の再確認)、[short quick sample](../internal/p4_002_observed_eta/acceptance-20261016/mac-rx-ry-low-quick.json) | Mac 48 checksは30回答（62.5%）、回答分median absolute relative error 8.6%、p95 18.5%、worst 67.5%、interval coverage 83.3%。Mac quick 4件は2回答、両unknownはepoch-boundary LOW_HELD_OUT_COVERAGE。quickは回答率改善の比較証拠ではない。Linux／multi-rankの現行source snapshot held-outとoverhead campaignは進行中で、完了証拠を追記する。 |

Mac quick sampleの条件は1 rank、buffered、cooperative、`rx-ry-low`、fraction 0.6、S0/S2×file/RAMの各1 trial。既存48-check評価とworkload集合・試行数が違うため前後差を主張しない。回答率改善は受入条件に含めず、未立証のまま扱う。
