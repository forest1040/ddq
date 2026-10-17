# P4-002：元の受入条件と現在の証拠（2026-10-17）

状態：**DONE（2026-10-17）**。production sourceは`89e8ad2`。この資料は[個別設計の6受入条件](phase4_tasks/P4-002_remaining_eta.md#受入完了)を実装・機能試験・実測へ対応付けるもので、元条件を変更せず完了判定の根拠を示す。**初期誤差目標はmedian 30%以内**のまま。p95／worst／絶対誤差／monitor overhead、unknown／error／未達caseを残す。新たなhardware条件、区間coverage目標や回答率目標は追加しない。

## 6受入条件の対応

| 元のcaseと判定 | source `89e8ad2`の実装 | 意味ある機能試験と現在の証拠 | 最終判定 |
|---|---|---|---|
| **小DAG oracle**：explicit DAGとcounted summaryのdependency/resource/countが一致 | [`runtime_eta.py`](../src/svsim/runtime_eta.py)のcounted nodeを依存・resourceごとに集計。lowerはcritical/resource bound、expected/upperは根拠のない並列性を足さずserial。seconds costを帯域で再除算しない。 | [`test_runtime_eta.py`](../tests/test_runtime_eta.py)の`test_counted_dag_matches_explicit_dag_oracle_for_dependencies_resources_and_counts`と`test_dag_lower_bound_and_serial_expected_upper_match_oracle`が、小さなcounted nodeを明示taskへ展開した旧modelのlower/expected/upper、resource demand、count/dependencyと比較。手動DAGだけでなく下の実dispatch oracleへ接続。 | **受入確認済み。** real runtime測定も下記へ別に保存。 |
| **virtual/fused/S2**：actual source coverageとlayout/history、Frame/logを反映し、単純gate countと区別 | [`executor_stages.py`](../src/svsim/executor_stages.py)と[`remaining_work.py`](../src/svsim/remaining_work.py)で実executor routeを共用し、read/resolve/packing/MPI/kernel/write/metadata/commitを有限stage DAGへ変換。S2のfused source集合、S1の実判断とlayout遷移を保持。inclusive親費用を一度だけ課金する。未証明cache/Frame/log/placement、remote payload/physical storage byteはunknown。 | [`test_executor_stages.py`](../tests/test_executor_stages.py)のactual kernel dispatch、virtual zero-fetch、fused one-pass、inclusive parent once、MCX packing、S1 conditional/remote route、storage coverage分離の各oracle。[`test_remaining_work.py`](../tests/test_remaining_work.py)のS2 source ID一回ずつ／future scheduler calls・epoch一致と、[`test_remaining_work_mpi.py`](../tests/test_remaining_work_mpi.py)の実MPI順序・状態oracle。 | **受入確認済み。** count、MPI payload、kernel、physical storage、critical-chain coverageを混同せず保存する。 |
| **in-flight/retry**：重複除去、未commit費用の残存、時間超過requestで偽のremaining=0がない | published frontierを維持し、private STAGED deltaを重ねる。completed chunkだけを除外し、validate/publish/flushが未完なら残す。条件付き実測residualが認証できる場合だけ残量を価格付けし、`mean-elapsed`のゼロ切捨てで完了を作らない。 | `test_private_staged_delta_keeps_published_cut_and_marks_unknown`、`test_call_residual_prices_the_remainder_of_a_certified_class`、[`test_runtime_eta.py`](../tests/test_runtime_eta.py)のcompleted removal／duplicate/excessive request拒否／covered-vs-uncovered flight coverage、[`test_execution_tasks.py`](../tests/test_execution_tasks.py)のunknown hole／failure未公開化を照合。 | **受入確認済み。** 未校正・overrunはunknownであり、ゼロ秒の完了には置き換えない。 |
| **unknown/stale/budget**：総ETA null、抽出打切り・不足・version違いを可視化、自動切替しない | captureのwindow/scan/group/byte上限、snapshot/context/profile/policy/plan版照合。必要costや形状が不明ならtotalはnullでreason/partial coverageを残す。明示endpointの大規模identityもbounded sentinel＋task unknownにし、実executionは継続。`baseline`／`observed`／`adapted`を分離し、ETA reportは`eligible_for_control=false`。 | [`test_remaining_work.py`](../tests/test_remaining_work.py)のstale/profile context／metadata fallback／huge unknown、[`test_runtime_eta.py`](../tests/test_runtime_eta.py)のpartial total null／構造unknownはprofileで上書き不可、[`test_runtime_cost_registry.py`](../tests/test_runtime_cost_registry.py)のpolicy epoch・held-out品質拒否、[`test_endpoint_identity_budget.py`](../tests/test_endpoint_identity_budget.py)の巨大source 0scan/0hash、spec/式上限、学習済sentinel cost遮断とnormal materialize/state oracle。 | **受入確認済み。** 数値回答があっても本ETAだけで自動切替を認可しない。unknownを削って回答率を上げない。 |
| **大きいcount**：全Block/occurrence/peer生成禁止、metadata≤64 MiB、summary上限検査 | countは加重算術、block/peerのworkは代数式で保持し全列挙しない。window≤32、group≤256、captureは設定された有限scan/byte予算を使う。既定metadataは64 KiB、設定上限8 MiBで、元条件64 MiBより小さい。endpoint条件は固定長fingerprintまたはunknown sentinel。 | `test_huge_count_with_finite_weighted_cost_remains_compact`、`test_large_count_is_weighted_without_expansion`、`test_huge_block_count_uses_algebra_and_remote_payload_stays_unknown`、`test_capture_bounds_window_scan_groups_and_metadata`、byte fallback・overflow試験。`len=10**12`のiteration/hash bombはgradient None/明示paramsとも0scan。小さい128 gateは同じexact identityを保つ。 | **受入確認済み。** 64 MiBを実際に割り当てた専用stress成功とは記さず、より小さい上限の検査・打切りと巨大count代数oracleで裏付ける。 |
| **real runtime**：held-out回路のsnapshot時刻から実完了までを照合、rank MAX/短区間もreport | [`RuntimeControl.update_eta`](../src/svsim/runtime_control.py)から実capture/evaluate、collectiveは合意済み順序でrank MAXを報告。実completionのrolling観測とimmutable profile adapterを使う。explicit expectation/gradient/checkpointとgate-onlyを区別する。query後のactual API completionをoracleにする。 | [既存chronological 48 checks](internal/p4_002_observed_eta/chronological-holdout-20261015/mac-48-checks.json)、[修正前endpoint baseline](internal/p4_002_observed_eta/endpoint-heldout-20261016/README.md)は診断履歴。最新Mac fast／Linux19 passは実MPIを含む機能証拠。[修正後endpoint135地点＋growth9件](internal/p4_002_observed_eta/endpoint-heldout-20261017/README.md)は完走し、[固定source Linux held-out／paired overhead全8case](internal/phase4_wave3_validation/linux-performance-v3/README.md)も完了・cleanup成功。 | **受入確認済み。** actual API wall／rank MAX／短区間、216点の78回答・138unknown・error0と全overhead群を保存。旧sourceや内部stageだけの成功へ読み替えない。 |

## endpointの実境界とidentity

source `83cfc36`で追加し`89e8ad2`で有界化した`materialize-prepare`は、入口からagree/Binding確認/ETA更新/初回gradient-origin準備までの実時間を測り、executor/epochのwhileより前で止める。明示expectationとgradientは将来API呼出しごとにprepareを1回ずつ持ち、両方なら2回。bodyと同じmain resourceへ直列加算する。checkpointにはこのprepareを足さない。gate taskは既存件数のままで、empty materialize反復でもexecutor completionは増えない。[実装資料](phase4_exact_future_eta_implementation.md)と[`test_endpoint_prepare.py`](../tests/test_endpoint_prepare.py)がtimer内の実ETA更新、gate開始前記録、task件数／actual class一致を確認する。

future endpointは残gate実行後なのでpending sources=0のprepare classを使い、pending gateあり入口のsampleと分離する。gradient bodyのtimerはmaterializeの後、parameter/context/schedule/vote/agreement/traceの前から実行までを含む。conditionの`measurement_scope=schedule-and-execute-v2`で旧execute-only profileと別classにし、旧profileを新境界の数値へ流用しない。prepareはsource件数、pending件数、gradient-origin flag、observable mask/method/parameter/circuit/checkpoint shape等のendpoint fingerprintを保持し、sourceやshapeの増加後は未測定classを借りない。

gradient identityのsourceが256件を超えたらNone／明示parametersともsourceを列挙せずcircuit hashを計算しない。小さいsourceでもexpression node/depthとfield、明示parameters/observableの項目・文字列・maskをhash前に検査する。超過は`ENDPOINT_IDENTITY_BUDGET`となり、sentinelが学習されてもtask unknownによりtotalはnull、通常materializeは継続する。実gradient executionが大きな回路を処理できることと、monitorがその費用を認証できることは別である。

`baseline`は最初のimmutable適合profileを保持し、`observed`は適合する最新profile／同runの認証済completion等を使う。`adapted`は別resolver/viewであり、未提供なら推測で埋めない。manual/injected/legacyのdiagnostic viewをproduction認証として扱わず、baselineをrolling observedで上書きしない。stale snapshot、profile/plan/policy/context違い、不足・driftはreasonを保持し、ETA reportのcontrol-ineligibleを維持する。

## 現在の機能証拠

- 固定source **`89e8ad2`のMac full fast：1363 passed／38 skipped／83 deselected**。[raw log](internal/phase4_wave3_validation/mac-wave6-fast-final.txt)と[source/選択の説明](internal/phase4_wave3_validation/README.md)。fastの総数を全測定の成功にしない。
- 同じ凍結sourceの[Linux endpoint/実MPI：19 passed／0 skipped](internal/phase4_wave3_validation/linux-endpoint-final/README.md)。prepare、bounded identity、report reducer、残workの実MPI/checkpoint shapeを測定開始前に確認した。所要時間26.74秒は機能検証の時間であり、overhead改善値ではない。
- DAG/runtime/endpointのfocused 84 passはbounded条件、source huge count、既存実dispatchの整合を補強する。異なる選択のpass件数を加算して1本の全suiteと呼ばない。

## 凍結した最終測定と完了結果

[事前v3計画](internal/phase4_wave3_validation/performance-v3-plan.md)にsource **`89e8ad2`**、image ID、順序・予算・試行designを固定した。Linux全8caseが固定sourceでcomplete、cleanup成功となった。[完全性監査・全集計とraw](internal/phase4_wave3_validation/linux-performance-v3/README.md)へ54-cell gridと各7 distinct measured pairsの記録を保存した。新しい目標値を追加せず、旧失敗・timeout・unknown・partialも残す。

| 測定 | 事前に固定した量とoracle | 最終結果欄 |
|---|---|---|
| Linux remaining ETA held-out | buffered/direct×P=1/2の4条件。各54 cells＝3 workload×S0/S1/S2×file/RAM×fraction 0.4/0.6/0.8、各1 trial。snapshot/query時点からactual completionまで、rank MAX、短区間、回答/unknown理由、相対median/p95/worst・絶対秒誤差と区間を保存。 | **完了。** 54×4＝216点、78回答／138unknown／error0。回答分の相対誤差medianはbuffered P1/P2＝5.76%／2.92%、direct P1/P2＝4.13%／3.50%。全workload別medianも初期30%目標内。未実施3 workloadはmanifestのomittedとして残し、全108-cell matrixを達成したとは記さない。 |
| Linux monitoring overhead | 同4条件で2 workloads（`rx-ry-low`／`mixed`20 layers）、各warmup2組＋measured7 distinct paired組。収集/集計/raw/ETAの費用を同条件で比較し、absolute秒・分布・pair単位の結果を保存。 | **完了。** 全8群でwarmup2組＋measured7組。buffered P1 mixedとdirect P2 rx-ry-lowは`eta-on-slower`、残6群は`inconclusive`。計測完走を「低overhead」や高速化の認定としない。`reported_extraction_seconds`は`capture_wall_seconds`の内数であり二重加算しない。backend要求と実I/O証拠を区別する。 |
| 修正後explicit endpoint held-out | 期待値／adjoint gradient／durable checkpoint、4/32/128 gate、各3 trial、校正16＋holdout5：135 holdouts＋checkpoint新shape9 growth。queryは次の実API completionより前。状態/期待値/勾配／resumeの数値oracleと、prepare＋bodyの内部stageも別記録。 | **完走・結果保存。** [全rawとsize別集計](internal/p4_002_observed_eta/endpoint-heldout-20261017/README.md)。135点中83回答、52unknown、API／数値失敗0。回答分の相対誤差medianはcheckpoint7.82%、期待値10.88%、gradient2.54%。growth9件は全てunknown＋実commit/resume成功。checkpoint区間coverage24.4%、短いprepareの認証不足、内部raw queue上限も明記し、全内部event無欠落や回答率改善を主張しない。 |

旧48-check評価の30回答、修正前endpoint baselineのexpectation API費用欠落、checkpointのunknown/低coverageは履歴として保持する。新測定で初期median30%目標を満たさないcaseがあれば、対象・原因・不足を記録する。未知に対する回答率や区間coverageへ新しい合格閾値を課すことも、unknown caseを除去して成功だけを提示することもしない。元6条件と実測を照合し、2026-10-17にP4-002をDONEとした。P4-004等は未完了のためM2とPhase 4全体は未達／IN_PROGRESSであり、今回の完了を適応運用や全性能matrixの完了へ拡張しない。


### 誤差tailと完了の意味

| Linux条件 | 回答 / 全54点 | 相対誤差% median / p95 / worst |
|---|---|---|
| buffered P1 | 30 / 54 | 5.76 / 55.97 / 66.26 |
| buffered P2 | 9 / 54 | 2.92 / 71.53 / 71.78 |
| direct P1 | 27 / 54 | 4.13 / 111.18 / 239.82 |
| direct P2 | 12 / 54 | 3.50 / 72.74 / 73.85 |

回答分のmedianは初期30%目標内でも、tailの大誤差・138点のunknown・overhead悪化2群を隠さない。絶対秒誤差、workload別、fraction 0.8の短区間、区間coverage、unknown理由と全pairは[最終Linux記録](internal/phase4_wave3_validation/linux-performance-v3/README.md)に保存した。回答率・区間coverageへ新閾値を設けた判定ではなく、元条件が要求する実runtime照合・不足の可視化・費用報告を完了したとの判定である。endpointのcheckpoint低coverage、短run prepare未認証、raw queue上限、巨大identityのunknown、control-ineligibleは既知限界として維持する。
