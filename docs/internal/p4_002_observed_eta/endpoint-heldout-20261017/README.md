# P4-002 explicit endpoint core 修正後 heldout（Mac、2026-10-17）

基準は[修正前baseline](../endpoint-heldout-20261016/README.md)。同じ runner の同じ既定protocol、27 case（checkpoint／expectation／adjoint gradient × 4／32／128 gate × 3独立trial）、実校正16回→query直後の次実completion5回、checkpoint growth9回を実行した。warmup・burn-in・認証閾値・identityを緩めていない。production completion collectorへ合成costを入れていない。

`mac-endpoint-heldout-fixed.json` が全raw。前trialや未来の値を手動で価格へ渡さず、各simulatorの実completionから通常のrolling/prequential認証を使う。heldoutの前に完了した別heldout sampleは次queryへ入り得る。query自体がcollector revisionを増やさず、query時shapeと次実completion shapeが一致することを実行中に検証する。

## source と環境

worktree revision `61798367b33225a7f7d360332ea48533899eda94`。productionの`async_scheduler.py`／`remaining_work.py`／`runtime_observed_costs.py`はmain `89e8ad2` とbyte一致を事前確認した。全runtime source（src内.py/.c/.h）のfingerprintは `08e168ab11e5eed05cb6817b08efea27eb5613c35521bd64b291f0a058f39139`。runner自体のhash、Python／platform、引数はraw内にもある。

親担当の最終Mac全fastが1363 passedで完了してから、root側の重いpytest・TeX等を止めた旨の合図を受けて開始した。測定中はこちらからもheavy testやTeXを実行していない。OS全体を予約したidle-host実験ではない。baselineの前半はTeX buildと重複しており、前後のwall timeや回答率の差を純粋な性能改善へ帰属させない。Linux別hostの実験はこのMacの実測には混ぜない。

## 測るものを区別する

1. **実験側quiescent準備**：query前の実drain／flush／clear。`quiescent_preparation_seconds`に別記し、比較対象wallへ入れない。実cacheを空にする操作であり、productionのcache走査上限を緩めたり未確認workをゼロへ置き換えたりしない。
2. **production materialize前処理**：future endpoint API内部のagree／Binding確認／ETA更新等。`prepare_completion`の実時間を新taskが価格付けする。source/pending/specのbounded identityを使い、gate/epoch bodyを二重計上しない。
3. **endpoint body stage**：`actual_stage_seconds`。gradientは新`measurement_scope=schedule-and-execute-v2`でcontext/schedule/collective/traceも含む。
4. **計測済みendpoint stage和**：`actual_measured_endpoint_seconds`。expectation/gradientはbody＋production prepare、checkpointはbodyだけ。`summary.same_shape_measured_stage`はこの和との比較で、`same_shape_body_stage`はbodyだけとの比較。
5. **実public API wall**：`actual_wall_seconds`。主要な精度・coverage集計はこれに対するもの。prepare/bodyのcompletion記録やその他のPython API費用まで完全包含するという主張はしない。

checkpointのtimerは今回広げていない。new source/history shapeのqueryは従来どおりunknownとなるべきで、直後の実checkpoint時間と理由を含めて保存する。snapshot resume後の全stateをeager oracleへ照合する。expectationはcos(.37)、adjoint gradientは−sin(.37)を毎回照合する。

## 判定の範囲

unknownの点をゼロ誤差として集計せず、135 heldout全体の回答率の分母へ残す。前処理が短時間でも未認証なら全体ETAをunknownのままにする。下記の精度は回答された点についてであり、回答率の低下を隠すものではない。runtime rolling intervalはconfidence intervalや全試行保証ではない。

APIに必要な前処理の欠落を補ったことと、全endpointの受入を達成したことは区別する。checkpointの区間coverage、prepare classの短run認証、gate-onlyの外部API入口費用、非quiescent cut、Linux／multi-rank／別host、監視overheadはこの実験だけでDONEにしない。TeX/PDFはユーザー指示に従い更新しない。

## 結果

全27 case、校正432・heldout135・growth9、374.67秒。実API error／数値oracle失敗は0。全stateの最大絶対誤差は0、checkpoint growth後のresume oracleは9/9成功。全576 endpoint完了、378 production prepareとの対応、144 queryのcollector revision不変、sourceとrunner hashをrawから検査した。測定後は追加のheavy pytestを行わず、数値とprovenanceの軽い検査だけを行った。

主要比較は**回答された点の実public API wallに対する誤差**。unknownは回答に数えず、check分母45には残す。機械可読なsize別・平均・内部stage和・baselineとの集計は `analysis-summary.json`。

| endpoint | 回答/check | 絶対誤差ms 平均 / median / p95 / worst | 絶対相対誤差% 平均 / median / p95 / worst | API区間coverage | 計測stage和のcoverage |
|---|---|---|---|---|---|
| checkpoint | 45/45 | 0.911 / 0.938 / 1.132 / 1.403 | 7.54 / 7.82 / 9.64 / 10.37 | 24.4% | 31.1% |
| expectation | 24/45 | 0.631 / 0.502 / 1.480 / 1.595 | 14.31 / 10.88 / 34.10 / 36.46 | 87.5% | 79.2% |
| adjoint gradient | 14/45 | 54.146 / 26.639 / 212.710 / 237.154 | 2.62 / 2.54 / 4.82 / 5.14 | 92.9% | 92.9% |

| endpoint / gate数 | 回答/check | 相対誤差% median / p95 / worst | API区間coverage |
|---|---|---|---|
| checkpoint / 4 | 15/15 | 8.70 / 10.13 / 10.37 | 0.0% |
| checkpoint / 32 | 15/15 | 6.88 / 9.16 / 9.61 | 33.3% |
| checkpoint / 128 | 15/15 | 6.88 / 8.55 / 9.65 | 40.0% |
| expectation / 4 | 7/15 | 9.64 / 20.06 / 23.32 | 100.0% |
| expectation / 32 | 9/15 | 13.26 / 25.74 / 29.49 | 100.0% |
| expectation / 128 | 8/15 | 12.11 / 35.72 / 36.46 | 62.5% |
| gradient / 4 | 6/15 | 1.96 / 2.94 / 3.12 | 83.3% |
| gradient / 32 | 4/15 | 2.95 / 4.44 / 4.65 | 100.0% |
| gradient / 128 | 4/15 | 3.55 / 5.03 / 5.14 | 100.0% |

expectationのproduction prepare実時間medianは4／32／128 gateで0.507／0.722／1.424 ms。この費用が新taskに入る。API wall／実body+prepare和のmedian比は1.042／1.040／1.035であり、baselineで128 gateのAPI/body比が1.446だった必須作業の欠落を補完した。これはAPI実行が高速化したという結果ではない。実API平均wallは同順で3.785／4.103／4.914 msで、baselineとはhost重複状況とsourceが異なる。

### unknownと保持範囲

- expectationは21/45 unknown。`LOW_HELD_OUT_COVERAGE`はprepareで17点、bodyで5点、うち1点は両方。対応する`MISSING_COST`も保持する。
- gradientは31/45 unknown。同理由はprepareで30点、bodyで4点、うち3点は両方。短いprepareが未認証でも既知bodyだけで合計を回答しない。
- checkpointのgrowth9 queryはすべて新shapeの`INSUFFICIENT_SAMPLES`＋`MISSING_COST`でunknown。直後の実commit時間とresume oracleは全件記録済み。
- 今回の128 gate以下のshapeでは `ENDPOINT_IDENTITY_BUDGET` は発火していない。oversized sourceを受け入れた証拠ではなく、その拒否は別のbounded identity単体回帰の範囲である。
- collectorのcost側`dropped_conditions/dropped_bytes/dropped_children/failures`は全caseで0。gradient内部の**bounded raw pending queue**では合計139,253 event行の`dropped_raw`が記録された。すべての内部event streamを無欠落で保存したという主張はしない。今回要求した校正・heldout・growthのendpoint576行とprepare378行はrunnerが別途直接保存し、全件の実completion／condition／数値を検査できた。各caseのcounterとfailed observationsもrawへ残す。

baselineに比べてexpectationの回答点の誤差とcoverageは変わった一方、回答率は45/45から24/45、gradientは45/45から14/45へ下がった。回答された集合も変わるため、精度改善率を全caseへ外挿しない。checkpointの低coverageとprepareの短run認証を残件として扱い、P4-002全体のDONEにはしない。
