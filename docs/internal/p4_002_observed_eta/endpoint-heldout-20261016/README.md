# P4-002 endpoint の実 runtime heldout（Mac、2026-10-16）

`examples/p4_002_endpoint_heldout.py` は gate-only の予測を流用せず、実 `set_eta_endpoint` → `update_eta` の observed view と、その**直後の次の public endpoint 呼出し**を比較する。production cost、identity、burn-in、必要sample数、coverage/drift閾値は変更しない。completion collectorへ合成行や手動costを注入しない。

## protocol

- Mac、MPI `COMM_SELF`、4 qubit／2 block bit、file store、buffered、cooperative、S2。4／32／128 gate、endpoint別・size別に独立したsimulatorを3 trialずつ作る。
- `RY(q0, theta=.37)` と他qubitのRXからなる固定回路を先にmaterializeする。期待値 `Z(q0)=cos(.37)`、adjoint gradient `−sin(.37)` を毎回照合し、各case最後の全stateもeager oracleと比べる。
- endpointはcheckpoint、expectation、adjoint gradient。各caseで16回の実完了から校正し、次の5回はquery→実completionの順で記録する。前のheldout completionはruntimeの通常rolling collectorへ入り、次のqueryで使用され得る **prequential** 評価である。完全に凍結した学習集合や他trialの費用の転用ではない。
- checkpointの初回dirty tailと再利用済tailは異なるshape。生データに各calibration/heldoutのcondition、query前の同class sample数、production certification理由を残す。query自体がcollector revisionを増やさないことを検証する。次の実completionのconditionもquery時のconditionと完全一致させる。
- 各呼出し前に実 `drain`／cache flush／`drain`／cache clearを行い、残gateなし・quiescent cutへ揃える。この準備のwall timeとcache entry数は別欄に保存し、予測との比較時間に混ぜない。endpoint全API wallと、その内部で実測されたstage wallは両方を保存する。
- checkpointのheldout後に4 sourceを追加して実行し、gate/historyの**未測定payload shape**をqueryする。unknownと理由を保存し、その直後にも実checkpointを行う。最後のsnapshotをresumeしてstate oracleを確認する。
- cold/warm状態、ファイル増加、host jitterを認証判定から除外しない。unknownをゼロ誤差にせず、回答率の分母に残す。回答について絶対誤差秒数と絶対相対誤差のmedian／p95／worst、実wallに対する区間coverageを集計する。p95は線形補間。

再現例：

```sh
PYTHONPATH=src .venv/bin/python examples/p4_002_endpoint_heldout.py \
  --output /tmp/endpoint-heldout.json --scratch /tmp/endpoint-heldout
```

outputとscratchは新しいpathを使う。sourceを増やさずcheckpointを反復してもcatalog/trace/rootは実際に増え、checkpointの全public APIが完全に一定時間になるとは仮定しない。

## 生データと時系列

本測定は `mac-endpoint-heldout.json`。runtime source hash、base revision、runner hash、全calibration/heldout/growth行、query時ETAと理由、各実completion、endpoint collector observations、collector drop/failure countersを保存する。実行環境は他のunit test等と分離していない。Linux性能jobへは接続・投入していない。

準備時の小probeも削除せず残す：

- `mac-preparation-drain-flush.json`：cache clearなし。gradientは実cost認証済みでもin-flight/cache残量のためETA unknown。
- `mac-preparation-cache-bound.json`：drain/flushでin-flightは除去したが、cache entries>64のproduction走査上限によりgradientが `CACHE_QUIESCENCE_UNINSPECTED`。これは未回答を保守的に保持した証拠である。

本測定は実cache clearを追加してquiescent endpointを測る。上限を64より広げたり、未知cacheをゼロ扱いしたりしていない。準備probeは本matrixの回答率へ混ぜない。prototype runner hashは本runnerと異なるため、本protocolの再現結果としては数えない。

## 解釈上の限界

public APIには、実stage計測の外にmaterializeの確認、drain、shape計算、completion記録、ETA失効等がある。生データはstageだけを比べた結果も別集計する。この差を手動費用で足したり、区間を広げて隠したりしない。checkpoint/expectationでpublic APIの区間coverageが低い場合は、end-to-end endpoint受入の残件として記録する。

同shapeで一度認証されたことは次試行でのcoverage保証ではない。これは小state・単一host・bufferedのprequential実測であり、Linux O_DIRECT、多rank／別host、未実行gateを含む全体ETA、parameter-shift/recompute gradient、monitor overheadを受け入れた証拠ではない。P4-002全体のDONEをこの実験だけで宣言しない。

## 修正前baseline結果

全27 case、校正432回、heldout135回、増加後checkpoint9回、計約376秒。実API errorと数値oracle失敗は0。parentからの同時作業記録によれば**前半はrootのTeX buildと重複した**。hostを隔離した性能比較ではなく、モデル欠落を探すbaseline診断として扱う。

| endpoint | 回答 / check | 絶対誤差 ms median / p95 / worst | 絶対相対誤差 % median / p95 / worst | API wall区間coverage | 内部stage区間coverage |
|---|---|---|---|---|---|
| checkpoint | 38 / 45 | 0.892 / 1.247 / 2.319 | 8.01 / 11.27 / 14.97 | 13.2% | 26.3% |
| expectation | 45 / 45 | 0.884 / 2.287 / 2.453 | 25.15 / 45.44 / 47.77 | 44.4% | 95.6% |
| adjoint gradient | 45 / 45 | 9.643 / 207.258 / 418.714 | 0.69 / 5.06 / 10.11 | 97.8% | 97.8% |

checkpointの未回答7回はすべて `LOW_HELD_OUT_COVERAGE`。新source/history shapeの9 queryはすべて `COST_UNCERTIFIED:INSUFFICIENT_SAMPLES:endpoint:checkpoint`＋`MISSING_COST`で未回答となり、直後の実commitとresume oracleは9件すべて成功した。

expectationのsize別相対誤差medianは4／32／128 gateで16.39／21.74／32.40%、p95は39.72／41.29／47.09%。API wall／内部stageのmedian比は1.169／1.235／1.446。checkpointの同比は約1.007〜1.009で、過小予測の大半をtimer外だけでは説明できない。

expectationでは、実stage timerより前の`materialize`がgateなしでもagree、Binding確認、sourceに依存するETA更新を実行する。この費用は必須API作業だが当時のendpoint taskには無く、size 128の差は有意である。gradientもmaterialize後のcontext/choose/vote/traceをtimer外に置いている。これらは別core commitで補完する。checkpoint timer拡張はin-flightとの重複計上を避けるため今回は行わず、上記の小さいAPI差と低coverageを残件とする。
