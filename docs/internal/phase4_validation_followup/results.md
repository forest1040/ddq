# Mac followup matrixの結果

2026-10-14。n=4、b=2、P=1/2/4、buffered/direct要求。成功した6構成を選択し、初回P=4の時間切れ2件も保存する。warmup 84 trialと本測定294 trial、合計378 trial。別枠のgradient context取得／auto確認は12 trial。rawと測定hash、source一致、rank MAX、RAM／SSD上限、勾配context／hash／数値検証結果を再照合した。

[機械可読summary](selected-results.json)、[再現条件と受入の境界](README.md)。rawのrevisionは実行開始時HEADで、未commitの実装はsource／worker hashで固定している。成功した全構成のsource／worker hashが一致する。commit直前にphase4_evaluation.pyと2つのtestの末尾空行だけを削除したため、commitのsource byte hashは測定時と異なる。機能変更は無い。

## レイアウト比較

primary medianはconstruct・forward・数値検証・checkpoint・close・cleanup込み秒。差はS2 − S0。7 pairのbootstrap CIは当該sampleの診断値で、予測区間や別回路への外挿には使わない。

| 要求backend／P | S0 median (s) | S2 median (s) | paired差 CI95 (s) | raw |
|---|---:|---:|---|---|
| buffered-p1 | 0.529 | 0.294 | [-0.236, -0.227] | [JSON](mac-matrix-20261014/buffered-p1.json) |
| buffered-p2 | 1.105 | 0.661 | [-0.488, -0.411] | [JSON](mac-matrix-20261014/buffered-p2.json) |
| buffered-p4 | 3.911 | 2.409 | [-1.545, -1.443] | [JSON](mac-p4-retry-20261014/buffered-p4.json) |
| direct-p1 | 0.545 | 0.299 | [-0.255, -0.234] | [JSON](mac-matrix-20261014/direct-p1.json) |
| direct-p2 | 1.194 | 0.696 | [-0.526, -0.490] | [JSON](mac-matrix-20261014/direct-p2.json) |
| direct-p4 | 3.980 | 2.436 | [-1.553, -1.489] | [JSON](mac-p4-retry-20261014/direct-p4.json) |

全6構成でS2が速い方向を観測した。これは固定layoutの比較で、自動適応の利益を示すものではない。

## 持続compute注入：fixed／auto

差はauto − fixed。fixed/autoの本測定84 trialすべてでDEGRADEDを観測した。eligible候補0、switch0、positive gain未検証。費用とVERIFYが無いため安全に拒否する。

| 要求backend／P | fixed median (s) | auto median (s) | paired差 CI95 (s) | 判定 |
|---|---:|---:|---|---|
| buffered-p1 | 1.992 | 1.995 | [-0.004, 0.017] | 未確認 |
| buffered-p2 | 2.556 | 2.568 | [-0.020, 0.022] | 未確認 |
| buffered-p4 | 5.992 | 6.034 | [0.004, 0.083] | autoが遅い方向 |
| direct-p1 | 1.981 | 2.004 | [0.006, 0.039] | autoが遅い方向 |
| direct-p2 | 2.663 | 2.668 | [-0.023, 0.052] | 未確認 |
| direct-p4 | 6.075 | 6.084 | [-0.058, 0.067] | 未確認 |

buffered-p4とdirect-p1ではautoが遅い方向を観測したため、性能改善として扱わない。background loadとOS cacheの外部変動は制御していない。

## 勾配の三方式

以下はwhole-trial median秒。factory／forward準備、runner、validator、cleanup込み。runnerだけのmedianは機械可読summaryとrawへ保存する。各方式warmup2＋本測定7、計162 trialが解析値と一致した。

| 要求backend／P | adjoint (s) | recompute (s) | parameter-shift (s) |
|---|---:|---:|---:|---:|
| buffered-p1 | 0.502 | 0.590 | 1.210 |
| buffered-p2 | 1.049 | 1.221 | 2.817 |
| buffered-p4 | 3.548 | 4.003 | 9.325 |
| direct-p1 | 0.510 | 0.620 | 1.273 |
| direct-p2 | 1.126 | 1.311 | 2.854 |
| direct-p4 | 3.609 | 4.075 | 9.259 |

認証済みenvelopeを生成しておらず、production costは全caseで0。autoはpilot無しでadjointへfallbackし、理由はINSUFFICIENT_BUILD_BASELINE。手法選択の性能改善は未達である。

## 失敗資料と制限

初回P=4はbuffered/directともhard wall300秒でtimed_out。raw reportは出力されず、[初回matrix](mac-matrix-20261014/matrix.json)と各job report／stdout／stderrを保存した。600秒で再実行した[P=4 matrix](mac-p4-retry-20261014/matrix.json)の両caseが完了した。残存scratchを再利用していない。

resolved backend情報はrawで空配列のため、各trialのF_NOCACHE適用をraw単体で認証できない。複数host、large state、actual storage／MPI注入、positive採用／VERIFY、包括admissionなどは未検証。P4-003／005／008はIN_PROGRESSのままである。
