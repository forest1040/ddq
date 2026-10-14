# P4-003／004：runtime生成の再計画費用、自動適用、VERIFY

更新日：2026-10-15。P4-004（費用に基づく再計画候補の評価と運用制御）とP4-003（劣化検出→変更→VERIFY）について、最初の**手書きevidenceを使わない実runtimeのpositive case**とnegative caseを実装した。P4-003／004とPhase 4はIN_PROGRESSのまま維持する。

## 仕組み

| 部品 | 内容 |
|---|---|
| `replan_cost_producer.remaining_calls` | 残りsourceを候補policyのlookaheadで区切り、schedulerと同じ`plan_window`／融合規則で実executor呼出し数を正確に数える |
| `replan_cost_producer.costs_for` | continue下限＝gate数×（gateあたりservice下限）＋現policyの呼出し数×（呼出しあたり超過下限）。候補上限＝gate数×（service上限）＋候補呼出し数×（超過上限）。planning／drainは実測、checkpointは同じrunで実測した`commit_snapshot`の最大値（無ければunknown）、migrationはS0／S2のみ0（S1はunknown）。evidenceのscopeが`injected`のときだけqualifiedになる |
| `AdaptiveEvaluation.estimate` | 劣化を検出したwindowについて、注入なしの単一gate baseline prefixと、その後の同じclassのevent（service＋delay）から、per-gate serviceと呼出しあたり超過を推定する。DEGRADEDのrowに添付する |
| `RuntimeControl.observe_evaluation` | 合意済みevaluation rowを受け、healthとevidenceを更新する（従来の`health`直接代入を置換） |
| `RuntimeControl.frontier` | auto／recommendで、全rankがDEGRADEDかつcooldown経過、累積切替上限（既定4）未満、VERIFY中でない場合に、全候補を`replan_cost.evaluate`で価格付けする。rankごとの最小gainと全rankのeligibleで統合し、最大gainの候補を選ぶ。autoのときだけ既存の`apply_candidate`（集団合意、stale／TTL検査、FRONTIER_REPLANはdurable snapshot）で適用する |
| VERIFY | 適用後に`VERIFYING`へ移る。各呼出しを予測上限（gate数×service上限＋超過上限、slack 10%）と比べ、window分続けて収まれば`STABLE`、超えれば`VERIFY_FAILED`。STABLE後は新しいexecutor classで再baselineし、以後の劣化を新条件で再判定する |
| 測定 | `AsyncDistributedSimulator.commit_snapshot`の実測wallを、FRONTIER候補のcheckpoint費用に使う |
| time-model ETA | policy変更後に条件（policy hash）を更新し、新policyで価格付けする |

**正の機構。** 注入はexecutor呼出しごとに一定の計算遅延を加える。S2はwindow内の局所gateを1回の呼出しに融合するので、支払う遅延の回数が減る。LIVE_TUNE候補（prefetch、I/O深さ等）は呼出し数を変えず、`INSUFFICIENT_GAIN`で正しく拒否される。

## 検証

**単体・統合**（`tests/test_adaptive_replan.py`）
- S2融合の呼出し数を正確に数えること。
- 持続劣化→S2を適用→`plan_version=2`→VERIFY→STABLE、状態がeager oracleと一致すること。
- negative：observe、recommend（推奨のみで適用しない）、残作業が短い、checkpoint費用unknown（`UNKNOWN_COST_COMPONENT`）、transient、baselineなし。いずれも切替もsnapshotも起きないこと。
- 旧期待「autoは切り替えない」を持つfollow-up試験は、新しい期待へ更新した。

**paired campaign**（`examples/phase4_remaining_campaign.py --action adaptive`、warmup 2、paired 7）。32 RX、lookahead 4、呼出しごとに20 msの注入。fixed＝observe、auto＝自動。positiveの判定規則は「全autoが1回切り替えてVERIFYに成功し、かつpaired差のCI95上限が0未満」。

| 環境 | scenario | fixed wall median | auto | 切替／VERIFY | 差のCI95 | positive |
|---|---|---|---|---|---|---|
| Mac P=1 | sustained | 0.969 s | 0.450 s | 7/7 | [-0.521, -0.516] | ✓ |
| Mac P=1 | recovery（注入4〜12） | 0.578 s | 0.350 s | 7/7 | [-0.253, -0.223] | ✓ |
| Mac P=1 | transient（4〜6） | 0.453 s | 0.451 s | 0 | [-0.010, 0.003] | —（正しく切替なし） |
| Mac P=1 | missing-baseline | 1.048 s | 1.044 s | 0 | [-0.006, 0.033] | —（正しく切替なし） |
| Mac P=2 | sustained | 1.103 s | 0.549 s | 7/7 | [-0.561, -0.546] | ✓ |
| Mac P=4 | sustained（初回） | 1.62 s | 1.054 s | 6/7 | [-0.578, -0.562] | ✗（1 trialは劣化を検出したが切替なし。当時は判断ログ未保存で理由不明） |
| Mac P=4 | sustained（判断ログ追加後の再実行） | — | — | 7/7 | 上限<0 | ✓ |
| Linux P=1、遅延100 ms | sustained | 10.92 s | 2.66 s | 6/7 | [-7.90, -4.25] | ✗（1 trialは遅い装置状態で不確実gainとして拒否） |
| Linux P=1、遅延100 ms | recovery | 10.00 s | 2.12 s | 5/7 | [-4.11, 0.17] | ✗（同上） |
| Linux P=1、遅延100 ms | transient | 10.50 s | 10.39 s | 0 | [-5.17, 0.21] | —（正しく切替なし） |
| Linux P=1、遅延100 ms | missing-baseline | 6.19 s | 6.24 s | 0 | [-0.03, 0.09] | —（正しく切替なし） |
| Linux P=2、遅延100 ms | sustained | 5.73 s | 2.72 s | 7/7 | [-3.09, -3.00] | ✓ |
| Linux P=1／2、遅延20 ms | 全scenario | — | — | 0〜1/7 | — | ✗（gainが不確実なため保守的に拒否） |

predicted gain lower（例：P=1で0.30 s）は、実測のgain（約0.52 s）より保守的だった。証跡は[Mac](internal/phase4_adaptive_validation/mac/)、[Linux 100 ms](internal/phase4_adaptive_validation/linux/)、[Linux 20 ms](internal/phase4_adaptive_validation/linux-20ms/)にある（Macの証跡はraw要約化の前の形式）。**Linuxで分かったこと。**
- **注入遅延の大きさ。** ent Docker（ext4）では、gateあたりのserviceが50〜80 ms程度でばらつく（durable commit）。20 msの注入では、保守的な上下限の差（gate数×（service上限−下限）≈0.5 s）がgain（≈0.36 s）を上回り、全候補が`INSUFFICIENT_GAIN`になった。P4-003の「不確実なgainは抑止する」に当たる正しい拒否である。
- **100 msの注入。** P=2は7/7で切替・VERIFYし、positiveになった。P=1でもsustainedは6/7で大きく改善した（CI上限-4.25 s）。切替しなかったtrialはいずれも装置が遅い状態（fixedでも約11 s）で、service推定の幅が広がって拒否されたものであり、誤った切替は起きていない。判定規則（全trialが切替）を満たさないので、positiveとは数えない。
- **campaignの修正。** trialごとのrawが32 eventで16 KiBの上限を超え、rank 0でだけ例外が起きていた（P=1は出力なし、P=2は残りrankの集団待ちでhang）。eventは要約（health別件数、初回DEGRADED／VERIFYING／STABLEのordinal）と先頭6件、判断logは最大6件にして解消した。rank 0だけの例外がhangになるcampaign構造は残件である。
- **検出遅延。** 注入開始から3呼出しでDEGRADEDを検出した（window 2×連続2の設定による）。

Mac全回帰は845 passed／14 skipped。Linuxでは関連61件が成功した。

## storage／MPI注入（資源別）

`configure_performance_injection`は、computeに加えてstorage（`storage-read`／`storage-write`、要求ごと、任意で1 byteあたり）とMPI（`mpi-transfer`、送信元の転送ごと）を受け付ける。storageとMPIの遅延はrank-localな待機で、集団通信は行わない。`AsyncStoreIO`のsubmit直前と`MPIProgressEngine`のIsend直前で支払う。executor groupの実測serviceの中に含まれるので、group observerには参考値として`resource_injected_seconds`を添える（二重計上はしない）。campaignは`--resource {compute,storage,mpi}`で選ぶ。storageのwork単位は書込み要求で、1呼出しはrankのBlock数だけ書く。MPIはrank bit（q3）の回転で、呼出しごとに交換が起きる。

| 環境 | 資源 | fixed wall median | auto | 切替／VERIFY | 差のCI95 | 判定 |
|---|---|---|---|---|---|---|
| Mac P=1 | storage | 3.77 s | 1.45 s | 7/7 | [-2.33, -2.31] | positive |
| Mac P=2 | storage | 6.22 s | 2.37 s | 7/7 | [-3.85, -3.84] | positive |
| Mac P=2 | MPI | 6.37 s | 6.38 s | 0 | [-0.01, 0.01] | 正しく拒否 |
| Linux P=1 | storage | 6.15 s | 2.66 s | 7/7 | [-3.55, -3.27] | positive |
| Linux P=2 | storage | 8.83 s | 8.57 s | 6/7 | [-8.23, -0.54] | ✗（1 trialは不確実gainで拒否。判定規則を満たさない） |
| Linux P=2 | MPI | 8.64 s | 8.65 s | 0 | [0.001, 0.022] | 正しく拒否（autoの候補評価費用として約10 ms遅い） |

- **storage。** file storeでは、各呼出しが触れたBlockを1回ずつ書き戻す。S2の融合で呼出し数が減ると書込み要求も減るので、呼出しあたり超過のモデルがそのまま当てはまる。
- **MPI。** 交換はrank bitを対象とするgateで起き、S2は局所gateしか融合しないので、呼出し数は変わらない（例：24→24、`INSUFFICIENT_GAIN`）。layoutを動かすS1はmigration費用がunknown（`UNKNOWN_COST_COMPONENT`）である。P4-003の「合法な候補が無ければ拒否が正しい」に従い、検出したうえで拒否する。転送は呼出しあたり複数あり、注入event上限（256）に早く達するため、run後半は注入が止まりHEALTHYへ戻る。
- **試験。** in-processのstorage positiveと、`mpiexec -n 2`のMPI拒否を追加した。Mac全回帰は847 passed／14 skipped、Linux関連は42件成功した。

## 残件

- MPIのpositive（rank bitを局所化するlayout migration候補と、その移行費用の実測が前提）。
- 時間triggerと追加barrier。
- cross-host（003C）。
- tile／layout migration候補と、移行費用の実測。
- 各段階のdurable fault試験（reserve／drain／install／prepare／decision／ack）。
- 切替後の期待値・勾配の一致。
- 検出遅延・FP/FN・ETA誤差・monitor比率のreport。
- 運用（production profile）でのqualified costの供給（現在は注入評価だけがqualified）。
- 装置状態の切替が多いLinux P=1での安定したpositive。service推定を装置状態別に持つ必要がある。
- rank 0だけの例外が集団待ちのhangになる構造の解消。
