# P4-004：layout migration候補と事前校正profile（実装）

更新日：2026-10-15。[設計](phase4_layout_migration_design.md)（案A：held-out検証済みの事前校正profileで移行後のclassを価格付けする）を実装した。P4-003／004はIN_PROGRESSのまま維持する。

## 構成

| 部品 | 場所 | 内容 |
|---|---|---|
| executor class profile | `executor_profile.py` | `export_profile(sim)`は、collectorの行のうちrolling認証（burn-in 3、8件以上、drift、held-out coverage 0.8以上）を通ったexecutor classと`physical-bit-swap`だけを書き出す。物理context（n、block_bits、backend、engine、ranks、threads）とenvelopeを持つ。文書はsha256のIDを持ち、`merge`は同じfingerprintの文書だけを結合する。`ExecutorProfile.cost_sample`は、class keyと物理contextが完全に一致するときだけ答える |
| fingerprint | `executor_profile.runtime_fingerprint` | host／build／allocationを、run directoryではなく**storage volumeのmount root**でprobeする。既存のprobeはdirectoryのpathを含むので、別directoryで校正したprofileが一致しなかった。同じvolumeなら一致し、別のdevice・build・allocationでは一致しない |
| 登録 | `RuntimeControl.attach_executor_profile` | fingerprintを全rankで照合し（不一致は集団で拒否）、kind `executor-class-seconds`をprovenance measuredで登録する |
| swapの計測 | `AsyncDistributedSimulator.physical_index_bit_swap` | swapをmeasured stage `physical-bit-swap`（bit組、path、n、b、backend、engine、ranks）として記録する。事前のmaterializeは計測に含めない |
| 候補 | `replan_candidates.layout_options` | 2 rank以上で、残り（endpointまで、最大4096 source）が最も使うrank bit上の論理qubitと、最も使われないlocal物理bit（同数なら小さいbit）の組を、最大2つ作る。capabilityは「rank bitとlocal bitの1組」だけを受け付ける |
| 価格付け | `replan_cost_producer.layout_costs_for` | 候補layoutで残りのepochを正確に区切り、executor class×回数をprofileのupperで足す。そこに、劣化evidenceの呼出しあたり超過upperを、MPIに帰属するときはrank bitに触れる呼出しだけ、それ以外は全呼出しに足す。さらに、profileのswap upper、checkpoint、planning、drainを足す。continue下限は既存の式である |
| 帰属 | `AdaptiveEvaluation.estimate` | 観測したeventの注入資源が1種類なら、`resource`として返す |
| 適用 | `RuntimeControl.apply_layout_migration` | 全rank合意、drain、公開cutで`physical_index_bit_swap(..., _pending=True)`（残りをmaterializeしない）、`plan_version`を進める、ETA無効化、durable snapshot |
| VERIFY | `_apply_selected` | 候補classのprofile upperの最大と、帰属後の超過で、呼出しごとに照合する |

**校正手順。** `examples/p4_004_layout_migration.py`をcampaignと同じrank数・policy・backendで実行する。現在のlayoutと移行後のlayoutで回路を実行し、swapを往復で繰り返す。結果は、rankごとの`executor-profile-rank<r>.json`である。campaignは`--executor-profile DIR`で添付する。

## 実装中に見つけた問題

- **materializeの再入。** `physical_index_bit_swap`は、`_pending`が無いと最初に残りの回路全体をmaterializeする。epoch境界（materializeの途中）から呼ぶと、swapの中で残り全体を実行してしまうので、適用経路では`_pending=True`を使う。
- **campaignのMPI注入窓。** `--resource mpi`の作業窓`[start,end)`と遅延は、executor呼出しではなくMPI転送の回数を数えていた。rank bitのgate 1回あたり、各rankはBlock数（4 qubit・b=2・2 rankで2）だけ転送する。そのため「sustained」の劣化は、数呼出しで終わっていた。storageと同じく、Block数で呼出し単位に換算するよう直した。直す前は、予測gainの下限（3.5秒）が実際のgain（1.4秒）を上回った。直した後は、予測下限2.31秒に対して実測4.6秒で、保守的になった。[既存のMPI拒否の結果](phase4_adaptive_replan_implementation.md#storagempi注入資源別)は直す前の設定によるが、合法な候補が無かったので、結論（拒否）は変わらない。

## 検証

`tests/test_layout_migration.py`：
- **profile。** 内容とIDの一致、measuredのheld-out以外の拒否、fingerprintが違う文書のmerge拒否。Binding・epoch・versionが違っても一致し、物理contextやbitが違えば一致しないこと。
- **capability。** rank bitとlocal bitの1組だけを受け付けること（1 rank、private cut、範囲外、他の更新との同時指定を拒否）。
- **候補の生成。** 最も使われるrank bit上のqubitを選ぶこと、局所の作業、1 rank、走査上限では何も出さないこと。
- **価格付け。** profileのupperと回数、MPIに帰属した超過、帰属が無いときの全呼出しへの超過、profileに無いclassやevidenceが無いときのunknown。
- **適用。** swapが失敗すると旧layout・旧plan version・同じfrontierのままで、その後の適用で`plan_version`が進み、最終状態がoracleと一致すること。
- **実MPI（2 rank、slow）。** 事前profileを作ってcampaignを実行する。profileありではautoが1回移行してVERIFYに成功し、profileなしでは切り替えない。

Macの高速版は918 passed、slowのMPI 2件は成功した。

**予備のcampaign（Mac P=2、MPI注入0.1秒／呼出し、paired 3、warmup 1）。**
- **fixed。** execute medianは6.03秒だった。
- **auto。** 1.43秒で、全trialが移行し、VERIFYに成功した。
- **差。** paired差は−4.61秒（CI95 [−4.63, −4.62]）で、予測gainの下限は2.31秒だった。

## paired campaign

2 rank、`--resource mpi`、warmup 2・paired 7で実施した（Sonnetが実行し、私が判断logを確認）。差は、paired wall（auto−fixed）のCI95である。予測は`predicted_gain_lower_seconds`のmedian、実測はpaired executeの差のmedianである。証跡は[内部記録](internal/phase4_layout_migration/README.md)にある。

| 環境 | scenario | 遅延／呼出し | profile | fixed | auto | 切替／VERIFY | 差のCI95 | 予測下限 | 実gain |
|---|---|---|---|---|---|---|---|---|---|
| Mac | sustained | 0.1 s | あり | 6.37 s | 1.73 s | 7/7 | [−4.68, −4.62] | 2.32 s | 4.62 s |
| Mac | sustained | 0.05 s | あり | 3.57 s | 1.26 s | 7/7 | [−2.36, −2.27] | 1.12 s | 2.31 s |
| Mac | sustained | 0.1 s | なし | 6.29 s | 6.33 s | 0/7 | [+0.03, +0.06] | — | — |
| Mac | missing-baseline | 0.1 s | あり | 7.16 s | 7.16 s | 0/7 | [−0.02, +0.01] | — | — |
| Mac | transient | 0.1 s | あり | 1.06 s | 1.10 s | 7/7 | [+0.04, +0.05] | 2.30 s | −0.05 s |
| Mac | recovery | 0.1 s | あり | 2.30 s | 1.73 s | 7/6 | [−0.57, −0.54] | 2.31 s | 0.64 s |
| Linux | sustained | 0.1 s | あり | 8.56 s | 3.97 s | 7/7 | [−4.64, −4.56] | 1.98 s | 4.56 s |
| Linux | sustained | 0.1 s | なし | 9.83 s | 9.76 s | 0/7 | [−0.07, +0.12] | — | — |
| Linux | sustained | 0.2 s | あり | 14.55 s | 5.40 s | 7/5 | [−9.32, −8.70] | 4.64 s | 9.23 s |
| Linux | transient | 0.1 s | あり | 4.66 s | 4.80 s | 7/1 | [−0.03, +0.18] | 2.49 s | −0.19 s |
| Linux | transient | 0.2 s | あり | 4.86 s | 5.06 s | 7/3 | [+0.18, +0.29] | 4.40 s | −0.20 s |
| Linux O_DIRECT | sustained | 0.1 s | あり | 21.66 s | 18.17 s | 6/3 | [−4.95, −3.02] | 2.64 s | 4.11 s |

**positive。**
- **判定。** 「全autoが1回切り替えてVERIFYに成功し、paired差のCI95上限が0未満」を満たしたのは、Mac 0.1 s／0.05 sとLinux 0.1 sのsustainedである。
- **予測下限。** いずれも実gainより保守的だった。
- **選ばれた候補。** 選ばれたのは常に`layout_swap [3,0]`で、MacではMPIの送信byteが8192から2304へ減った。
- **profileが無いとき。** layout候補は`UNKNOWN_COST_COMPONENT`で拒否され、切り替えなかった。他のpolicy候補は従来どおり`INSUFFICIENT_GAIN`である。

**分かった限界。**
- **transientとrecovery。** 予測下限は「観測した超過がendpointまで続く」という条件付きの下限である。transientでは、注入窓（2 rankでは転送数の差で約3〜4呼出し）が劣化検出の窓（2呼出し×連続2）と同じ長さなので、検出の直後に回復した。それでも移行を選び、全試行で約0.05〜0.2秒遅くなった。recoveryでも、実gain 0.64秒は予測下限2.31秒を下回った。
  - **既存のS2候補との違い。** compute注入のtransientは2呼出しで、検出前に終わっていた。
  - **今後の判断。** 劣化の持続を将来へ外挿しないことは、検出側のhysteresisに依存している。移行のように一回きりの費用がかかる候補では、より長い持続の証拠を要求するかどうかを決める必要がある（未決）。
- **LinuxのVERIFY失敗。** 移行後の呼出しが0.083〜0.102秒で、profileのupper 0.073秒を超えた（durable commitのばらつき）。VERIFY_FAILEDとして報告され、隠れて成功扱いにはならない。profileのupperは、校正runの最大に10％を足しただけなので、装置状態の切替を含まない。
- **O_DIRECT。** 6/7が切り替え、VERIFYは3件成功した。baselineが`INSUFFICIENT_BASELINE`になったtrialでは、予測下限（9.5／8.7秒）が実gain（3.1／5.1秒）を上回り、保守的でなかった。注入前のprefixが揺れると、service下限の推定が崩れる。
- **fingerprintとcontainer。** probeの`host_id`はhost名のhashなので、`docker run`のたびにhost名が変わると、同じ装置でもprofileが拒否される。ent Dockerでは`--hostname`を固定して実行した（[Linux開発環境](linux_development.md)に追記）。Macで校正したprofileは、Linuxでは構成不一致として拒否された。

## 残件

- **劣化の持続。** 一回きりの費用がかかる移行で、劣化の持続をどこまで要求するかが未決である。transientでの無益な切替は、この判断次第である。
- **profileの幅。** Linuxの装置状態の切替を含む区間になっていない。また、baselineが揺れると予測下限が保守的でなくなる（O_DIRECT）。

- **production。** production（注入なし）で劣化を検出する経路は無い。evaluationの注入下でだけ動く。
- **profile。** profileは、校正runと同じclass（角度、policy、engineまで）にしか答えない。
- **適用中の故障。** 実processのkillによる段階別fault（swap中、snapshot中）と、cross-hostが残る。
- **候補の範囲。** 3 rank以上や複数bitの移行、tile候補（P4-006）との組合せは対象外である。
