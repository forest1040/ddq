# Phase 5 計測・時間推定・比較評価の検証記録

2026-10-14。`flow.sh`（Mac、zsh）と`flow_linux.sh`（ent Docker、bash）は、probe → `calibration-suite` → `benchmark` → `calibrate-time` → held-out 4件の`estimate`／`benchmark` → `report`を順に実行する。raw run・plan・model・campaign・reportを保存する。probeとrunにはpathやhost情報が含まれるため内部資料とする。

## Mac（`mac/`）

1 rank、b=4、RAM store、buffered、OMP等1 thread。suite 52件（5 class × n=5..8 × count 1/4/12、空回路を含む）を各3回、held-out 4件を5回測った。全run completedでoracleに合格した。最終版（model v2）でheld-out 3件のmedian |相対誤差|は0.42%、worstは2.0%、interval coverageは3/3。sync probeは0.021〜0.032 ms。`held-c`はjoint RXが未校正classなので`NO_TIME_PREDICTION`となった。`held-b`はgate数が校正範囲外で、extrapolatedと表示される。

## Linux（`linux/`）

ent Docker（4 CPU、8 GiB、network none）。最初にMacと同じsuiteを600秒の上限で実行したところ、cooperative deadlineで全caseが`partial`になり、model入力から正しく除外された。同じphaseでMacと比べると、executeが約24倍遅かった（pass当たり約160 ms、Macは約6.6 ms）。ext4上で、catalog／storeのcommitごとのdurability syncが原因と推測しているが、未検証である。縮小suite（n=5..7、count 1/4、27件×3回）で再実行し、全runがcompletedでoracleに合格した（`linux/reduced/`）。同時にPhase 5関連テスト36件もcontainer内で全て成功した。

| held-out | 予測expected [lower, upper] 秒 | 実測median 秒 | 相対誤差 | interval | evidence |
|---|---|---|---|---|---|
| held-a (n=7) | 5.44 [5.08, 6.90] | 5.81 | -6.3% | 内 | extrapolated（gates／passes） |
| held-b (n=8) | 13.83 [13.09, 17.28] | 27.49 | -49.7% | 外 | extrapolated（blocks等全feature） |
| held-c (n=6) | n/a | 2.88 | n/a | n/a | joint RX未校正 |
| held-d (n=8) | 6.64 [6.18, 8.38] | 6.73 | -1.4% | 内 | extrapolated（blocks／gates／passes） |

held-outのmedian |相対誤差|は6.3%で30%目標内だが、worstは49.7%、interval coverageは2/3だった。Linuxではexecuteのtrial間ばらつきが大きい。例として、held-dは22.7／5.95／5.96秒、held-cは8.61／2.25／2.04秒、held-bは25.2／14.3／25.6秒だった。ホストのload averageは約1.3で、他containerのCPU使用は小さかった。H local・n=8でcountだけを変えた追加測定（`linux/growth/`、64／192／384 pass、各2回）では、pass当たりのexecuteが49.6／84.9／54.7 msとなり、単調な超線形増加ではなかった。g12の2 trialも9.96秒と22.63秒に分かれた。したがってheld-bの過小予測は、主に間欠的な遅いmodeが3回中2回出たことによると考える。校正medianの多くは速いmodeだった。遅いmodeの原因はI/O／durability側のstallと推測しているが、未検証である。

Macではpass当たり約6.3〜6.8 msでサイズに対して平坦で、このbimodalityは観測されていない。Linuxで時間モデルを使うには、次が必要である：repetitionを増やしてtailを含む分布を取ること、stallの原因をtrace／syscall計測で特定すること、bimodalな分布ではmedianではなくp95等の上側modelでconditional upperを作ること、校正範囲を予測対象のblocks／passesまで広げること。現modelのconditional upperはこのstallを保証しない。

## Linux：遅いmodeの原因と model v2

**原因の切り分け。** 同一case（H local、n=8、12 gate）を6回cProfileした（`stall_profile.py`）。self timeのほぼ全てがSQLiteの`execute`（約27,500回）と`commit`（858回）だった。速いtrialは9.5秒、遅いtrialは37秒で、1 commit当たり2.2 ms→10.3 msと約4.6倍になった。catalogは`synchronous=FULL`・`journal_mode=DELETE`で、transactionごとに複数のfsyncを行う。4 KiBのpwrite+fsync（`fsync_latency.py`）では、Linux（ent NVMe ext4）が約5 ms、Macが0.02 msだった。SQLiteのautocommit insertはDELETE/FULLで10.6 ms対0.22 ms、WAL/FULLで5.5 ms、WAL/NORMALで0.02 msだった。macOSのfsyncは装置cacheをflushしない（F_FULLFSYNCではない）。**Macの時間はdurability費用を含んでいない。**

**状態の相関。** 各trial前に16回のfsync probeを取るよう変更した（時間外）。同一caseを8回測ると、probeが約5 msのtrialはexecute 36〜37秒、約0.8 msのtrialは9〜10秒で、装置は2状態の間を切り替えていた（`linux/sync-probe/`）。

**model v2**（`linux/v2/`、n=5..8・count 1/4の35件×3回、held-out 5回）。work featureに加え、feature×trialのsync probeを説明変数とし、trial単位でNNLS適合する。lower／expected／upperは、校正時sync latencyの最小／median／最大で評価する。校正期間のsyncはmedian 0.82 ms（最大5.5 ms）で、速い状態が中心だった。held-out期間では遅い状態が約4割のtrialで出たため、expectedは-49〜-60%の過小予測、coverageは3/3（upperは約10倍の幅）だった。trialのprobe値を与えたconditional予測は、状態がtrial中に変わらない場合+2〜+19%で一致した。大きく外れたのは、probeが速い状態でもtrial中に遅くなった例（-49〜-56%）である。

modelの構造は妥当で、外れの原因は装置状態の非定常性にある。estimateでは`sync_scenarios`（速い／遅い状態の総時間）を常に出す。校正のsync幅が2倍を超えると`DEVICE_SYNC_LATENCY_NONSTATIONARY`を警告する。実行直前のprobe値は`--sync-latency-seconds`で渡せる。`linux/v2/`のplanはこの出力追加より前のコードで作ったため、これらのfieldを含まない。
