# Phase 4：注入・paired report・勾配校正のMac検証

日付：2026-10-14。対応は[P4-003／005／008の先行実装](../../phase4_validation_implementation.md)。今回cluster／SSH接続・PBS job投入は行っていない。公開時の扱いはrepositoryのinternal資料方針に従う。

[結果表](results.md)と[機械可読summary](selected-results.json)に、完了した6構成、raw hash照合、warmup84／本測定294 trial、初回時間切れ2件を保存した。勾配context取得とauto確認の12 trialは別枠。P=4再実行はbuffered／directとも完了した。

## 再現条件

既存uv lock、Python、local MPIのMac環境を使用する。actual caseはn=4、b=2、P=1/2/4、complex128、owner別のlocal FILE、要求backend buffered／direct、cooperative・1 thread。Macのdirect要求はF_NOCACHEを使う既存実装へ渡す。source／worker／native C/header／uv lock hash、Python／MPI／platform、thread環境はrawへ保存する。sampleから将来の予測区間を作らない。

```sh
UV_CACHE_DIR=/private/tmp/ddq-uv-cache uv run --offline --no-sync python \
  examples/phase4_followup_matrix.py \
  --output-dir docs/internal/phase4_validation_followup/mac-matrix-20261014 \
  --scratch /private/tmp/ddq-p4-followup-matrix-20261014 \
  --repetitions 7 --warmup 2 --wall-seconds 300
```

LocalDevelopmentRunnerは最大同時job数1。host RAM512 MiB、managed RAM8 MiB／rank、SSD64 MiB／rank、job hard wall300秒を宣言する。trialごとのscratchをclose後に削除する。soft campaign deadlineは各trial／callback返却後に確認し、期限不足はunmeasuredへ残す。in-progress callbackを強制終了する保証ではない。process／MPI／OS RSSはmanaged peakの測定scope外で、host RAMは余裕付きの運用予約である。

main matrixは上記6構成を逐次実行し、他の検証と重ねない。各構成はlayout S0/S2を別axis、同じsourceのfixed/autoを別caseとしてpaired比較する。warmup2、measured7。gradientsは三方式ごとにwarmup2／measured7を別trialで実行する。calibration context取得とraw-profile auto fallback確認は反復数の外に明示した追加trialである。

最初のmatrixではP=1/2の4構成が完了した。P=4の2構成はparameter-shiftの終盤でjob hard wall300秒に達し、FAILEDとなった。[初回matrix](mac-matrix-20261014/matrix.json)とjob reportを残し、成功したcaseだけを再利用する。P=4はRAM／SSD／回路／反復数を変えず、独立scratchと600秒のhard wallで再実行し、[両caseが完了](mac-p4-retry-20261014/matrix.json)した。

```sh
UV_CACHE_DIR=/private/tmp/ddq-uv-cache uv run --offline --no-sync python \
  examples/phase4_followup_matrix.py \
  --output-dir docs/internal/phase4_validation_followup/mac-p4-retry-20261014 \
  --scratch /private/tmp/ddq-p4-followup-retry-20261014 \
  --ranks 4 --repetitions 7 --warmup 2 --wall-seconds 600
```

resolved backendのtrial telemetryは今回欠損しており、rawの`resolved_backends`は空配列である。async I/Oが別途所有するhandleをこのcollectorから取得できていない。要求backendとbackend実装の回帰は記録できているが、raw単体で各trialのF_NOCACHE適用を認証するものではない。今後はtraceのhandle identityから取得する。hard wall終了時の残存scratchは失敗資料用で、別jobへ再利用しない。

## 測定区間と数値

paired primary wallはconstruct、gate登録／bind、forward、eager state検証、durable checkpoint、close、scratch cleanupを含む。steady execute wallは別metricでpaired統計を返す。Python import／MPI startupはprimary前の外部起動であり、method内部のmigration／pilot／validationを落とす理由にはしない。rank wall MAX、send/read/write SUM、managed／scratch MAXを分ける。background loadも保存し、OS cacheや外部jobを強制制御した測定とはしない。

stateのrtol／atolは1e-10。rawにmanifest hash、source hash、trial ID、scope、validator、rank wallとresource metricを含めhashで照合する。sourceはactual GateIRを事前生成して固定する。RAM／SSD違反、scope差、欠損、failed trialは比較に採用しない。

compute注入は同じ12個のRX source、同じsemantic prefix、work unit law／seedを使う。最初の4 sourceのactual serviceからevaluation専用baselineを作り、後続にcall当たり0.15秒のseeded jitter、planned delay総量1.5秒以下を付ける。main matrixはsustained case。実serviceとpoll overshoot込みwaitを分離する。fixedはobserve、autoは有限候補検索するがtransition costがunknownなので採用しない。injected sampleはproduction costから除外する。

勾配はupper H＋shared Exprの2 occurrence RY、同Binding0.37、同Pauli Z。三方式を`-sin(0.37)`に1e-9、期待値を`cos(0.37)`に1e-10で照合する。actual contextをrunner前後で固定し、whole trial／runner、resource peakとfailureをrawへ保存する。認証済みenvelopeは作らず、production costは0。normal autoもpilotせずadjointへ戻る。

## 回帰と受入の境界

全回帰は **615 passed／2 skipped、522.77秒**：[log](pytest-full.txt)。run開始後にpaired metric／完全なraw hash、worker failure記録、observable generator対応等を補強した。最終対象回帰は **72 passed、88.38秒**：[log](pytest-focused.txt)。Mac内のP=1/2/4 actual campaign、三方式、既存batch／gradient／runtimeを含む。全回帰615件を最終treeの総件数とは扱わない。

初期pilotではGuard plan_idとMPI poll APIの接続不足、resource byteをfloatで渡した不適合を検出した。修正し、P=4のactual state／expectation／gradientと検出／safe-refusalを確認してからmain matrixへ進んだ。pilotを性能統計／profileの資料へ混ぜない。

unitはwork/time／resource driver、cancel／duplicate／clock／overflow／event上限、Guardの持続／瞬間／復旧／baseline不足、manifest／raw／rank MAX／RAM／SSD／scope／pair／statistical bounds、actual context／producer／raw改変／coverage／resource／measurement scopeを確認する。

P4-003／005／008はIN_PROGRESS。actual storage／MPI・time-trigger、positiveな費用込み切替／VERIFY、全mandatory matrix、large state／tile／cache／fairness／fault、認証済みgradient autoの性能、包括host／disk／workspace admission、Linux／cross-hostは今回未達。main matrixでdirectionが確認できても、small same-hostの当該caseの観測だけである。
