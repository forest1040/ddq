# P4-002 observed ETA viewのheld-out評価

2026-10-15。runtime ETAの`observed`view（`RuntimeControl.update_eta`、実run内のrolling測定costだけで残り全体を価格付けする）を、実際の残り時間と比べた。スクリプトは`examples/p4_002_observed_eta.py`、集計は`analyze.py`。src/は変更していない。

## 条件

- workload6種。固定layerのL回繰り返し4種（`rx-ry-low` n=6 b=2、`upper` n=7 b=3でtargetは全てb以上、`cz` n=6 b=2、`mixed` n=8 b=3でH/RX/RY/RZ/CX）と、layerごとに固定seedで角度が変わる2種（`rx-ry-var` n=6 b=2、`mixed-var` n=7 b=3、angle-pooled fallbackの確認用）。全て120〜150 gate、lookahead 4で30〜37 epoch。
- policy S0／S2、store file／RAM、engine cooperative（workerはMacの1 rankのみ、trial 1回）、rank 1／2（COMM_WORLD）。`metadata_reserve_bytes=8 MiB`、`ram_budget_bytes_per_rank=64 MiB`、構築直後に`configure_measurements(max_bytes=2 MiB)`。
- 1 trialにつきETA checkを1回。`materialize(frontier=k)`（kは終端の40／60／80%）→`update_eta(endpoint_frontier=last, collective=True)`→`materialize()`の実時間（rank MAX）を残り時間とした。checkは時間外。(workload,fraction)ごとに3 trial。期待値はrank MAX、lower／upperはrank間のmax。
- 「answered」はobserved viewの期待値がnullでないcheck。誤差は|予測−実測|/実測をansweredだけで集計し、coverageはlower≤実測≤upperの割合。unansweredは分母に含めて「answered %」で示す。

## 実行コマンド

Mac（リポジトリ直下）：

```sh
uv run python examples/p4_002_observed_eta.py --output docs/internal/p4_002_observed_eta/mac/cooperative-p1.json --scratch $S/a --engines cooperative
uv run mpirun -n 2 python examples/p4_002_observed_eta.py --output .../mac/cooperative-p2.json --scratch $S/b --engines cooperative --overhead-pairs 5
uv run python examples/p4_002_observed_eta.py --output .../mac/worker-p1.json --scratch $S/c --engines worker --trials 1 --overhead-pairs 1
uv run python examples/p4_002_observed_eta.py --output .../mac/midepoch-p1.json --scratch $S/m --stores ram --policies S0 --trials 1 --workloads rx-ry-low,upper,cz,mixed --mid-epoch --skip-overhead
```

Linux（ent、Docker `ddq-linux:dev-20261014`、`~/ddq-p4002`へrsync。4 CPU・8 GiB・network none、コードはread-only mount、scratchはホストext4）。内容は`docker run ... uv run --offline --no-sync [mpirun -n 2] python examples/p4_002_observed_eta.py`で、引数は次のとおり。4 jobを同時に流した。

```sh
--stores ram --skip-overhead --workloads rx-ry-low,cz,mixed-var          # buffered 1 rank／2 rank
--stores file --policies S2 --workloads rx-ry-low                         # buffered 1 rank
--backend direct --stores file --policies S2 --workloads rx-ry-low --skip-overhead   # O_DIRECT 1 rank
--stores file --policies S2 --workloads rx-ry-low --trials 0              # overhead（上の群の終了後に単独）
```

環境：Mac＝macOS 26.7.1 arm64、Python 3.12.13。Linux＝kernel 6.8.0-138、Docker内Linux glibc 2.36、Python 3.12。

## srcのrevision

Macの1 rank cooperativeは`d55c7e3`で実行した。同2 rank・worker・mid-epochはその後`c17e36d`（P4-009の予約回収。ETA経路に差分なし）の上で実行した（JSONの`env.git_rev`）。Linuxは`d55c7e3`のsrcをrsyncした（imageにgitが無いため行JSONLにはenvが無く、overhead JSONの`DDQ_GIT_REV`で記録）。全て`9f23637`・`d55c7e3`以降である。

## 結果

### ETA check（observed view）

| 環境 | checks | answered | median | p95 | worst | coverage |
|---|---|---|---|---|---|---|
| Mac 1 rank cooperative（6 workload×S0/S2×file/RAM×9） | 216 | 78（36%） | 5.8% | 21.1% | 82.7% | 82.1% |
| Mac 2 rank cooperative | 216 | 63（29%） | 7.9% | 56.3% | 83.8% | 87.3% |
| Mac 1 rank worker（trial 1） | 72 | 27（38%） | 6.5% | 55.3% | 66.9% | 81.5% |
| Linux buffered 1 rank RAM（3 workload） | 54 | 16（30%） | 3.7% | 24.7% | 46.6% | 93.8% |
| Linux buffered 2 rank RAM | 54 | 8（15%） | 8.1% | 14.2% | 14.4% | 100% |
| Linux buffered 1 rank file（rx-ry-low S2） | 9 | 5（56%） | 3.8% | 41.6% | 50.4% | 80% |
| Linux O_DIRECT 1 rank file（rx-ry-low S2） | 9 | 8（89%） | 3.9% | 39.8% | 56.4% | 87.5% |

answeredだけのmedianは30%目標内だが、答える割合が低い。内訳は`python3 analyze.py <json>`（workload・policy・store別、unknown原因）を参照。Mac 1 rankのworkload別answered：`cz` 75%、`rx-ry-low` 50%、`mixed` 44%、`upper` 39%、`mixed-var` 8%、`rx-ry-var` 0%。

### mid-epoch（private epoch、1 rank、RAM、`after_executor`内の非collective check）

32 check（4 workload×S0/S2×60／80%×2）、answered 22（69%）、median 4.6%、p95 9.8%、worst 13.2%、coverage 100%。全て負（過小）側で-0.6〜-13%。実測側はcheck自身の時間を引いた。

### monitor overhead（S2、file、cooperative、eta=True対False、warmup 1＋5 pair）

| 環境 | workload | median差 | 比 | 備考 |
|---|---|---|---|---|
| Mac 1 rank | rx-ry-low | +0.044 s | 1.044 | pairは全て正 |
| Mac 1 rank | upper | +0.101 s | 1.022 | 同上 |
| Mac 2 rank | rx-ry-low | -0.672 s | 0.61 | pairが-1.4〜+0.15 sにばらつく。判定不能 |
| Mac 2 rank | upper | +0.134 s | 1.042 | pairが-0.86〜+2.9 s。判定不能 |
| Linux buffered 1 rank | rx-ry-low | +1.44 s | 1.17 | pairが-27.8〜+30.1 s（装置状態の切替）。判定不能 |

Mac 1 rankでは2〜4%の増加。それ以外はjitterが差より大きく、overheadの受入には使えない。Linuxのoverheadは1 workloadのみ。

## unansweredの原因（検証済み）

per-class診断（各checkで、window＋futureの各classを`observed-task-seconds`で解決し、`cost_decisions`の理由を記録。JSONの`class_diag`）から：

- **認証されないclass。** Mac 1 rankの138件のunansweredのうち、classの判定が`LOW_HELD_OUT_COVERAGE`を含むものが85行、`INSUFFICIENT_SAMPLES`が45行、`DRIFTING`が10行（1行が複数を持つ）。2 rankでは88／49／42行。実行が30〜40 epochと短く、1 classあたりのsampleが少ない（burn-in 3件を捨て、認証に8件必要）ため、前半と後半の水準差で保持coverageが0.8を割る。`MISSING_COST:future:boundary`（epoch boundary）が最も多く、全unanswered行に現れる。boundaryは1 epochに1回で、runの前半は個数が少なく、水準が動きやすい。
- **`cz`のLinux 2 rank（answered 0）。** 窓内のCZ class（`window:3`／`future:3`）が`LOW_HELD_OUT_COVERAGE`で拒否された。2 rankの同時実行でCZのper-call時間が揺れたことが原因とみられる（per-call時間の系列は未保存）。
- **`rx-ry-var`のunanswered（Mac 36行、全て）と`mixed-var`の一部。** `FUTURE_CLASS_LIMIT`により将来のexecutor shapeが確定せず（`future_complete=False`、`FUTURE_EXECUTOR_SHAPE_UNKNOWN`）、tailのsemantic classで価格付けするため、classが多数に分かれて認証されない。`rx-ry-var`（epochごとに4つの別角度）を1 rank・S0で直接捕捉して確認した。`mixed-var`は一部のcheckでshapeが確定し（`future_complete=True`、31 class）、そこでは答えた。
- **angle pooling。** 全checkのclass解決でpooled-angleが答えたのは、Mac 1 rankで608件（exactは1,356件）、2 rankで1,130件（同2,462件）、Linux buffered 1 rank RAMで114件（同264件）、2 rank RAMで211件（同490件）。`mixed-var`ではpoolingでclassが認証されるが、answeredは36 checkのうち3件（1 rank）と1件（2 rank）で、残りは別classの認証失敗かshape未確定である。pooling件数は「task class」を単位とし、1 checkで複数回数えられ、rank数でも倍になる。
- **fraction効果。** Mac 1 rankのanswered率はfrontier 40%で32%、60%で36%、80%で40%。後ろほど増えるが差は小さい。

## 外れ（worst）の原因

Mac 1 rankのworst 82.7%は`rx-ry-low` S2 file、60%のcheck（予測0.48 s、範囲0.43〜0.58 s、実測2.76 s）。2 rankのworst 83.8%は`rx-ry-low` S2 RAM、60%（予測0.21 s、実測1.27 s）。上位の外れは全て`rx-ry-low`／S2の過小予測で、同じ条件の他trialの残り時間（約0.2〜0.5 s）の数倍に実測が延びた個別trialである。つまり予測が誤ったのではなく、check後に実行が停滞したtrialで、intervalの上限も超えた（file）か広すぎた（RAM、上限6.8 s）。停滞の原因（OSのスケジューリング、他process、fsync）は計測していないので未特定である。Linuxのworst（47〜56%）はfile storeの装置状態切替と整合するが、状態を直接測っていないため推定である。

## 制約と注意

- Linuxは、file storeが極端に遅い（S0・rx-ry-lowの残りが85〜110 s、Macは2.4 s）ため、file storeを`rx-ry-low` S2だけ（trial 9）に縮小し、RAMも3 workloadに絞った。4 jobの同時実行で、時間はjob間の干渉を含む。最初の全workload実行（40分上限、全trial 3回）は、時間内に終わらないと判断して打ち切った。Linux 2 rank file、`upper`／`mixed`／`rx-ry-var`のLinux実行は未実施。
- Linuxの結果JSON（要約付き）は`overhead-buffered-p1.json`のみ。他は終了時にcontainer内の`git`不在で例外が出たため（のちに`DDQ_GIT_REV`と例外処理を追加）、行JSONL（`*.rows.jsonl`）が一次証跡で、`analyze.py`が直接集計する。
- worker engineはtrial 1回、`overhead_pairs=1`で、受入の対象にしていない。
- 30%のmedian目標は、answeredの範囲では環境によらず満たした。ただし、answeredの割合が15〜56%（O_DIRECT fileは89%だが9 checkのみ）で、「残り時間を全run中の任意の点で答えられる」とは言えない。

## 修正後の再測定（Mac 1 rank、2026-10-15）

上の分析で分かった2つの原因を直した（`remaining_work.py`）。
- **future classの上限。** futureのclassを、角度を除いたgate形状で集約する。角度だけが違うclassは`pooled-angle`として価格付けし、角度poolingの認証済みcostで答える。その結果、`FUTURE_CLASS_LIMIT`は出なくなった。
- **epoch boundaryの費用。** semantic tailがclass上限で溢れると、epochごとにtailとfutureを最初から走査し直していた。そのためboundaryの費用が残りの量に比例し、12 msから5 msへ変化してdriftになっていた。溢れた後は新しいclassを足さないようにし、overflow件数も前進分だけ引くようにした。修正後のboundaryは4〜5.4 msでほぼ一定である。

`examples/p4_002_observed_eta.py --workloads rx-ry-var,mixed-var,rx-ry-low,mixed --skip-overhead --trials 2 --engines cooperative`（S0/S2 × file/RAM × 40/60/80％）の結果は次のとおり。[JSON](mac-rerun/incremental-overflow-4workloads.json)

| workload | answered | median ｜誤差｜ |
|---|---|---|
| rx-ry-low | 21/24 | 7.8% |
| mixed | 12/24 | 17.5% |
| rx-ry-var | 9/24（修正前0/36） | 6.2% |
| mixed-var | 6/24（修正前3/36） | 4.8% |
| 全体 | 48/96（50%） | 6.6%（p95 19.7%、worst 22.3%、coverage 93.8%） |

- **残る未回答。** 主にclassの認証失敗（`mixed`の複数class、`*-var`のboundaryとfuture class）である。runが30〜40 epochと短いことが効いている。
- **認証規則。** 認証規則（burn-in 3、8件、held-out coverage 0.8）は緩めていない。

なお`pooled-future-var.json`は、future classのpooling後、overflow修正前の中間結果である（`rx-ry-var` 4/24）。
