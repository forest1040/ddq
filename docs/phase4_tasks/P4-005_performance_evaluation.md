# P4-005：Phase 4完了条件の性能比較・trace report

状態：IN_PROGRESS（2026-10-14）。依存：001〜004、006〜009。011は移行の完了証跡へ、010は利用可能環境のnative証跡へ接続する。共通契約は[全体設計](../phase4_remaining_design.md)。


manifest／raw測定hash／rank MAX／resource検証、全metricのpaired統計と有限Mac runnerを実装した。large state／tile／cache／control／lifecycle／別hostの最終matrixは残る。 [実装範囲と検証](../phase4_validation_implementation.md)。

## 現状・成果物

既存[Mac比較](../phase4_benchmark.md)は小回路のsync S0／async S0／async S2で、改善の主因はlocal fusion。既存cluster reportはcorrectnessと小規模経路検証。比較範囲を広げ、unsupported/拒否/悪化を含む完了matrixを作る。

変更対象：`examples/phase4_benchmark.py`、`examples/phase4_benchmark_worker.py`、`development_runner.py`。新規予定：`phase4_evaluation.py`、case manifest、raw/run/comparison validator。Phase 5の将来の一般benchmark/report CLIへreuseできるAPIにするが、その全機能実装をPhase 4の前提に追加しない。

## Case manifestと区間

source/initial state/hash、Binding/exact witness、observable/parameters、n/b/P、host/volume/backend/build/thread、policy、RAM/disk/host/drain、scope、seed、warmup/trials、injection、測定区間、精度・事前acceptanceを固定する。variant間でsame-case identityを検査する。

primary end-to-endはinitial import/compile/bind/plan→forward→指定endpoint→durable結果→cleanupとする。steady-state区間も補助として返す。環境配布/uv installは別startup時間で保存し、disk quotaには含める。monitor/planning/pilot/drain/migration/commitをprimaryから落とさない。warmup/trial用scratchは分離し、cache状態を規定する。

同じaxisの比較ではそれ以外の条件を固定する。sync vs asyncで対応しないmodeはunsupported rowとして保存する。全軸を一度に変えた差を一つの最適化効果と呼ばない。

大きいstateの検証でも全振幅debug gatherでrank budgetを超えない。小nは独立NumPy oracle、大nは同sourceの同期referenceとBlock/tile streaming比較・observable/reductionを使う。oracle実行を測定外にする場合は全variantで同じ区間定義とし、candidate内のvalidation費用は測定から外さない。

## 必須matrix

| group | 対照・variant | 必須endpoint/負荷 |
|---|---|---|
| layout/compiler | S0/S1/S2、local sequential/fused | 同semantic source、actual tape、全bit階層、commuting境界 |
| physical swap | ordered単独/fused full/tile | pair overlap、inverse、Frame、COW、pass/read/write/network |
| execution | sync/async、worker/cooperative | rank shard payloadがmanaged budgetを超えるMPI＋file case |
| control | fixed/observe/recommend/LIVE_TUNE/frontier replan | 003のpositive/negativeと同injection |
| batch/cache | naive/plan/prefix、LRU/future-cost | 複数Binding、合法prefix共有、writer/reader競合、active job公平性 |
| gradient | fixed三方式/auto | 同observable/parameter精度、2 stream、recompute/shift/prefix費用 |
| lifecycle | checkpoint/resume/lease/GC、011移行 | decision前後fault、owner-local access、partial保存の扱い |
| transport | actual packed/full、Linux worker/direct | 同payload支持条件、same-host/cross-hostを別集計 |

small gate/oracle gridから始め、n/b/Pとbudgetを有限candidateから選ぶ。全直積を無制限に生成しない。mandatory groupごとに実行可能な代表caseをpreregisterする。予算不足で未実施ならそのgroupを未達として残し、小case成功で大caseを置換しない。明示したunsupported gate/routeはreason付きで保存する。

MacはP=1/2/4、RAM/file、buffered/F_NOCACHE、stateより小さいrank managed budgetを含む。別hostは許可後の2 node/2・4 rankでowner-local filesystem、Linux/O_DIRECT、actual link/MPI環境を記録する。同一MacのMPIをnetwork scalingへ外挿しない。

## 観測と統計

| 分類 | 保存するmetric |
|---|---|
| time | compile/bind/plan、queue/progress、kernel/Frame/packing、storage/fsync、MPI/control、observable/gradient、checkpoint、monitor/migration、end-to-end |
| work | semantic/native/fused count、actual source order、swap pass、read/write logical/aligned bytes、send payload/metadata |
| resource | managed、RSS/PSS可否、cache/resident/COW/rollback/pin、host/device disk、reservation/unused prefetch、job wait |
| adaptation | detection latency、false decision、switch数、gain予測/実測、ETA snapshot error、quality/scope |
| numerical | phaseを合わせないstate/expectation、gradient、reduction order、source/witness/actual tape一致 |

各stageの重なりを示し、stage wall SUMをend-to-end wallと同一視しない。rank wall MAX、CPU/resource SUM、peer不均衡を分ける。warmup2・paired7を初期設定にしてseed付き交互順序、median/p95/min/max/run variationを保存する。p95少数trialはdiagnostic。

比較はpaired end-to-end差とratio、手法付きbootstrap interval、絶対差を出す。0近傍はratioを無理に計算しない。ETAは同snapshotからの実残時間でmedian/p95/worst/absolute、coverage/unknown、hold-out classごとに評価する。平均誤差だけを良くするcase除外は禁止。

## Report・運用

`comparison-report` kindにcase/variant/provenance/profile ID、raw/hash、status（completed/failed/unsupported/insufficient-budget/unmeasured）、metric/差/qualityを保存する。公開summaryと内部host/path/rawを分離し、匿名化後も条件差とhash対応を維持する。failure/拒否もsummary表の行として出す。

launcherはJobSpecでthreads/ranks/RAM/環境込みdisk/wall/scratchをpreflightする。測定同士を重ねず、background loadを記録する。クラスタはcaseと上限をreview後に許可を確認し、PBS scriptをrenderしてから投入する。caseが許可時間を超える場合は分割trialまたは未実施として、sample品質と別job間変動を残す。

005A validator/manifest、005B Mac各axis、005C adaptive/gradient/cache統合、005D許可後別host＋最終matrixに分ける。必須groupの数値・予算・fault・費用込み比較、改善/悪化両case、再現raw/reportが揃ってDONE。Phase 4完了は他IDの受入も別に確認する。
