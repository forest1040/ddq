# P4-003：持続低下から改善確認までの適応検証

状態：設計／OPEN。依存：001、002、004、009。共通契約は[全体設計](../phase4_remaining_design.md)。Guard/policy APIの存在から、再現可能な正味改善の検証へ進める。

## 現状・変更

現`RuntimeControl.frontier()`はGuardのDEGRADEDでprefetch creditを減らし、VERIFYINGへ移る。caller sample/evidenceの単体試験はあるが、実completion→ETA→候補→切替→実利益が連続する試験は未達。新規予定：`performance_injection.py`、`adaptive_evaluation.py`、`examples/phase4_adaptive_check.py`。controllerの動作は004の契約へ合わせる。

## Caseと注入

`InjectionSpec`はseed、資源class、発火基準、開始/終了、遅延または帯域制約、適用operation、scope、上限を持つ。OS/networkの設定や外部jobへ負荷をかけず、自jobのprogress/サービスだけへ有界に適用する。

| 資源 | 注入方法とpositive候補 |
|---|---|
| storage | 自job I/O queueのtoken/ready制約。先読みとforegroundの争用を軽減するcredit候補 |
| MPI | 合意済みpayload/protocol段階のdelivery遅延。合法なpacked経路/layoutでpayloadを減らす候補 |
| compute | executor境界のbounded service delay。合法fusion/planで実invocationを減らす候補 |

注入は実requestを途中で成功扱いにしない。completionとpin/terminal/ackの契約を維持し、main-thread progressを止める無制限sleepを入れない。rawにactual serviceとinjection追加分を分離して保存する。

発火は二種類のcaseで評価する。work-triggerは同じsemantic prefix barrierを両armに入れ、source reorder/epoch長で注入位置が変わらないようにする。time-triggerは同じrun区間の開始からの経過時間で資源制約を発火し、armの進捗差も結果として残す。両方式を混ぜて「同じ時刻/量」としない。policy名に応じて注入を弱める仕組みは使わず、実request数/bytesへの同じ法則を適用する。

## 実験手順

1. source/initial/Binding/witness/observable、n/b/P、budget、profile/build、測定区間、injectionを固定しcase hashを作る。
2. fixedとautoをseed付き交互paired orderで実行する。初期cache/stateを揃え、arm間でmutable root/observed profileを共有しない。
3. collectorと同condition baselineからGuardを起動する。校正caseと評価caseを分け、評価runの未来を候補へ渡さない。
4. BASELINED→SUSPECT→DEGRADED→候補/LIVE_TUNE→必要時FRONTIER_REPLAN→VERIFYING→STABLEまたは拒否をtraceする。各段階の時刻、cut、費用、condition変更を保存する。
5. end-to-endには監視、探索、pilot、drain、migration、validation、commit、cleanupを含め、最終state/expectation/gradientをreferenceと比較する。

新条件のVERIFYにはmatchedな新baseline/比較workを使う。injected校正は評価registryだけで有効にし、production profileへ登録しない。runtimeから得たcostで候補を生成し、positiveになるcaller evidenceを手書きしない。

## 必須matrix

| case | 期待結果 |
|---|---|
| 持続storage/MPI低下、十分な残work | 有効候補が利益を出すpositiveを各資源で実証 |
| compute低下 | 設計に含む資源として検出/採否を確認。合法候補がなければ拒否を正答とする |
| 短いremaining/負gain/uncertain gain | 切替を抑制、reasonとETAを残す |
| baseline不足/条件不一致/low-quality | 総ETA unknown、性能の自動切替なし |
| 瞬間低下/通常変動 | consecutive window/hysteresisで誤切替を抑える |
| 復旧/再悪化/cooldown | oscillationを抑制、新profile/conditionで再判定 |
| observe/recommend/dry-run | 指定どおり計測/候補保存まで。migration/追加snapshotなし |
| manual変更、失敗、cancel | origin保存、合法性、authoritative old/new cut、pending peer drain |

## 受入判定とreport

positiveはpaired end-to-end差の符号と事前指定した統計評価を保存し、費用込み改善を同条件で再現する。warmup2／paired7を初期設定とし、paired差のbootstrap intervalを手法・seed・sample数付きで出す。少数試行/intervalが0を跨ぐ場合は未確認とし、改善保証を作らない。seedを都合よく選んだcaseだけで合格させない。

検出遅延、false positive、false negative、切替回数、gain予測/実測、baseline/observed/adapted ETA誤差、monitor/planning/migration比率、RAM/pin/disk peakを出力する。時系列eventをcounterfactual gainの証明と取り違えず、fixed対照との差を評価する。

003A fake clock/決定的completion、003B Mac actual MPI P=1/2/4のinjected試験、003C許可後の実別hostでprotocol/適応を検証する。scopeはfake、injected、same-host、cross-hostを保存する。Mac注入による利益を実ネットワーク性能の改善として報告しない。必須positive/negativeとfault再開、費用込みreportが揃ってDONE。
