# P4-008：勾配方式の自動校正と安全な選択

状態：IN_PROGRESS（2026-10-14）。依存：001、004、009。cost countは002、cache共有は007。共通契約は[全体設計](../phase4_remaining_design.md)。


明示actual calibration、strict envelope adapter、runtime attachmentと未認証fallbackを実装した。認証済みauto性能、包括admission、generator／Frame／replan／別hostは残る。 [実装範囲と検証](../phase4_validation_implementation.md)。

[追加実装](../phase4_safety_monitoring_implementation.md)で、明示resource_probeによる両peakとcertifierのRAM／disk upperをheld-out rawから再検証する。unknownな資源値を0で補わず、観測maxを保証へ昇格しない。

## 現状・差分

[gradient_scheduler.py](../../src/svsim/gradient_scheduler.py)はMethodCostをcallerから受け、適用可能な三方式を選ぶ。不足時はadjointへ戻す。[phase4_gradient.py](../../src/svsim/phase4_gradient.py)はadjoint/recompute/occurrence shiftを実行し、actual layout/historyを扱う。async gradient呼出しは主にRAM budgetだけを渡す。

新規予定：`gradient_calibration.py`、`gradient_cost_adapter.py`。costを実method completionから生成し、disk/host/COW/prefix/2 streamのadmissionと接続する。既存explicit methodと手動MethodCost APIは維持する。

## Applicabilityと同一性

method比較のcontextはsource/initial/Binding/witness、observable hash・group/upper-X分布、requested parameter/occurrence、n/b/P、Frame/layout/actual tape、policy/backend/build、prefix/checkpoint、精度。parameter数だけが同じ別回路へwhole-method costを転用しない。

adjointはunitary actual tapeとobservable/reverse workspace、recomputeはorigin/checkpointと同branch/source replay、shiftはsource occurrenceのgenerator spectrumと合法ruleを要求する。shared Exprは各occurrenceの角度derivativeをchain factorとして合算する。controlled rotationは既存認証ruleのみ、controlled PAULI_ROTや未知generatorは拒否。finite differenceはoracle。

## 校正と費用構築

1. `GradientCalibrationSpec`でrequested method、case、exact context、scratch、RAM/disk/wall、warmup/sampleを固定する。明示calibration command/jobだけでtrialを実行し、通常gradientを呼ぶたび三方式を勝手に試さない。
2. whole-method wallとforward/reverse取得、Frame、MPI/reduction、Oψ、checkpoint/restore/recompute、shift jobs、prefix/COW/cleanupを収集する。
3. whole-method cellが完全一致すればそれを一回採用する。そうでなければcoverage付きcomponent countを使い、unknown成分の総costはnull。parameter/shift数で未検証の線形外挿をしない。
4. methodごとのmanaged/RSS/host、disk append-only、retained prefix/checkpoint、pin、job concurrencyとobserved peakを保存する。校正runでcleanupしたpeakと全iteration保持scenarioを分ける。

`MethodCostEnvelope`にはruntime CostEnvelopeと同じ品質・scope・条件・raw参照を持たせる。既存MethodCostのconfidence=1既定値を統計認証として利用しない。caller手動confidenceはmanual/injected referenceに分類する。

## 選択と切替

まずrequested endpointに合法なmethodと資源feasibleなmethodを絞る。matched measured envelopeが必要候補で揃えばconditional upperの最小を選び、pilot・prefix構築・method切替費用も含める。必要baselineが欠ける場合はauto比較を行わない。

unknown cost時のfallbackはadmissionできるadjoint。adjointが資源的に不可能でrecomputeだけ合法なら、証明されたminimum workspace/checkpointと明示fallback reasonでrecomputeを選ぶ。資源もunknownなら`INSUFFICIENT_RESOURCE_BASELINE`、どれも不可能なら拒否する。「costがないからfeasible」を自動運用へ継承しない。

method選択はjob開始または検証済みgradient segment frontier。部分gradientはobservable/source occurrence/parameter/Binding/method/actual-stepのkeyでledger管理し、切替後に重複加算しない。途中method切替の変換が未証明ならjob開始だけをsupportし、mid-segment candidateを拒否する。

replan後のadjointはsemantic index逆順ではなくactual historyのinverse layoutとphysical gateを使う。origin/prefix leaseとshift workspaceは明示保持し、shifted angle overrideをsemantic checkpointとして保存しない。parent/childの共有budgetと各diskをaggregateでadmitする。

## 受入・分割

| 段階 | 必須検証 |
|---|---|
| 008A applicability/admission | generator、open control、shared Expr、witness、unknown cost、adjoint不足、全method不足、disk/RAM/host |
| 008B actual calibration | 三方式whole wall/各stage、prefix構築/cleanup込み、raw検証、context差拒否、quantityの二重加算なし |
| 008C auto/replan | fixed methodとのpaired比較、S2/fused inverse、checkpoint/recompute、segment provenanceと重複防止 |

state/expectation1e-10、analytic gradient1e-9、finite difference step sweepを行う。autoのmethod選択理由、実時間、ETA誤差、RAM/disk peakを保存し、手動costと数値一致だけで性能を完了としない。Mac P=1/2/4と許可後の実別hostでbaseline/gradient/replanの同一性が揃ってDONE。
