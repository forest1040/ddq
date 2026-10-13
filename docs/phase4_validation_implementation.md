# Phase 4：適応検証・比較report・勾配校正の先行実装

更新日：2026-10-14。P4-003／005／008を進めた。Lunaが注入driver、比較schema／統計、勾配校正／adapterを担当し、Codexがruntime・actual campaignへ接続した。3項目はIN_PROGRESS。前回の[ETA／候補／tile／cache](phase4_remaining_implementation.md)と同様に、実装した単位と未達の受入条件を区別する。今回はMacだけを使い、クラスタ接続・PBS job投入は行っていない。

## P4-003：有界注入と実completionによる観測

[performance_injection.py](../src/svsim/performance_injection.py)の`InjectionSpec`はresource、work／time trigger、開始／終了、operation、semantic barrier、delay、seed、最大追加時間とevent数を固定する。scopeはruntime-calibration、specは4 KiB、ticketは最大256。ready時刻をpollし、real request自体を完了扱いにしない。cancel／duplicate、nonfinite／overflow、後退するclockを拒否する。

delayには同seedのjitterを適用し、policy名によって制約を変えない。planned delayの累計上限と、poll overshootを含むobserved waitは別metric。actual serviceも分離する。time-trigger／byte比例のstorage・MPI lawはfake-clock driverで検証する。

実runtimeの公開APIは`sim.runtime.configure_performance_injection(spec, observer=...)`。初期対応は **compute・work-trigger・executor-group・call比例**。time-trigger／storage／MPIはactual adapterとして未対応のため拒否する。source countでwork cursorを進め、named semantic prefixを両caseで揃える。source reorderやfusionをまたぐ一般barrier処理は未実装。

gate実行前に有限delayを待ち、その間もI/OとMPIをlocal pollする。発行前の異常はordered voteへ通す。gate成功後にticketをcompleteし、失敗はcancelする。foreground epoch／pin／ackの既存契約を維持する。observerエラーは次のordered voteへ渡す。spec／ticket／snapshot用poolをmanaged RAMへ予約し、closeとshared child budgetで一回精算する。

注入をattachしたsessionのI/O／MPI／executor observationは`evidence='injected'`。production `cost_observations()`から除外し、運用ETAも`EVALUATION_INJECTION_ACTIVE`でunknownとする。通常のI/O Guardへこのsessionを登録しない。注入終了後もsession全体をinjected資料として保持する。

[adaptive_evaluation.py](../src/svsim/adaptive_evaluation.py)はactualな非注入prefixのgroup wallをbaselineにし、その後のservice＋waitをevaluation専用Guardへ渡す。source IDを除いたexecutor shape／layout／Bindingを照合し、異なるclassへbaselineを流用しない。eventは最大64既定、dropped／失敗／baseline不足を理由付きで保存する。

actual campaignは同一の反復RX回路でfixed（observe）とautoを比較する。持続遅延、瞬間遅延、復旧、baseline不足のwork caseを選べる。autoの有限候補とunknownによる拒否を保存し、手書きのpositive ReplanEvidenceは作らない。**今回の結果は観測と安全な拒否の検証**。calibrated transition costとVERIFYが未接続なので、positiveな性能改善を実証したとはしない。

残件：actual storage／MPI注入、time-triggerの全rank protocol、追加barrier、real costによる採用、検出遅延／誤検出の網羅、VERIFY／再悪化／cooldown、durable faultと別hostのpositive／negative matrix。

## P4-005：同一条件とrawを検証するpaired report

[phase4_evaluation.py](../src/svsim/phase4_evaluation.py)に`CaseManifest`、`Variant`、`Trial`、`Campaign.compare()`を追加した。manifestはsource／initial／Binding／witness／observable／endpoint、n/b/P、resource、backend／build、seed、許容誤差、warmup／反復数／時間上限を固定する。

最大16 variant、各100 trial。baselineから一つのaxisだけが変わることを検査する。seedで最初の順序を決め、pairごとに開始caseを交代する。warmupを本集計に混ぜない。completed以外のfailed／unsupported／insufficient-budget／unmeasuredも行として保持する。

raw content hashに加えmanifest hash、variant ID、trial index、scopeを照合する。rank wall vectorはP個、primary wallはrank MAXでなければ比較しない。validator、RAM／SSD peak、時間上限の違反もrawを残してfailed扱いとする。scope差、欠損pairは比較から除外し、改善主張をunconfirmedにする。

median／p95／min／max／variationと、paired absolute difference／ratio、seed付きmedian差のbootstrap intervalを保存する。7 pair未満、欠損pair、intervalが0を跨ぐ場合は未確認。少数trialのp95とintervalは診断値で、予測区間や外挿ではない。overflow／0除算を有限JSONで表現し、rawは16 KiB、reportは4 MiB以下とする。

actual比較は[phase4_remaining_campaign.py](../examples/phase4_remaining_campaign.py)で、S0/S2とfixed/autoを別caseにする。primaryはconstruct→gate登録／bind→forward→state検証→durable checkpoint→close→scratch cleanup。steady execute wallは補助metric。stage wallの合計をprimaryと同一視しない。semantic source一致、eager state、rank MAX、send/read/write、managed peak、scratchとbackground loadを保存する。

[phase4_followup_matrix.py](../examples/phase4_followup_matrix.py)は既存LocalDevelopmentRunnerを使い、P=1/2/4、buffered／Mac F_NOCACHEを逐次実行する。thread環境を固定し、JobSpecでhost RAM512 MiB、managed RAM8 MiB／rank、SSD64 MiB／rank、時間と独立scratchを宣言する。失敗jobとreport内の未検証trialを成功扱いにしない。Python／MPI／OSのRSS全体をmanaged peakとは呼ばず、host予約は余裕を含む運用値である。

残件：stateがrank budgetを超えるpaired case、full／tile、cache／公平性、batch、各control modeのpositive採用、勾配autoの認証済み比較、lifecycle fault、Linux／cross-host。今回のsmall matrixだけでPhase 4完了条件を置換しない。

## P4-008：明示したactual勾配pilotとstrict adapter

[gradient_calibration.py](../src/svsim/gradient_calibration.py)の`calibrate_gradient_methods()`は明示呼出しだけでtrialを実行する。factoryは毎回新しいcontext manager／trialを返し、runner、numeric validator、actual context getter、resource probeを渡す。既定の三方式を個別に測り、warmupを除外する。通常の`gradient()`で三方式を試す処理は追加していない。

contextは[gradient_runtime_context.py](../src/svsim/gradient_runtime_context.py)でsource／initial／Bindingとwitness identity、observable、requested parameter、n/b/P、layout／actual tape／frontier／placement、policy／backend／store、build、origin、Frame policy、complex128、endpointを識別する。actual contextをrunner前後で検査し、factory／enter／runner／validator／close失敗をrawへ残す。

whole-trial wallはsetup／forward準備、runner、validator、context manager cleanupを含む。runner wallとsetup等の残区間は別metric。method内のread／reverse／shift等のstageをwhole wallへ再加算しない。上限はwarmup／測定各100、context256 KiB、report4 MiB。時間はtrial間とcallback返却後に確認するsoft limitで、callbackの途中を強制停止するものではない。resource超過も以後のtrialを止める。

raw min/max、observed resource peakは診断値。cost採用には、独立training ref、model ID、held-out raw ref、測定区間、実coverageとcount、scope／evidence／unit、明示RAM／disk upper、exact contextが必要。[gradient_cost_adapter.py](../src/svsim/gradient_cost_adapter.py)はversioned producerとreport／row hashを再検査し、raw timeからcoverageを照合する。hashは改変検出用であり、署名や測定の外部認証ではない。

normal gradient-call-after-forwardのMethodCostにはrunner envelopeだけを使う。whole-trial envelopeは`MEASUREMENT_SCOPE_MISMATCH`。manual／injected／unknown resourceをproduction費用へ変換しない。通常の手動MethodCost APIはmanual-referenceとして維持する。

`sim.runtime.attach_gradient_calibration(report)`は64 KiB以下の明示reportをmanaged poolへ保存する。attach自体はpilot／state変更を行わない。autoは完全一致・build一致・非注入のqualified costが必要候補に揃う場合だけ既存schedulerへ供給する。不足時は従来adjointと既存executor admissionを使い、`latest_gradient`に理由・method・実wall・context・successを残す。RAM limitと既知disk quotaを比較するが、host／残容量／2-streamの包括的pre-admissionは残る。

actual campaignはshared Exprの二つのRY occurrence、upper bit H、Pauli Z期待値を使い、三方式を解析値`-sin(0.37)`（1e-9）、期待値`cos(0.37)`（1e-10）と比較する。raw reportはnormal autoへattachしても認証済みcostが無いため、pilotせずadjointへ戻る。今回production costは0で、method最適化の利益を実証したとはしない。

残件：全generator／open control／witness matrix、Frame／prefix／2-stream／host-diskの包括admission、stage cellとminimum workspace、認証済みprofileでのmethod選択、held-out実誤差、fixed/auto paired比較、job開始のsegment ledger、replanと別host。

## 検証資料

unit、actual Mac P=1/2/4、三方式の勾配、全回帰と有限paired matrixは[内部検証資料](internal/phase4_validation_followup/README.md)へ保存する。受入条件は[backlog](phase4_backlog.md)に残し、P4-003／005／008をIN_PROGRESSで管理する。
