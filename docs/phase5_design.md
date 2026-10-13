# Phase 5 設計書：Resource Estimatorと実機ベンチマーク

- 作成日：2026-10-13。
- 状態：設計。以下の新CLI、schema、統合推定器、校正campaignは未実装。本設計の作成では計測・クラスタ接続・job投入を行わない。
- 対応仕様：[開発計画 Phase 5](../plan/distributed_quantum_circuit_simulator_development.md#phase-5resource-estimatorと実機ベンチマーク)、同計画§4、§13、§14、§19。
- 前提：[Phase 4実装](phase4_implementation.md)、[Phase 4 backlog](phase4_backlog.md)、[再計測とQulacs比較](slides/performance_remeasurement.md)、[量子化学最適化の検討](quantum_chemistry_optimization.md)。
- Pythonはuv。MacのMPI・local schedulerを開発環境とし、Linux・実複数nodeの評価はPBS割当内で行う。site固有情報は[内部計画](internal/lab_cluster_execution_plan.md)に分離する。

## 1. 到達目標とPhase 4との分担

同じ回路・精度・配置・実行器に対して、必要RAM／disk、I/O／MPI量、実行時間を事前推定し、実測との誤差とbottleneckを説明できるようにする。計測、静的計画、実行中の再予測に同じ条件付きcostを使う。

**Phase 4は未完了のまま管理する。** 先にPhase 5の計測・schema・静的推定を整備し、その基盤でPhase 4のETA／適応制御を完成させ、最終評価へ進む。基盤実装だけでPhase 4／5の完了とはしない。

| 作業 | Phase 5の責務 | Phase 4側の依存 |
|---|---|---|
| discovery／profile／校正 | 共通schema、計測CLI、再現性・品質、cost registry | P4-001の条件一致・runtime計測を共通化 |
| 静的推定 | bounded workload解析、容量・時間、候補比較 | 現在の合法なexecutor経路だけを使う |
| 残時間と再計画 | 同じcost API、予測と実測の評価器 | P4-002／P4-003／P4-004の抽出・探索・適応を完成させる |
| cache／gradientの比較 | 同じ精度・budgetで損益分岐を測る | P4-007／P4-008の自動費用評価と接続 |
| tile／disk容量 | capabilityと予約制限を推定へ反映 | P4-006／P4-009。未対応経路を成功予測に含めない |
| 最終report | 静的／逐次推定誤差、A/B、容量拡大 | P4-005の性能比較と同じcampaignを共有 |

初期成果物は **svbench → hardware-profile.json、svsim estimate → execution-plan.json**。完了目標の32〜36Q実測、40Q推定、50Q dry-runを、実行可能性と許可資源を検査して段階的に行う。

## 2. 既存実装の再利用と不足

| 既存部品 | 再利用する役割 | Phase 5で補うもの |
|---|---|---|
| [lifecycle.py](../src/svsim/lifecycle.py) | HardwareProfiler、HardwareProfile、BenchmarkCondition/Sample、PlanTemplate、RuntimeProfile | strict JSON codec、条件正規化、CPU／build／allocation情報、schema移行 |
| [benchmark.py](../src/svsim/benchmark.py) | bounded RAM copy、4 blockのread/write | raw trial、fsync、持続／混合負荷、tail、総資源・時間上限 |
| [mpi_benchmark.py](../src/svsim/mpi_benchmark.py) | 実transportの双方向校正、packing | peer選択と上限、profile統合、protocolとfabric測定の区別 |
| [frame_benchmark.py](../src/svsim/frame_benchmark.py) | Frame／Pauli kernel校正 | condition・精度・allocationと共通schemaへのadapter |
| [runtime_cost.py](../src/svsim/runtime_cost.py) | 条件一致の有限CostTask DAG、未知baseline拒否 | counted task／repeat、static estimator adapter。runtime抽出はP4-002 |
| [runtime_control.py](../src/svsim/runtime_control.py) | dry-run候補、cut／version／期限、利益下界、collective適用 | 自動校正・探索・運用検証をP4側で接続 |
| [observable_plan.py](../src/svsim/observable_plan.py)、[gradient_scheduler.py](../src/svsim/gradient_scheduler.py) | Pauli partner分類、合法なgradient候補 | actual cost／取得量、objective込みの比較 |
| [development_runner.py](../src/svsim/development_runner.py)、[pbs_adapter.py](../src/svsim/pbs_adapter.py) | local queue、資源上限、PBS script生成 | benchmark JobSpec、campaign／report生成 |

現状のpyprojectにはconsole scriptがない。HardwareProfileは列挙情報と平均sampleを持つが、統合ResourceEstimatorではない。RuntimeCostModelのexpected／upperはtask費用の加算であり、実行中の全DAGを自動抽出しない。既存fallbackは限定されたnamed gateのexact loweringで、任意Uの一般的なClifford+T合成を実装済みと扱わない。

## 3. データと制御の流れ

~~~mermaid
flowchart LR
  H["環境・割当・build"] --> B["svbench / 校正campaign"]
  B --> P["条件付きHardwareProfile"]
  W["WorkloadSpec / 不変semantic IR"] --> A["bounded workload解析"]
  P --> E["ResourceEstimator"]
  A --> E
  E --> S["ExecutionPlan / StaticEstimate"]
  S --> R["既存runtime / runner"]
  R --> T["Trace / RuntimeProfile"]
  T --> V["推定誤差・A/B report"]
  T --> C["P4 ETA / Guard / safe replan"]
  P --> C
  C --> R
~~~

estimate／registerは振幅を生成しない。run／benchmarkは別commandで、明示したbudget・保存先・allocationに従う。未知costを発見したestimateから自動的にbenchmark、SSH、qsubを呼ばない。

## 4. 共通schemaと再現性

新しいwire envelopeはschema_version=1とし、kindをhardware-profile／workload／execution-plan／benchmark-run／comparison-reportで区別する。lifecycleの既存SCHEMA_VERSIONやv4 checkpoint schemaを一括変更しない。legacy adapterを通し、読めないmajor versionは明示拒否する。

全成果物はsource revision、dirty treeのcontent manifest、uv.lock hash、Python／NumPy／mpi4py／MPI、backend build／binary hash、seed、作成時刻を持つ。revisionだけで未commitの計測内容を同定しない。raw fileはimmutable、summaryからhash付き相対参照で到達できるようにする。profile再校正は新ID／versionを作り、登録時のprofileを上書きしない。

| データ | 必須情報 |
|---|---|
| HardwareProfileDocument | profile_id、configuration_fingerprint、allocation、host/device等価class、hardware facts、backend capability、条件index、raw参照、profile version |
| CalibrationRecord | condition key、operation／scope、sample数、warmup、per-trial wall/CPU/bytes、集計・品質、provenance |
| WorkloadSpec | semantic IR／asset hash、initial state recipe、Binding、observable、algorithm、precision、retention、capability |
| ExecutionPlanDocument | workload/profile hash、source/layout／policy、実行経路、capacity、traffic、time、coverage、未知条件、採否理由 |
| BenchmarkRunDocument | 固定した条件、phase別時間、rank／host peak、result／精度、exit、trace/raw参照、実行support |
| ComparisonReport | case／variant／repeat対応、予測と実測、interval種別、差分、正味改善、bottleneck、未実施理由 |

内部のbytes／countはPython整数。wire上は全bytesと大きなcountを10進文字列に正規化し、2^53を超えるJSON値をJavaScript等で丸めない。qubit／rank／thread数など小さい整数は数値。時間はfiniteな秒、表示時だけ日／年へ換算する。unknownはnull＋reason、ゼロ／Infinity／架空の速度で埋めない。

profile_idは個別immutable文書の同定、configuration_fingerprintはhardware／build／allocationの構成照合、condition_keyは個別operationの測定条件照合に使い分ける。hashだけでなく条件本体も比較する。legacy adapterは失われる条件とunknown fieldの一覧を返し、欠けたbuild／thread／cache情報を推測して補わない。未知のoptional fieldは往復時に保持し、実行supportの判断に必要な未知fieldは拒否する。

profile値の根拠は **discovered、measured、derived、assumed、extrapolated、unknown**。scopeは **same-host、cross-host、injected、external-backend、planning-only**。根拠とscopeは別の軸で、例えば遅延注入後の実wallはmeasured／injectedとなる。

## 5. Condition keyとprofile照合

condition keyはcanonicalな安定項目から生成する。時刻、trial ID、観測CPU/wall、負荷の生値を完全一致keyへ混ぜない。これらはsample結果・品質情報として保持する。

| 条件軸 | 内容 |
|---|---|
| 実行器 | svsim／Qulacs、revision／binary、compiler flags、OpenMP runtime、kernel／materialization経路 |
| 並列 | rank／node数、ranks/node、requested thread、runtime thread設定、MPI thread level、binding／NUMA方針 |
| データ | dtype、block／tile／payload、gate support、control、packed active率、Frame/layout class、Binding共有 |
| storage | resolved backend、device class、slot/alignment、read/write比、iodepth、flush方針、cache条件 |
| 通信 | provider／interface class、peer topology、同時peer数、typed protocol／raw fabric、control/ackの包含 |
| 混合負荷 | compute/I/O/MPIの並行構成、queue／credit、background負荷の採用区分 |

requested thread数を実並列度とみなさない。process CPU/wall、OpenMP capability、runtime thread情報を結果へ記録する。開始／終了のCPU idle、測定中の負荷、Performance/Efficiency core、allocation上限を別々に残す。空きコアのsnapshotから全実行期間の専有を仮定しない。

Linux O_DIRECT、macOS F_NOCACHE、bufferedは異なる条件。OS cache coldを確認できなければunknown／warm扱いにし、F_NOCACHEをLinux direct I/Oと同じ保証にしない。raw NIC、1 MiBのcontrol込み転送、bulk exchangeも別conditionにする。

初期のauto適用は条件が一致したmeasured cellだけを使う。derived interpolation／external-backend／assumed／extrapolatedはラベル付き静的参考値に限定し、既存GuardやRuntimeCostModelへmeasuredとして偽装しない。欠けた条件は再校正候補として出力する。

## 6. WorkloadSpecと圧縮した解析

初版入力はcanonical GateOp JSONとhash付きmatrix assets、built-in workload builder。既存normalizer、symbolic Expr／ExactPi、source_op_idを使う。任意Pythonの実行や完全なOpenQASM frontendを初版必須にしない。QASMは対応subsetを明示したadapterとして後から追加し、未対応構文を無視しない。

algorithmはgeneric circuit、VQE objective-gradient、IPEA-Trotter planningを識別する。以下を不変semantic sourceと別に保持する。

- initial stateのseed／generator recipe、observableのexact Pauli masks、parameter occurrenceとBinding ID。
- n、dtype=complex128、block候補、rank/node、RAM／disk予算、layout／swap policy、gate execution mode。
- repeat、iteration／trajectory数、gradient method、prefix cut、retention／checkpoint方針。
- state／expectation／gradientの数値許容差、合成誤差の定義、QPE/Trotterの誤差認証状態。

巨大repeatはRepeat／Sequence／Range／MaskHistogramで表す。source IDはtemplate ID＋iteration pathで一意にし、op_id配列へ展開しない。基本単位の頻度・boundary費用を解析し、条件が反復で変わる場合はphaseを分ける。反復境界のfusionは合法性とactual executor supportが証明された場合だけ差し引く。

50Q dry-runでは全振幅、全Block descriptor、全source occurrence、全peer pairを作らない。解析時間・メモリは明示IR／集約group／有限windowに依存させ、2^nや巨大repeat数の列挙へ依存させない。現runtimeのGlobalSequencer.operationsが全gateを持つことと、推定器の圧縮表現は区別する。圧縮したQPEをruntimeが実行できると主張しない。

初期解析上限はinput/asset参照metadata 16 MiB、working metadata 64 MiB、output summary 4 MiB、cost条件256、candidate32、source window32。超過時はLIMIT_EXCEEDED／部分推定と未解析範囲を出力し、capacity_feasible=trueにはしない。raw traceは別ファイルへ有界にstreamする。

## 7. 容量推定と実行可能性

固定rankのcomplex128を対象に、state S=16×2^n、payload B=16×2^b、Block数=2^(n-b)を整数で求める。rank担当量は既存OwnershipMapの規則から算出し、host合計はranks/nodeを反映する。Pとn/bの対応を検査する。

capacity_feasible、execution_supported、calibration_coverageを独立に判定する。状態がdiskに収まっても未実装IPEAを実行可能とはしない。calibration不足でも容量は求められるが、時間はnullまたは明示した仮定による別scenarioとなる。

### 7.1 RAM

cache、compute workspace、旧／新COW出力、rollback root、Frame解消、observable／adjointの両stream、MPI／I/Oのpin、prefix共有、Binding suffix、metadataを生存区間で計上する。同一bufferのpinを追加copyと誤算しない。未知のfusionを使ってfull-block workspaceをtileへ過小評価しない。

BufferLease.allocation_costとOS page／alignmentを使い、logical payloadとallocation bytesを分ける。managed peak、process RSS/PSS、host合計、MPI/library／OS余裕を別欄にする。RSS合算はshared pageの二重計数を含む保守値と明記し、Macで未取得のPSSを推定した実測にしない。

capacityの上限はphysical RAMではなく、明示budget、PBS/cgroup割当、取得可能なavailable RAMから決める。availableが不明なら明示budgetを要求する。Qulacsのin-place一状態RSSをsvsimの2-state／COW／adjoint要件に置換しない。

### 7.2 Disk

slot=ceil(payload/alignment)×alignment、segment tail、catalog／journal／asset、metadata chunk、COMMIT、orphan、drain予約を含める。phase別にbytes writtenとlive peakを分離する。

append-only peakは **initial extents＋累積new extent＋metadata＋追加export＋未完了予約**。checkpoint参照だけで振幅を二重計上しないが、ancestor closureに保持される旧extentは除かない。実際のlease／GC／compactionが保証されない候補に、current/next数個分のworking-bank容量を適用しない。

batch全iterationの保存量と、次のBinding admission前にscratchを回収したpeakを別scenarioにする。diskはdataset/host/deviceの総予算、statvfs空き下限、環境サイズ、他job、保存寿命を検査する。P4-009未完了中はSQLite成長の追加余裕を明記し、strictな容量保証を主張しない。容量拡大の前にP4-009を解消する。

### 7.3 Metadataとadmission

Frame共有・sparse override、phase、gate/parameter dependency、catalog index、plan/trace、asset、checkpoint metadataも計数する。block単位の実indexを作る実行時費用と、dry-runのrange summary費用は別。未知のrecord/page成長はrangeと根拠を示す。

例としてfull-block jointで必要なgが予算を超える場合、P4-006のtile routeが未対応ならcandidateを拒否する。別volumeのv3 migrationも同様にsupport欠落として扱い、推定器から暗黙コピーしない。

## 8. 時間・trafficモデル

semantic gate数、native/fused/fallback kernel invocation、実際のblock pass、swap、Frame解消、metadata collective、checkpoint、observable/reverse/recompute/shiftを分けて集約する。T-countはcompiler指標で、振幅更新時間をT-countだけから求めない。

同じcost分類をstaticとruntimeで共有するが、work量は別adapterから得る。Phase 4の有限DAGにcounted task／range summaryを追加し、runtime残DAGの抽出はP4-002で実装する。

~~~text
Tserial = compile + planning + sum(kernel/Frame/packing/storage/MPI/control/commit)
Tlower = max(critical_path_lower_seconds, max_r(resource_work_r / capacity_r))
Tcandidate = remaining_work + planning + drain + migration + validation + commit
gain_lower = continue_lower - candidate_upper
~~~

measured full-memory kernel時間にRAM費用を再加算しない。control/ackを含むMPI時間にlatencyを再加算しない。sendとrecvは診断に双方保存しても、network workは送信payloadを一回数える。logical payload、alignment込みstorage bytes、protocol metadataを別指標にする。

resource_workとcapacityは単位を対にする。storage／MPIはbytesと同条件のaggregate bytes/s、kernelはamplitude updatesとupdates/s、CPU占有はcore-secondsと利用可能core数。既にsecondsとして得た費用をbytes/s等で再除算しない。共有device／NIC、main-thread直列処理もresource制約へ含め、未測定の並列capacityを台数から作らない。既存RuntimeCostModelはresource別lower secondsの和をcritical pathとのmaxに使うだけで、この拡張式は新推定器の設計。既存結果を変更する際は別model versionで検証する。

未検証overlapは速度利益にしない。初版expected／conditional upperは保守的なserial加算。mixed cellが同じ並行構成を測っている場合はgroup費用として一度置換し、構成componentを再加算しない。台数・thread数で帯域を線形倍増させない。

size interpolationは同じ条件class内で、rawとheld-out試験に基づくderived値として扱う。負latencyや未知saturationを許さず、範囲外はextrapolated。同一Macのprofileを別CPU／node配置へコピーする場合はplanning-onlyとする。

time reportはlower／expected／conditional upper、根拠coverage、unknown成分、sensitivityを持つ。coverageはtask個数だけでなくbytes／kernel workのcoverageも示す。重要な未校正成分を小task割合で隠さない。sample min/max、bootstrap interval、model boundsは別kindとし、各componentのinterval加算をend-to-endの95%保証と呼ばない。

## 9. svbench校正campaign

全probeは指定local scratchに自分の一意なfileだけを作り、RAM／disk／wall上限とcleanupを持つ。probeのみのcommandはread-only。benchmark commandを使う際は対象と上限を必須にし、全diskを自動消費しない。

| suite | 主な条件と出力 |
|---|---|
| RAM／kernel | block 4 MiB〜1 GiBの有限候補、single／multi、copy/read/write、1Q/2Q、diagonal、permutation、Pauli、MCXのcontrol/support、dense≤4Q |
| storage | buffered／resolved direct、seq/random、read/write/mixed、depth、slot、fsync／directory sync、catalog growth |
| MPI | 同host／別host、size／tile、packed/full、片方向／双方向、protocol control、同時peer、scalar/vector reduction |
| Frame／metadata | keep/materialize、group/phase、alignment、virtual swap、source/index/rule lookup、Binding/fusion費用 |
| mixed | compute＋storage、compute＋MPI、storage＋MPI、三者＋metadata。同じqueue/creditでjoint wallと各stageを測る |
| lifecycle／gradient | prefix/COW、commit/resume、lease/GC、adjointの2 stream、recompute、occurrence-specific shift |
| layout | S0/S1/S2、単独／fused swap、全bit階層、実際のinverse/layout、追加disk/MPI |

warmupとmeasured trialを分離する。通常はwarmup≥1、measured≥5、小さな単独kernelは7回。許可wall内で反復を減らした場合は品質をlower-qualityとして残し、失敗／timeout／short I/Oを成功sampleへ混ぜない。median、mean、p95、min/max、run間変動、sample数を保存する。少数試行のp95を安定したtail推定と宣伝しない。

case間順序はseed付きshuffle、A/Bは交互paired trial。同じscratch/backend/cache条件を保持し、別の自分の試験と重ねない。background／温度・clock情報は取得できる範囲で記録し、取得不能はunknown。明示的なcore bindingが使えないMacではpinしたと主張しない。

既存calibrate_mpiの全pair列挙は小Pに限定し、large Pでは有限のpeer topology classから代表を選ぶ。peer数／file bytes／metadata／時間上限を設け、未測定peerをunknownとする。異種hostは別profile、worst peer／rank MAXと不均衡を残す。

MPIはmain threadで進め、THREAD_SINGLE時はcooperative経路に合わせて校正する。MPIを使うLinux pytest／probeもsiteで必要ならmpiexec -n 1経由にする。profileを作るためにuserのmpi-selectorやshell profileを自動変更しない。

## 10. Parameter batch・observable・gradientの比較

同じsemantic source／initial／Bindings／observable／精度／P/n/b／budgetで次を比較する。準備・compile・warmupの除外区間と、全反復を含むend-to-endを併記する。

| variant | 数える費用と成立条件 |
|---|---|
| naive K回 | 毎回のcompile/bind/forward/evaluation、cleanup、結果保存を含むreference |
| plan再利用 | specialization／bind guard／validation、実際のrecompile頻度 |
| prefix＋COW | prefix作成・commit・lease、suffix read/write、共有の寿命と回収 |
| parameter-shift | 合法なoccurrence rule、shared Expr chain、評価回数、並列admission、objective |
| adjoint | objective/Oψ、forward/reverse両stream、layout alignment、scalar/vector reduction、checkpoint |
| recompute | restore/replay、追加forward、checkpointの間隔とRAM/disk peak |

observable数、Pauli項数とupper-X分布、parameter数／occurrence数、Binding数、suffix長を有限gridで変える。実際のIRから取得groupを数え、任意HamiltonianにJWのX/Y≤4という制約を勝手に適用しない。maskが未提示の計画値はbounds／assumedにする。

Block別fetch／remote partner／reduction bytes、二状態容量、checkpoint read/write、reverse pass、elapsed MAX、managed/RSS/disk peak、objective・gradient誤差をreportする。prefixの利益はprefix作成を含めたK反復総時間で判断する。未対応generatorのshiftや未検証のOψ partner reuseを最適化済みとして数えない。

## 11. Compiler・SWAP比較と精度

public modeはauto-nativeとforced-decomposition、basisはclifford-t-rz／clifford-t。既存runtimeのauto／fallback／structured-exactへ明示adapterを置き、対応しているrouteをcapability registryに登録する。S0/S1/S2も同じregistryで判定する。

forced比較は実際にlowered IRとcountsを生成して行う。初版はexact named gateと限定structured loweringを使い、forceで残ったnative gateを黙って許可しない。未対応gateはUNSUPPORTED_DECOMPOSITIONとする。一般U／連続angleのClifford+T合成は、新たなbounded synthesis adapterと受入試験が必要であり、仕様名の追加だけで完成としない。

合成adapterはbackend/version、gate-set、source/witness、target順序、control/open-control、ancilla、phase、normによる誤差bound、operation/time上限を返す。controlled Uのglobal phaseを無視せず、ExactPiと近いfloatを同一witnessにしない。総合成誤差は各gate・repeat回数へ配賦し、誤差の定義と加算boundを残す。一般的な位相合わせの近似誤差を相対位相の保証に置換しない。

time/bytes、native/fused/fallback counts、basis gate/T-count、IR長、ancilla、synthesis error、materialization、swap増分I/Oを比較する。強制basis比較が未対応ならcaseを未実施として残し、Phase 5全項目の完了とはしない。QPEをT-countに依存した時間へ変更することは要求しない。

## 12. 量子化学と外部baseline

VQEとQPEのendpointを分ける。VQEは1 objective、1 objective＋gradient、K iteration。QPEは1 trajectoryと独立K trajectory、条件付きIPEA branch。state数、active space、Hamiltonian項数／実mask、initial/ground state準備を記録する。

QPEは現指定の **1 mHa、2次対称Trotter、逐次位相推定** を既定planning caseにする。phaseとTrotterに誤差予算を分け、Lambda、step、phase bit、ground overlap／成功確率の根拠を示す。積分／交換子やstep sweepが無ければbias_certified=false。basis／active space近似を含むchemical accuracyを保証しない。

compressed Trotter powersのcountingは実装対象だが、measurement/reset/feedforwardとbranch再開は別capability。未対応ならestimateはplanning-only、runは拒否する。測定後stateにinverse gateを掛けて測定前へ戻った扱いにしない。途中測定を跨ぐrecomputeは同じbranch/outcomeとcheckpointを検証する。

Qulacsはoptionalな外部baselineとしてuvの別環境で用い、repoの必須dependencyにしない。wheel／OpenMP source build、single／multi、sign convention、controlled lowering、full state／block、in-place／COW、optimizer、precisionを記録する。微小kernelのrateをsvsim全体や未計測VQEのobjective-gradientへ転用しない。

既存[再計測資料](slides/performance_remeasurement.md)はlegacy入力として保持する。1億更新/sの目標表、Python kernel感度、Qulacs少数合成項からのQPE外挿を別scenarioにする。1 MiB転送3回をbulk帯域へ、Omni-PathをInfiniBand実測へ、single-nodeの速度を8 nodeへ置換しない。

## 13. 逐次ETA・適応A/B評価

P4-002から残work summary、P4-001から成功completion／queue／profile差分を受ける。登録時baseline ETA、現条件observed ETA、切替費用込みadapted ETAを別versionで出す。checkpointのcut／plan／policyと整合しない予測は採用しない。

対照はfixed／observe／LIVE_TUNE／replan。全variantで同じinitial、Binding、劣化を発生させる論理work地点／量、seedを使い、monitored traceでwall時刻も記録する。OS全体や他jobへ負荷を掛けず、対象job内のI/O／MPI／compute delayだけを注入する。

持続低下→検出→低リスク変更→必要時replan→VERIFYINGのpositive caseと、短い残回路／利益下界なし／未知baseline／瞬間低下／復旧のnegative caseを必須にする。monitor、探索、drain、migration、commit、validationを含むelapsed MAXを比較し、検出遅延、誤検出、切替回数、予測利益と実利益を保存する。

sample delay注入のcostはscope=injectedで保存し、通常profileやcross-host scalingへ混ぜない。candidateを推定できても適用は既存RuntimeControlのcollective frontiersを経る。report toolは実行中のpolicyを変更しない。

## 14. CLI／Python API案

以下は未実装の仕様例。pyprojectにsvbench／svsim entry pointを追加し、python -m経路も同じparserへ接続する。quantityはMiB/GiB/TiBを明示して整数parse、出力はnew run directoryへ保存する。

~~~sh
uv sync --locked --extra mpi
uv run --locked --extra mpi svbench probe --output hardware-discovery.json
uv run --locked --extra mpi svbench all --profile hardware-discovery.json --scratch /private/tmp/svsim-phase5-new --ram-budget 1GiB --disk-budget 2GiB --walltime 120 --repetitions 5 --output hardware-profile.json
uv run --locked --extra mpi svsim estimate workload.json --hardware hardware-profile.json --ram-budget 8GiB --disk-budget 32GiB --swap-policy S0 --gate-execution auto-native --output execution-plan.json
uv run --locked --extra mpi svsim register workload.json --hardware hardware-profile.json --output plan-template.json --estimate static-estimate.json
uv run --locked --extra mpi svsim estimate workload-50q.json --hardware virtual-hardware.json --dry-run --metadata-budget 64MiB --output hpc-estimate.json
uv run --locked --extra mpi svsim benchmark --plan execution-plan.json --scratch /private/tmp/svsim-phase5-run-new --warmup 1 --repetitions 5 --output benchmark-run.json
uv run --locked --extra mpi svsim report --campaign campaign.json --output comparison-report.json --markdown comparison-report.md
~~~

MPI commandはallocation外でspawnせず、uv run経由でmpiexecがworkerを起動する。storageとcwdはnode-local、別hostで同じpath名でも共有treeとは扱わない。rank-local outputはMPIでboundedに集約し、小さなcoordinator summaryとhash参照を保存する。

~~~python
# 新API案。型は将来実装、既存public APIは変更しない。
profile = ProfileDocument.load("hardware-profile.json")
workload = WorkloadSpec.load("workload.json")
plan = ResourceEstimator(profile, limits=AnalysisLimits()).estimate(workload, policy)
plan.save("execution-plan.json")
campaign = BenchmarkCampaign.from_plan(plan, limits=RunLimits())
# 実行は別entry。estimate()からrun()を呼ばない。
report = ReportBuilder.compare(campaign, observed_runs)
~~~

exitは0=要求した成果物生成、2=input/schema、3=resource/capability不適合、4=計測失敗、5=要求したmatched推定のbaseline不足。planning-onlyのreferenceを明示指定した場合だけ不足を含むpartial成果物で0を返し、statusをPARTIALにする。機械利用ではstdoutをJSON、進捗をstderrへ分ける。

## 15. 出力例とsupportの判定

~~~json
{
  "schema_version": 1,
  "kind": "execution-plan",
  "plan_id": "plan-<content-hash>",
  "workload_id": "workload-<content-hash>",
  "hardware_profile_id": "profile-<content-hash>",
  "precision": {"dtype": "complex128", "state_atol": 1e-10},
  "analysis": {"status": "PARTIAL", "source_expanded": false},
  "resources": {
    "logical_state_bytes": "18014398509481984",
    "managed_peak_bytes": null,
    "disk_peak_bytes": null,
    "capacity_feasible": null
  },
  "capability": {"execution_supported": false, "missing": ["runtime-scale-validation"]},
  "time": {
    "lower_seconds": null,
    "expected_seconds": null,
    "conditional_upper_seconds": null,
    "evidence": "unknown",
    "reason": "INSUFFICIENT_BASELINE"
  },
  "assumptions": [],
  "unknown_conditions": ["unmeasured-hpc-placement"],
  "raw_references": []
}
~~~

これは50Qの状態容量だけ確定した例で、実行成功予測ではない。実planにはnode/rank/Block、各host/deviceのcapacityとadmission式、traffic、counts、metadata、quality、candidate拒否理由を追加する。候補は未計測でも列挙できるが、unknown costをゼロ扱いしてbestにしない。

## 16. 大規模実測と運用条件

| 段階 | 対象 | admissionと証跡 |
|---|---|---|
| 小規模 | Mac単独／MPI 1/2/4 rank、Linux小規模 | oracle、profile/schema、RAM超state、node-local lifecycleを維持 |
| 32Q | 状態64 GiB、適切なRAM／disk構成 | 実空き、複数state、COW／catalog／checkpointを含むpreflight後 |
| 33〜36Q | 状態128 GiB〜1 TiB | Mac単一RAMへ全サイズを無条件に載せない。SSD／複数nodeの全保持版と資源を再検査 |
| 40Q | 状態16 TiBの容量／時間推定 | measuredとscale extrapolationを分離。全振幅実行を初期必須にしない |
| 50Q | 状態16 PiBのHPC dry-run | state allocationなし、有限metadata、仮想profile／layoutのsupportを表示 |

32〜36Qは目標とし、各サイズで容量・予算を満たすconfigを定めて実測する。実行前profileと見積もりを保存し、最初の小さいサイズでtraffic／peakを検証してから拡大する。real runが許可資源内で成立しない場合はそのstageを未実施とし、推定だけで実測完了に置き換えない。

過去のクラスタ許可は各node4 CPU／8 GiB RAM、環境込みdisk8 GiB、各job10分の小規模検証であり、Phase 5の大規模測定は含まれない。設計・dry-run・Macの試験を先に行い、対象job、実資源、scratch、cleanup、時間、配布内容を具体化してから利用許可を得る。停止node起動、NVMe mount/format、管理者導入は含めない。

コード／venv／cache／振幅／catalog／詳細reportはlocalに置き、ownerデータはMPIで取得する。共有diskの小さな制御情報・要約は必要な場合だけ内部計画の制限で利用する。PBSAdapterはrenderのみを維持し、設計CLIから自動qsubしない。

公開profile／reportはhost/deviceを同一性を保つ匿名IDへ置換し、path、queue、job ID、内部network、library buildのsite文字列を除く。詳細rawとsite profileはdocs/internal側へ保存する。匿名化でCPU/build／条件の違いまで消さず、公開版もhashとtrace scopeを維持する。

## 17. 検証と完了条件

| 分類 | 受入条件 |
|---|---|
| schema | strict roundtrip／major拒否／legacy adapter、unknown null、整数2^53超、hash／不変version、匿名化 |
| capacity | 小nで実store／budget／alignmentと一致、必要workspace拒否、保持版／lease／cleanup差、PBS割当優先 |
| state-free解析 | 50Q／巨大repeatで振幅・全Block・全Gateを禁止、64 MiB working metadata等のlimit、exact count |
| profile | backend/build/thread/peer/cacheの不一致拒否、rawから集計再現、short/error除外、未測定をauto採用しない |
| static時間 | 同hardwareのheld-out workload、compile等を含むend-to-endと照合、未計測coverageと差の原因を報告 |
| semantic精度 | 小n NumPy oracle、位相合わせなしstate/expectation 1e-10、analytic gradient 1e-9、finite difference step sweep |
| modes | 支持されたforced basis／S0/S1/S2のcounts・精度・費用、非対応拒否、T-countとsimulation時間を分離 |
| parameter/gradient | naive／plan／prefix／各methodを同精度で比較、共有Expr／controlled gate／replan後も一致 |
| adaptive | 同じ劣化のfixed A/B、positive/negative、費用込み改善、誤検出・切替・ETA誤差 |
| scale | 32〜36Q実測と40Q推定／50Q dry-runの別report。許可・容量不足なら未達と記録 |

予測精度の初期評価目標は、matchedかつ校正と分離したworkload群でstatic median相対誤差30%以内、runtimeの残workが十分な区間でETA median相対誤差30%以内。これは保証でなく評価目標。最後の短い区間／実時間0近傍は絶対誤差と別集計にする。p95・worst・interval coverageも示し、誤差超過caseを除外して達成値だけ報告しない。

bottleneckはcompute／RAM／storage／MPI／Frame／metadata／checkpoint／monitor・migrationをworkとcritical pathの両方で示す。混合時間のcomponent単純和を観測wallと誤同一視せず、排他的時間／重なった時間を区別する。predictionとactualには同じcompile/planning/run/cleanup区間を使う。

最終完了には、計測・静的推定のCLI、再現可能なraw/report、runtime ETA・適応A/B、parameter/gradient／compiler/SWAP比較、大規模stageの証跡と推定誤差・bottleneck分析が必要。optional Qulacs／io_uring不在のskipは明示できるが、Phase 4の未完了や必須32〜36Q実測を隠す理由にはしない。

## 18. 実装順と成果物

| 順序 | 成果物 | 完了判定／依存 |
|---|---|---|
| 5A | profile/workload/plan/report codecs、condition registry、CLI skeleton、state-free集約 | legacy互換、schema・巨大count・limit試験 |
| 5B | bounded svbench、負荷／build／thread／raw、mixed calibration、profile adapters | Mac小規模、必要時Linux小規模。P4-001へ共通profile供給 |
| 5C | ResourceEstimator、StaticEstimate、candidate counts、capacity／traffic／time | 小n実測oracle、held-out時間、40/50Q state-free dry-run |
| P4へ戻る | P4-009、002／004／003、007／008と必要な006を完成 | 容量拡大前のdisk補完、適応positive/negativeと合法な再計画 |
| 5D | parameter/gradient／S0/S1/S2／sync-async／fixed-adaptive比較 | P4-005と共通campaign、数値一致と正味改善 |
| 5E | strict forced basis adapterとprecision/report | supported gateで実lowering・T-count・error、一般合成のsupport不足解消 |
| 5F | 許可済み32〜36Q実測、40Q推定、50Q dry-run、最終report | 各stageの資源・coverage・誤差・bottleneckを記録 |

計測前提なしで5Aとcapacity-only解析を始められる。5B/5CはPhase 4の全残件完了を待たず進める。5D/5Fで適応結果を完成済みとして扱うにはPhase 4側の該当完了証跡が必要。

予定moduleはprofile_document、calibration_conditions、benchmark_campaign、workload_spec、workload_summary、resource_estimator、estimate_report、capability_registry、cli。既存flatなsrc/svsim構成を維持し、checkpoint／runtimeの大規模移動を伴うpackage再編はしない。

初期のdocs/phase5_implementation.mdとbenchmark reportは実装・計測後に作る。schemaとcaseを先に固定し、後から測った値だけに都合よく条件を変えない。Phase 4 backlogは担当する項目へ関連commit/reportを追記して更新する。

## 19. 対象外と未確定の判断点

GPU、complex64実行、rank/node loss後の継続、replication、実行中のrank数変更、QPE精度認証済みの分子計算一式、完全なOpenQASM互換はこのPhaseの初期実装に含めない。precision/runtime capabilityとして明示し、将来の入力を無視せず拒否する。

大規模測定のnode数／budget、strict Clifford+T合成backend、実分子のgeometry／active integrals、cold cacheの取得可能性は、実装・小規模校正で根拠を得て確定する。QPEのTrotter／配置最適化は[別資料](quantum_chemistry_optimization.md)の実験として扱い、未測定の利益をbaselineに先取りしない。
