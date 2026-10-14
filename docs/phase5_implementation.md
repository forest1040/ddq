# Phase 5 実装：計測・静的推定の基盤

更新日：2026-10-13。[設計書](phase5_design.md)の5A〜5Cの初期基盤を実装した。Mac上のCLI・小規模ストア・ローカルMPIで検証済み。Phase 5全体とPhase 4 backlogは継続中であり、総実行時間のheld-out評価、大規模実測、適応制御は今回の完了範囲に含めない。

## 実装した機能

| 部品 | 現在の機能 |
|---|---|
| `profile_document` | schema v1、不変profile ID、構成fingerprint、duplicate/NaN/巨大入力拒否、legacy HardwareProfileのloss付き変換 |
| `calibration_conditions` | operation／block／target／control／Pauli／backend／device条件の正規化とhash、完全一致照合 |
| `benchmark_campaign` | 明示scratchと予算によるRAM copy、H／RX／Pauli rotation／Pauli inner、storage read/write/fsync、直列mixed計測 |
| `workload_spec` | GateOp意味検証、Expr／ExactPi／stable parameter ID、4Q以下のinline dense asset、REPEATの圧縮集計 |
| `resource_estimator` | 単一候補のrank/host容量、cache・workspace・rollback・pin・metadata・append-only disk、trafficと対応可否 |
| `estimate_report` | 不変plan ID、未知値、wire integer、保存・再読込時の内容hash検証 |
| `cli` | `svbench probe/all`、`svsim register/estimate/calibration-suite/benchmark/calibrate-time/report`。自動benchmark、SSH、qsubは行わない |
| `benchmark_run` | 実DistributedSimulatorでのworkload実行計測。phase別rank MAX wall、oracle検証、seed付き交互順序、予算preflight |
| `time_model` | 実測runから条件class別のphase時間モデルをNNLSで適合、derived／extrapolated／unknownの総時間予測 |
| `comparison_report` | 予測planと実測runの照合、held-out誤差・interval coverage・bottleneck・A/B比較のJSON／Markdown（Geminiへ委譲して作成・検証） |

byte量と大きなcountはJSONの10進文字列、qubits/ranks/threads等の小さい構成数は整数。unknownはnullと理由を保持する。profile IDは時刻・raw trialも含む文書の同一性、configuration fingerprintはhost ID・CPU・OS・package・build・thread環境・割当の同一性である。空き容量・観測時刻は構成fingerprintから除く。revision、dirty source hash、変更ファイルのhash manifest、uv.lock hashを記録する。checkpoint／既存lifecycleのschemaは変更していない。

legacyの平均帯域を新しいraw trialとして扱わない。外部backend、legacy aggregate、条件が一致しないrecordは自動照合しない。同条件の再校正は新profileへ追加し、照合には最新のrecordを用いる。

## uvでの利用

```sh
uv sync --locked --extra mpi
mkdir -p /private/tmp/ddq-phase5-scratch

OMP_NUM_THREADS=1 OPENBLAS_NUM_THREADS=1 VECLIB_MAXIMUM_THREADS=1 \
uv run --locked --extra mpi svbench probe \
  --storage-path /private/tmp/ddq-phase5-scratch --threads 1 \
  --output /private/tmp/ddq-phase5-probe.json

OMP_NUM_THREADS=1 OPENBLAS_NUM_THREADS=1 VECLIB_MAXIMUM_THREADS=1 \
uv run --locked --extra mpi svbench all \
  --profile /private/tmp/ddq-phase5-probe.json \
  --scratch /private/tmp/ddq-phase5-scratch \
  --ram-budget 134217728 --disk-budget 8388608 --wall-seconds 20 \
  --block-bits 12 --repetitions 5 --threads 1 --backend direct \
  --output /private/tmp/ddq-phase5-measured.json

uv run --locked --extra mpi svsim register \
  --input docs/examples/phase5_local_workload.json \
  --output /private/tmp/ddq-phase5-workload.json

uv run --locked --extra mpi svsim estimate \
  --workload /private/tmp/ddq-phase5-workload.json \
  --profile /private/tmp/ddq-phase5-measured.json --block-bits 12 \
  --ram-budget-per-rank 134217728 --ram-budget-per-host 1073741824 \
  --disk-budget-per-host 134217728 --planning-only \
  --output /private/tmp/ddq-phase5-plan.json
```

既存outputには暗黙で上書きしない。再実行時は別名か`--overwrite`を指定する。`--threads`は割当ラベルで、thread設定やcore bindingは変更しない。計測時の実thread環境は別に保存する。上の例は環境変数も同じにしてprobeとcampaignの構成一致を保つ。異なるhost/build/thread/割当のprofileで`all`を実行すると拒否する。profileのprobe対象storage pathも揃える。

`all`にはscratch／RAM／disk／wall上限が必須。block_bits≤20、measured/warmup≤1000、seed≤uint64、suite組合せを検査し、requested blockが予算を超えたら縮小せず拒否する。RAM予算は管理するarray・raw recordの保守的見積もりで、Python/libraryのRSS上限ではない。wall上限はcase/trial境界で検査するcooperative deadlineであり、開始済みsyscallやoracleの強制中断は行わない。deadline超過trialは成功sampleへ加えず、未到達caseもomittedとして残す。自分の一意なscratchだけを削除する。

campaignはwarmupを除いたwall/CPU秒、seed、sample数、median/mean/p95/min/max、負荷観測、条件、検証方法を保存する。対象kernel単体を時間計測し、初期化・oracle・copy-back・内容確認を外へ出す。storage fsyncはdirty write後の`os.fsync` syscallを測る。mixedは同じmain threadでH＋write＋readを順に測り、並列overlapの利益として使わない。

`<profile>.run.json`はbenchmark-run manifestで、raw trialを含むprofileファイルのpath／SHA-256を記録する。raw profileはパスやソフトウェア構成を含む内部資料である。

## Workloadと解析の制限

[小規模例](examples/phase5_local_workload.json)は16QでH、RX、Pauli rotationを各1回、[50Q例](examples/phase5_50q_workload.json)はH/RXを1兆回反復する。`REPEAT`はcountとbodyのまま保持する。condition histogramへ集約しても元template ID・count・fingerprintを保存し、source occurrenceを列挙しない。反復pathを使うruntime occurrence adapterは今後追加する。bindingsはstable parameter ID→有限数。seeded-randomはseedを必須とする。dense要素は数値または`[real, imag]`で、unitary検証を通す。

入力≤16 MiB、JSON tree≤65,536 node／depth≤32、明示IR≤4096 node、semantic condition≤256、lowered IR≤4096 node、出力≤4 MiB。反復積は4096 bitで打ち切り、候補は1件。全振幅、全Block descriptor、全peer pair、反復op_id列を生成しない。解析のworking metadata目標64 MiBを、40/50Qのtracemallocで確認した。任意入力でprocess RSSをOSから64 MiBへ制限する機能ではない。

static referenceはeager full-state passを各lowered gateへ適用し、fusion、virtual rule、未実装tile、未知のoverlapを差し引かない。H/RX/Pauli以外の正当なGateOpもcountできるが、kernel校正は現在の有限cellだけ。forced-decompositionは既存FallbackDecomposerで実loweringし、非対応gate／連続Clifford+T合成は明示して拒否する。

capacity、executor support、calibration coverageは独立。S0／materialize／gradientなしのcircuitが初期reference。Frame keep、S1/S2の配置推定、gradient、VQE/QPE endpoint、巨大repeatのstreaming executorは未対応として返す。QPEメタデータは1 mHa・2次対称Trotter・逐次位相推定を受け付けるが、誤差認証や測定/reset/feedforwardを実装済みとはしない。

## 容量と時間の読み方

状態payload=`16×2^n`、slotはalignment込み、lease allocationは実BufferLeaseと同じOS page規則。rank数はpower-of-twoかつBlock数以下、nodesへ均等配置する。workspace、旧／現／新RAM root、I/O/MPI pin、clean cacheの旧version保持、runtime IRとmetadata、library／OS余裕を別欄にする。RAMは明示budget／割当／観測availableの最小、physical RAMは上限だけに使う。availableと明示host budgetが両方なければcapacityを確定しない。

file payloadはinitial＋全eager passのnew extentをappend-onlyで計上し、segment tail予約、catalog/journalの仮定、追加disk余裕、drainを加える。RAM-onlyにもcatalog用diskが必要。metadataはextent512 byte、gate4096 byte、Block256 byte等の明示したreferenceで、SQLite成長の厳密上限ではない。checkpointやparameter prefixの追加保持は今回のscenarioに含めない。P4-009が残るため`strict_capacity_guarantee=false`を返す。

`reference_kernel_seconds`は、完全一致したlocal H/RX/Pauli cellのmedian×eager invocation countだけである。校正済みkernelにRAM copy費用を重ねて加えない。RX/Pauliは現在angle=0.37のconst cellのみを照合し、別角度／Expr／ExactPiへ外挿しない。rank/thread/node構成が違う場合も使用しない。

初期版はcompile、initialization、metadata、copy/hash、commit/cleanup、storage/MPI等の統合費用が未校正なので、**総時間lower／expected／conditional upperはnull**。reference componentも全く未校正ならnull。missing条件・成分・coverageを必ず併記する。sample extremaは信頼区間やend-to-end boundではない。multi-rankへ単一processの帯域を台数倍しない。MPI payload量はroute adapter未実装としてnullと理由を返す。

CLI終了codeは、正常登録/明示planning=0、schema/入出力=2、予算または対応不足=3、campaign失敗/省略=4、総時間baseline不足=5。`estimate`は不足時もplanを保存する。`--planning-only`はpartial/unsupported planの作成成功を0で返すので、容量flagの確認が必要。

## 計測・時間推定・比較評価（2026-10-14追加）

```sh
uv run --locked --extra mpi svsim calibration-suite --block-bits 4 --qubits 5,6,7,8 --counts 1,4,12 --output-dir suite
uv run --locked --extra mpi svsim benchmark --workload suite/<case>.json ... --profile probe.json \
  --scratch <probeと同じscratch> --output-dir cal --ram-budget-per-rank 67108864 --disk-budget 67108864 \
  --wall-seconds 400 --block-bits 4 --repetitions 3
uv run --locked --extra mpi svsim calibrate-time --run cal/<case>.run.json ... --output time-model.json
uv run --locked --extra mpi svsim estimate ... --time-model time-model.json --output held.plan.json
uv run --locked --extra mpi svsim benchmark --workload held.json --plan held.plan.json --label held ... --output-dir held
uv run --locked --extra mpi svsim report --campaign campaign.json --output report.json --markdown report.md
```

**計測。** `svsim benchmark`は明示scratch内のtrialごとの新規datasetで、実`DistributedSimulator`（S0、Frame materialize、auto-native、RAM／file store）を実行する。setup（store作成・初期状態import）、compile（gate登録・bind）、execute（materialize）、flush（cache flush、fileはstore flush）、cleanup（close・削除）を各rankのlocal wallで測り、rank MAXを保存する。totalはphase MAXの和。oracle gather・誤差計算・disk走査は時間外。n≤`--oracle-max-qubits`（既定14、上限20）では毎trialでeager referenceと最大絶対誤差1e-10以内を確認し、超える場合はvalidation `not-run`としてtime model入力に使えない。複数`--workload`はround単位でseed付きshuffleした順に交互実行し、warmup roundを破棄する。実行前に静的推定でmanaged peakとdisk参照量を予算と照合し、trial後の実disk使用量も検査する。展開実行は4096 gate、case数は256まで。現在のhost/build/allocationとprofileのconfiguration fingerprintが異なれば拒否する（source変更後はprobeから再実行）。出力は`benchmark-run`／`workload-execution`文書で、raw trial、phase summary、work feature、制限、provenanceを持つ。

**時間推定。** featureは`workload_features`がlowered IRから厳密に数えるrank当たりの量：constant、blocks、gates、passes（全pass）、`passes:<gate>:<local|joint>`（route class別pass）。phase別に、setup・flushはconstant＋blocks、compileはconstant＋gates、executeはconstant＋gates＋route class別pass、cleanupはconstant＋blocks＋passesの非負線形モデルを、run別median／min／maxへそれぞれNNLSで適合する。`calibration-suite`はclassを1種類ずつ含むworkload（空回路を含む）を作り、featureの共線性を避ける。fitは同一configuration fingerprint・同一条件class（block_bits、ranks、threads、storage、backend、mode、frame、swap、execution、initial_state）の完了・oracle合格runだけを受け付ける。

model v2では各trialを1行とし、各featureと「feature×そのtrialのsync probe latency」の両方を説明変数にする。probeはtrial直前・時間外に4 KiB pwrite+fsyncを16回行い、そのmedianを取る。durable catalog commitの費用が装置のsync latencyに比例することをmodelに入れるためである。予測は条件・fingerprintが一致した場合だけ行う。未校正のroute classを含めばunknown（`UNCALIBRATED_WORK_FEATURES`）でゼロにしない。いずれかのfeatureが校正範囲外ならevidence=`extrapolated`、scope=planning-onlyでcoverageはpartial、範囲内なら`derived`／same-hostでcoverage completeとなる。lower／expectedは校正時sync latencyの最小／medianで評価する（`--sync-latency-seconds`で直前のprobe値も使える）。conditional upperは`最大sync latencyでの値×(1+最大相対残差)+最大絶対残差`。相対残差は観測1 ms以上のtrialだけで計算する。これは信頼区間ではない。`sync_scenarios`には速い／遅い状態の総時間を出す。校正のsync幅が2倍を超えると`DEVICE_SYNC_LATENCY_NONSTATIONARY`を警告する。circuit以外のalgorithmはunknownのまま。巨大repeat等のexecutor未対応workloadはextrapolated参考値とし、`execution_supported=false`を維持する。rank数・thread数・Block幅・backendをまたいだscalingは行わない。

**async executorとruntime ETA。** `svsim benchmark --executor async`は、Phase 4の`AsyncDistributedSimulator`を別の条件class（`mode=distributed-async`、policy hash）として校正する。各trialはflush後にもsync probeを取る。開始と終了で装置状態が2倍以上変わったtrialは、時間モデルの適合から除外する。`--eta-model`を付けると、実行途中のruntime ETAと実残時間を記録する。接続の詳細は[P4-001／002資料](phase4_time_model_eta_implementation.md)にある。

**比較評価。** `svsim report`はcampaignの各caseのplan／runをSHA-256付きで1回だけ読み、workload・plan_id・configurationの不一致、未完了・oracle不合格、予測なしを理由付きで`not_compared`へ分ける。符号付き誤差（予測−実測median）、held-out／calibrationを分けた|相対誤差|のmedian・p95・worst、observed 0.05秒未満の絶対誤差集計、interval coverage、phase別誤差と観測／予測bottleneck、baseline variantとのA/B比（勝敗、trial範囲の重なり）を出す。30% median目標は評価目標で保証ではない。

Mac（1 rank、b=4、RAM store、buffered）では、52件のsuiteを各3回、held-out 4件を5回測った。3件の|相対誤差|はmedian 0.42%、worst 2.0%で、3件ともintervalに入った。joint RXを含む1件は未校正classとして予測なしになった。

ent Docker（Linux ext4／NVMe）では、catalogのdurable commit（`synchronous=FULL`、`journal_mode=DELETE`）が時間の大半を占めた。装置のfsyncは約0.8 msと約5 msの2状態を切り替え、executeも約4倍変わる。macOSのfsyncは装置cacheをflushしないため（約0.02 ms）、**Macの時間はdurability費用を含まず、Linux／実機の参考にならない。** model v2では、状態がtrial中に変わらなければ、probe値を与えた予測が+2〜+19%で一致した。一方、校正期間と評価期間で遅い状態の頻度が違ったため、校正medianで評価したexpectedは-49〜-60%になった（coverage 3/3、upperの幅は約10倍）。Linuxで時間推定を実用にするには、(1) 実行直前のprobeと`sync_scenarios`による幅の提示、(2) Phase 4側でsync回数を減らすことのいずれかが必要である。(2)はWAL化やepoch単位のcommit集約などで、durability契約の変更を伴うためPhase 4の設計判断とする。詳細と証跡は[内部記録](internal/phase5_timing_validation/README.md)にある。

## 検証と次の作業

最終回帰：uv環境、Mac上のローカルMPIを含む420 passed／2 skipped。新しい基盤の42件は、strict JSON／hash、raw summary再現、未知値・2^53超、50Qの割当禁止、repeat/condition上限、CLI exit、quota、実ストアの振幅・disk・managed peakとの照合を含む。Linux専用skipの扱いは既存と同じ。

[Mac計測・dry-run資料](phase5_benchmark.md)と[内部raw](internal/phase5_mac_calibration/)を保存した。クラスタ接続／job投入、大規模振幅実行は行っていない。

case順序randomization／交互A/B、end-to-end held-out時間評価、report/benchmark CLIは上記で追加した。残りはFrame/metadata/lifecycle/MPI/gradient cellと既存MPIProfile adapter、storage cache/working-set grid、複数候補比較、multi-rank／file store／大Blockでのtime model校正である。その後、共通profileをPhase 4のruntime cost／残DAG／適応候補へ接続する。P4-001〜009をこの基盤だけでDONEへ変更しない。32〜36Q実測・QPE/VQE endpoint・合成・匿名化export・Linux実機校正も別の残件である。
