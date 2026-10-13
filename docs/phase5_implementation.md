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
| `cli` | `svbench probe/all`、`svsim register/estimate`。自動benchmark、SSH、qsubは行わない |

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

## 検証と次の作業

最終回帰：uv環境、Mac上のローカルMPIを含む420 passed／2 skipped。新しい基盤の42件は、strict JSON／hash、raw summary再現、未知値・2^53超、50Qの割当禁止、repeat/condition上限、CLI exit、quota、実ストアの振幅・disk・managed peakとの照合を含む。Linux専用skipの扱いは既存と同じ。

[Mac計測・dry-run資料](phase5_benchmark.md)と[内部raw](internal/phase5_mac_calibration/)を保存した。クラスタ接続／job投入、大規模振幅実行は行っていない。

次はFrame/metadata/lifecycle/MPI/gradient cellと既存MPIProfile adapter、storage cache/working-set grid、case順序randomization／paired A/B、複数候補比較、end-to-end held-out時間評価、report/benchmark/run CLIを追加する。その後、共通profileをPhase 4のruntime cost／残DAG／適応候補へ接続する。P4-001〜009をこの基盤だけでDONEへ変更しない。32〜36Q実測・QPE/VQE endpoint・合成・匿名化export・Linux実機校正も別の残件である。
