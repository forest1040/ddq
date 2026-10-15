# P4-001（001C）：共通のruntime cost registry

更新日：2026-10-15。P4-001の受入条件001C「ETA／Guard／cache／gradientが同じregistryを使う。injectedをproductionへ流用しない」の実装である。P4-001はIN_PROGRESSのまま維持する。

## 構成

`RuntimeControl.costs`（`runtime_cost_registry.RuntimeCostRegistry`）を、runtimeで唯一の費用源とする。各sourceは`kind`、`source_id`、`provenance`、resolverを持つ。consumerは`resolve(kind, context, condition, purpose=...)`で問い合わせる。

| purpose | 受け付けるprovenance | 主な利用者 |
|---|---|---|
| production | measured | ETA（baseline／observed）、gradient自動選択、cache eviction、運用時のreplan |
| evaluation | measured、injected | 注入評価中のreplan |
| health | measured、legacy | Guardのbaseline |
| diagnostic | すべて（labelは残す） | 明示ETA view、time-model view |

拒否したsourceは`INJECTED_NOT_PRODUCTION`、`MANUAL_NOT_PRODUCTION`、`LEGACY_NOT_PRODUCTION`、`DERIVED_NOT_PRODUCTION`、`UNITS_MISMATCH`、`NO_MATCHING_COST`のいずれかで記録する。sourceが無ければ`UNKNOWN_COST`を返し、既定値で埋めることはしない。新しいsourceを優先する（ETA baselineだけは最初のprofile）。登録できるsourceは最大64、解決の集計は最大256 keyに制限した。

| kind | 登録元 | provenance |
|---|---|---|
| `task-seconds` | `attach_profile_document`（`RuntimeProfileAdapter`の条件付きenvelope、seconds/call） | measured |
| `eta-view:<name>` | `set_eta_cost_view`（明示診断） | manual |
| `eta-remaining` | `attach_time_model`（Phase 5時間モデル、`time-model:<model_id>`） | derived |
| `guard-baseline` | `configure_guard`（HardwareProfile.benchmarks、build／fingerprint／品質の来歴なし） | legacy |
| `gradient-method` | `attach_gradient_calibration`（`GradientCostAdapter`のheld-out検証済みcost） | measured |
| `cache-entry` | `set_cache_cost_hints`（callerのresolverは宣言されたprovenance、既定はmanual。hintの自己申告fieldだけならmanual） | 宣言どおり |
| `replan-per-gate` | `observe_evaluation`（`AdaptiveEvaluation.estimate`） | injected |
| `observed-task-seconds` | runのcompletion collector（`runtime_observed_costs.observed_task_cost`） | measured |
| `cache-entry`（`runtime-collector-io`） | runのstorage read／write completion（`observed_cache_cost`） | measured |

**利用側の切替**
- **ETA**：各viewとtime-model viewをregistry経由で解決する。sourceのlabelは従来どおり。
- **Guard**：`observe`はhealth purposeでbaselineを解決し、`condition_provenance`を記録する（legacyのままで、productionの費用にはならない）。
- **gradient**：自動選択はproduction purposeで解決する。injectedやbuildの無いcontextはresolverが理由付きで拒否し、`cost_origin`に残す。
- **cache**：evictionはproduction purposeのregistry resolverを通る。manualは採用しない。
- **replan**：`_evaluate_candidates`は注入中ならevaluation、そうでなければproductionで解決する。`qualified`は解決できたかどうかで決まる（従来の`scope=='injected'`判定を置換）。

**挙動の変化**：runtime経由のcache evictionでは、callerが「measured」と自己申告しただけのhint／resolverは採用しなくなった。measuredとして登録されたsourceが必要である。そのproducerはまだ無い（残件）。

## runtime観測をmeasured sourceとして登録（2026-10-15追加）

- **採用する観測。** completion collectorの`cost_observations()`（成功・検証済み・measured・warmup外。injectedは含まない）だけを使う。`MIN_SAMPLES`（5）件に達したclassだけが答える。下限・上限は観測の最小・最大であり、信頼区間ではない。
- **executor groupの照合。** 発生ごとの`op_id`／`source_op_id`を除いたgate classで照合する。角度、layout、policy、engineは一致を要求する。
- **ETA。** baseline viewは最初の不変profileのままにした。observed viewは、最新profileのcellが無ければrunのrolling観測で価格付けする（label `rolling-observed`、profileがあれば`conditional-envelope+rolling-observed`）。
- **cache eviction。** storage readのmedianを`whole_fetch`、writeのmedianを`dirty_writeback`とするmeasured `CacheCost`を登録した。callerが自己申告したcostは引き続きproductionへ流れない。前回の残件「productionでcacheのmeasured sourceが無い」はこれで解消した。

### rolling観測の鮮度と品質（2026-10-15追加、sonnetへ委譲して実装・検証）

- **鮮度。** executor groupとstorage completionのcontextに`policy_epoch`と`plan_version`を記録する。現在の値と一致する行だけを使うので、policy切替前の観測で切替後の作業を価格付けしない。各classは新しい32件（`WINDOW`）までに限る。
- **品質。** 窓内の値を古い半分（学習）と新しい半分（held-out）に分ける。学習の[min, max]を相対10％（`TOLERANCE`、ETA誤差の目標と同じ）広げた範囲にheld-outが入る割合（coverage）を測り、8件以上（`CERTIFY_SAMPLES`）かつcoverage 0.8以上のclassだけを認証する。数件のmin／maxだけでは、同じ分布からの次の値の一部しか覆わない（5件なら期待値で約67％）ため、幅を持たせた。
- **registryへの登録。** 認証済みの値だけを、measured source（`runtime-collector`／`runtime-collector-io`）として返す。認証されない値は、derivedの診断source（`*-diagnostic`）が理由とcoverageのlabel付きで返す。productionはmeasuredしか受けないので、未認証の観測でETAのobserved viewやcache evictionを決めることはない。class別の最後の判定理由は`RuntimeControl.cost_decisions`に残す（`CERTIFIED`、`INSUFFICIENT_SAMPLES`、`LOW_HELD_OUT_COVERAGE`、`STALE_POLICY_EPOCH`）。
- **burn-in。** executor classごとに、新しい観測のうち最も古い3件（`BURN_IN`）を捨ててから窓を取る。捨てた行は`CERTIFY_SAMPLES`に数えない。policyが変わると鮮度条件でclassが空になるので、burn-inもやり直しになる。storage completionにはburn-inを掛けない。
- **drift。** burn-in後に、学習とheld-outのmedianの差が学習medianの50％（`DRIFT_LIMIT`）を超えたら`DRIFTING`として認証しない。判定順は`INSUFFICIENT_SAMPLES`→`DRIFTING`→`LOW_HELD_OUT_COVERAGE`→`CERTIFIED`。
- **実runでの確認。** 以前は、warmup中の単調な変化でcoverageが0.00になり、認証されないことが多かった。sonnetが実runを観測したところ（1件あたり約8.6〜9.7 ms）、最初の数件が遅く、その後3〜6％程度の水準変化が残る。burn-inだけではcoverageが0.13〜1.0とばらつき、原因は幅のないmin／maxだった。10％の幅を加えると、合成行を使わない実executorだけの試験（RX 40個、36行）が、Macでは再試行なしで8回連続で認証された。entのLinuxでは15回中2回、run途中で1件あたり約60 msから約245 msへ水準が変わり、`DRIFTING`と`LOW_HELD_OUT_COVERAGE`で正しく拒否された。そこで試験では、この2つの理由による拒否のときだけ（診断sourceがlabel付きで答えることを確認したうえで）最大3回までrunをやり直す。

## stage記録（2026-10-15追加）

次をcompletion記録の独立したstageにした。いずれも、開いているexecutor／epoch groupの子として`parent_id`を持つので、group sampleとの二重加算は`consumed_children`で防がれる。

| stage | 計測区間 | condition |
|---|---|---|
| `catalog-commit` | gate batchのdurable COMMIT | durability、journal、吸収したcommit数のclass、catalog規模のclass |
| `frame-resolve` | fetch時の`phase2.resolve`（Frame、phase、logがある場合だけ） | block_bits、frame種別、log件数 |
| `mcx-packing` | packed MCXのpack／unpack（転送は既存のMPI記録） | block_bits、control数、tile、active振幅 |
| `checkpoint-prepare`／`-decision`／`-ack` | snapshotのprepare（flush〜snapshot行）、global decision、decision copy | schema、store／writer |

制約：async runtimeは`_execute`を上書きしていてpacked MCX経路を持たない。そのため`mcx-packing`は同期runtime上でだけ発生し、そこにはcollectorが無いので、現状は計測されない（残件）。collectorの既定上限（`metadata_reserve_bytes//16`、約8 KiB）を超えたstageは、設計どおり捨てられて計上される。

## raw記録のディスク退避とepoch集計（2026-10-15追加）

- **raw記録。** collectorは、submitしたすべての観測（成功・失敗・RAMの集計から外れたものを含む）を、上限付きのpending列（collectorの`max_bytes`まで、超過分は`dropped_raw`で数える）に1行ずつ積む。RAMのringは集計用のまま、rawはディスクへ逃がす。
- **書出し。** `RuntimeControl.epoch_boundary()`は`frontier()`の先頭で毎epoch呼ばれる（集団処理）。pending行を`rank-NNNN/measurements/raw-<run>.jsonl`へ追記する。追記は`AppendReservation`（操作名`measurement-append`、ledger leaseと同じchunk予約）で、書く前に必ずディスク予約に覆われる。総量の上限（既定16 MiB）を超えた行は捨てて数え、`raw_spill_truncated`を立てる。fsyncはしない（診断用のrawで、durabilityは要求しない）。
- **epoch集計。** そのepochのmeasured行を、stageごとの件数・合計秒・最大秒にまとめ、全rankでallgatherする。rank MAX、平均、不均衡（最大÷平均）、最も遅いrank、eventの最大を、`epoch_summaries`（最大64 epoch）に保持する。遅いrankの情報を平均で消さないためである。
- **監視自身の費用。** 書出しと交換の所要時間を、stage `monitor-raw-flush`／`monitor-summary-exchange`として記録する（次のepochでrawに出る）。

## 監視overheadの対照（2026-10-15追加）

`RuntimeControl.configure_monitoring(eta=..., epoch_summary=...)`で監視の構成要素を切り替える（全rankで合意）。completion収集は既存の`configure_measurements(enabled=...)`で切り替える。ETAを止めると`remaining_seconds`はunknownになる。epoch集計を止めると、raw行は上限付きのまま書き出されない。`examples/phase4_remaining_campaign.py --action monitor`は、1つの軸`monitor`で全停止→収集→epoch集計・raw→ETAの4段を比べる。どの段も同じcollector poolをRAMに予約するので、RAM圧の差は混ざらない。回路は9 gate、warmup 2・paired 7で、差のCI95はpaired bootstrapで求めた。

| 環境 | P | 全停止 wall median | +収集 | +epoch集計・raw | 全部（+ETA） |
|---|---|---|---|---|---|
| Mac | 1 | 93.6 ms | +4.2%（[3.3, 4.8] ms） | +7.5%（[6.7, 12.1] ms） | +9.5%（[7.6, 9.6] ms） |
| Mac | 2 | 128.7 ms | +5.3%（[3.1, 8.5] ms） | +8.2%（[10.2, 10.8] ms） | +10.5%（[12.1, 59.5] ms） |
| Linux | 1 | 515.8 ms | +0.7%（[-31.0, 34.2] ms） | +6.6%（[10.5, 53.4] ms） | +1.7%（[-36.9, 36.2] ms） |
| Linux | 2 | 608.8 ms | +0.8%（[0.5, 17.2] ms） | +4.2%（[20.6, 36.6] ms） | +4.0%（[14.3, 44.3] ms） |

- **固定費。** 監視全体で、1 runあたり約8〜15 ms（Mac）の固定費が乗る。gate数とepoch数に比例する費用で、Linuxでは実行時間の大半がdurable commitなので、比率は小さくばらつきに埋もれる段がある。
- **epoch集計。** epoch集計・raw書出しの段は、両環境で区間が0を含まない増加だった。
- **未測定。** 大きなBlockや長いrunでの比率は測っていない。
- **証跡。** [内部記録](internal/phase4_monitor_overhead/)。

## Linux O_DIRECTの実runtime校正（2026-10-15追加）

ent Dockerで、`svbench runtime`（8 qubit、b=4、O_DIRECT、5回、warmup 1）をworker／cooperativeと1／2 rankで実行した（[証跡](internal/phase5_timing_validation/odirect-linux/)）。この表は初回（5回）の値で、再測定（10回）の結果は次節にある。研究室クラスタ2 nodeの結果は[cross-host記録](internal/phase5_crosshost/README.md)にある。

| engine | rank | kernel-storage | memory-mpi | storage-mpi |
|---|---|---|---|---|
| worker | 1 | 7.084 s | — | — |
| cooperative | 1 | 1.719 s | — | — |
| worker | 2 | 6.525 s | 0.622 s | 5.164 s |
| cooperative | 2 | 1.656 s | 0.612 s | 2.000 s |
| worker（cluster、2 node） | 2 | 1.109 s | 0.499 s | 1.299 s |

- **engineの差。** 初回は、O_DIRECTのstorageを含むsuiteでworkerがcooperativeの約2.6〜4倍遅かった。しかし10回の再測定では再現しなかった（次節）。engineの差とは結論できない。
- **品質。** 初回はすべて`partial`で、品質は「finite diagnostics only」だった。原因は次節で解消した。数値検証はすべて合格した。
- **未実施。** 1 rankではMPI系のsuiteは対象外である。

### partialの解消とheld-out envelope（2026-10-15追加）

**partialの原因。** 反復数ではなく、監視の損失判定だった。
- stage記録を足した結果、RAM summary ring（128 KiB）が溢れた（`dropped_bytes`）。これを損失と数えていた。summary ringはETAなどの要約用で、溢れてもcompletionの証跡は失われない。
- 本当に失われるのは、epoch境界でdiskへ書く前のraw行である。校正の1回のmaterializeにはepoch境界がほとんど無いため、raw待ち行列（同じ128 KiB上限）も溢れていた。1 rankで1 trialあたり188行を失っていた。

**修正。**
- 損失判定をraw行の喪失（`dropped_raw`）だけにした。ring溢れは`dropped_bytes`として報告を続ける。
- collectorにraw待ち行列専用の上限（`raw_max_bytes`）を設けた。校正では、summaryは128 KiBのまま、rawを1 MiB（rank RAM予算の1/16以下）にした。RAM予算の予約にもraw分を加えた。
- trialごとのobservationsを本文から外し、`<output>.observations.jsonl`へ移した。本文には件数、行範囲、sha256を残す。repetitions 10・2 rankでは本文が文書のノード上限（20万）を超えたためである。

**envelope。** suiteごとに、validatedでwarmupでない`execute_seconds_max`が10件以上あれば、古い半分で[min, max]を作り、相対10％広げる。新しい半分（5件以上）が入る割合が0.8以上なら、`candidate_envelope`（conditional-prediction、held-out検証、件数・coverage・configuration fingerprint付き）を出し、品質を`held-out-envelope`にする。満たさないsuiteは理由を`envelope_decisions`に残す。

**ent O_DIRECTでの再測定（repetitions 10、warmup 1）。** [証跡](internal/phase5_timing_validation/runtime-envelope/)。4条件ともcomplete、raw損失0。1 rankは最終コードで取り直した（1 rankのMPI suiteを`NOT_APPLICABLE_SINGLE_RANK`と表示）。2 rankはその表示変更の前のコードで測ったが、判定に違いは無い。

| engine | rank | kernel-storage | memory-mpi | storage-mpi |
|---|---|---|---|---|
| worker | 1 | 1.65–1.92 s、認証 | — | — |
| cooperative | 1 | 1.60–1.88 s、認証 | — | — |
| worker | 2 | 1.58–1.65 s、認証 | 0.59–0.65 s、認証 | 1.90–2.07 s、認証 |
| cooperative | 2 | 1.65／4.7／6.5–6.9 s、認証（幅広） | 2.1–2.3 → 0.61–0.65 s、coverage 0.20で不認証 | 7.9／5.3 → 1.93–1.97 s、認証 |

- **engineの差。** 初回の「workerが2.6〜4倍遅い」は再現しなかった。1 rankではworkerとcooperativeがほぼ同じだった。
- **二峰性。** cooperativeの2 rankでは、同じ条件で約1.6 sと約6.7 s（kernel-storage）、約2.2 sと約0.6 s（memory-mpi）の2つの水準が出た。Linuxで以前記録した装置状態の切替と同じ現象と考えられる。kernel-storageは両方の水準を含む広い区間（1.49〜7.36 s）で認証された。認証は「held-outが区間に入る」ことの確認であり、区間が狭いことは意味しない。memory-mpiは途中で水準が変わったため、正しく不認証になった。
- **Mac。** 同じ設定（buffered、worker）で、1 rank・2 rankともcompleteになり、全suiteが認証された。

## P4-001の完了証跡（2026-10-15）

`examples/p4_001_evidence.py`（Geminiが実装し、Sonnetが検証、私が生のrunと照合）で、内部証跡ディレクトリ群から再現用の一式を生成した。[manifest](internal/p4_001_evidence/manifest.json)と[report](internal/p4_001_evidence/README.md)の内容は次のとおり。
- **manifest。** 1,330ファイルすべてのSHA256、生成時のgit revision（clean）、bundle SHA256（`17086977…`、生成rev 400d4fd）。
- **benchmark run。** 条件classごとのmedian、p95、worst、workload間のばらつき、run内の試行間ばらつき。
- **その他の要約。** time model、runtime ETAの精度、比較report（監視overheadの4段を含む）、不変profileのID／fingerprint、runtime校正。

**新しく分かったこと。** run内の試行間ばらつき（(max−min)/median の中央値）は、Macで0.02〜0.05、Linux O_DIRECTで0.06〜0.10、クラスタ2 node（O_DIRECT）で0.08だった。Linux bufferedだけが0.8〜2.9と大きい。Linuxで装置状態が切り替わる影響は、page cacheを経由するbufferedのdurable同期で特に大きく現れ、O_DIRECTではMac並みに小さい。

## 検証

`tests/test_runtime_cost_registry.py`（6件）で次を確認した。
- purposeとprovenanceの許可表、拒否理由、新しいsourceの優先とfirst選択、単位の不一致、未登録時のunknown。
- 実async runtimeで、ETA view、Guard、gradient、replanがregistryを通ること（resolveの呼出しを記録して確認）。
- injected、manual、legacyがproductionに答えないこと。
- cacheは、measuredと宣言したsourceでだけ答えること。
- sourceが無ければ全kindがunknownのままで、ETA viewは`unavailable`になること。

既存試験（ETA、remaining work、adaptive replan、段階別fault、gradient、cache）は変更なしで成功した。追加分の試験は次のとおり。
- rolling観測：合成行を使わず、実executorの同じclassの呼出し（RX 40個）でobserved-task-secondsが答え、ETA observed viewが価格付けすること。burn-inで単調なwarmupを除いて認証すること、段差を`DRIFTING`にすること、burn-in行を認証件数に数えないこと。
- runtime校正（`tests/test_runtime_calibration.py`）：held-out envelopeの認証／件数不足／低coverage、observationsのsidecar分離とsha256。
- cache：measuredのread／writeからeviction scoreを作り、injectedの観測を数えないこと。
- stage記録（`tests/test_stage_records.py`）：catalog-commitが子eventとしてgroupの消費集合に入ること、checkpointの3段、collectorのon/off、injected scope。raw行のlease下での書出し、epoch集計、監視自身の記録、総量上限での打切りと計数。

Mac全回帰は885 passed／14 skipped（partial解消とburn-inの追加後）。Linuxの高速版（`-m "not slow"`）は816 passed／14 skipped（9分9秒）。

## 引継ぎ事項（P4-001はDONE）

2026-10-15にP4-001をDONEとした。raw chunkのflushとrank summary、Linux O_DIRECTとcross-hostの校正、完了証跡、runtime校正のpartial、rolling観測のburn-inは解消した。次は後続の作業へ引き継ぐ。
- async runtimeにはpacking経路が無い。mcx-packingは同期経路でだけ記録され、async側では未計測である。
- cross-hostは研究室クラスタ2 node・小規模の回路だけである。
- 大きなBlockや長いrunでの監視overheadの比率は測っていない。
- Linux cooperative 2 rankの二峰性は、envelopeを広げるか不認証にする。原因は未特定である。
