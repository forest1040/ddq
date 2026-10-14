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

## 検証

`tests/test_runtime_cost_registry.py`（6件）で次を確認した。
- purposeとprovenanceの許可表、拒否理由、新しいsourceの優先とfirst選択、単位の不一致、未登録時のunknown。
- 実async runtimeで、ETA view、Guard、gradient、replanがregistryを通ること（resolveの呼出しを記録して確認）。
- injected、manual、legacyがproductionに答えないこと。
- cacheは、measuredと宣言したsourceでだけ答えること。
- sourceが無ければ全kindがunknownのままで、ETA viewは`unavailable`になること。

既存試験（ETA、remaining work、adaptive replan、段階別fault、gradient、cache）は変更なしで成功した。追加分の試験は次のとおり。
- rolling観測：同じclassのexecutor呼出し8回でobserved-task-secondsが答え、ETA observed viewが価格付けすること。
- cache：measuredのread／writeからeviction scoreを作り、injectedの観測を数えないこと。
- stage記録（`tests/test_stage_records.py`）：catalog-commitが子eventとしてgroupの消費集合に入ること、checkpointの3段、collectorのon/off、injected scope。raw行のlease下での書出し、epoch集計、監視自身の記録、総量上限での打切りと計数。

Mac全回帰は868 passed／14 skipped。

## 残件（P4-001）

- async runtimeでのpacking経路と、その計測。
- rolling観測の鮮度・品質基準（現在はsample数のみ）と、held-outのenvelope化。
- 上限付きraw chunkのflushと、epoch境界でのrank summary。
- 監視overheadの全体対照。
- Linux O_DIRECTとcross-hostの校正。
- 完了証跡（raw、immutable profile、manifest、比較report）。
