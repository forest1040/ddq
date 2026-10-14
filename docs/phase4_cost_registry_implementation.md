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

**利用側の切替**
- **ETA**：各viewとtime-model viewをregistry経由で解決する。sourceのlabelは従来どおり。
- **Guard**：`observe`はhealth purposeでbaselineを解決し、`condition_provenance`を記録する（legacyのままで、productionの費用にはならない）。
- **gradient**：自動選択はproduction purposeで解決する。injectedやbuildの無いcontextはresolverが理由付きで拒否し、`cost_origin`に残す。
- **cache**：evictionはproduction purposeのregistry resolverを通る。manualは採用しない。
- **replan**：`_evaluate_candidates`は注入中ならevaluation、そうでなければproductionで解決する。`qualified`は解決できたかどうかで決まる（従来の`scope=='injected'`判定を置換）。

**挙動の変化**：runtime経由のcache evictionでは、callerが「measured」と自己申告しただけのhint／resolverは採用しなくなった。measuredとして登録されたsourceが必要である。そのproducerはまだ無い（残件）。

## 検証

`tests/test_runtime_cost_registry.py`（6件）で次を確認した。
- purposeとprovenanceの許可表、拒否理由、新しいsourceの優先とfirst選択、単位の不一致、未登録時のunknown。
- 実async runtimeで、ETA view、Guard、gradient、replanがregistryを通ること（resolveの呼出しを記録して確認）。
- injected、manual、legacyがproductionに答えないこと。
- cacheは、measuredと宣言したsourceでだけ答えること。
- sourceが無ければ全kindがunknownのままで、ETA viewは`unavailable`になること。

既存試験（ETA、remaining work、adaptive replan、段階別fault、gradient、cache）は変更なしで成功した。Mac全回帰は863 passed／14 skipped、Linuxの関連57件は成功した。

## 残件（P4-001）

- runtimeのcompletion観測（`RuntimeMeasurementCollector.cost_observations`）を、measured sourceとして登録すること（cache evictionや、rolling observed ETAのproducer）。
- Frame、packing、metadata commitの独立したstage記録。
- 上限付きraw chunkのflushと、epoch境界でのrank summary。
- 監視overheadの全体対照。
- Linux O_DIRECTとcross-hostの校正。
- 完了証跡（raw、immutable profile、manifest、比較report）。
