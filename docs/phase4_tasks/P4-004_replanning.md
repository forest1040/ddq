# P4-004：費用に基づく有限候補探索とsafe切替

状態：設計／OPEN。依存：001、002、009。tile route利用時は006、cache/gradient比較は007/008。共通契約は[全体設計](../phase4_remaining_design.md)。

## 現状と対象

[runtime_control.py](../../src/svsim/runtime_control.py)は明示updatesとReplanEvidenceを検査し、drainしてpolicyを適用する。swap/lookahead変更はcheckpointを作る。`frontier()`はDEGRADED時にprefetchを0へするが、新条件の候補探索・費用込み利益・再校正は未実装。[async_scheduler.py](../../src/svsim/async_scheduler.py)ではengine/worker変更がrestart-required。

新規予定：`replan_candidates.py`、`replan_cost.py`、`safe_replanner.py`。RuntimeControlを運用窓口に維持し、candidate generation/analysisとcollective適用を分離する。

## 候補とcapability

| 候補 | 分類 | 条件 |
|---|---|---|
| identity/継続 | reference | 必ず含める。同じremaining snapshot |
| prefetch、I/O/MPI credit、job active上限 | LIVE_TUNE | native ring/tag/metadata/foreground/drain容量内、posted requestはdrain |
| S0/S1/S2、lookahead/beam、合法なfused group | FRONTIER_REPLAN | source coverage、commuting、Frame、actual tape、全費用が検証可能 |
| packed/full取得、physical layout swap≤2 pair | FRONTIER_REPLAN | existing route＋owner routing、COW/disk、inverse、profile cellが対応 |
| tile transpose | FRONTIER_REPLAN | 006のroute certificateが対象Frame/bit階層で成立 |
| engine/worker/native capacity超過、n/b/P変更、別volume移行 | RESTART_REQUIRED/unsupported | live候補から除外。現在のpolicy文字列だけで対応済みにしない |

capability registryはexecutor adapter/version、shape制約、最低working set、必要profileと逆経路を返す。APIに値があるだけでは合法としない。source occurrence、Binding/witness、observable、精度は全候補で同一。配置候補の取得先はStoragePlacementのactual ownerから解決する。

## 探索と評価

1. published cutからRemainingWork/contextをfreezeし、coordinatorが有限candidateを生成する。identity込み16、window32、beam既定4、pair≤2。
2. semantic依存、Frame整合、route capability、RAM/RSS/host/device quota、drain/checkpoint headroomを先に検査する。理由付きで拒否し、unknown costを安い候補として残さない。
3. candidate workを実lowering/layout adapterからcountする。Phase 5 eager referenceは容量の照合用で、自動gainのruntime countを代用しない。
4. 継続lowerとcandidate execution upper、探索・drain・migration・calibration・validation・checkpoint・cleanupを同じendpointで比較する。資源争用、未検証overlap、Frame解消も含める。
5. `gain_lower > max(absolute_margin, minimum_gain_fraction * continue_lower)`かつ全費用既知・quality/freshness成立の場合のみeligible。absolute marginの既定案0.1秒、割合は現policyの0.1を継続する。
6. 探索wall≤min(1秒, continue lowerの1%)とmetadata64 MiB。打切りはbest-so-farのcomplete候補だけを返し、未評価候補をeligibleにしない。

対象conditionが変わるcandidateに旧baselineを使わない。校正が必要なら予算付きpilotを別候補の費用に含める。pilot自体のcapability/capacityが未知なら推薦まで。pilot完了前の自動migrationを正当化しない。

## 運用状態と適用

予定API：`generate(snapshot, capabilities, limits)`、`evaluate(candidate, profile_set)`、`request_replan(..., dry_run=True)`、`apply_candidate(id, origin)`。既存手動APIはadapterで維持する。modeごとの動作はobserve=推定、recommend=保存、auto=eligible適用。手動policy変更はorigin=manualを記録し、合法性・admission・safe cutは同じ検査を通す。

candidate IDにcut、frontier、source/Binding/witness、layout/placement、plan/policy/profile set、updates、証明hashを含める。coordinatorがTTL/cooldownを合意し、rank-local clockの期限でcollective順序を分けない。snapshotが進んだcandidateは再評価が必要。

適用はadmission停止→drain/terminal ack→metadata/COW/disk予約→private transition→coverage/checksum/Frame/layout/inverse検証→prepare→authoritative decision→配送ack→公開。LIVE_TUNEは振幅migrationゼロでもqueue metadataとdrain/検証費用を数える。FRONTIER_REPLANはpolicy、root、layout、placement、actual historyとplan版を同じdurable decisionへ束ねる。

COMMIT前失敗は旧policy/cut、COMMIT後は新decisionで停止・再開。途中でpublic snapshotやobservableへprivate rootを見せない。resume時にhardware/profile条件が変われば自動適用を無効化する。過去candidateの再適用、古いobserved registry、低quality pilotを拒否する。

## VERIFYと失敗

新planでwarmup後の同work-classを観測し、予測/実利益を記録する。旧condition baselineへの直接Guard比較で改善と判定しない。replanの再変更はcooldown・累積費用・回数上限で制御する。改善しない場合も無条件の物理逆migrationをせず、費用と合法性を再評価する。durable切替の単純rollbackは行わない。

容量圧迫によるprefetch/admission抑制はsafety throttleとして分離する。未知baselineで「性能が改善する」として選択しない。停止/復旧/再悪化/手動操作のtransition reasonを残す。

現行autoの「DEGRADEDならprefetch=0」は無条件の性能変更としては廃止し、candidate評価または実際のadmission圧迫へのsafety throttleへ移す。既存の手動policy/sample APIを維持することと、この自動挙動を維持することを混同しない。該当regressionも新しい採否契約へ更新する。

## 受入・分割

004A registry＋identity/LIVE_TUNE、004B full-block layout/S0/S1/S2評価、004C durable変更＋VERIFY、004D tile/cache/gradient接続。

必須試験はunknown/短いremaining/負gain/TTL/stale/unsupported/native capacity、全rank同じ候補、reserve/drain/install/prepare/decision/ack各fault、切替後のstate/expectation/gradient一致。gainの項を個別変化させるoracleで二重加算を検査する。実runtimeから生成した候補を003のpositive/negativeで評価してDONE。手動evidenceだけでの採用成功は完了証跡にしない。
