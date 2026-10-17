# Phase 4 残件：ETA・候補探索・tile・cacheの先行実装

更新日：2026-10-14（P4-006のP>1 fragment通信は2026-10-17に追加、下記）。P4-002に続き、追加の3タスクP4-004／006／007を進めた。Codexが統合、Lunaがcounted ETA、cache identity／費用、tile routeと各oracleを担当した。4項目は **IN_PROGRESS**。設計の全受入条件、Phase 4全体の完了を宣言するものではない。今回はMac内で検証し、クラスタへの接続・PBS job投入は行っていない。

## 残workとETA：P4-002

[remaining_work.py](../src/svsim/remaining_work.py)はpublished cutのroot、frontier、layout、Binding ID、placement、source長、plan／policy／profile版をfreezeする。Binding IDには値とExactPi witnessが含まれるため、snapshotへ全parameter vectorを複製しない。次windowは最大32 sourceとし、実executorと同じS2 plannerの順序／fusionを使い、groupにsource集合を持たせる。実行groupとepoch publicationを別taskにする。

suffixは最大256 classのcounted summary。既定は1回256 source／20 msまでscanし、同cutで続きを再開する。全Block、全peer、繰返しinstanceの配列を生成しない。semantic suffixは将来のfusion／Frame／layoutを確定したtask数に置き換えず、`FUTURE_EXECUTOR_SHAPE_UNKNOWN`を保持する。S1、SWAP後、未bind、window切詰めも理由付きunknownにする。

private epochでは最後のpublished snapshotを基準に、window内のSTAGED sourceとI/O／MPI requestのdeltaを読む。STAGEDでも公開frontierとremaining source数は進めず、publicationを残す。`staged_sources`は有限windowで確認できた件数。private residualは未校正のため総ETAはnull。request平均からelapsedを引いた残時間は作らない。policy／profile／Binding／source／endpointが変わればstaleとする。

[runtime_eta.py](../src/svsim/runtime_eta.py)は最大512 task classのcounted DAGを評価し、下界はdependencyと共有resource、expected／upperはserial sumとする。completed requestを除外し、未完了・未測定・scan不足・stale・overflowでは総時間nullとreasonを返す。known partial componentとcountによるcoverageは診断用で、payload／critical-chain coverageではない。dirty writebackや失敗workが残る場合、gate数ゼロだけで0秒にしない。

`RuntimeControl.update_eta(endpoint_frontier=..., collective=...)`を公開し、materialize開始、epoch境界、1秒間隔のlocal I/O completionで更新する。collective解析とtrace失敗は次のcollective前にvoteする。rank MAXは全rankのexpectedが既知のときだけ返す。local callbackの監視失敗はsnapshotを無効化し、成功したI/O自体の失敗にはしない。

endpointは **gates-only**。observable／gradient、durable checkpoint、cleanupの時間は含めない。baselineは最初にattachした不変profile、observedは現在のprofile、adaptedは明示resolverがある場合のみ。完全一致・held-out検証済みconditional envelopeだけをproduction adapterから読み、sample min/maxを予測区間にしない。Binding IDを実計測contextにも加えた。profileが不足すればnullが正常な動作である。

`set_eta_cost_view(name, resolver)`はoracle／診断用。結果に`diagnostic-explicit`を記録し、全viewを制御採用不可とする。policy／cost view／source append／checkpoint／layout変更で旧reportをstaleにする。旧explicit `estimate_eta(tasks)` APIは維持した。

抽出用poolはserialized上限の8倍をmanaged RAMへ予約する。既定serialized上限はmetadata reserveの1/32、1024〜8192 byte。監視予約だけでforeground eviction／I/Oを起こさず、予約不足では`ETA_MONITOR_BUDGET`のunknownを返す。window入力とsummaryをbyte上限で打ち切り、request詳細を省略したfallbackもunknownを保持する。batch／shiftで共有budgetを移すときはobserverとETA両poolを二重解放しない。

残る範囲：細粒度read／Frame／packing／kernel／metadata／flush DAG、future branch、endpoint拡張、全stage profile registry、payload／critical coverage、real held-out ETA誤差とmonitor overhead。median30%目標の合格判定は未実施。

## 有限候補と費用oracle：P4-004

[replan_candidates.py](../src/svsim/replan_candidates.py)はidentityを必ず含め、prefetch、I/O／MPI credit、active-job上限、S0／S1／S2、lookahead、beamの候補を最大16生成する。既定探索deadlineは100 ms、指定上限は1秒。native queue、MPI TAG_UB、queue RAM、private cut、restart-required変更をcapabilityで拒否する。collective探索ではcoordinatorの候補をbroadcastし、各rankの拒否理由を集約する。

候補IDはsource context、remaining snapshot、updates、分類を含む。TTLは診断proposalへ記録するが、新proposalの適用APIには接続していない。physical layout／migration／tileの候補はadvertiseしない。全生成候補は`eligible=False`で、未知costを安い候補として扱わない。

[replan_cost.py](../src/svsim/replan_cost.py)は継続lowerと、candidate execution upper＋planning／drain／migration／calibration／validation／checkpoint／cleanupを比較する。各項を一回加算し、unknown・overflow・capability拒否・context不一致／未qualifiedなら採用不可。正の利益はabsolute marginとminimum fractionの大きい方を厳密に超える必要がある。このoracleの数値は実runtimeの校正／自動採用へまだ接続していない。

既存の手動ReplanEvidence／policy APIは維持した。**DEGRADEDだけを根拠にautoでprefetchを0へ変える挙動は廃止**し、bounded searchと`INSUFFICIENT_CALIBRATED_COST`のtraceを返す。admission自体の容量制御は既存経路を使う。未実証の改善をVERIFY成功とは呼ばない。

残る範囲：実loweringからのcandidate work／transition費用、自動qualified評価、deadlineとcontinue lowerの連動、durable safe_replanner、TTL／cooldown／累積費用の運用、切替fault、VERIFYとP4-003のpositive／negative実証。

## aligned tile転置：P4-006

[tile_transpose.py](../src/svsim/tile_transpose.py)、[tile_resolver.py](../src/svsim/tile_resolver.py)、[tile_extent_writer.py](../src/svsim/tile_extent_writer.py)を追加。公開APIは`AsyncDistributedSimulator.tile_index_bit_swap(pairs, tile_bytes=4096)`。StoredSimulatorにも同じhelperを適用できる。

初期対応は **P=1、FileBlockStore、local owner、identity Frame、empty log、解消済みscalar phase**。最大2組のordered physical swapを逆順pairのsource mappingで実行する。**2026-10-17にP>1のfragment通信を追加**した。P>1では`_execute_tile_swap_distributed`が、destination blockごとにsource fragmentを`_tile_groups`で求め、source rankが`_read_source_range`で読んでtyped MPIでownerへ送り、ownerがdestination tileへ配置して書く。rank bit、mixed、2組のpairとinverseをMac実P=2/4で検証した。remote owner、非identity Frame、deferred log、RAM storeは出力root／extent作成前に理由付きで拒否する。自動full-block fallbackは行わない。

sourceのmetadataとchecksumをlazyな二回の走査でpreflightする（P>1では`_preflight_distributed`が各rankのlocal blockを検査し、collectiveでvoteする）。managed BufferLeaseはP=1ではaligned source page一枚とdestination tile一枚、P>1ではsource fragment一枚（tile size）とdestination tile一枚。全Block／fragment／amplitude mapping表を保持しない。source pageを再利用し、destinationからsourceを求めてscalar phaseを反映する。full blockが作業RAMに入らないcaseでもこの経路は実行できる。

writerはfull physical slotをallocator SQL前に予約する。逐次offsetと整数coverage cursorで穴／重複／順序逆転を拒否し、short I/Oを失敗とする。paddingはzero、checksumはlogical byte順のbounded再readで作り、paddingを含めない。全slot完成後にprivate root／layout／placement／inverse historyを公開する。失敗時は旧root／layout／historyを保持し、partial出力を公開しない。

checksum preflightと出力checksum再readは追加I/Oであり、転置一回だけの帯域費用として見積もらない。reservationのmaterializeはslot seal時で、途中write中は保守的な予約課金が続く。今回の公開はruntime cutであり、durable replan用decision protocolの完了ではない。scalar実装の性能改善も未評価。

残る範囲：別owner／Linux O_DIRECT、一般Frame resolver、SQL／fsync／decision／ack fault、tile後の勾配、sequential／fused／tileの実費用比較と候補採用。

## cache identityと費用fallback：P4-007

[cache_identity.py](../src/svsim/cache_identity.py)でrawのdataset／store／version／owner／format／dtypeと、resolvedのsource coordinate／layout／placement／Frame／log／phase／依存parameter値／ExactPi witnessを区別する。非依存parameter差では同一、依存値／witness差では別identityとなる。identityはdescriptorであり、既存raw bufferのnamespacing置換やresolved bufferのBinding間共有はまだ有効にしていない。reader lifetimeの証明を先行させる。

[cache_cost.py](../src/svsim/cache_cost.py)のfuture summaryはwindow32、hint64、context16、ID長256以下。root／layout／Binding／placement／policy／profile／source長のcontextをruntime wrapperから渡し、eviction時にlive contextと比較する。`RuntimeControl.set_cache_cost_hints(summary, cost_resolver=...)`で明示hintをinstallできる。

同unit・validated measured・source／scope付きの費用が全eligible entryに揃う場合だけcost policyを使う。保持で避けられる再取得／再解消費用×reuseとdirty writebackを加算し、最小の損失からevictする。whole-fetchがあればread／Frame componentを重ねない。同点はclean、LRU順。unknown、stale、hint不足、unit不一致、overflowではLRUへ戻る。旧version-distance hintだけではcost policyを駆動しない。foreground／pending I/O pin、writeback失敗時のbuffer所有は維持する。

reason counterと最大256件のdecision traceを持つ。hint／traceのcardinalityは有界だが、個別MemoryBudget pool課金はまだ追加していない。cost policyをbudget保証済みとは扱わない。prefetchも既存のforegroundをevictしないadmissionを維持する。

残る範囲：P4-002からのautomatic FutureUse生成、strict condition付きactual cost供給、hint／traceのRAM課金、Binding／prefix reader lifetimeと共有、job fairness、host/device競合と同budgetのLRU paired性能比較。

## 検証

uv環境でunit／oracle／実Mac MPIを検証した。詳細・実行logは[内部検証資料](internal/phase4_remaining_validation/README.md)。injected costのrank MAX試験は計算／collective順序のoracleであり、実性能、校正済みETA、正味replan利益の証拠ではない。

設計の各受入条件は[backlog](phase4_backlog.md)と個別設計へ残した。次はP4-001の不足stageとP4-002の細粒度captureを接続し、実remaining時刻に対するheld-out評価を進める。
