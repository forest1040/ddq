# P4-009 書き込み経路監査（現在）

更新日：2026-10-17。**P4-009はDONE**。現在の実装一覧である。2026-10-14の初回監査（`bb87551`）に書かれていた「export未予約」「payload回収なし」「catalog synchronous=FULL」は現状ではない。履歴はGitに残し、この表を過去の未修正一覧として読まない。以下の予約はmanaged disk ledgerによるsoft admissionであり、全filesystem metadataへのphysical quotaや非協調writerの強制ではない。`strict_capacity_guarantee=false`を維持する。

元の009A〜Dとの対応、最新source別証跡、完了判定は[受入対応表](phase4_p4_009_acceptance_2026-10-16.md)を参照する。Mac高速1317 passに加え、Linux最終追加367 pass／13非適用skipとPBS265180の実2host/4rank全8case成功を照合した。旧timeout/guard拒否の記録を残し、保存JUnitのdirect非適用1件はread-only再判定を保存した。

## managed runtimeのwriter

| writer / 主なcallsite | admissionと公開境界 | owner停止後の回収と証跡 |
|---|---|---|
| `DiskBudget` ledger / metadata pool | 本番では有限metadata bootstrap poolとVFS main/side-file capを使う。quota/min-free/drain、root inventory、pendingを同transactionで検査。SQLiteはDELETE/EXTRA、WAL/disk-temp等を拒否。 | exclusive ledger lease、policy/schema/identity検証、exact CASとbounded receipt。ancestor/volume/capability/pool preflightの後にparent mkdirする。 |
| `VolumeCoordinator` ledger / claims / external markers | 独立ledger・明示external claimを集約、有限metadata poolとVFS、DELETE/EXTRA。external growth超過はfail closed。 | claim generation・directory leaseとinventoryで回収。登録export rootsもcoordinatorへpublishし、ledger materialize後もphysical bytesを継続観測。新規coordinatorのpool/FS preflightはparent mkdir/SQLite openより先に行う。 |
| `CatalogConnection` / Phase2 catalog / resident metadata | SQL open・transaction・backupの前に`catalog-transaction`、allocatorやversion公開も同envelope。main/journal VFS、byte/page bound、DELETE/EXTRA。 | inode/birth付きcatalog owner、full-store inspection、rollback後のattribution証明。catalog初回createもsource/birth/parent/generation付きstartup intentで検証。実SQL/VFS/kill試験は[関連資料](phase4_writer_durability_implementation.md)とtestsを参照。 |
| 同期 `FileBlockStore.write_version` | allocator SQL・segment create/writeの前に`amplitude-extent` slot全体を予約。fileと新segment directoryをfsyncしてからcatalog versionを公開。 | orphanもobserved usageに残す。停止後の全store inventory・published extent closureを検査しpendingを一度だけ精算。segment自体はVFS外で、software slot bound。 |
| `AsyncStoreIO` / `TileExtentWriter` / RAM spill | enqueue・allocatorより前にslot予約。tileは重複/全被覆/checksumを検証。buffer pinは完了まで保持。RAM spillは同期writerへ委譲。 | partial extent、completion前kill、version公開前後の既存kill matrix。resident amplitudeが残りsourceを証明できない場合は回収拒否。 |
| `Journal` | ctorは`journal-open`をmkdirより前に予約。appendはserialize・1 MiB bound・`journal-append` creditの後にfile create/open/partial-tail repairを行う。64 KiB chunkは再利用しcloseまで保持する。 | quota拒否/serialization失敗では初回file作成も既存tail truncateも行わない。protected cursorを越えるrepair/rollbackを拒否。`journal-open`はnamespace class、appendはappend classとして、owner deathと全store inventory後にpendingだけ回収する。 |
| `StreamingExecutionTrace` | mkdir/open前に`trace-open`、各record前に`trace-append`。短write/flush失敗は予約保持中に元の長さへ戻す。rollback失敗はtokenをretainしpoisonする。 | `trace-open`も既知namespace class。traceのpartial tailはauthorityに使わず、artifactを保存して課金する。traceの予約・mkdir・open直後killを追加検証。 |
| raw measurements (`RuntimeControl._spill_raw`) | `measurement-append`の`admit(1)`がmkdir/openより前。bounded raw spillとrecordごとの追加credit。 | append class。計測用recordをproduction calibrationへ混入しない。 |
| checkpoint JSON / fork lease / copy / repair | `atomic-json`のserialized bytesをmkdir前からfile fsync/rename/dirfsync/cleanupまで保持。prepare/global decision/decision copyはstage intentをbind。 | committed cutはcheckpoint recovery、uncommitted cutはowner-dead recovery。fork leaseは専用`dataset-lease-json` labelで全store inspection後のsidecar classへ接続し、purpose不明のunlinked `atomic-json`は拒否する。回収では、target/tempを削除しない。 |
| checkpoint metadata payload/chunk | `checkpoint-metadata-payload/chunk`をdataset/snapshot/digest/generation intentへbind。bounded transfer、digest、fsync、write-once publication。 | [専用実装](phase4_checkpoint_metadata_recovery_implementation.md)。実kill、semantic validation、dry-run/apply/CAS/receipt、artifact保持。 |
| matrix asset / stored snapshot sidecar | `matrix-asset` / `snapshot-export`のexact bytesをnamespace前に予約しatomic sidecar保存。 | sidecar class、停止後のmanaged inventoryとsemantic cutを保護。 |
| stored / distributed state export | [専用実装](phase4_state_export_admission_implementation.md)。明示output rootsもusageへ含め、stored file/全rank shard/manifestを予約。bounded streaming、file/dir fsync、atomic replace/whole-directory exchange、old-generation cleanup後に解除。 | `state-export*` purposeとbirth/identity/lease/generation intent。実SIGKILL 5段階、oracle、回収冪等性。別volume/cross-host未証明namespaceは変更前拒否。 |
| stored snapshot migration | temp namespaceを`migration-open`で作成前からcleanupまで予約。temp catalogを`sim.store.directory`内に置き、rank rootしか登録していない構成でもinventoryから外さない。backup catalogとsegmentは各々上記の予約を使う。 | [source-bound intent/clone回収](phase4_migration_recovery_implementation.md)はsource snapshotと実payloadをstreamingで検証する。sourceにもexclusive leaseを要求し、cloneを通常storeとして誤認せず全physical closureを証明する。実backup途中killはadmitted hot-journal rollback後に精算する。 |
| batch/fork/parameter-shift | 子datasetは親の明示ledger/coordinatorを継承し、root合算とpolicy一致を検査。stored prefixも明示注入budgetをconstructorへ保持し、自前privateは独立connection ownershipを維持。RAM shareとは別管理。 | 子closeでdurable bytesを勝手に消さず、引き続きinventoryへ課金。kill/独立dataset/Binding matrixは各既存tests。 |
| offline temporary / branch GC | exclusive owner停止証明、全inventory/cutとplan digestを確認し、maintenance writerも同ledger/coordinatorを使用。segment番号は最小6桁formatの全桁を扱い、JSON escape後のmanifest参照も保護。 | 削除候補のdevice/inode/birthをcatalog変更前とunlink直前に再確認。上限超過・未知manifestは拒否。百万番台の実live extentがGC後もchecksum／数値一致する[回帰](phase4_gc_acceptance_audit_2026-10-16.md)。 |
| standalone distributed shard reader | local private ledgerが無いだけでは履歴を作り直さない。catalogのpersisted shared locatorをread-onlyで読み、shared policy/root包含/volume/排他fileを検証して既存budget/coordinatorを継承。 | store leaseが同時offline回収を阻止。真のprivate欠落、別root/private ledger、policy衝突はnamespace変更前拒否。[reader回帰](phase4_standalone_shard_reader_2026-10-16.md)。 |

## namespaceと最初のadmission

`trace-open`、`journal-open`、`migration-open`の1 byteは「明示的なlogical capacity拒否がmkdir/openより先に起きる」ためのprobeであり、directory entryやfilesystem metadataの実割当が1 byte以内という主張ではない。namespace tokenの回収ではartifactを削除せず、kernel exclusive leaseによるowner停止証明、全storeのcatalog/journal/extent/trace inventoryを要求する。不明なcatalog、resident amplitude、hot sidefile、symlink、未証明cutは従来どおり拒否する。

`Journal.append`は、最初のrecordのcreditを得る前には既存partial tailも変更しない。明示rollbackは容量を増やさないreclaim操作であり、protected cursor・現在のfile長を検証する。rollbackで容量を増やすtruncateは許可しない。authorityはdurable snapshot/global decisionとcommitted journal cursorであり、traceやprobe tokenではない。

managed budgetを受け取るatomic sidecar、Journal、trace、raw measurementは、予約やnamespace変更前にcanonical destinationが永続generic roots／明示export rootsへ含まれることも検査する。root外、parent symlink脱出、leaf symlink/hardlinkは拒否する。正規rootへ戻るparent alias表記とbudgetなしAPIは維持する。traceはopen後にbudgetが付いた場合も次record前に検査する。[閉包補修と無変更oracle](phase4_sidecar_inventory_audit_2026-10-16.md)を参照。

## toolingとbootstrapの継続監査

以下をmanaged runtimeの修正済みwriterと混同しない。元からDiskBudgetを受け取らないAPIであり、この一覧から除外したことをP4-009完了の根拠にはしない。

| callsite | 現状と継続事項 |
|---|---|
| `profile_document.write_document`、`runtime_calibration.export_observations`、CLI markdown/report | caller指定profile/reportを直接保存するtooling API。byte limitの有無を各APIで明示する。managed budgetを受け取らず、runtime quotaの保証を付けない。 |
| benchmark/campaign/calibration scratch、pretrial sync probe | storage測定そのもののwrite。runtimeのcost認証やexplicit benchmark budgetとは区別する。未管理のscratchを管理済みと主張しない。 |
| development runner / PBS scriptとmetadata | local job/log/report出力。job wall/output制限はdisk ledgerの代用ではない。 |
| native VFS extension build | compilerが一時directoryへlibraryを生成する。runtime catalogに対するVFS admissionとは別のhost build artifactである。 |
| `FileBlockStore` / `DistributedSimulator` / `DiskBudget` / `VolumeCoordinator`の最初のroot/subdir mkdir | [bootstrap preflight](phase4_bootstrap_namespace_implementation.md)で祖先volume上のquota/min-free/drain/metadata pool/FS capabilityをmkdir前に検査する。既存外部budgetがあるstoreはstore-open tokenも使う。private coordinator/policyと既存managed bytesをmkdir前に検査し、root/subdir/ledger成立直後の実killでretryと未知partial DB拒否を確認済み。既存coordinatorのglobal envelopeもprivate/distributedでroot前に事前claimし、他ledger pendingによる拒否とretry数値oracle、claim直後実killのpool冪等性を確認。namespace probeだけで鶏卵問題を隠さない。 |

## 検証

今回のnamespace matrixは、Journal/traceの1 byte境界拒否、初回appendのexact byte拒否、既存partial tailのbyte不変、invalid serializationのbyte不変、exact creditによるrepairとcommitted cursor保持を確認する。実SIGKILLはnamespace予約直後、mkdir直後、trace open直後、およびdataset lease JSON予約/temp open直後へ置き、全artifactのbyte不変、dry-run→apply→再実行の冪等性、旧snapshotの実numerical oracleを検証する。

既存writer/VFS/ENOSPC/kill/coordinator testsの結果と今回の新規結果は各実行logで区別する。SIGKILLは実power-loss durabilityの代用ではなく、SQLite EXTRAとnative VFS durability imageの検証もfilesystem/OS契約の範囲を明記する。bootstrapだけではなく、全managed writerのMac/Linux fault・回収、runtime接続、実別hostの結果を[最終対応表](phase4_p4_009_acceptance_2026-10-16.md)へ照合してDONEとした。

## runtime切替・forkのadmission継承監査（2026-10-16）

`16e587f`＋bootstrap配線`9d5b0f0`を基点にstore生成callsiteとruntime切替を照合した。disk quotaとRAM shareは別々に検証する。

| 経路 | 実callsiteとledger/coordinatorの引継ぎ | 検証 |
|---|---|---|
| frame / deferred phase | `StoredSimulator`の既存`store`と`Phase2Catalog`を使用。frame asset SQLは同じ`CatalogConnection`、resolve後payloadは同じcache→store writer。 | `test_phase2_runtime`、`test_stored_recovery`のframe付きsnapshot/resume oracle。 |
| LIVE_TUNE / FRONTIER_REPLAN | `RuntimeControl.apply_policy`→`_install_policy`はpolicyと既存IO engineのqueue設定を替え、store/budgetを生成し直さない。drainとdurable checkpointも現budgetを参照。 | `test_replan_faults`のreserve/prepare/decision/install fault、state/expectation/gradient oracle、実process killとMPI rollback。 |
| stored prefix fork | 明示注入された`_store_kwargs['disk_budget']`を子constructorへ引き継ぐ。以前はtransport factoryだけを渡していたため、供給済shared ledgerからprivate ledgerへ逸れる／missing ledger拒否になる穴があった。自前private budgetは引き続き同config・同persisted ledgerへ子自身のconnectionを開く。 | 新`test_runtime_budget_inheritance`。shared ledger/coordinator同一性、private ledgerを作らないこと、共同quota拒否と数値oracle、child close後の親budget維持。自前privateはcoordinator有無の双方でparent先close→child materialize/read/snapshot成功。 |
| distributed prefix / parameter batch / parameter-shift | `distributed_checkpoint.fork`が`replace(sim.config, directory=...)`でcoordinator/policyを保持し、node-localの`disk_admission_ledger`を親の実ledgerへ固定。子bootstrapのprospective rootも事前claim。`ParameterBatchExecutor`と`phase4_gradient`のshiftはこのforkを使う。 | `test_parameter_batch`、`test_phase4_gradient`、`test_volume_coordination_runtime`のfork/batch/shiftと実MPI 1/2rank。 |
| adjoint / recompute gradient | 既存simのroot/cache/store上にforward/adjoint/recompute rootを作り、専用の無管理storeを作らない。終了時に元cutを復元し、durable filesはinventoryに残る。 | `test_phase4_gradient`の三方式finite difference、layout tape、long chain reclaim。 |
| tile physical transpose | `execute_tile_swap`は既存`sim.store`を捕捉し、全writerが`TileExtentWriter`へ同storeを渡す。writerがallocatorより前に全slotを`store.disk_budget`で予約。 | `test_tile_transpose`、`test_tile_extent_writer`のcoverage/checksum/unsupported拒否と数値oracle。 |
| RAM payload / flush | node-local RAM storeへの交換は明示`disk_budget=self.disk_budget`。`MemoryBlockStore.flush_to_file`は`super().write_version`へ委譲し、同budgetのslot予約を通る。resident metadata SQLも同catalog VFS。 | `test_memory_store`のdurability/flush失敗/retry・immutable oracle。volatile residentがある状態のoffline recoveryは既存fail-closed契約を保持。 |
| physical-layout migration | `apply_layout_migration`→既存simのphysical swap→既存cache/store writer→同budget checkpoint。Phase2 snapshot輸送のcloneも`FileBlockStore(...,disk_budget=sim.store.disk_budget)`。 | `test_layout_migration`のswap fault/old cut保持/実oracle、`test_distributed_checkpoint`とsource-bound migration kill matrix。性能campaignの再実行はこの機能監査には含めない。 |

このruntime継承監査の追加修正はstored prefixの明示budget継承である。自前private ledgerの所有権を親子で共有してparent close時に子まで閉じる変更は行わない。standalone readerの互換修正とappend/sidecarのdestination閉包も統合済みであり、上表と専用資料の受入試験へ接続した。

結合監査の検証結果：上記frame/prefix/replan/gradient/tile/RAM/layout/volume機能回帰108 passed（実MPI 1/2rank、decision前後の実process killを含む）、性能campaign1件は意図的に除外。追加inheritance試験は全5 passed（外部shared ledger prefix、private子所有権2条件、RAM flush/tile volume ledger各1件）。source-bound migration/checkpointの実kill・oracle回帰も16 passed。性能改善やbaseline認証の再実証としては扱わない。
