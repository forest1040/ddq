# P4-009 書き込み経路監査（現在）

更新日：2026-10-16。現在の実装一覧である。2026-10-14の初回監査（`bb87551`）に書かれていた「export未予約」「payload回収なし」「catalog synchronous=FULL」は現状ではない。履歴はGitに残し、この表を過去の未修正一覧として読まない。以下の予約はmanaged disk ledgerによるsoft admissionであり、全filesystem metadataへのphysical quotaや非協調writerの強制ではない。`strict_capacity_guarantee=false`を維持する。

## managed runtimeのwriter

| writer / 主なcallsite | admissionと公開境界 | owner停止後の回収と証跡 |
|---|---|---|
| `DiskBudget` ledger / metadata pool | 本番では有限metadata bootstrap poolとVFS main/side-file capを使う。quota/min-free/drain、root inventory、pendingを同transactionで検査。SQLiteはDELETE/EXTRA、WAL/disk-temp等を拒否。 | exclusive ledger lease、policy/schema/identity検証、exact CASとbounded receipt。pool preflight前のparent mkdirは下記bootstrap監査の修正対象。 |
| `VolumeCoordinator` ledger / claims / external markers | 独立ledger・明示external claimを集約、有限metadata poolとVFS、DELETE/EXTRA。external growth超過はfail closed。 | claim generation・directory leaseとinventoryで回収。export inventoryのcoordinatorへの伝播は親担当の別修正。coordinator parent mkdirの順序はbootstrap対象。 |
| `CatalogConnection` / Phase2 catalog / resident metadata | SQL open・transaction・backupの前に`catalog-transaction`、allocatorやversion公開も同envelope。main/journal VFS、byte/page bound、DELETE/EXTRA。 | inode/birth付きcatalog owner、full-store inspection、rollback後のattribution証明。catalog初回create intentはstartup担当の別修正。実SQL/VFS/kill試験は[関連資料](phase4_writer_durability_implementation.md)とtestsを参照。 |
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
| batch/fork/parameter-shift | 子datasetは親の明示ledger/coordinatorを継承し、root合算とpolicy一致を検査。RAM shareとは別管理。 | 子closeでdurable bytesを勝手に消さず、引き続きinventoryへ課金。kill/独立dataset/Binding matrixは各既存tests。 |

## namespaceと最初のadmission

`trace-open`、`journal-open`、`migration-open`の1 byteは「明示的なlogical capacity拒否がmkdir/openより先に起きる」ためのprobeであり、directory entryやfilesystem metadataの実割当が1 byte以内という主張ではない。namespace tokenの回収ではartifactを削除せず、kernel exclusive leaseによるowner停止証明、全storeのcatalog/journal/extent/trace inventoryを要求する。不明なcatalog、resident amplitude、hot sidefile、symlink、未証明cutは従来どおり拒否する。

`Journal.append`は、最初のrecordのcreditを得る前には既存partial tailも変更しない。明示rollbackは容量を増やさないreclaim操作であり、protected cursor・現在のfile長を検証する。rollbackで容量を増やすtruncateは許可しない。authorityはdurable snapshot/global decisionとcommitted journal cursorであり、traceやprobe tokenではない。

## toolingとbootstrapの継続監査

以下をmanaged runtimeの修正済みwriterと混同しない。元からDiskBudgetを受け取らないAPIであり、この一覧から除外したことをP4-009完了の根拠にはしない。

| callsite | 現状と継続事項 |
|---|---|
| `profile_document.write_document`、`runtime_calibration.export_observations`、CLI markdown/report | caller指定profile/reportを直接保存するtooling API。byte limitの有無を各APIで明示する。managed budgetを受け取らず、runtime quotaの保証を付けない。 |
| benchmark/campaign/calibration scratch、pretrial sync probe | storage測定そのもののwrite。runtimeのcost認証やexplicit benchmark budgetとは区別する。未管理のscratchを管理済みと主張しない。 |
| development runner / PBS scriptとmetadata | local job/log/report出力。job wall/output制限はdisk ledgerの代用ではない。 |
| native VFS extension build | compilerが一時directoryへlibraryを生成する。runtime catalogに対するVFS admissionとは別のhost build artifactである。 |
| `FileBlockStore` / `DistributedSimulator` / `DiskBudget` / `VolumeCoordinator`の最初のroot/subdir mkdir | ledgerが存在しない最初のnamespaceについて、祖先volume上でquota/min-free/drain/metadata poolをmkdirより前にpreflightし、generation付きstagingとbind/publicationを組み合わせる別修正を継続する。namespace probeだけで鶏卵問題を隠さない。 |

## 検証

今回のnamespace matrixは、Journal/traceの1 byte境界拒否、初回appendのexact byte拒否、既存partial tailのbyte不変、invalid serializationのbyte不変、exact creditによるrepairとcommitted cursor保持を確認する。実SIGKILLはnamespace予約直後、mkdir直後、trace open直後、およびdataset lease JSON予約/temp open直後へ置き、全artifactのbyte不変、dry-run→apply→再実行の冪等性、旧snapshotの実numerical oracleを検証する。

既存writer/VFS/ENOSPC/kill/coordinator testsの結果と今回の新規結果は各実行logで区別する。SIGKILLは実power-loss durabilityの代用ではなく、SQLite EXTRAとnative VFS durability imageの検証もfilesystem/OS契約の範囲を明記する。bootstrapの継続作業を完了する前にP4-009の全writer受入をDONEとはしない。
