# Phase 4 checkpoint reservation recovery implementation

更新日：2026-10-14。node-local v4 checkpoint sidecar予約の明示offline recoveryを追加した。これはcheckpoint metadataの限定的なrecoveryであり、全disk writerの回収ではない。設計と境界は[設計資料](phase4_checkpoint_recovery_design.md)、実行検証の詳細は[内部validation資料](internal/phase4_checkpoint_recovery_validation/README.md)を参照する。P4-009とPhase 4はIN_PROGRESSのままである。

## APIと実行条件

src/svsim/checkpoint_recovery.pyのrecover_checkpoint_reservations(config, comm=None, snapshot=None, apply=False, ...)が入口である。呼び出し側は全関連runtime/writer processが終了した明示offline時点で実行する。関数はnode-local設定と既存datasetを確認し、rootおよび各rank shardのDirectoryLease exclusive flockを得る。runtimeが持つshared leaseと競合すれば処理を拒否する。全rankが同じcollective sequenceを実行する必要がある。

apply=Falseが既定で、検査結果とwould-settle receiptだけを返す。dry runも既存ledgerをrecovery-only modeで開くが、schema migration/create、root登録、shared policy変更は行わない。reservation状態の精算とrecovery receipt保存はapply=TrueのCAS時だけ行う。recovery-only openはschema/policy/root/volume/native VFSとphysical headroomを確認し、logical quota・pending・drain marginを回収拒否条件から除く。pool/page/journal上限や実I/O errorは引き続き拒否する。詳細は[recovery pool実装](phase4_recovery_pool_implementation.md)を参照。入力snapshotが指定された場合も最新committed v4 decisionのIDと一致させ、以前のcutへ戻すことはしない。scope不一致、lease取得失敗、ledger locator不一致、どれかのrankでmetadata検査失敗があれば、vote後の適用段階へ進まない。

## 検査と適用の流れ

各rankの既存node-local directoryと、作成時と一致するdisk policyを設定したconfigを用意して呼び出す。runtimeやprefix readerを閉じた後に実行する。

```python
from mpi4py import MPI
from svsim.checkpoint_recovery import recover_checkpoint_reservations

report = recover_checkpoint_reservations(config, comm=MPI.COMM_WORLD)
# 検査結果を確認して、reservation精算を明示適用する場合。
applied = recover_checkpoint_reservations(config, comm=MPI.COMM_WORLD, apply=True)
```

inspection上限はoffline helper自身の上限であり、通常runtimeのmanaged RAM budgetへ課金する機能ではない。既定はintent 128件、個別sidecar／metadata payload／temporary 16 MiB、catalog fileとcommitted journal prefix合計はそれぞれ64 MiBである。catalog上限にもmax_journal_bytesを使う。payloadを含むstateの完全検証は、別途通常resumeと数値検証で行う。

rank 0がglobal manifestの候補数に上限を設け、最新candidateのchecksum/schemaを検査する。metadata payloadと既存loaderも使ってauthoritative decisionを確定し、rank数、block size、要求snapshotと照合して全rankへ配布する。

各rankは自分のprepare manifestとdecisionを照合する。read-only catalogのintegrity、dataset/store identity、snapshot row、sealed semantic rootを確認し、committed journal cursorsのprefix recordをchecksum付きで読む。未commit tailの変更やjournal recoveryは行わない。nonempty catalog side fileやresident amplitude metadataを見つけた場合は停止する。空のrollback journal/WAL/SHM side fileは拒否理由にしない。

DiskBudget checkpoint link一覧から現在のrankのintentを得る。rootのdevice/inode、dataset/snapshot、stage/path、operation、owner/generation、byte数、digest、target/temp inventoryを現在のv4 cutと照合する。target/tempはsymlinkを通さず、通常file、hardlink数、device、byte上限を検査する。decisionとdecision-copyの意図bytesはverified decisionから再構成し、prepare intentは検証済みのprepare bytesを使って期待digestとreservation lengthを照合する。

全rankのvalidation voteが成功した後、apply時は各tokenについてexpected reservationとexpected linkの完全一致を要求する。DiskBudgetは同じSQLite transactionでterminal state、materialized accounting、recovery receiptを保存する。receipt documentはcanonical UTF-8 JSONで8 KiB以下とし、evidence文字列のsuffixにproof SHA256を含める。tokenごとのtransactionは原子的だが、複数rank/tokenを包む分散transactionはない。適用中に失敗すると先行tokenはterminalのまま残り得るため、次回は最新cutと現inventoryを再検証して残存active tokenのみを処理する。

戻り値はkind、apply、snapshot ID、metadata_only、artifacts_preserved、strict_capacity_guarantee、およびrankごとのreceiptを含む。receiptにはreservation ID、settled/would-settle、terminal state、proof、解放されたpending byte数を示す。

## DiskBudget連携

DirectoryLeaseはdirectory inodeのkernel flockを使う。DistributedSimulatorは参照するdataset root、FileBlockStoreとReadOnlyBlockStoreはshard directoryのshared leaseを保持する。closeに失敗した場合はleaseを残し、正常なcloseまたはprocess終了まで回収を拒否する。取得時・検査後・適用直前にdirectory identityを照合する。flockを使わない外部processのrenameや書換えまで防ぐ仕組みではない。

src/svsim/disk_budget.pyではreserveのcheckpoint_linkは任意で、従来の呼び出しは維持する。link入力はdataset/snapshot UUID、rank/stage、absolute/relative target/temp、digestを受け取り、canonicalization時にroot device/inode、relative path、target_existed、expected_bytesを加える。rootはregistered rootに覆われ、同volumeである必要がある。stage規則、root内symlink/..、既存target/temp、UTF-8 4 KiB上限を検査する。reservation rowとlink rowは同一transactionで挿入する。

checkpoint_linksはbounded cursor scanでterminal行を含む一致rootのintentを返す。上限超過はtruncateせず拒否する。reconcile_checkpointはactive rowとexact old reservation/linkをCASし、materialized bytesの単調性とterminal stateを通常reconcileと同じ規則で検査する。任意のevidence documentはcanonical JSON、8 KiB以下、evidence末尾digest一致が必要である。recovery_receiptは保存済みproofを返す。explicit releaseでreservationが実削除された場合にのみlinkとreceiptを削除する。

## 適用範囲と制限

回収対象は明示link付きcheckpoint sidecar予約だけである。partial temporaryは削除・truncateしない。prepareとglobal decisionのtargetは必須で、欠けていれば拒否してtokenを残す。decision-copy targetは欠損を許し、partial temporaryがあれば観測byteをmaterialized accountingへ加えてRECONCILEDにし、artifactをproofへ残す。通常resume/restoreは欠けたdecision-copyを再作成するが、recovery時に残ったpartial temporaryを削除・cleanupしない。intentのないtokenやuncommitted writer、extent/segment、catalog/journal、payload、trace、asset、exportの予約を推定・解放しない。振幅値や全physical closureは検査しない。GC、全writerの統合inventory、別ledgerや外部jobの調停、cross-host recovery、電源断耐久性は未受入である。Linuxの一部検証は済んでいるが、Linuxを含む全writer fault/recovery matrixは未完了である。

volume内rankはpersisted catalog locatorまたは明示locatorを同一にし、同じpersistent ledgerを共有する。DiskBudgetの既存metadata poolとrecord/page boundsを使うが、すべてのwriterがhard guardで覆われるわけではない。statusはstrict_capacity_guarantee=falseを維持する。全writer fault/recoveryと容量保証が揃うまではP4-009およびPhase 4を完了扱いにしない。

recovery-only openはprecharged metadata pool内でcheckpoint CAS/receiptだけを許す。logical quota/pending/drain margin超過は回収を止めないが、min_freeと未割当pool、page/journal capは維持する。schema migration、root/policy変更や通常のadmission mutationは行わない。実hardware ENOSPC、電源断耐久性、全writer recoveryは未受入である。

## gate列とhistoryの塊（chunk）保存（2026-10-15追加、利用者の判断：案1）

v4 checkpointのmetadata payloadは、checkpointのたびに全gate列（`gates`）とhistory全体を書き直していた。そのため、定期的にcheckpointを取るrunでは、総費用がO(N²)になっていた。payload形式2では、この2つの列を固定長（512件）の塊に分ける。
- **塊の保存。** 各塊を正準JSONのsha256で名付けたwrite-onceのfile（`rank-NNNN/manifests/chunk-<sha256>.json`）として保存する。payloadには、塊の参照（sha256、byte数、件数）だけを持たせる。
- **書込み（`distribute_payload`）。** rank 0が塊の参照を配る。各rankは、自分に無い塊を申告する。和集合の塊だけを、既存の流し込み（予約・fsync・atomic rename）で配る。参照だけのpayloadは、その後で同じ経路で配る。
- **cache。** 公開済みcutより前の要素はその場で書き換えられない（rollbackは公開cutで止まる）。そこで、前回のcommitまでの満杯の塊の参照を、列の同一性（list identity）と組でcacheし、再encodeしない。再開・fork・読込みで列が置き換わると、cacheは使わない。
- **読込み（`read_payload`→`expand_payload`）。** 各塊のbyte数・sha256・件数を検証し、列を展開する。不一致は`Checkpoint metadata chunk checksum mismatch`で拒否する。
- **再開とfork。** 読み込んだ形式のまま配り直す（形式1は従来どおり全体を流す）。形式1の既存checkpointも読め、再開後の新しいcheckpointは形式2になる。
- **一時file。** 塊の一時fileは`chunk-<sha>.<hex>.tmp`で、既存のtemporary GCの対象になる。塊fileは削除しない。合計はO(N)である。

**効果（Mac、1 rank、file store、64 gateごとにcheckpointを40回、2560 gate）。**

| | 1回目 | 40回目 |
|---|---|---|
| 変更前 | 31 ms | 742 ms |
| 変更後 | 20 ms | 113 ms |

- **残りのO(N)。** CPUだけである：全gateの`gate_data`変換と全体hashによる全rank合意（約0.1秒）、`validate_root`、`circuit_hash`。diskとMPIへの書込みは、変わった末尾の塊（gateとhistoryで各1〜2個）とpayloadだけになった。
- **試験。** `tests/test_checkpoint_chunks.py`は次を確認する。新しい塊だけが書かれること、再開が厳密に一致すること、改ざんした塊を拒否すること、形式1のcheckpointを読み込んで再開できること。既存のcheckpoint・回収・multi-rank試験も成功した（Macの高速版987件、MPIのcheckpoint系82件）。
