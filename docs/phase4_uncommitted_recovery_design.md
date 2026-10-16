# P4-009 未commit予約の回収とtemporary GCの設計

2026-10-15。[checkpoint reservation recovery](phase4_checkpoint_recovery_design.md)が扱うのは、最新committed v4 sidecar intentに結び付いたtokenだけである。catalog batch、ledger append、extent、trace、asset／exportのtokenが途中killで残っても、intentがないため既存recoveryは対象にしない。本設計はその残りを、明示的なoffline／node-local操作として扱う。`strict_capacity_guarantee`は立てない。

## 原則

- 既定はdry runである。applyはdry runが返した`plan_sha256`を必ず添え、現在の計画と一致しなければ拒否する。レビューした計画だけを適用する。
- owner停止の証明はkernel flockのleaseだけで行う。PID、時刻、heartbeatは使わない。ledgerの親directoryと、登録rootの配下にあるdataset directory（深さ2まで）の全てを排他lockする。生きているwriterは`DiskBudget`が親directoryの共有lockを、storeがdataset directoryの共有lockを保持するため、1つでも取得できなければ拒否する。ownerが死ねばkernelがlockを解放する。
- 未知の状態は推測せず拒否する。1件でも拒否理由があれば、applyは何も変更しない（全か無か）。
- 生き残ったbyteは解放しない。観測使用量（登録root配下のfile走査）はそのまま課金され、applyが精算するのは「owner死亡により二度と書かれない未物理化の予約分」だけである。materialized記録は減らさない。apply前後の観測使用量が一致しなければ失敗として報告する。artifactの削除・切詰め・書換えは予約回収では一切行わない。
- 二重適用はledger内のCAS（期待行との完全一致、状態がRESERVED／WRITINGであること）と、同一transactionで書く証跡receiptの主キーで拒否する。同時実行は排他lockで拒否する。

## 判定

owner停止の証明後、各storeを検査する。拒否理由は次のとおり。

| 検査 | 拒否条件 |
|---|---|
| catalog側file | `-journal`／`-wal`／`-shm`が空でない（admitted reopenでSQLiteにreplayさせてから再実行する） |
| catalog | `quick_check`失敗、読取不能、unflushedな`resident_versions` |
| extent | 公開済みversionのoffsetがsegment file長以上、またはsegmentが存在しない |
| journal | 検証できる記録の前に壊れた行があり、その後に有効な記録がある（末尾の途中行は残余byteとして報告するだけ） |
| namespace | symlink、通常fileでない／nlink≠1のcatalog |

tokenはintentを持たないものだけを対象にする（intent付きは件数を報告し、checkpoint recoveryに任せる）。operation別の扱いは次のとおり。

- `trace-append`／`journal-append`／`measurement-append`：追記tokenである。書込み済みbyteは観測使用量に含まれる。
- `amplitude-extent`／`tile-amplitude-extent`：slot予約である。未publishのslotはversion行が`segment IS NULL`か、未参照のholeとして残り、byteは観測される。
- `matrix-asset`／`snapshot-export`／`sidecar-write`：sidecar予約である。
- `catalog-transaction`：ownerが`catalog-v3:<canonical path・device・inode・birth identityのsha256>`に一致し、generationが64桁hexであること。登録storeの実DB identityに帰属できなければ拒否する。path hashだけの旧tokenは同じ名前のDBがあっても置換を識別できないため拒否する。
- 上記以外（`unspecified`、`allocation`、intentのない`atomic-json`など）は拒否する。

## temporary GC

予約とは別の操作として、同じ証明とdry run／apply分離で次を行う。

1. writer世代temporary：`<name>.<32 hex>.tmp`形式の通常file（nlink=1）で、checkpoint intent（終端状態を含む）のtemporary名に現れないものだけを削除候補とする。削除直前にdevice／inode／sizeを再確認する。名前が合わないfileと、intentが参照するfileは保持する。admission creditを使わずに空きを増やすだけなので、quota満杯でも実行できる。
2. dead branch：`snapshots`行のrootと、manifest（store、およびその隣のglobal decision）に現れるrootの祖先chainをliveとし、それ以外のroot、そのviews／root edges／log bounds／checksums、どのliveなviewにも参照されないversion、versionを持たないsegment fileを削除する。意味は既存の`StoredSimulator.collect_garbage`と同一で、allocator境界を超えるextentは触らない。dead runのactive_runs行も、全ownerの停止を証明済みなので削除する。catalog編集はadmitting budgetの`CatalogConnection`で行い、admission余裕がなければ拒否する（先にtemporary GCで空きを作る）。snapshotがない（liveなrootが空）storeは、明示指定がなければ拒否する。

## 範囲外

共有policyで束縛されたledgerと、次節のstore-private ledgerを対象にする。cross-host、NFS、別ledger／外部job、電源断、payload checksumの全検証は対象外である。

## store-private ledgerの回収

`DiskBudget`を渡さないstore（`FileBlockStore`が自前で開く）は、`<store>/metadata/disk-ledger.sqlite3`をprivate ledgerとして使う。新規ledgerの`private_policy`へquota／min_free／drain／bootstrap／page／record／volume／accounting modeを永続化し、通常openとrecovery-only openで一致を検証する。

- ledger親directoryとstoreのexclusive leaseでowner停止を証明する。PIDや時刻で解放しない。
- ledger名と厳密layout、rootがstore 1個、shared policy行なし、schema／trigger／DELETE journal／page envelope／native VFS／物理headroomを検証する。catalog未作成の場合は[新規startup intent](phase4_catalog_startup_recovery_implementation.md)で存在条件だけを補完する。
- 2026-10-16以降、private policyのtable／rowがない旧ledgerは既定値でも拒否する。historical quotaやdrain余裕をcaller defaultsから推測しない。通常の`FileBlockStore` reopenも無言でpolicyをbindせず拒否し、generic `DiskBudget`がflagを省いて保存済みprivate bindingを迂回することも拒否する。移行するには独立した明示offline migrationで過去policyの根拠を確定する必要があり、本回収APIは移行しない。
- 保存policyがあれば非既定値も検証して回収する。`recovery_only=True, store_private=True`は新規admission／migration／root登録を行わず、logical quota／pending／drain超過時にも事前課金pool内のCASとreceiptだけを許す。min_free、未割当pool、page／journal capと実I/O失敗は維持する。
- dry run→plan digest付きapply、exact CAS＋receipt、artifactを削除せず存在byteを課金する原則は共有policyと同じである。GCも同じscopeと保存policyを利用する。

SQLiteが非空のsideファイルを読む前に拒否するため、`_persisted`のread-only openより前にside fileを検査する（共有policy modeにも適用）。`wal`が残る場合にread-only openが`-shm`を作る副作用を避けるためである。

## catalog予約のfile単位帰属（追加）

catalog leaseのownerはDBのcanonical path、device、inode、birth identityを固定する。別catalogのDBへ交換した場合、同じ内容のDBで置換した場合、別volumeやsymlink／hardlinkへ変更した場合は一致しない。writerはDB自身のsymlinkを拒否し、親directoryの表記はresolveしてcanonical pathへ正規化する（root外のparent symlinkと`..`を含む既存APIを維持する）。writerはopen中のDB identityを記憶し、各transaction開始時に確認してから予約する。lease再利用にもowner一致を要求する。新規DB作成は[durable startup intentとinode CAS](phase4_catalog_startup_recovery_implementation.md)で扱い、open成功時に通常のidentity付きtokenへ原子的に移す。以前の未帰属startup tokenは推測で回収しない。

rollback journalはDBの固定suffix `-journal`（WAL／SHMも検査対象）のpathへ帰属する。journalのinodeはSQLiteが作成・削除するため、token作成時のinodeをDBと同様に固定することはしない。offline検査は各side pathの存在、device／inode／byteを計画に記録し、非空side fileは常に拒否する。admitted reopenでjournalを消費してから再検査する。DBと全side fileのpath／identity／byteはtokenの`catalog_attribution`とreceiptにも保存する。applyではレビュー済み計画の再計算に加え、tokenを変更する前に全catalog namespaceを再確認する。共有ledgerに複数catalogがある場合もtokenごとに独立した帰属を報告する。schemaの変更や旧tokenの自動移行は行わない。

inode再利用による誤帰属を避けるためbirth identityもownerに含める。macOSは`st_birthtime_ns`があればそれを、なければ`st_birthtime`のfloat値をhexで固定する。Linuxはlibc `statx`の固定256-byte ABIをread-onlyで呼び、`STATX_BTIME|STATX_INO`の返却maskと同時取得したdev/inoの一致を確認してnanosecond birthtimeを使う。API／filesystemがbirthtimeを提供しない場合、writerは未帰属tokenで続行できるがoffline精算は拒否する。接続中のidentity確認もbirth identityを含む。macOS float値の分解能、filesystemのbirthtime分解能を超える再利用衝突は検出できないため、完全なファイル世代識別保証ではない。通常のSQLite DELETE journal transaction/reopenはDB inode/birthtimeを維持し、proxyは`VACUUM`を拒否する。意図的にbirthtimeを保存して複製・偽装する非協調processは対象外である。
