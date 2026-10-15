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
- `catalog-transaction`：ownerがstoreのcatalog pathのsha256に一致し、generationが64桁hexであること。登録storeに帰属できなければ拒否する。
- 上記以外（`unspecified`、`allocation`、intentのない`atomic-json`など）は拒否する。

## temporary GC

予約とは別の操作として、同じ証明とdry run／apply分離で次を行う。

1. writer世代temporary：`<name>.<32 hex>.tmp`形式の通常file（nlink=1）で、checkpoint intent（終端状態を含む）のtemporary名に現れないものだけを削除候補とする。削除直前にdevice／inode／sizeを再確認する。名前が合わないfileと、intentが参照するfileは保持する。admission creditを使わずに空きを増やすだけなので、quota満杯でも実行できる。
2. dead branch：`snapshots`行のrootと、manifest（store、およびその隣のglobal decision）に現れるrootの祖先chainをliveとし、それ以外のroot、そのviews／root edges／log bounds／checksums、どのliveなviewにも参照されないversion、versionを持たないsegment fileを削除する。意味は既存の`StoredSimulator.collect_garbage`と同一で、allocator境界を超えるextentは触らない。dead runのactive_runs行も、全ownerの停止を証明済みなので削除する。catalog編集はadmitting budgetの`CatalogConnection`で行い、admission余裕がなければ拒否する（先にtemporary GCで空きを作る）。snapshotがない（liveなrootが空）storeは、明示指定がなければ拒否する。

## 範囲外

共有policyで束縛されたledgerのみ対象である（recovery-only openの既存条件）。storeごとのprivate ledgerは未対応。cross-host、NFS、別ledger／外部job、電源断、payload checksumの全検証は対象外である。
