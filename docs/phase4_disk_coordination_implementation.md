# Phase 4：同volumeの共有disk admissionとforeground RAM

更新日：2026-10-14。P4-009の同volume調停と、P4-007の任意metadataによる作業領域不足を追加対応した。[writer監査](phase4_writer_audit.md)で確認した境界を実装へ反映する。Macで検証し、クラスタ接続やPBS投入は行わない。全writerのfault／回収とLinux検証は残し、P4-009／007はIN_PROGRESSを維持する。

## 同じledgerを使うdataset／fork

`DistributedConfig.disk_admission_ledger`を追加した。node-localの絶対pathだけを受け付ける。同じhost／volumeのrankが異なるpathを選ぶ場合は、ledgerを開く前にcollective拒否する。hostごとのpathは異なってよい。同じpath文字列が別hostの通信や共有filesystemを要求するわけではない。

通常のbootstrapは従来のhost／volume／root集合からledgerを選び、共有policyをbindする。独立datasetで合算する場合は、同じlocal ledger pathを明示する。forkは新しいcatalogのbootstrap前から親の実ledger pathを継承する。batch／parameter-shiftの内部forkもこの経路を通る。

```python
config = DistributedConfig(
    directory="/private/tmp/ddq-job/dataset-a",
    block_bits=2,
    storage_scope="node-local",
    disk_quota_bytes=64 * 1024**2,
    disk_admission_ledger="/private/tmp/ddq-job/host-ledger.sqlite3",
)
```

quotaは共有ledgerに登録したroot集合の合算上限である。各datasetに同じquotaを別々に与えた独立上限とは扱わない。別volumeは別ledgerを使う。明示ledgerを共有しない独立jobや外部fileまで自動的に調停するものではない。

## 永続policyとroot登録

`DiskBudget(shared_policy=True)`はquota、min-free、drain、metadata pool、実page cap、record cap、device、injected accountingの有無を、同じSQLite transaction内で初回bindまたは完全一致照合する。bind済みledgerはgeneric callerがflagをfalseにしても照合を省略できない。不一致は新root／tokenを書き込まず拒否する。

旧ledgerにactive reservationがある場合は初回bindを拒否し、tokenを保護する。quiescentな旧ledgerはbindできる。旧tokenの判断をPID／期限だけで行わず、既存の明示recoveryでowner／journal／inventory／cutを確認する。

root登録後の全root使用量＋metadata pool＋未物理化予約＋drain余裕と、実空きの下限をtransaction内で確認する。不足時はroot登録もrollbackする。snapshotは共有policyの有効性と登録root数を返す。

永続root表は祖先pathが子孫pathを覆う最小集合とする。親の下のsuffixを毎回別rootへ追加しない。後から親を登録する場合も子を置き換え、実file使用量と既存tokenを保持する。pathの文字列prefixではなく`Path.is_relative_to`を使い、`Root.A`と`Root.A2`を混同しない。非重複rootは256件、呼出しごとの入力も256件以下。

suffixのcloseだけでdurable directoryを自動削除しない。閉じたbranchの実fileはquotaへ残す。任意の共有prefix／履歴／snapshotを削除して空きとして扱うことはせず、temporary branchの安全な回収は別のlifecycle残件とする。root圧縮はfile GCではない。

## 最低foreground作業領域の保護

追加したcache診断課金が、状態をRAM予算より大きくした既存MPI試験で最低bufferを圧迫する問題を検出した。予算や試験条件を緩めず、`AsyncBlockCache.ensure`で実行要求のheadroomを確保する。

trace／counterの新規予約はforegroundの要求byteを残した範囲だけで行う。cache evictionだけで満たせない場合に限り、trace→pool／counter→legacy hint→cost summaryの順で任意metadataを精算する。cost summaryを捨てた後はLRUへ戻る。削除した診断行はdrop countへ加え、metadata解放回数も有界に残す。context getterが失敗した場合もunknown／staleとして安全にfallbackする。

managed RAMのdeclare値を変えない単体試験と、stateがrank RAMを超えるMac P=1/2/4のMPI試験で確認する。managed poolの予約と全processのRSSは別scopeである。

## 検証結果

Mac／uvの最終全件回帰は650 passed／2 skipped。関連回帰107件に、実P=1/2/4のbuffered／F_NOCACHE、独立dataset／fork／template parameter-shift、既存大状態とcheckpointを含む。source／worker hash、初期failure、最終log、再現条件を[内部検証資料](internal/phase4_disk_coordination_validation/README.md)へ保存した。
