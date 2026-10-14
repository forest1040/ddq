# P4-009 recovery-only metadata pool検証

2026-10-14。Macのuv環境で検証後、許可済み`ssh ent`のLinux Dockerで実行した。研究室`lg`／PBSは使用しない。host／absolute path／capability／receiptを含む内部資料で、外部共有時の非公開化／匿名化対象とする。

## 対象と条件

[設計](../../phase4_recovery_pool_design.md)と[実装](../../phase4_recovery_pool_implementation.md)にある、既存production shared ledgerのrecovery-only openを検証する。対象はcheckpoint sidecar reservationのCAS＋proof receiptであり、全writer recoveryの完了証跡ではない。

新規ledger試験は通常openがquota超過で拒否される状態で、pool内の回収を実施する。新しいcredit、通常release、root登録、一般reconcileを拒否し、root／policyを維持する。missing ledger、unbound/mismatched policy、DDL／PK／FK／trigger改変、unknown root、native VFS不在、pool拡大、nonempty ledger sidefile、物理headroom不足では変更前に拒否する。

SQLiteのnative bounded VFSを使い、256 KiB poolのmain page capまでmetadataを埋めた状態で大きいreceiptを拒否し、reservation／receiptを巻き戻す。小さいreceiptが既存page内に収まる場合の再適用も検査する。journal capを1 byteへ狭める注入では実VFSが最初のjournal writeを拒否する。receipt INSERT／COMMIT前のI/O例外注入はrollback後にclose/reopenしてintegrityと再適用を確認する。これらを実hardware ENOSPC、突然のdisk removal、電源断試験と同一視しない。

checkpoint end-to-end試験はdecision-copy writerのmaterialize記録前に13 byte temporaryを残し、observed＋pool＋pending＋drainがquotaを1 byte超える状態を作る。通常openを拒否した後、dry runのledger SHA-256不変、明示applyでのpending精算、temporaryとpressure fileの保持を検査する。テスト用pressureはlogical file sizeをtruncateで作り、実volumeを満杯にしない。数値resumeの前にfixture側だけがpressureを削除する。helperはデータを削除して物理空きを増やす操作ではない。

新規MPI pressure試験はP=2のrank別root／共通rootで同じ容量境界を作り、全rank検査vote、corrupt ownerによる全token拒否、pool内の全owner回収、resume数値一致を確認する。従来P=1／2／4と同一root P=2の通常回収も維持する。単一hostの検証であり、cross-host evidenceではない。

## 実行環境と手順

Macの[collection](mac/collection.txt)、[source hash](mac/source-hashes.json)、[JUnit](mac/regression.xml)、[raw](mac/regression.txt)を保存する。`UV_CACHE_DIR=/tmp/ddq-uv-cache uv run pytest -q ... --junitxml=/tmp/ddq-recovery-pool-mac.xml`で次の13 moduleを実行した。実MPIに必要なsocketはsandbox制限外で使用した。

```text
test_disk_recovery_pool.py test_checkpoint_recovery.py
test_checkpoint_recovery_mpi.py test_checkpoint_reservation_ledger.py
test_disk_reservation_ledger.py test_disk_failed_allocation.py
test_catalog_capability.py test_catalog_admission.py
test_checkpoint_atomic_admission.py test_admitted_files.py
test_distributed_checkpoint.py test_disk_coordinator.py test_phase4_lifecycle.py
```

Linux作業rootは`/tmp/ddq-p4-recovery-pool-20261014.yDiiIL`。既存image `ddq-linux:dev-20261014`を再利用する。[実行script](run-ent.sh)はnon-root、network none、4 CPU、8 GiB RAM、512 PID、256 MiB shmに制限し、source／tests／examplesをread-only、専用local ext4 scratch／artifactsをwrite mountする。`.git`、internal／ref、Mac venvは転送しない。既存コンテナ、host package／daemon／kernel設定は変更しない。

通常回帰のwhole-command上限は1800秒、nativeは600秒。MPI各ケースは既存120秒、native MPIは180秒の上限を保つ。native I/Oは限定seccomp profile＋`DDQ_NATIVE_URING=1`でkernel I/O 6ケースとbuffered／O_DIRECT各2 rankのlifecycleを別実行する。新規metadata回収はpayload backendの性能校正ではない。

## 結果と照合

最終Macは**174 passed、95.22秒**。Linuxは**171 passed、318.82秒**、nativeは**8 passed、99.42秒**。3実行すべてfailure／error／skipなし。結果とartifact SHA-256を[manifest](manifest.json)に保存する。Mac専用direct coordinator P=1／2／4の展開により、Linux通常回帰との件数差は3件になる。[検証script](verify-results.py)をuvで実行し、JUnitのnode ID集合と各platformのcollection、source hashとworkspaceの一致、failure／error／skipを照合した。

初回の関連97件と新規試験の途中結果を最終件数へ合算しない。新しいpressure試験のimport漏れと、テスト編集時に重複したsettle呼び出しを修正し、Lunaのレビューを受けて保護対象tableのPK/FKを含むDDL照合を厳密にした。最終sourceで13 moduleを実行した。

[capture script](capture-ent.sh)は終了したscratchをread-onlyで読む。実native catalog 4件のcreation capabilityを抽出してC source hashを検査し、回収試験が実ledger transactionで保存した[receipt 21件](linux/recovery-receipts.json)のcanonical SHA-256とevidence suffixを照合した。単独capability probeとtest中のbinary fingerprintは区別する。過去の[f3dfa9a時点の回収記録](../phase4_checkpoint_recovery_validation/README.md)はそのsourceの履歴として保持する。

回帰所要時間を性能校正値に使わない。metadata poolは論理的にprechargeされ、OS上で物理preallocationされた領域ではない。open／BEGIN前／BEGIN直後にmin_free＋未割当poolを確認するが、外部jobによる突然の空き容量減少やfilesystem metadataまで保証しない。

## 残件

未知の旧DDLやhot ledger journalを修復する操作、実hardware ENOSPC／rollback失敗時の完全なfault matrix、電源断、cross-host、全writer／uncommitted／unlinked tokenの回収、temporary branch GC、外部job／別ledgerの調停は未受入。P4-009 IN_PROGRESS、Phase 4未完了、`strict_capacity_guarantee=false`を維持する。
