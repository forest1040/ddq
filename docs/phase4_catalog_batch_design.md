# P4-009／Phase 4性能：materialize中のcatalog commitのgate単位集約（設計案）

作成日：2026-10-15。状態：設計確定、実装中。[ledger lease](phase4_ledger_lease_implementation.md)でdisk予約ledgerのcommitは減った。残るLinux実行時間の大半と、装置状態による変動の主因はcatalog（`synchronous=FULL`、`journal_mode=DELETE`）のcommitである（[runtime ETA matrix](phase4_time_model_eta_implementation.md)）。ここではその集約を設計する。

## 1. 現状の計測

1 rank、16 Block、12 gateのasync runtimeで、catalogのcommitは約858回だった（ent Docker、`sql_profile.py`）。commitの発生源は次のとおり（path:line）。

| 粒度 | 発生源 | 回数 |
|---|---|---|
| Blockごと | `cache.adopt`→`store.new_version`（storage.py:254） | 1 |
| Blockごと | `_publish`→`store.publish`（views／views_v2／catalog_checksums、storage.py:222、phase2_runtime.py:147） | 1 |
| Blockごと | `_publish`のextent_owners（distributed.py:429） | 1 |
| Blockごと、RAM store | `_memory_reclaim`の`delete_unwritten`／`reclaim_versions`（storage.py:282、block_store.py:115） | 約1 |
| Blockごと、file store | allocatorの`BEGIN IMMEDIATE`と完了時の`versions.segment`更新（async_store_io.py:62、85） | 約2 |
| stageごと | `_new_root`、`phase2.progress`、`_commit_stage`（seal、`active_runs`）、`_rule`、`compact_metadata` | 約4〜5 |

合計はgate当たりRAM約68回、file約84回で、各commitがLinuxの装置fsync（約0.8 msまたは約5 ms）を複数回払う。

## 2. 前提：materialize中のcatalog durabilityはrecoveryに使われない

- **resume。** `resume`→`load_global`（distributed_checkpoint.py:147）は、global decision manifestが指すsnapshotのroot／frontierだけを復元する。`restore`は、そのrootの祖先chainだけを`validate_root`する。最後のsnapshotより後に作られたroot・view・versionは到達不能で、crash後はhot journalのrollbackと到達性GCで捨てられる。
- **失敗したmaterialize。** Python側で元のrootへ戻る（async_scheduler.py:478）。
- **durableな境界。** `commit_snapshot`（distributed_checkpoint.py:83-91）だけが永続境界である。cache flush→RAMの`flush_to_file`→journal fsync→segment fsync（`store.flush`）→`validate_root`→`seal` commit→prepare manifestのfsync→`snapshots`行のcommit→global decision、の順に進む。
- **途中のcatalog commitは、byteのdurabilityも保証していない。** file storeのsegment byteは、snapshot時の`flush`まで`fsync`されない。
- **RAM store。** 振幅は揮発し、`versions.segment`はNULLのままである。永続的な意味を持つのは、「振幅をRAMに持ったことがある」ことを記録する`resident_versions` markerだけで、resumeをfail-closedにする（block_store.py:26）。

したがって、gate内のcommitをまとめても**crash recoveryの契約（最後にcommitしたsnapshotから再開する）は変わらない**。変わるのは、process内でのrollback粒度とSQLiteのlock期間である。

## 3. 設計

### 3.1 集約の仕組み

1. `CatalogConnection.batch()`を追加する。これはcontext managerで、`BEGIN IMMEDIATE`で書込みlockを取り、`_depth`を1つ上げる。
2. batch中は次のようにふるまう：
   - 平の`commit()`は何もしない。統計のため回数だけ数える。
   - `execute('BEGIN IMMEDIATE')`はBEGINを省く。
   - 入れ子の`with db:`は既存の`_depth`規則に従う。
   - 外側の`batch()`を出るときに1回だけ`COMMIT`する。
3. envelopeは開始時にBlock数×statement数から見積もり、`_start(minimum_main_bytes)`で予約する。transactionあたりのSQL入力上限（現在64 KiB）もbatchの見積もりに合わせて引き上げる。VFSのmain／side capは引き続き必ず設定する。

### 3.2 集約の範囲

対象は1 gate stageで、`_new_root`から`phase2.progress`、`_commit_stage`までである。async runtimeの`_execute`、`_execute_local_fused`、`_rule`、sync runtimeの`_execute`を、それぞれ1つのbatchで囲む。epoch単位にまとめるとSQLが大きくなり、16 KiBのpage cacheを溢れて、EXCLUSIVE lock下のspillとjournal fsyncが増える。そのため初版はgate単位にする。期待値はgate当たり約68〜84回のcommitが1〜2回になることである。

### 3.3 batchの外に残すもの（同期のまま）

- `commit_snapshot`の全手順と`snapshots`行。`seal`より前に、開いているbatchが無いことをassertする。
- `fork`の`backup_from`、`collect_garbage`、`flush_to_file`、`commit_prefix`、migration。
- disk ledgerのreserve／materialize。これは別DBで、lease設計のまま変えない。順序「予約→byte書込み→catalogの`versions`更新」は、同じtransactionか後のtransactionで保つ。

### 3.4 rollbackとPython状態の整合

batch内のstatementが失敗すると、gate全体のcatalog変更がrollbackされる。そのため、次のPython状態も同じ単位で戻す。

- 対象：batch中にadoptしたcache entry、`MemoryBlockStore.resident`の追加分、`reclaim_versions`で閉じたlease。
- 方法：batchは、作成したversion IDと解放予定のleaseを記録する。COMMITが成功してから解放を確定し、rollback時には作成分をpurgeする。lease解放をcommit後へ遅らせる分、RAMのpeakは最大1 stage分増える。この増分はadmissionの見積もりへ加える。
- 既存の経路：materialize失敗時のrootの巻戻し（async_scheduler.py:478）から、このhookを呼ぶ。

### 3.5 async file writebackの完了

完了callback（async_store_io.py:81-94）は、mainの`poll`／`wait`の中でbatch途中にも走る。batchがrollbackされると、完了したextentの`segment`が消える一方で、cacheはclean扱いのまま残る。初版では次のどちらかを採る。

- (a) 完了更新をbatchへ入れず、次のgate batchの先頭でまとめて適用する（推奨）。
- (b) rollback時に、該当entryをdirtyへ戻す。

### 3.6 RAM storeのfail-closed

`resident_versions`の行を書くのは、最初のRAM書込み時に1回だけにする（run単位のmarker）。batchが失われた場合でも、markerは確定済みであるようにする。

## 4. 受入条件

| 区分 | 試験 |
|---|---|
| 数値 | 既存のeager oracle、期待値、adjoint、checkpoint／resumeを全てbatch有効で再実行して一致すること |
| crash | materialize中（batch commit前・後）に実processをkillし、最後のsnapshotからresumeして状態が一致すること。RAM storeはmarkerによってfail-closedを保つこと。新規試験 |
| rollback | batch内のstatement失敗とenvelope超過で、catalog・cache・resident・leaseが元に戻ること。後続の`ref()`が失敗しないこと |
| async | 完了callbackがbatch途中に来てもsegmentとclean状態が一致すること。rollback時もそうであること |
| lock | 同じcatalogの第2接続とbackupが、batch外でだけ成功すること |
| hot journal | batch規模のhot journalからのrecovery openが、VFSのside capに収まること |
| 性能 | ent DockerとMacで、gate当たりのcatalog commit数、execute時間、装置状態ごとの変動を前後比較すること。runtime ETA matrixを再測定すること |

## 5. 決定事項（2026-10-15、利用者確認）

1. 集約の単位はgate stageとする。
2. file storeの非同期完了は、次のgate batchの先頭でまとめて適用する（3.5の(a)）。
3. lease解放をcommit後へ遅らせることによるRAM peakの増加（最大1 stage分）を許容し、admissionの見積もりへ加える。
4. RAM storeのmarkerは、run単位で最初の書込み時に1回だけ書く（fail-closedの意味は保つ）。
