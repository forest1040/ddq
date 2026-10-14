# P4-009／Phase 4性能：catalog commitのgate単位集約（実装）

更新日：2026-10-15。[設計](phase4_catalog_batch_design.md)の決定事項に従って実装した。crash recoveryの契約は変えていない：最後にcommitしたsnapshot（global decision manifest）から再開し、それ以降のcatalog行は到達不能として捨てる。

## 実装

- **`CatalogConnection.batch()`**
  - `BEGIN IMMEDIATE`で書込みlockを取り、batch内では次のようにふるまう：
    - 平の`commit()`を吸収して回数だけ数える（`absorbed_commit_count`）。
    - 入れ子writerの`BEGIN`は省く。
    - 入れ子の`with db:`は既存の`_depth`規則で外側に合流する。
  - envelopeの扱い：statementごとのenvelope検査とVFS capは従来どおりかける。入力envelopeが満ちたら、それまでの作業をcommitしてtransactionを続ける（`batch_split_count`）。この時点までの`on_commit` hookを実行し、`on_rollback` hookは破棄する。
  - batch内のrollback（statement失敗など）でbatchは`broken`になり、以後のstatementは拒否する。
  - hookは3種類：
    - `on_commit`：durable commitの後に実行する。
    - `on_rollback`：rollback時に逆順で実行する。
    - `after_batch`：commitでもrollbackでも、transaction終了後に別transactionで1回実行する。
- **batchの範囲**
  - async runtime：gate／fused groupごとと、epoch末尾（`drain`、`_new_root`、`progress`、`_commit_stage`）。
  - sync runtime：gate stageごと（`DistributedSimulator._catalog_batch`）。
  - `commit_snapshot`は、batchが開いていれば開始前に拒否する。
- **Python状態との整合**
  - `BlockCache.adopt`は、rollback時に採用したversionのentryを捨てる。
  - `MemoryBlockStore.write_version`は、rollback時にresidentを解放する。
  - `reclaim_versions`のlease解放とresidentの削除は、batchのcommit後に行う。決定3のとおり、RAM peakは最大1 stage分増える。
- **file extent**
  - 完了した`versions.segment／offset／checksum`は、sync書込みもasync完了callbackも`after_batch`で別transactionとして記録する（`_catalog_extent`）。その間、`ref()`は保留表（`_pending_extents`）からアドレスを返す。
  - allocatorはbatch内で進める。batch終了後には毎回、in-memoryの高水位まで床上げする（`_allocator_floor`）。こうしてrollbackされたallocatorが、既に渡したslotを再利用しないようにする。

## 設計からの差分

1. **async完了の適用時期。** 決定2では「次のgate batchの先頭で適用」としていた。実装では「batch終了直後に別transactionで適用」にした。次のbatchに入れると、そのbatchがrollbackされた時に、既にbyteが存在するextentのアドレスも消えるためである。目的（rollbackでの不整合を避ける）は同じである。
2. **RAM storeのmarker。** 決定4では「run単位で最初の書込み時に1回」としていた。実装ではversionごとのmarkerをbatch内に残した。commitされたbatchのmarkerがある限りresumeはfail-closedのままで、markerが失われるのはcommit前に失われたgateだけである。そのgateの振幅はどのsnapshotからも参照されないので、最後のsnapshotからのresumeは正しい。なお、markerはcache eviction時にだけ書かれる（従来どおり）。

## 検証

- **新規`tests/test_catalog_batch.py`（9件）**
  - 内側commitの吸収、BEGINの合流、hookの順序。
  - 失敗batchの全rollbackとafter-hook。
  - broken batchの拒否とenvelope split。
  - file／RAMで途中のgate stageを失敗させても、cache・residentが整合して再実行できること（eager oracle一致）。
  - 実子processをgate batch内（1回目と3回目のstage）で`os._exit`させても、最後のsnapshotの状態でresumeでき、新しいsnapshotを作れること。
  - RAM residentとmarkerが、batchのcommit／rollbackに従うこと。
- **回帰**：Mac全回帰は828 passed／14 skipped（新規9件は別途成功）。Linux（ent Docker）全回帰は828 passed／14 skipped（15分34秒）。
- **ent Docker（`sql_profile.py`、1 rank・RAM store・8Q・H×12、速い装置状態）**

| | ledger lease前 | ledger lease後 | catalog batch後 |
|---|---|---|---|
| catalog commit | 858 | 858 | 69 |
| ledger commit | 2,452 | 16 | 16 |
| SQLite時間 | 17.2秒 | 2.6秒 | 0.47秒 |
| execute | 8.2〜9.5秒 | 2.74秒 | 0.39秒 |

実行の高速化にあわせて、runtime ETA（[P4-001／002](phase4_time_model_eta_implementation.md)）をLinuxで再校正・再評価した（[証跡](internal/phase5_timing_validation/matrix-linux-batch/)）。各40 checkで、直前probeによる予測である。

| rank／storage | 集約前 median／worst／coverage | 集約後 median | p95 | worst | coverage | 不安定除外 |
|---|---|---|---|---|---|---|
| 1／RAM | 3.2%／80%／87.5% | 6.8% | 17.9% | 21.6% | 100% | 4 |
| 2／RAM | 31.3%／185%／100% | 18.5% | 47.9% | 48.4% | 100% | 7 |
| 2／file | 32.8%／235%／52.5% | 22.5% | 63.9% | 77.6% | 100% | 6 |

durable commitの回数が減って装置状態の影響が小さくなり、worstとcoverageが大きく改善した。全条件でmedian 30%目標を満たす。held-out期間の装置状態は、1 rankは全て速い状態、2 rank RAMは遅い状態が28/40、2 rank fileは12/40だった。

## 残件

- batch規模のhot journalについて、VFSのside capでのrecovery openを専用試験で確認すること。
- 第2接続／backupとの競合（batch外でだけ成功すること）の専用試験。
- epoch単位への拡大の評価。
- multi-rankで同時にfsyncする場合の装置状態変動の再評価。
