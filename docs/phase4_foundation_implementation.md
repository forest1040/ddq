# Phase 4 残件：容量予約・runtime計測の基盤実装

更新日：2026-10-14。対応：[P4-009](phase4_tasks/P4-009_disk_admission.md)、[P4-001](phase4_tasks/P4-001_measurement.md)。先行する実装単位をMacで検証した。両backlogは `IN_PROGRESS` とし、Phase 4全体とM1の完了条件は維持する。今回はクラスタ接続・PBS job投入を行っていない。

## 容量予約

[CatalogConnection](../src/svsim/catalog_admission.py)がconnection、cursor、script、native backupを同じ予約経路へ通す。初回open／hot-journal recoveryより先にtokenを取得し、transaction開始前にmainとrollback journalの成長を予約する。commit／rollback後にguardを無効化して精算する。同process／同catalogの複数connectionはenvelopeを同時所有しない。

通常transactionのmain成長は256 KiB、累積SQL入力は64 KiB以下。backupはsourceのpage数から専用envelopeを作る。入力が小さくても `INSERT … SELECT zeroblob(...)` 等の実成長はpage／VFS上限で停止する。

[native VFS](../src/svsim/native/sqlite_budget_vfs.c)は `sqlite3ext.h` のdispatch pointerでPython自身のSQLiteへ登録し、別SQLite libraryにはlinkしない。下位VFSへwrite／truncateを渡す前にcapを検査する。DELETE journal、FULL synchronous、mmap無効を使用し、WAL、ATTACH、VACUUM、未管理のdisk tempは拒否する。SQLite versionと実capを照会できる。

既存C compilerと `sqlite3ext.h` が必要。MacではHomebrew SQLiteのheaderも探索する。writable storeを開く際にbuild／loadし、import、probe、静的推定だけではbuildしない。dependencyは自動installしない。quota付きstoreと本番ledgerはguardが利用できなければ開始を拒否する。

[DiskBudget](../src/svsim/disk_budget.py)は同volumeのrankが使うpersistent token ledger。operation／owner／generation、物理化済み量、state、回収根拠を保持し、途中停止したtokenを時刻だけで返さない。`reserve(..., writing=True)` は予約とWRITING状態を一つのdurable transactionで保存する。

ledger自身には8 MiBの固定metadata poolを事前課金する。mainは最大4 MiB、rollback journalは残り4 MiB。VFS、page／record／入力上限で拡張を制限する。同じledgerのreopenでpoolを重複課金せず、物理化したpool部分はfree検査の未使用予約から除く。injected usage／free hookは別scopeで、実容量の保証に使わない。

extentはallocator SQLより先に予約し、成功write後にmaterialized bytesを更新してからversions SQLを確定する。失敗extentは再利用せず、残ったfile bytesは次の容量検査へ含める。不変Frameの再internは既存rowと内容を照合し、同じものへSQLを書き直さない。

| writer | 接続・境界 |
|---|---|
| 初期catalog、allocator、views、Frame、root、history、GC | FileBlockStoreのCatalogConnection、初期作成からguard対象 |
| ledger／rollback journal | 固定metadata pool、同じSQLiteのVFS |
| synchronous／async extent、RAMのfile flush | 発行前token、成功後の物理化精算 |
| semantic journal／trace | 全recordのbyte予約。traceをflushしてから返却。recordは1 MiB以下 |
| matrix asset | temp作成前に全体を予約し、fsync／renameまで保持 |
| node-local checkpoint／lease／payload copy | 既存atomic JSON／chunk交換の予約を維持 |
| prefix catalog copy／stored migration用catalog | backup専用envelope、migration tempはtargetのlocal directory内 |
| stored snapshotの補助JSON | file書込み前予約。authoritative snapshotはcatalog内のcommit |
| caller指定のstate export、build temp、campaign report | 今回のcatalog guardの保証scope外 |

全namespace、独立dataset、外部processを含むhost総量の保証は未達なので、`strict_capacity_guarantee` はfalse。外部jobの実空き消費やOS metadataも予約だけでは防げない。全writerのfault／回収証跡、pool境界の網羅、独立dataset間のcoordinator、Linuxでの検証がP4-009の残件。

## completionとprofile

[collector](../src/svsim/runtime_measurement.py)はmain threadからcompletionを受ける。ringは最大64／condition、conditionは最大256。serialized bytes、event／group識別子、indexにも上限がある。collectorとsnapshot用領域をmanaged RAMへ予約し、prefix batch／parameter-shiftでは親の共通ledgerへ移して二重精算を防ぐ。

I/Oはqueue wait、file service、callback終了までのwhole-request wallを区別する。短read、取消、worker error、checksum／catalog callback errorは成功sampleへ入らない。resolved backend、device、alignment、payload／slot、engine、queue上限とsubmit時のin-flight数を残す。不明なcache／working setはunknownを保持する。

MPIはsource側のack込みwhole protocolを記録する。gate／fusion／epochはgroup wallとCPU時間を持ち、pure kernel serviceと混同しない。失敗group配下のchildをcostから除き、warmupをI/O・MPI・groupへ共通に伝える。

[adapter](../src/svsim/runtime_profile_adapter.py)は不変ProfileDocumentのfingerprint、stable context、全condition、scope、品質を比較する。attach時も現allocation／buildと照合する。injected／manual／legacy／外部backendのcellはproduction costへ変換しない。min／maxから予測区間を作らず、明示したheld-out検証とcoverageを持つconditional envelopeだけを受け付ける。

raw campaignは専用kindでbaselineを書き換えない。完全なprofile registry、Frame／packing／metadataの独立stage、有界raw chunk flush、監視overheadの対照、ETA／cache／gradientへの自動接続、Linux／cross-host校正はP4-001に残る。

## 有限runtime campaign

`svbench runtime` は明示したscratch、RAM、SSD、時間予算で実executorを動かす。estimate／probeでは実行・MPI初期化をしない。

```sh
uv run --locked --extra mpi mpiexec -n 2 .venv/bin/svbench runtime \
  --scratch /private/tmp/ddq-runtime-new --output /private/tmp/ddq-runtime-new.json \
  --num-qubits 6 --block-bits 2 --ranks 2 \
  --ram-budget 268435456 --disk-budget 268435456 --wall-seconds 120 \
  --repetitions 5 --warmup 1 --backend direct --engine worker \
  --max-io-requests 2 --suites kernel-storage,memory-mpi,storage-mpi
```

kernel＋storageはFILE、compute＋MPIはRAM、storage＋MPIはFILEで、owner bitへ実通信する回路を使う。各trialでeager oracleとのstate一致とnormを検査する。P=1のMPI suiteはnot-applicableで、転送ゼロをMPI性能に数えない。

startup、gate登録、oracle、state検証をexecute wallと分ける。whole-groupは準備／検証／close込みの診断値。execute wallと資源peakはrank MAX。warmupを除いてcase別min／median／maxを残す。今回overlapの正味gain、confidence interval、未来の予測envelopeは作らない。

時間上限はtrial間で確認するsoft deadlineで、collectiveを強制停止しない。matrix driverにはprocess timeoutがある。SSD予算はrank数で割った保守的quotaとして同host ledgerへ適用する。metadata pool・drain余裕を含め最低10 MiB／rankを要求する。warmup／測定反復は各1〜100、nは16以下、reportは16 MiB以下。

再現script：[phase4_foundation_matrix.py](../examples/phase4_foundation_matrix.py)。raw／結果は[内部検証資料](internal/phase4_foundation_validation/README.md)。今回の基盤のcorrectness資料であり、量子化学の時間推定や最終性能比較の置換には使わない。
