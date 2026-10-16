# Phase 4 最終 Linux lifecycle 差分受入 runner

2026-10-16、基点 `5cedc1c`。この文書の段階では runner の準備と静的検査だけを行い、新しい Linux test・Docker・性能測定は起動していない。実行は既存 ent 性能測定の終了後に親担当が行う。

## 実行契約

`examples/run_phase4_linux_lifecycle.sh EXPORTED_SOURCE_ROOT FRESH_RESULTS_DIR SUPPLIED_REVISION` を承認済み ent の非root userから実行する。輸送とsource同期は親担当。コンテナへ渡すsourceは `src/`、`tests/`、`examples/` のread-only bindだけで、repository全体、`.git`、docs、実験artifactや環境設定をexportしない。別のwritable bindは新しい結果directoryだけである。

固定image `ddq-linux:dev-20261014` はローカルのものを `--pull never` で使用し、image IDも保存し、inspectしたimmutable IDを実runへ渡す。4 CPU、8 GiB（swapも8 GiB）、非root UID/GID、固定hostname `ddq-phase4-lifecycle`、`--network none`。kernel ENOSPC用に **このcontainer専用** の空の `/full` tmpfs 16 MiBを作り、`SVSIM_FULL_FS=/full` を設定する。container外や既存mountを埋めない。tmpfs、mountサイズ、空であることをrunnerが事前検証する。

Python全体deadlineは1,200秒、stage deadlineの合計は1,080秒。実際の予算は各stage上限と残り時間の小さい方。host側GNU `timeout` は1,230秒＋TERM後15秒で、終了時に当該container名だけをcleanupする。stage失敗・timeout・予期しないskipでは先へ進まず、未実行stageを明示する。速いMac全suiteを再現せず、Linuxのbirth/statx、volume/lease、native SQLite、O_DIRECT、実容量不足の結合差を確認する。目標所要時間は10分以内だが、これは実測値ではない。

再試行は元attemptのtimeout/logを保持し、別の空resultsへ`--stages physical-direct --stage-timeout-seconds 900`等を追加して実行できる。shellは3必須引数以降をPythonへ転送し、outputとsource revisionは必須引数に固定する。`--stages`は既知名のcomma listで、重複・未知・空名を拒否し、指定順によらず元のSTAGES順に実行する。stage上限overrideは1..1200秒、全体deadlineも最大1200秒を維持し、実際の待機は残り全体時間との小さい方になる。`--plan`で環境やtestを起動せず選択を確認できる。

summaryには全6個の`planned_stages`、選択した`selected_stages`、意図的な`intentionally_omitted_stages`、選択済みだが未開始の`not_run`を別々に保存する。subsetのpassedはその選択だけの結果であり、元attemptの失敗を消したり全6stageの新規成功へ読み替えたりしない。再試行先もfresh outputを要求し、元artifactを上書きしない。

## 受入mapping

| stage / 上限 | 選択と埋める差分 | backendの扱い |
|---|---|---|
| Linux事前probe | source3dirの全file SHA256 manifest、実Linux O_DIRECT fd flag・4096 alignment・write/fsync/read checksum roundtrip、実birth identity、native SQLite capability/build provenance。 | direct要求がbufferedへfallbackしたら失敗。probeは結果bind上の一時fileを削除する。 |
| `linux-contracts` / 240秒 | `test_catalog_file_identity`・`test_catalog_startup_recovery` の実birth/ABI/creation CAS、`test_bootstrap_namespace`、`test_runtime_budget_inheritance`、最新standalone shard reader/policy/quota境界の6関数、`test_writer_namespace_admission`全体、admitted sidecarのdestination escape/alias/canonical spelling、append実owner-death、migration clone/source-bound recovery全8件。Linuxでread-only readerとrecovery lease、bootstrap unknown状態、parent/child ownership、append/sidecar閉包を確認する。 | 一回だけ。これらの多くは構成がbuffered固定であり、envをdirectにしてもO_DIRECT実績にはしない。migration source/targetも現試験ではbuffered。 |
| `native-vfs-fault-points` / 120秒 | `test_vfs_fault_points`全体。実native SQLiteのjournal-sync/main-sync/delete前後kill・IOerror、old/new cut、integrity、新規work再開。 | backend非依存のため一回。`SVSIM_VFS_TEST_FAULTS=1`はこのstageだけ。native unavailableによるskipを成功にしない。 |
| `physical-buffered` / 240秒 | private ledger全試験、private共同quota/offline oracle/実別volume拒否、最新`test_uncommitted_gc`全体、同期/非同期segment stage kill全体、segment durable-image model全体、extent byte境界quota sweep1条件＋midwrite failure1条件。 | 各testが実際に読む `SVSIM_TEST_BACKEND=buffered`。GCの新しいsegment名・manifest/birth再検査とphysical inventory、private recoveryのreal cutを実payloadで検証する。 |
| `physical-direct` / 240秒 | 上記と同じphysical workload。 | `SVSIM_TEST_BACKEND=direct`により、各対応storeの実payloadはLinux O_DIRECT。native SQLite catalog自体のO_DIRECTを主張しない。一部policy-only分岐や補助readerはbufferedのままなので、全内部FDがdirectという主張もしない。 |
| `real-enospc-buffered` / 120秒 | `test_disk_real_enospc`全15条件。catalog/trace/journal/sidecar/extent、admission後に別processが実tmpfsを埋める5条件、minfree/exhaustedで変更前拒否10条件。空き回復、old cut、offline settlementと再開を確認。 | buffered payload。logical quotaは巨大にしてkernel ENOSPCと区別。bare unlinked atomic-jsonは既存testの明示caller精算であり、一般offline認証済とはしない。 |
| `real-enospc-direct` / 120秒 | 同じ15条件。 | extent workloadが実O_DIRECT。他4writerはSQLite/通常append/atomic JSONであり、directラベルはpayload backend指定を表す。 |

全stageは直列で、`/full`を使うtestの並列起動はしない。全体結果、stage別command/exit/秒数/JUnit/log、skip理由、source snapshot、image ID、未実行stageを結果directoryへ保存する。skipは原則受入失敗。唯一、有限probe中にinodeが再利用されなかった／birth分解能で識別不能だった `test_reused_linux_inode_is_distinguished_by_birth_identity` はskipを記録して他受入を継続できる。これは実inode再利用の成功証跡には数えない。

## 今回繰り返さない証拠と限界

Mac core高速suite、frame/replan/gradientの全semantic cross-product、performance campaign、MPI throughputやbaseline認証を再実行しない。上記の小state数値oracleは維持する。実multi-host/PBSは親担当の別証拠であり、固定hostname `--network none` のこのcontainerは別host通信を証明しない。native **SQLite** faultpointsとnative **io_uring**は別で、後者はopt-inしない。containerのio_uring制限をbuffered fallbackで成功扱いにもしない。

segment durable-image modelはnegative controlを含むfsync順序検証で、実電源断ではない。SIGKILLもpower lossと同一視しない。全filesystem metadata・非協調writerに対するhard physical quotaの主張は行わず、`strict_capacity_guarantee=false`を維持する。実Linux実行前のこの準備commitを、テスト成功やP4-009の新しい受入完了として記載しない。
