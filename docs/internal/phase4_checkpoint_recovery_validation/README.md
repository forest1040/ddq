# P4-009 checkpoint reservation recovery検証

2026-10-14。Macのuv環境と、許可済み`ssh ent`のLinux Dockerで実行した。研究室`lg`／PBSは使用しない。host、絶対path、toolchainとreceiptを含む内部資料であり、外部共有時には非公開化／匿名化する対象である。

## 対象

最新committed v4 checkpointのprepare／global decision／decision-copyにリンクした容量予約の、明示offline・metadata-only回収を対象にする。[設計](../../phase4_checkpoint_recovery_design.md)と[実装](../../phase4_checkpoint_recovery_implementation.md)にAPIと境界を記載した。

実行中runtime、単独FileBlockStore／ReadOnlyBlockStore reader、正常終了／強制終了した子process、constructor／close失敗、directory inode変更、symlink／root内`..`、世代とCAS、receipt挿入失敗時のtransaction rollback、破損した最新cut、journal／catalog／sidecar、hot catalog side file、異なるledger、unlinked token保持を検証した。回収APIは振幅payloadを読まず、通常resume後の数値oracleでfixtureの状態一致を別途確認する。

実子processを予約直後、13 byte partial write後、rename後かつdirectory fsync前に`os._exit`で終了させる。kernel lease解放後に回収し、partial temporaryを保持したままpending accountingを精算する。通常resumeは欠損decision-copyを作成し、temporaryは削除しない。

実MPIの新規回収workerはP=1／2／4のrank別directoryとP=2の同一rootで実行する。各rankが他rankのartifactを読まないこと、全rank検査vote前の不一致が全tokenをactiveに残すこと、適用後のresume数値一致を検査する。これは単一host検証であり、cross-host証跡ではない。

## 環境と実行

Macの[環境](mac/environment.json)、[最終collection](mac/final/collection.txt)、[最終source hash](mac/final/source-hashes.json)を保存する。最終回帰は17 moduleを指定した`UV_CACHE_DIR=/tmp/ddq-uv-cache uv run pytest -q ... --junitxml=/tmp/ddq-p4-recovery-mac-final.xml`。実MPIに必要なsocket制限外で実行した。

```text
test_checkpoint_recovery.py test_checkpoint_recovery_mpi.py
test_checkpoint_reservation_ledger.py test_disk_reservation_ledger.py
test_disk_failed_allocation.py test_catalog_admission.py test_storage.py
test_storage_directory_lease.py test_dataset_lease.py
test_checkpoint_atomic_admission.py test_admitted_files.py
test_distributed_checkpoint.py test_stored_recovery.py test_stored_budget.py
test_phase4_lifecycle.py test_disk_coordinator.py test_memory_store.py
```

Linux専用rootは`/tmp/ddq-p4-recovery-20261014.WwrDGc`。source／tests／examplesをread-only mountし、`.git`、内部資料、ref、Mac venvは転送しない。既存image `ddq-linux:dev-20261014`を再利用し、[image](linux/final/image.json)、[kernel](linux/final/kernel.txt)、[local filesystem](linux/final/filesystem.txt)、[source hash](linux/final/source-hashes.txt)を保存する。uv／OpenMPI／liburing環境の構築手順は[Linux開発環境](../../linux_development.md)を参照する。

[最終実行script](run-ent-final.sh)はnon-root、network none、4 CPU、8 GiB RAM、512 PID、256 MiB shm、専用local ext4 scratchに制限する。初回sourceのcontext／scratch／artifactsと最終sourceのcontext-final／scratch-final／artifacts-finalを分け、実行中のsourceを変更しない。通常回帰のwhole-command上限は1800秒、nativeは600秒。既存MPI回帰と回収workerの各subprocess上限は120秒、native MPIは180秒である。既存の他コンテナ、host package／daemon／kernel設定は変更しない。

Nativeは限定seccomp profileと`DDQ_NATIVE_URING=1`でkernel I/O 6ケース、buffered／O_DIRECT各2 rankのlifecycle 2ケースを実行する。実engine、state／expectation／gradient、checkpoint／resumeを既存workerで確認する。新規回収helperそのもののbackend別・native全fault matrixを完了扱いにしない。

## 結果と証跡

最終Macは[JUnit](mac/final/regression.xml)と[raw](mac/final/regression.txt)の**184 passed、112.16秒**。Linux通常回帰は[JUnit](linux/final/regression.xml)の**181 passed、436.55秒**。Nativeは[JUnit](linux/final/native.xml)の**8 passed、155.21秒**。最終3実行すべてfailure／error／skipなし。artifact集合は[manifest](manifest.json)へ記録する。[検証script](verify-results.py)をuvで実行し、collectionとJUnitのtest IDを照合する。source hash、persisted receipt digest、実native capabilityも検査する。Mac専用direct coordinatorのP=1／2／4展開があるため、Linux通常回帰との件数差は3件になる。

[初回Mac](mac/initial.xml)は172 passed／1 failedだった。新規テストがFileBlockStoreに存在しないcontext-manager APIを使ったためで、explicit closeへ直した。Lunaのレビューで単独ReadOnlyBlockStoreのlease不足も修正し、適用直前のdirectory identity再検査を加えたsourceで17 moduleを再実行した。初回成功分を最終sourceの証跡として合算しない。

続く[Mac回帰](mac/regression.xml)は181 passed／112.07秒、[Linux初回](linux/initial/regression.xml)は178 passed／513.11秒、[native初回](linux/initial/native.xml)は8 passed／128.35秒だった。その後root外のsymlink／`..`で実保存先とintentのcanonical pathが異なり得る正規化を修正した。root外aliasの成功と別directoryの拒否、実writer／recovery／resumeのend-to-end試験を追加し、[path関連49件](mac/path-regression.xml)を確認した後、17 moduleとnativeを最終sourceで再実行した。初回／中間結果と最終結果を合算しない。

[最終capture script](capture-ent-final.sh)は完了したscratchをread-onlyで読む。実native catalog 4件のcreation capabilityを保存し、C source hashを照合した。また回収試験がledger transactionに保存した[receipt 16件](linux/final/recovery-receipts.json)を抽出し、canonical proof SHA-256とevidence suffixの一致を検査した。単独capability probeは別processのbuildであり、test中のcompiled binaryと同一だとは扱わない。

回帰所要時間を性能校正値に使わない。最新cutを選ぶ順序は既存loaderと同じmanifest mtime基準であり、独立したdurable sequence allocatorを導入した検証ではない。

## 受入境界

回収は協調する現在のsvsim runtime／readerとのkernel flock排他を前提とする。外部writerのrename／書換えを防止するものではない。検査vote後の適用はper-token CAS＋receipt transactionであり、全rank／全tokenの分散atomic transactionではない。途中成功を巻き戻さず、残ったactive tokenを再検証して再実行する。

dry runでも既存ledgerのschema／root／policy metadataが初期化・更新され得る。reservation精算とreceipt保存はapply時だけ行う。offline helperのinspection上限は通常runtimeのmanaged RAMへ課金される予算ではない。

uncommitted／unlinked、extent、catalog／journal、payload、trace／asset／exportを含む全writerの回収、temporary branch GC、別ledger／外部job調停、振幅と全physical closureの完全検証、cross-host、電源断は未受入である。`strict_capacity_guarantee=false`、P4-009 IN_PROGRESS、Phase 4未完了を維持する。
