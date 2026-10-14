# P4-009 writer予約・永続化境界の検証

2026-10-14。Macと許可済み`ssh ent`のLinux Dockerで実施した。研究室`lg`／PBSは使用しない。ホスト名、絶対path、toolchain情報を含む内部資料であり、外部共有時の非公開化／匿名化対象とする。

## 対象と環境

対象はcheckpoint JSON、matrix asset、単独snapshot JSON、trace、SQLite capabilityとdisk reservationである。全writerの受入、性能校正、電源断、複数hostの完了証跡ではない。

Macはuv環境を使用し、sandboxのsocket制限外で実MPIを実行する。Linuxは既存の`ddq-linux:dev-20261014` imageを使う。uv／Python／OpenMPI／liburingの環境は[Linux初回検証](../ent_linux_validation/README.md)と同じで、今回の[image記録](linux/accepted/image.json)でimage IDを確認する。

Linux作業rootは`/tmp/ddq-p4-writers-20261014.jopUVR`。ソースの`.git`、internal／ref資料、Mac venvを転送しない。コンテナはnon-root、network none、4 CPU、8 GiB RAM、512 PID、256 MiB shmで、専用のlocal ext4 scratchだけを使う。default policyの通常回帰と、限定seccomp profileのnative opt-inは分ける。既存の2コンテナ、ホストpackage／daemon／kernel設定は変更しない。

## 実行とraw

Macの最終コマンドは次の14 moduleを指定した`uv run pytest -q ... --junitxml=/tmp/ddq-p4-writer-mac-accepted.xml`。使用したmoduleは[collection](mac/accepted-collection.txt)で照合する。

```text
test_catalog_capability.py test_admitted_files.py test_catalog_admission.py
test_disk_reservation_ledger.py test_disk_failed_allocation.py test_trace_admission.py
test_checkpoint_atomic_admission.py test_stored_recovery.py test_stored_budget.py
test_telemetry.py test_disk_coordinator.py test_runtime_calibration.py
test_distributed_checkpoint.py test_phase4_lifecycle.py
```

Linuxの[最終script](run-ent-accepted.sh)は`context-accepted`をread-onlyでmountし、`scratch-accepted`と`artifacts/accepted`を分離する。通常回帰は1800秒、nativeは600秒のwhole-command上限を持つ。各native MPIケースは180秒、既存coordinator／lifecycleのMPIケースは120秒のまま実行する。

Nativeはkernel I/O 6ケースに加え、buffered／O_DIRECTの各2 rankを指定した。workerが実engine=`io_uring`、THREAD_SINGLE、状態／期待値／勾配、fork／lease／GC、checkpoint／resumeを検査する。P=1／4のnative再検証や全障害matrixは今回の対象に含めない。

## レビュー後の再検証

最終Mac関連回帰は[JUnit](mac/accepted.xml)の**124 passed、94.17秒**。通常のLinux関連回帰は[JUnit](linux/accepted/regression.xml)の**121 passed、449.39秒**。Nativeは[JUnit](linux/accepted/native.xml)の**8 passed、121.10秒**で、skip／fallbackはない。3件の通常回帰の差は既存coordinator testのMac専用direct × P=1／2／4のparameter展開による。Linuxのdirectはnative 2 rankで別検証し、Linuxの全coordinator direct matrixを完了扱いにしない。

初回のMac112件、Linux109件、native8件の成功rawを残している。続くSQLite不一致fallback拒否の追加試験はMac／Linux各19件だった。これらだけで最終sourceの検証済みとしない。

その後のLunaレビューで、flush済みbytesのpending／usage二重計上、temp cleanup失敗時のtoken解放、trace rollback失敗後の追記継続を修正した。temp名衝突、inventory不明、closed trace、明示的なfailed allocation retentionも試験へ追加した。最終sourceで14 moduleの回帰とnative8件を改めて実行した。初回／中間rawを上書きしない。[集計](merged-results.json)と各platformのcollectionのnode ID集合を照合し、最終Mac／Linuxのsource hashとworkspaceが一致することを確認した。[manifest](manifest.json)へartifact SHA-256を保存する。

回帰の所要時間を性能校正値に使わない。初回／最終でsourceと試験数が異なり、token物理化のSQLite FULL transactionも追加している。予約・物理化・精算を含むmonitor／metadataの費用比較はP4-001／005に残る。

## Capabilityの証跡

[Macのcatalog記録](mac/catalog-capabilities.json)は実テストが永続化したcreation capabilityをread-onlyで抽出した。[Mac単独probe](mac/capability.json)は別processのbuildであり、テスト中のcompiled binaryと同一hashだとは扱わない。

Linux nativeの[実4 catalog](linux/accepted/native-catalog-capabilities.json)からcreation capabilityを抽出し、C source hashとcapability fingerprintを照合した。[capture script](capture-ent.sh)はread-onlyでcatalogを読み取る。これも[単独probe](linux/accepted/capability-probe.json)のbinaryとは区別する。HeaderとPython runtimeのbuild configuration全体の一致は主張しない。

## 受入の境界

実ファイルのold target＋tempを観測するquota境界、file／directory fsync、rename前後、cleanup／inventory失敗、実子processの異常終了、traceのpartial writeとrollback失敗を検証する。WRITING tokenが存在することとpending byte数は区別する。flush後にmaterializeされたbytesは観測usageが計上し、pendingは0になる。

writerのwriteとmaterializeの記録の間には保守的に拒否する区間がある。全writerとauthoritative cutのrecovery連携、temporary branch GC、外部export、異なるledger／外部jobの調停、電源断、cross-host性能は未受入で、`strict_capacity_guarantee=false`とP4-009 IN_PROGRESSを維持する。
