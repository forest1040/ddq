# P4-009 新規catalogのstartup回収

2026-10-16。新規DBを開く前の予約が`catalog-unbound-v3`のまま残り、正常なopen直後のprocess停止も回収できなかった穴を補修した。`catalog_startup.py`が既存ledgerの`checkpoint_reservations.link`をcreation intentとして利用する。schemaの追加や旧tokenの推測移行は行わない。SQLiteの`EXTRA`同期は維持する。

## 作成とbinding

既存の登録root、既存catalog parent directoryを前提に、次の順序を守る。

1. canonical final path、parentのdevice／inode／birth identity、登録root、32桁generation、世代名staging path、予定byte envelopeを`catalog-startup` tokenと同じtransactionで保存する。baselineはfinal／stagingとも不在である。parent birth取得不能は予約・file作成前に拒否する。
2. `.<catalog-name>.<generation>.startup`をexclusive createし、空DB inodeとparent directoryをfsyncする。この段階ではSQLiteは開かない。
3. reservation行とcreation intentのexact CASで、staging inodeのdevice／inode／birth identityをbindする。再試行、古い期待行、終端tokenは拒否する。
4. Macの`renamex_np(RENAME_EXCL)`またはLinuxの`renameat2(RENAME_NOREPLACE)`でfinalを公開し、parentをfsyncする。競合するfinalを上書きしない。Macの定数はSDKの`sys/stdio.h`を確認した。
5. 予約済みnative envelopeの下でSQLiteをopenし、DELETE journal／EXTRA／page／input制限を設定する。その後のexact CASで通常の`catalog-transaction` owner（path／inode／birth identity）と64桁generationへ移し、creation linkを同じtransactionで外す。同じleaseを以後のtransactionで再利用する。通常のunlinked reservation APIとledger commit回数を維持する。

正常失敗でも作成済みartifactとcreation tokenを勝手に削除しない。constructorが失敗したleaseはruntimeの自動release集合から外してoffline回収を要求する。publish前に別ownerがfinalを作った場合は、そのfileと自分のstagingを保持して失敗する。既存root／subdirectoryをledger成立前に作るbootstrap namespace問題は本変更とは別である。

## offline回収

ledger directoryとdatasetの既存exclusive leaseに、creation intentが示すparent directoryのexclusive leaseを加える。parentは登録root内、canonical、同volume、保存したbirth identityと一致する場合だけ認める。PID／時刻では解放しない。

- 予定段階：finalは存在せず、stagingは不在またはexclusiveな空fileであること。
- inode bind後：保存したdevice／inode／birth identityを持つstagingまたはfinalがちょうど1個存在すること。stagingは空であること。
- final：SQLite quick_checkを通り、非空journal／WAL／SHMがないこと。熱journalはadmitted reopenでreplayするまで拒否する。reopen後も同じDB identityなら回収できる。
- DB作成前に停止したstore-private ledgerだけは、catalog存在条件をcreation intentで補完する。厳密な`<store>/metadata/disk-ledger.sqlite3`配置、rootがstore 1個、永続policy、schema／pool／side-file検査は維持する。startup intentのないcatalog欠損は拒否する。

`recover_uncommitted_reservations`は`catalog-startup`としてinventoryを計画とreceiptへ保存し、dry run→plan digest→全体再検査→exact CASでpendingのみを精算する。staging、空DB、途中DB、既存byteは削除・書換えせず、観測課金を維持する。open完了後の通常catalog tokenは従来のfile帰属回収を利用する。旧path hash／未帰属tokenは引き続き拒否する。

## 試験

`tests/test_catalog_startup_recovery.py`と実子process workerは、shared／privateそれぞれでreserve、staging fsync、inode bind、publish、SQLite open直前、open直後、通常owner CAS、constructor完了の8位置に停止を入れる。dry runがledgerを含め無変更、apply後のartifact／観測量不変、pending 0、receiptと二重適用拒否を確認する。熱journalの拒否とadmitted replay後の精算、DB／parentの置換、generation改変、旧unbound、same-inodeでもbirthが変化する場合、birth取得不能、CAS再実行、final競合で上書きしないことも検証する。Linuxには512回を上限に実inode再利用を探す回帰を用意し、再利用が起きないfilesystemは明示skipする。

関連catalog／uncommitted／store-private回収試験も再実行し、従来のsnapshotからのresumeのnumpy oracle一致と新たなsnapshot成功を確認した。birthtimeはMac floatとfilesystem時刻分解能の制約があり、同じbirth値まで複製・偽装する非協調processの検出を約束しない。

## capability拒否を作成前へ移動

startup intentを保存する前にguardを取得し、`:memory:` connectionでbinary hashとdispatch identityを再検証する。既知guardの検証失敗をoptional fallbackへ混ぜず拒否し、native必須でguardが使えない場合もreservationやcatalog/staging fileを作らない。新規file作成後までbinary検査を遅らせて空DBとpendingを残すregressionを修正した。capability 8件とstartup／catalog関連52件（Linux専用1件skip）で確認した。
