# P4-009：state exportの事前admissionとatomic保存

更新日：2026-10-16。`StoredSimulator.export_state`と`DistributedSimulator.export_state`を既存disk ledgerへ接続した。公開signature、storedのlogical/physical order、distributedの`rank-NNNN.bin`と`manifest.json`の形式は維持する。全stateのRAMコピーは作らない。

## 課金と保存

- **inventory。** 出力先の既存parent（未作成のparentの場合は最寄りの既存ancestor）を、optionalな`export_roots(path, device, inode, birth)`表へ永続登録する。sourceのgeneric rootsとは分離し、store-private ledgerの正規store root条件は変更しない。`DiskBudget._observed_usage`は双方のrootの和集合をancestorで重複排除し、既存output、新generation、cleanup前の旧generationを課金する。parent内の他のregular fileも保守的に課金する。rootのdevice/inode/birthを再検証し、birth identityを取得できない環境や別volumeではoutput・stateの変更前に拒否する。
- **stored。** state全体のbyte数を先に予約し、UUID generation名のtemp fileへblock単位でstreamingする。logical orderは16 byte単位のscatter、physical orderは既存block memoryviewの書込みである。短writeを`nbytes`で検出し、file fsync後に同directory内のreplaceを行い、directory fsync後に予約を解除する。途中のwrite/fsync/rename失敗では、durableにtempをcleanupできた場合だけ解除し、既存outputを保持する。rename後のdirectory fsync失敗は新outputが見える可能性のあるdurability不確定として報告し、tokenを保持する。
- **distributed。** 全rankのshardとrank 0のmanifestを、output directoryを作る前に全て予約する。実materializeの後、block単位のfetchからUUID temp directoryの各shardへ書き、manifestも予約内で保存する。全fileとtemp directoryをfsyncした後、新規outputはrename、既存outputはwhole-directory atomic exchange（Mac `renamex_np(RENAME_SWAP)`、Linux `renameat2(RENAME_EXCHANGE)`）で公開する。parent fsync、交換後の旧generation cleanup、再度のparent fsyncまで全rankの予約を保持する。部分rankの予約拒否でも全rankがcleanup collectiveへ参加する。
- **追加parent。** 作成したparentの各directory entryも予約を保持したままfsyncする。export root自体にはwriterのshared directory leaseを保持し、offline recoveryはexclusive leaseを要求する。

## kill後の回収

operationは`state-export`、`state-export-shard`、`state-export-manifest`であり、`unspecified`へ分類しない。`checkpoint_reservations.link`を使い、`purpose=state-export`、source datasetとoutput rootのdevice/inode/birth、target/temp relative path、generation、種別、予約byte数をbindする。`export_admission.py`がnormalization、dry-run plan、recheckを提供し、payload担当の共通recovery dispatch（`79e4413`）へ接続する。

owner deathはledger・source dataset・export rootのkernel leaseで証明する。root、dataset、artifactのbirth identity、generationとtemp UUID、regular fileのnlink、同volume、path閉包を検査する。dry-runとapplyのattributionが変われば拒否する。applyは生きたartifactを削除せず、pending creditだけを一度回収し、残ったbyteはinventoryで引き続き課金する。directory exchange直後のkillではtemp directoryが旧outputを持つため、そのshardが新規予約量より大きくても勝手に消さない。

## 検証と制限

Macで新規export試験は **23 passed**（MPI 2 rankの1件と実SIGKILL 5段階を含む）。既存admission／ledger／uncommitted recoveryを合わせた回帰は92 passed、MPI 1件を別実行した。payloadの共通dispatchとの統合、metadata recovery、RAMを超えるstored exportを合わせた確認は44 passed、MPI 1件を別実行した。Linuxの輸送・実行は親担当であり、この資料のMac結果へ混ぜない。

`tests/test_state_export_admission.py`は、数値oracle、quotaによるmaterialize前の拒否、manifest/shardの事前予約、短write、IO error、file fsync、rename/exchange、directory fsync、cleanup失敗、予約保持、birth変更を検証する。実processのSIGKILLをstoredのwrite/rename、distributedのshard/manifest/exchange直後へ置き、oldまたはnewのwhole output、purpose付きtoken、dry-run→apply→冪等な再実行、artifact保持を確認する。MPI 2 rankでは実shardのoracle比較、rank 1だけのquota拒否からのcollective復帰、既存directoryの交換を確認する。既存のRAMより大きいstored stateのexport試験も維持する。

- outputはsource ledgerと同volumeに限定する。別volume用budgetの自動作成は行わない。
- distributedは同hostと確認できるoutput namespaceに限定する。cross-hostの共有namespaceの証明は残件であり、変更前にcollectiveで拒否する。
- 既存distributed directoryはexport fileだけを持つregularなdirectoryに限る。無関係なfile、symlink、hardlink、nested directoryがある場合は削除せず拒否する。
- atomic directory交換機能のないplatformでは既存outputの交換を失敗として報告し、旧outputを保持する。
- admissionは既存のsoft disk accountingであり、filesystemが実割当を強制するquotaではない。directory fsync後の実power-loss durabilityはfilesystem契約に依存し、この試験のSIGKILLとは区別する。

### Export inventory と volume 集計の結合（2026-10-16）

export root を ledger に登録する transaction 内で、既存ファイルを含む
ledger quota/free 条件を再検査し、volume coordinator にも root を先に登録する。
通常の `register_roots` による追加 dataset も同様に coordinator へ反映する。
これにより export の物理化通知で pending が減っても、出力 byte が volume
集計から落ちない。coordinator commit 後に ledger commit が失敗した場合は
root が保守的に残り、余分な空きにはならない。再 open 時も export root を再登録する。

ledger の通常 inventory 走査は `scandir` へ変更し、読取り不能な subtree を
空きとして扱わず admission を拒否する。別 volume の subtree も拒否する。
関連回帰は Mac で **90 passed**（実 MPI を含む）。追加 oracle は独立した
2 ledger の export 60,000 byte＋別予約40,000 byteで上限到達、その次の1 byteの
拒否、再 open 後の課金、登録時 quota 拒否のrollback、追加dataset root、
観測不能時の変更前拒否を含む。
