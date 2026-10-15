# P4-009 checkpoint payload／chunk予約の回収

2026-10-16。`checkpoint_payload._exchange`がoperation未指定のtokenを残し、checkpoint転送中にprocessが停止するとoffline回収が`unknown-operation:unspecified`で拒否する穴を補修した。新しいpayload／chunk writerは既存の`checkpoint_reservations.link`へdurable intentを予約と同時に保存する。schemaは変更しない。

## writerと帰属

purposeは`checkpoint-metadata-payload`／`checkpoint-metadata-chunk`、intent stageは`metadata-payload`／`metadata-chunk`である。snapshot／dataset UUID、rank、dataset directoryのdevice／inode／birth identity、固定target path、世代UUID付きtemporary、予定byte数とSHA-256を保存する。payloadは`payload-<snapshot>.json`、chunkは`chunk-<digest>.json`。targetはwrite-onceで、既存の同size／同digestは予約せず再利用し、競合する既存targetは上書きせず拒否する。target不在をbaselineとして予約し、temporaryはexclusive createする。public metadata referenceのformatは従来どおりname／bytes／sha256である。

予約はtemporary作成とtransfer/writeより前から、file fsync、atomic rename、directory fsync、cleanupまで保持する。process停止ではtemporary／finalとtokenをそのまま残す。別volume、symlink、非排他的な通常fileを既存のchecked-path検査で拒否する。birth identity取得不能、根拠がない旧`unspecified` tokenは推測で変換しない。

## offline回収

`checkpoint_metadata_recovery.plan_recovery`が既存のexclusive ledger／dataset lease下でintentとinventoryを検証する。temporaryは世代名、regular file／nlink、byte上限、完全長ならdigestを確認する。finalは予定の完全長とdigestに一致する場合だけ認める。両fileが残る不明状態で予定byteを超えれば拒否する。inventoryはpath／device／inode／birth identity／byte／digestを含む。

未commit snapshotではartifactを保持したままpendingだけを精算する。committed snapshotではglobal decisionを検証し、rank 0のauthoritative payloadとchunkのsize／digest、dataset／snapshot UUID、decision record、chunk参照まで照合する。rank 0のauthoritative copyが完全なら、別rankの欠損／部分copyは保持して通常resumeの再配布に任せる。古いcommitted snapshotに結び付いたtokenも、`recover_uncommitted_reservations`はそのsnapshotのdecisionへ照合する。

`recover_uncommitted_reservations`は両stageをcommitted／uncommittedとも`checkpoint-metadata`として扱い、dry run／plan digest／全体拒否／再検査／exact CAS／receiptを適用する。物理byteは削除せず観測使用量で課金を維持する。最新cutのmetadata tokenはcollective `recover_checkpoint_reservations`からも回収できる。intentとfile inventoryはreceiptに残る。旧token、新catalog startup、private policyの移行は本変更の対象ではない。

## 試験

`tests/test_checkpoint_metadata_recovery.py`は実子processでpayload／chunkそれぞれをreserve直後、部分write、file fsync前後、rename前後、commit完了後に停止する14ケースと、committed metadataのcollective recovery 2ケース、generation／final content／symlink／dataset birth identity／旧unspecifiedの拒否5ケースを持つ。dry runのledgerを含む無変更、applyのartifact無変更、receipt、古いplan／二重適用拒否、旧／新snapshotからのnumpy oracle一致、回収後のcheckpoint成功を検証する。`SVSIM_TEST_BACKEND=direct`で同じwriterを選べる。Macで21件成功した。

## export recoveryとの共通dispatcher

export担当の`export_admission`が提供する`normalize_intent`、`export_inventory_roots`、`plan_recovery`、`recheck_recovery`へ共通recoveryからdispatchする。export rootはgeneric rootsへ混ぜず、専用inventoryを同volume・identity確認してexclusive leaseに加える。export intentをcheckpointのglobal-decision判定へ混ぜない。apply前にexport inventoryを再計算し、dry-run計画と一致することを確認する。export module側のwriter／schema／専用検証は別実装である。
