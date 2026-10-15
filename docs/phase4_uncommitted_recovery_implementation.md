# P4-009 未commit予約の回収とtemporary GCの実装

2026-10-15。[設計](phase4_uncommitted_recovery_design.md)を`src/svsim/uncommitted_recovery.py`に実装した。P4-009はIN_PROGRESSのままである。

## API

- `recover_uncommitted_reservations(ledger, apply=False, confirm_plan=None)`：intentを持たない生きたtokenのdry run／apply。返却値に`plan_sha256`、owner停止の証明（lease path一覧、`pid_or_timestamp_used: False`）、store検査、token別の判定、拒否理由、`would_release_pending_bytes`、`strict_capacity_guarantee: False`を含む。拒否時のapplyは`UncommittedRecoveryRefused`（報告を保持）。
- `collect_temporary_artifacts(...)`、`collect_dead_branches(..., allow_no_snapshot=False)`：GCのdry run／apply。applyは同じ`plan_sha256`を要求する。dead branchのapplyは書込みbudgetを再取得して計画を再計算し、変化があれば拒否する。

## 既存コードの変更

- `DiskBudget`は、writerとして開く間、ledgerの親directoryに共有flockを保持する（`close`で解放）。ledger fileそのものはlockしない。macOSではflockとSQLiteのfcntl lockが衝突し、ledgerが開けなくなることを実測した。offline保守は排他lockを`ledger_lease`で渡し、writer budgetに引き継げる。
- 追加メソッド：`live_reservations`（intentの有無で分類）、`reconcile_unlinked`（recovery-only専用のCAS精算とreceipt保存）。証跡digestの検査は`reconcile_checkpoint`と共有した。

## 試験

- `tests/test_uncommitted_recovery.py`（19件）：実子processを`os._exit`で終了した後の回収。extent（slot確保後、slot途中書込み）、journal（半行）、trace、catalog batch中（rollback journalが残る）。dry runが全byteで無変更、apply前後でartifactが同一、観測使用量が一致、二重適用と古い計画の拒否、ledger／dataset leaseを保持する生きた子processによる拒否とSIGKILL後の成功、回収中の新規writer拒否、未知operation／帰属不能catalog／journal内部破損／symlink／catalog読取不能／範囲外extentの拒否、intent付きtokenの据置き。
- `tests/test_uncommitted_gc.py`（10件）：実`FileBlockStore`で作ったcommitted rootとdead branchに対する計画と適用、manifest参照によるroot保持、snapshotなしの拒否、未知rootの拒否、temporaryの選別（intent参照と名前不一致を保持）、レビュー後に変化したtemporaryの拒否、quota満杯でのtemporary GC、回収とGCの合成。
- 発見した問題：ledgerの排他制御にflockを直接使うとmacOSでSQLiteと衝突した（上記のとおり親directoryへ変更）。

## 追補：未commit snapshotのintent付きtoken

[複数rank checkpoint kill](phase4_disk_fault_matrix_implementation.md)で、prepare／global decisionの途中で落ちたrankのintent付きtokenが、checkpoint recoveryにも本操作にも回収されないことが分かった。`recover_uncommitted_reservations`は、global decisionが存在しないsnapshotのprepare／decision stageのintent付きtokenを`checkpoint-uncommitted`として扱う。dataset rootが排他lock済み、device／inode一致、target・temporaryがgeneration付き固定名、directoryの別名化なし、排他的な通常fileであることを検査し、`reconcile_checkpoint`のexact CASとreceiptで精算する。decision-copyはcommit後にだけ存在するため、global decisionのない同tokenは不整合として拒否する。global decisionが存在するsnapshotのtokenは`linked_live_tokens_deferred`として従来どおりcheckpoint recoveryへ任せる。`DiskBudget.checkpoint_link`を追加した。global decisionの探索は、登録root、検出store、intentのdataset root配下に限る。別host・別rank rootに置かれたdecisionは見ないため、単一host構成に限る。

## store-private ledgerの回収（追加）

[設計](phase4_uncommitted_recovery_design.md#store-private-ledgerの回収)のとおり、`<store>/metadata/disk-ledger.sqlite3`のprivate ledgerを同じdry run／plan digest／CAS＋receiptで回収できるようにした。`DiskBudget(..., recovery_only=True, store_private=True)`を追加し、`recover_uncommitted_reservations`、`collect_temporary_artifacts`、`collect_dead_branches`が束縛policyのないledgerを上記レイアウトの場合だけ受け付ける。返却値に`ledger_layout`（`store-private`／`shared-policy`）を追加した。`_persisted`はSQLiteを開く前にledgerのside fileを検査する。

`tests/test_private_ledger_recovery.py`（16件）：`DiskBudget`なしの`StoredSimulator`を実processでkill（extentのwrite直後、trace／journalのtoken、catalog batch中の`os._exit`）した後、dry runがledgerを含めbyte単位で変更しないこと、apply後にartifactが同一で観測使用量が一致すること、receiptがtoken数と一致し`shared_policy`行が作られないこと、二重適用と古い計画の拒否、最後のsnapshotからのresumeがnumpy oracleと一致すること。hot catalog journalは拒否され、admitted reopen後に回収できる。ledger／dataset leaseを保持する生きた子processとlease単独での拒否、SIGKILL後の成功。ledgerの改変（余分なroot、schema変更、trigger削除、`user_version`、非空`-journal`／`-wal`、未知operation、store外へ移動）は全て拒否され、ledger・artifact・tokenが不変であること。recovery-only budgetが通常のreserve／register_rootsを拒否し、非既定policyと複数rootを拒否すること。temporary GCとdead branch GCがprivate layoutで動くこと。`SVSIM_TEST_BACKEND=direct`でも同じ試験を実行できる。

## 残り

残りは、別ledger／外部job／複数hostの調停と一括回収、extent公開とsegment fsyncの電源断時の順序（kill試験では検証できない）、SQLite fsync内部への決定的なstage注入、private ledgerのpolicy非永続による検査の弱さである。


## catalog予約のfile単位帰属（追加）

`catalog_admission.catalog_file_owner`がversion付きのcanonical path・device/inode・birth identityのhashを生成し、`CatalogLedgerLease`はowner一致を再利用条件に含める。`CatalogConnection`はDB自身のsymlink／hardlinkをopen前に拒否し、親directoryはresolveしてcanonical pathへ正規化する（root外parent symlinkと`..`を含む既存APIを維持）。接続中のDB置換とbudgetとは異なるvolumeをtransaction前に拒否する。新規DB作成前のstartup tokenは未帰属として扱い、次のmutation時に実DBへ帰属する予約へ切り替える。従来のpath hashだけのtoken、未帰属startup tokenはrecoveryでfail-closedとなる。

回収計画と各catalog tokenの`catalog_attribution`にDBおよび固定suffix side fileのpath、device、inode、観測byte（side fileは存在状態も）を記録する。全fileの再検査後に精算し、帰属はreceiptにも残す。非空journalの拒否とadmitted reopen後の回収は維持する。`DiskBudget`の既存owner上限・CAS・receipt APIを利用し、ledger schemaは変更しない。

追加回帰は実processで2個のcatalogを同じledgerへ作成して`os._exit`した後の独立帰属・無変更apply・receipt、同内容の別inode DBへの置換、2個のDB入替、旧owner、identity未確定startup tokenの拒否、writer接続中の置換とsymlink aliasの拒否である。catalog tokenのfile単位帰属は本追補で実装済みである。新規startupの未帰属tokenや既存の旧tokenは手動調査を要し、自動変換しない。

inode再利用対策としてownerを`catalog-v3:<path/device/inode/birth identityのhash>`とした。Linuxはlibc `statx`からbirthtimeを読み、返却maskとdev/inoを照合する（Linux headerの固定256-byte ABIを確認）。macOSは`st_birthtime_ns`または`st_birthtime.hex()`。取得不能は未帰属tokenとなり回収拒否。DBの`birth_identity`を計画とreceiptにも保存する。試験はLinux ABI bufferのoffset/mask/identity不一致、birthtime取得不能の全token据置き、実fileへの追記後identity維持、再作成後の変化、同inodeでもbirth identityが変化した接続の拒否を含む。macOS float／filesystemの時刻分解能の限界と、非協調processによるbirthtime保存・偽装は上記設計の制約に残る。

親directoryのaliasとDB自身のaliasを区別する回帰を追加した。`tests/test_checkpoint_recovery.py::test_checkpoint_writer_and_recovery_accept_root_external_symlink_parent_traversal`に加え、parent aliasからcatalogを再openしてcanonical ownerが一致し通常transactionが成功すること、DB自身のsymlinkは引き続き拒否することを検証する。
