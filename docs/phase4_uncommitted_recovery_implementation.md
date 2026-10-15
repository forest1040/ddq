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

## 残り

私的ledger（store内`disk-ledger.sqlite3`）の回収、intent付きだが未commitなcheckpoint tokenの回収、複数rank／MPIでの本操作、catalog tokenの精密な帰属（owner hashは計画に使うが、書込みfileごとの帰属は持たない）、cross-host調停、O_DIRECTを含むLinux全matrix、実ENOSPC、電源断である。
