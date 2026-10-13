# P4-009：SQLiteを含む書込み前disk admission

状態：IN_PROGRESS（2026-10-14）。[基盤実装](../phase4_foundation_implementation.md)でcatalog／ledgerのVFSとwriter予約を追加した。全受入条件は維持する。共通契約は[全体設計](../phase4_remaining_design.md)。現行の「次回admissionでcatalog growthを観測」を、operation前の予約とenforcementへ置き換える。

## 設計時点（6aa8581）の現状とwriter棚卸し

[disk_budget.py](../../src/svsim/disk_budget.py)はlocal SQLite ledgerでquota/free/drain予約を合算する。[storage.py](../../src/svsim/storage.py)、[async_store_io.py](../../src/svsim/async_store_io.py)はextentを予約するが、allocator/versions等のSQLが事前予約外。catalog・Phase2 rule・root/history・lease・checkpoint・GC・trace・ledger自身を対象にする。

変更対象はstorage、async_store_io、phase2_runtime、journal、distributed/checkpoint、block_store、parameter_batch、telemetry、disk_budget。新規予定：`catalog_admission.py`、`disk_write_guard.py`、必要なnative VFS adapter。全SQL callsiteをoperation名付きtransaction wrapperへ移し、直接mutationの取り残しをauditする。

## 上限の意味と採用方針

SQL row数×固定byteだけではB-tree/index/journal/tempの成長上限を保証できない。`max_page_count`はdatabase本体のpage上限で、side-file全体の予約は別に必要。[SQLite公式PRAGMA](https://www.sqlite.org/pragma.html#pragma_max_page_count)、[一時ファイル仕様](https://www.sqlite.org/tempfiles.html)。`journal_size_limit`を実行中のpeak上限として扱わない。[公式説明](https://www.sqlite.org/pragma.html#pragma_journal_size_limit)

採用する二段構成は、operationごとの保守的transaction envelopeを先に予約し、実書込みをguardでその上限内に制限する方式。未知SQL形状を受け付けず、DB/page/journal/temp mode・SQLite buildをcapabilityに含める。supported modeの最初は既存durabilityを保つrollback journal構成。WAL・ATTACH・VACUUM・bulk flattenはそれぞれ別envelopeを証明するまで拒否または明示offline operation。

main DBのpage capは設定値をread-backして検査する。side-fileと拡張writeは、同じSQLite connectionが使うbudgeted VFSのwrite/truncate guard、または検証した同等のenforcementで扱う。Python sqliteと異なるSQLite libraryへVFSを登録して有効になったと扱わない。native adapterが利用不可なら保守予約までを実装し、hard enforcementとP4-009完了は未達と表示する。新dependencyの自動導入を前提にしない。

## Reservationとtransaction

`DiskReservation`はjob/dataset/host/volume、owner/run/generation、operation、DB/journal/temp/segment/metadata別上限、materialized bytes、状態、authoritative cut参照を持つ。ledgerは同host/volumeのrank/Bindingで共有し、他hostにはMPI経由でsummaryだけを配布する。異なるvolumeを一つのfree値で判定しない。

1. read-only analysisでoperation形状、入力byte/row上限、現在page/file状態を取得しenvelopeを作る。
2. volume coordinatorがquota・free下限・drain poolを検査して永続tokenを発行する。catalog lock取得より前に予約し、ledgerとcatalogの逆順lockを禁止する。
3. catalog BEGIN直前にDB generation/page数を再確認する。変化してenvelope不足ならmutation前に再予約し、timeout/不足はcollective拒否。
4. write guardへtokenを渡し、全対象fileのwrite/truncate growthが許可範囲内か検査する。上限超過時はSQL/書込みを失敗させ、勝手に余裕を借りない。control callbackから追加collectiveを出さない。
5. commit/rollbackのdurabilityとfile実サイズを確認して精算する。failed extent/hot journal/orphanは残存量として課金し、予約を全額空きへ戻さない。

既存`usage + pending`が同じ物理化済み量を二重計上する問題を分ける。quotaはobserved/materialized charge＋未物理化予約＋必要drain余裕。async writeのmaterialized通知はvolume ledgerで整合させ、不明な途中量は保守的二重予約として記録する。capacity不足でdrainできなくなる精算を避ける。

ledger自身のbootstrap/成長/rollback journalは予約の再帰を起こす。初期local metadata poolをprechargeし、ledgerのpage/record上限と固定emergency slotを設ける。pool拡張は既存pool内で別の明示admissionを行う。上限に達したら新tokenを止め、release/recoveryに必要なslotを温存する。ソフトウェア環境・既存fileもquota内の課金対象。

## Crash・drain・厳密性

状態はRESERVED→WRITING→DURABLE/ROLLED_BACK→RECONCILED。process crashのtokenはlease expiryだけで解放しない。owner不在、hot journal復旧、extent/file inventory、cut/prepare/decision、in-flight終端を確認するexplicit recoveryで精算する。ID/generationを使い、二重releaseとPID再利用を防ぐ。

通常taskはdrain poolを使えない。drain/decision/ledger recoveryはpurpose付きtokenと最小予約を使う。COMMIT前不足は旧durable cut、COMMIT後のdelivery失敗は新decisionから再開。外部jobによるfree消費は物理空き保証の対象外で、定期statvfsとENOSPC時の停止を維持する。

`strict_capacity_guarantee`を立てるにはmanaged namespaceの全writer/enforcementとaccounting対象を証明する。予約wrapperだけでは立てない。保証scope、未管理の外部file/OS/library、実空きの動的性を別欄にする。Phase 5のcatalog固定byte仮定は置換せずreferenceとして保持し、enforced capの実値をruntime admissionへ接続する。

## 受入・実装単位

| 段階 | 必須試験 |
|---|---|
| 009A ledger/transaction | 同volume複数rank/Binding、bootstrap満杯、lock順序、mutation前quota拒否、精算の二重計数 |
| 009B enforcement | page/index split、large bounded asset、journal/temp growth、build/mode不一致、guard上限超過で旧cut維持 |
| 009C crash/lifecycle | reserve/SQL/commit/fsync/decision前後kill、hot journal、partial extent、orphan、drain pool、二重回収拒否 |
| 009D 接続 | async completion、RAM flush_to_file、prefix/gradient/replan/tile/offline copy、全writer audit |

faultはfake byte backend→Mac実SQLite/worker I/O→許可後Linux/O_DIRECTの順。quota境界を1 byte/page単位で掃引し、書込み前の拒否と書込み途中の安全な失敗を区別する。全writerのenforced reservationとrecovery証跡が揃ってDONE。自由SQL、未証明temp growth、native guard不在で厳密保証を出す実装は受け入れない。
