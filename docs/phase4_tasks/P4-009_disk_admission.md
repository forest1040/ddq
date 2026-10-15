# P4-009：SQLiteを含む書込み前disk admission

状態：IN_PROGRESS（2026-10-14）。[基盤実装](../phase4_foundation_implementation.md)でcatalog／ledgerのVFSとwriter予約を追加した。全受入条件は維持する。共通契約は[全体設計](../phase4_remaining_design.md)。現行の「次回admissionでcatalog growthを観測」を、operation前の予約とenforcementへ置き換える。

[writer永続化境界の補強](../phase4_writer_durability_implementation.md)でcheckpoint／traceの変更前admission、asset／snapshotのatomic fsync、SQLite capability記録とdispatch照合を追加した。sidecarの実process crashでは予約とinventoryを保持する。全writerのauthoritative cutに結び付いたreconcile、GC、host調停は未完了。

[追加実装](../phase4_safety_monitoring_implementation.md)で実子process異常終了後のtoken／partial file保持、active stateからの明示reconcile、二重回収と既記録byte減算の拒否を確認した。owner停止／journal／cutの外部確認はcallerの責務で、全writerのfaultとhost調停は残る。

[writer監査](../phase4_writer_audit.md)にcallsite、予約／VFS enforcement、精算／回収、SQLite identityの確認範囲、未受入fault matrixを整理した。ソース監査は実fault試験の代わりにはしない。

[共有coordinator実装](../phase4_disk_coordination_implementation.md)で、同volume独立datasetの明示共通ledger、forkのbootstrap前継承、永続policy照合、全root合算admissionを追加した。祖先が覆うsuffixを重複登録せず、非重複root256件に制限する。temporary branchの安全なGCと、全writerのcut結合／faultは残る。

[checkpoint reservation recovery設計](../phase4_checkpoint_recovery_design.md)と[実装資料](../phase4_checkpoint_recovery_implementation.md)で、明示offline/node-local recoveryを追加した。全writerの終了とkernel exclusive leaseを前提に、最新committed v4 sidecar intentだけをmetadata/cut/inventory照合する。dry runを含むrecovery-only openはschema migration/create、root登録、policy変更を行わず、reservation精算とproof receipt保存はapply時だけ行う。prepare/global decisionは必須target、decision-copyは欠損を許す。partial temporaryは削除せず、decision-copy欠損時は観測byteを精算して通常resumeがcopyを再作成し、temporaryを保持する。intent不在/uncommitted、振幅/全physical closure、全writer、GC、別ledger/外部job、cross-host、電源断は対象外。内部記録は[validation資料](../internal/phase4_checkpoint_recovery_validation/README.md)。P4-009はIN_PROGRESSのままであり、全writer fault/recoveryとstrict capacity保証は未達。

2026-10-15、[未commit予約の回収とtemporary GC](../phase4_uncommitted_recovery_design.md)（[実装](../phase4_uncommitted_recovery_implementation.md)）を追加した。intentを持たないtokenを、ledgerとdataset directoryの排他flock（PID・時刻は不使用）でowner停止を証明した上で、dry run→`plan_sha256`付きapplyで精算する。未知のoperation／帰属不能なcatalog／hot journal／journal破損は全体を拒否し、artifactは削除せず、生存byteは観測使用量として課金を残す。temporary GCはintentが参照しない世代付きtemporaryの削除と、committed cutが参照しないroot／version／segmentの回収（quiescent `collect_garbage`と同じ意味）を行う。残りはstore内private ledger、intent付き未commit checkpoint token、複数rank／MPIでの実行、cross-host調停、O_DIRECT／ENOSPC、電源断である。

[recovery-only metadata pool](../phase4_recovery_pool_design.md)と[実装](../phase4_recovery_pool_implementation.md)を追加した。dry runを含むrecovery openは既存production shared ledgerのschema/policy/root/volume/native VFSを検証し、migration/create/root登録/policy変更を行わない。logical quota・pending・drain marginを除外してcheckpoint CAS/receiptだけを許し、min_freeと未割当metadata pool、page/journal capは維持する。schema不一致、hot sidefile、pool/page full、physical headroom不足、実I/O errorは拒否する。検証記録は[recovery pool validation](../internal/phase4_recovery_pool_validation/README.md)。

[disk faultマトリクス](../phase4_disk_fault_matrix_implementation.md)で、catalog／trace／journal／sidecar／extentのquota境界を1 byte・1 page刻みで掃引し、書込み前の拒否（namespace・durable cut不変）と書込み途中の安全な失敗を区別して確認した。実processのkill matrix（catalog gate batchの各stage、ledger lease append、sidecarのfsync／rename前後、SIGKILL連続commit）で旧／新cutの完全性、hot journal、partial temporary、token保持と単一reconcileを確認した。Mac・Linux（ent）で成功した。unlinked/uncommitted reservationとtemporary branch GCは[追加実装](../phase4_uncommitted_recovery_implementation.md)で扱った。

[追加のfault試験](../phase4_disk_fault_matrix_implementation.md)で、(1) size制限tmpfs上の実ENOSPC（空き枯渇時の書込み前拒否と、admission後に外部processが空きを消費した場合の安全な失敗、catalog／trace／journal／extent／sidecarの5 writer、offline reconcileでの回復）、(2) O_DIRECT backendでのquota掃引・kill matrix・未commit回収・GC・ENOSPC（ent ext4）、(3) 2 rankの実processをprepare／global decision／decision-copyの各位置で落とした後のcollective回収・resume・数値一致を確認した。この試験で、未commit snapshotのintent付きtokenが既存のどの回収でも精算できない問題を見つけ、`recover_uncommitted_reservations`に`checkpoint-uncommitted`として追加した。store内private ledgerの回収、複数host・別ledger・外部jobの調停、segment fsync前後のkill、extentのkernel／電源断fault、cross-hostは残り、P4-009はIN_PROGRESSのままである。`strict_capacity_guarantee`は立てない。

2026-10-15、[extent segmentのstage kill](../phase4_disk_fault_matrix_implementation.md)（同期／非同期の両経路で、segment write前後、catalog commit前後、segment fsync前後。回収後のdurable cutの完全性、書込み済みbyteの課金維持、token単一精算、snapshotからのresumeのoracle一致。`SVSIM_TEST_BACKEND=direct`対応）と、[store-private ledgerの回収](../phase4_uncommitted_recovery_design.md#store-private-ledgerの回収)（`store_private`のrecovery-only open、同じ排他lease証明・dry run・plan digest・CAS＋receipt・未知状態の拒否）を追加した。Mac全回帰とLinux（ent、buffered／O_DIRECT）で成功し、実装バグは見つからなかった。残りは、別ledger／外部job／複数hostの調停と一括回収、電源断（公開行とsegment fsyncの順序を含む）、SQLite fsync内部への決定的なstage注入、catalog tokenのfile単位の帰属、private ledgerのpolicy検査の弱さである（実ENOSPC、2 rankのprepare／decision kill、未commit snapshotのintent付きtokenは上の追加試験で対応済み）。P4-009はIN_PROGRESSのままである。`strict_capacity_guarantee`は立てない。

## 設計時点（6aa8581）の現状とwriter棚卸し

上記recovery-only metadata poolはlogical quota/pending/drain超過を越えて既存pool内のcheckpoint CASを許す。物理headroom、min_free、native journal/page capは引き続き強制する。実hardware ENOSPCや電源断時durability、全writer回収は009Cの残件である。

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

faultはfake byte backend→Mac実SQLite/worker I/O→許可されたLinux/O_DIRECT環境の順。Linuxの一部機能検証は済んでいる。Linuxを含む全recovery／writer fault-durability matrixとquota境界を1 byte/page単位で掃引し、書込み前の拒否と書込み途中の安全な失敗を区別する証跡は未完了である。全writerのenforced reservationとrecovery証跡が揃ってDONE。自由SQL、未証明temp growth、native guard不在で厳密保証を出す実装は受け入れない。

2026-10-15追加：[fault matrix実装](../phase4_disk_fault_matrix_implementation.md#extent公開前のsegment-fsync電源断順序2026-10-15追加)で、extent公開前のsegment fsync（電源断時に公開行がbyteを失わない順序、試験専用の電源断模型で検証、費用Mac +1.6％／ent buffered +6％／O_DIRECT +10％）、SQLite内部xSync／xDeleteへの決定的fault注入（test flag付きbuildのみ）、新規store-private ledgerのpolicy永続化を追加した。受入条件は全writerのfault／recovery、別ledger・外部job・cross-host調停、catalog tokenのfile単位の帰属が未達のため、IN_PROGRESSを維持する。

2026-10-15追加：[host調停の設計と実装](../phase4_disk_fault_matrix_implementation.md#host調停別ledger外部jobcross-host設計と実装2026-10-15追加)で、volume単位のcoordinator（`src/svsim/volume_coordinator.py`、`DiskBudget(volume_coordinator=...)`）を追加した。独立ledger同士がvolume予算を共有し、宣言済み外部jobのclaim（lease、generation、dry run→plan digest→CAS→receiptで回収）を数え、実processの競合で許可量ちょうどだけが受理され、lock順序（ledger→coordinatorの葉）で実process試験が完走する。allowlist外・判定不能のfilesystem、別host名のDB、別deviceを拒否する。`DistributedConfig`への配線、private ledger／recovery-onlyの対応、宣言超過の検出、cross-host調停、全writerのfault／recovery、catalog tokenのfile単位の帰属は未達のため、IN_PROGRESSを維持する。
