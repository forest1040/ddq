# Phase 4 recovery-only metadata pool design

更新日：2026-10-14。P4-009のcheckpoint reservation recoveryに必要な、precharged SQLite metadata poolからの限定回収を定義する。全writerのadmissionやdurabilityを完了した設計ではない。checkpoint cutの検査条件は[checkpoint回収設計](phase4_checkpoint_recovery_design.md)、残る受入条件は[P4-009 backlog](phase4_tasks/P4-009_disk_admission.md)を参照する。検証記録は[内部validation資料](internal/phase4_recovery_pool_validation/README.md)に置く。

## 課題と方針

crash後にpartial writeが観測usageへ移ると、通常のDiskBudget bootstrapやshared-root capacity検査は、quota・pending・drain marginの超過を理由にledger open自体を拒否し得る。精算を進められない状態を避けるため、既存のproduction shared ledgerに限ったrecovery-only openを設ける。これは追加のdata creditを発行せず、既に予約済みのmetadata pool内でcheckpoint tokenのCASとproof receiptだけを許す。

open前にledgerの既存regular-file identity、同volume、native bounded VFS、既存schema、完全一致するbounded DDLとprotected trigger、DELETE journal mode、page cap、immutable shared policy、登録rootを検査する。既存DBのread snapshotを保持したままguard付きexisting-only connectionを初期化し、その後もBEGINの前と直後にidentityとfree spaceを再確認し、transaction内でschema/policy/rootを照合する。migration、create、policy bind/change、root登録、通常reserve/release/reconcileは行わない。

recovery-onlyはlogical quota、pending合計、drain marginをopen時・精算時の拒否条件から外す。ただしpersisted min_freeとmetadata poolの未割当分を確保できなければ拒否する。CAS/receiptはSQLiteのbounded main/journal VFS capの下で同一transactionに置き、I/O errorやcommit失敗はrollbackする。物理pool不足、DB/page上限、rollback journal上限、receipt容量不足ではtokenを精算しない。`strict_capacity_guarantee`はfalseのままとする。

## 許可する操作と前提

回収専用インスタンスは既存 intent のbounded readと、callerがcut・inventoryを検証した後の `reconcile_checkpoint` proof必須CASのみを許す。proofとterminal accountingは同transactionで保存する。root単位の一覧は上限を持ち、超過を切り捨てず拒否する。新規intent作成や他種reservation操作を行えない。

callerは全writer process終了後の明示offline運用、kernel exclusive lease、最新committed v4 cut、catalog/journal metadata、世代とtarget/temp inventoryを確認する。recovery-only DiskBudget自身はcut、owner停止、lease、file inventoryを証明しない。複数token/rank適用は分散transactionでなく、途中成功後は再実行で残るactive tokenを再検査する。

## 拒否条件と未受入範囲

ledgerやsidefileがpool/page capを越える、hot/nonempty journal/WAL/SHMがある、schema/DDL/trigger/policy/root/deviceが不一致、native VFSが使えない、free spaceがmin_free + 未割当poolを満たさない場合は拒否する。recoveryは既存poolを使うため、metadata poolの不足を通常quota超過から救済する機能ではない。実hardware ENOSPC、突然のfree-space消費、電源断耐久性、cross-host、異なるledger、外部job、全writer回収は受け入れていない。ledger-fullで新規schemaやreceipt領域を後付けするmigrationも行わない。

これはP4-009の狭い回収経路である。GC、unlinked/uncommitted token、extent/payloadや全physical closure、全writer調停は対象外であり、P4-009およびPhase 4は未完了である。
