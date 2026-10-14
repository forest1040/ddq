# Phase 4 recovery-only metadata pool implementation

更新日：2026-10-14。既存production shared ledgerに対するrecovery-only openを追加し、logical quota・pending・drain margin超過で通常openが拒否される場合でも、precharged metadata poolに余裕がある範囲でcheckpoint reservation CASを可能にした。設計は[recovery pool設計](phase4_recovery_pool_design.md)、cut検証は[checkpoint recovery実装](phase4_checkpoint_recovery_implementation.md)、検証記録は[内部validation資料](internal/phase4_recovery_pool_validation/README.md)を参照する。P4-009とPhase 4は未完了である。

## Recovery-only open

`DiskBudget(..., shared_policy=True, recovery_only=True)`は既存ledgerだけを開く。通常bootstrapが行うschema作成/migration、policy bind、root登録、shared capacity admissionを呼ばず、read-only SQLite snapshotでschema version、保護対象5テーブルの正規化DDL完全一致、2つのbounded label trigger、DELETE journal、page envelope、persisted shared policy、登録rootを検証する。read snapshotを保持してnative bounded VFSのexisting-only connectionを確立し、接続後もDDL/policyを再確認する。missing ledgerや不明な旧DDLは修復せず拒否する。

recovery-onlyはproduction shared mode、native VFS、matching constructor/persisted policy、既存root coverageを必須にする。`reserve`、`release`、一般`reconcile`、`materialize`、`mark_writing`、`register_roots`などは拒否する。checkpoint intentのbounded readと、proof document必須の`reconcile_checkpoint`のみ許す。通常の`apply=False` dry runもrecovery-only openを用いるため、schema migrationやroot/policy更新は行わない。reservation精算とreceipt保存は`apply=True`のCAS transaction時だけである。

## 容量と失敗動作

open前、CAS transaction開始前、`BEGIN IMMEDIATE`後にledger identityとmetadata poolを再確認する。logical quota、pending、drain marginはこの既存pool内recoveryの拒否条件に含めない。persisted `min_free_bytes`とpoolの未割当metadata envelopeは維持し、free spaceが足りなければtransaction前またはrollback可能な範囲で拒否する。main DB page capとrollback journal capは既存policyに対応するnative VFSで強制し、synchronous FULLを要求する。CASとreceipt INSERTは同一SQLite transactionで、receipt/commit I/O失敗ではrollbackを試みる。rollback成功時はtokenがactiveのままでreceiptを保存しない。rollbackやdurabilityが不明な実障害は成功と報告せず、別の復旧を必要とする。

nonempty/hot ledger sidefile、pool/page envelope超過、policy/schema/root/volume mismatch、native guard不在、物理headroom不足、実際のSQLite fullは拒否対象である。既存poolのbounded metadata操作でも、実hardware ENOSPCや電源断耐久性を証明するものではない。`strict_capacity_guarantee=false`を維持する。

## 検証と状態

検証は[recovery pool validation](internal/phase4_recovery_pool_validation/README.md)を参照。公開記録では固定の件数を示さず、validation資料を進捗の根拠とする。追加対象にはschema制約改変、quota pressureでのcheckpoint recovery、metadata page/journal限界、receipt/commit I/O rollback、再openが含まれる。Mac/Linuxの全matrix、全writer fault/recovery、外部job・cross-host・電源断は未受入であり、P4-009とPhase 4の完了を意味しない。
