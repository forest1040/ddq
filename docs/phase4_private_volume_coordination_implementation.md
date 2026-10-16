# P4-009 store-private ledgerのvolume coordinator対応

2026-10-16。`StorageConfig`と`FileBlockStore`のowned private ledgerを、既存の同host／同volume coordinatorへ接続した。独立private store同士がledgerを統合せず、pending／観測root／metadata poolを共同quotaに含める。`strict_capacity_guarantee`は引き続きfalseである。

## APIとbootstrap

`StorageConfig`へ`volume_coordinator: str | None`、`disk_quota_bytes: int | None`、`min_free_bytes`、`drain_reserve_bytes`を追加した。coordinatorはopt-inの絶対node-local pathで、catalog／private ledgerと同じpathは拒否する。quota／min_free／drain値もconstructorで検証する。

`private_disk_budget.open_private_budget`が新規owned private ledgerを開く前にcoordinatorを取得し、同じpolicyで`DiskBudget(persist_private_policy=True, volume_coordinator=..., own_volume_coordinator=True)`を作る。登録・上限検査・増加をledger commitより先に公開する順序・減少の後公開・epoch・leaf lock順序は既存の実装を再利用する。coordinator接続はprivate budgetが所有し、close／失敗時に閉じる。別volumeはcoordinator fileを作る前に拒否する。

外部から`disk_budget`を渡したstoreではそのbudgetがauthorityであり、`StorageConfig.volume_coordinator`も指定した場合にはlocator／volume／coordinator policy／ledger policyの全ての一致を検証し、同じ提供済み接続は開き直さず所有権も奪わない。distributed runtimeの既存shared budget経路は維持する。

## 再開と回収

- private ledgerの保存済みvolume bindingを、configから省略／別pathへ変更したresumeは拒否する。
- 再開時は既存coordinatorを`open_bound`で開く。coordinatorが消えていれば空DBを再作成しない。保存policyとconfigの不一致も拒否する。
- 既存のunbound private ledgerへ通常openでcoordinatorを後付けすることは拒否する。bindの変更には明示offline migrationが必要である。
- owned private storeのresumeでledger自体が消えていれば、admission historyを作り直さず拒否する。外部budgetを使う明示migration経路とは区別する。
- offline recoveryはprivate ledgerの`volume_binding`も読み、token変更前にcoordinatorを既存policyで開く。全owner停止のexclusive lease、dry run／plan digest／CAS／receiptは維持する。apply後に真のpendingを既存`republish_volume_claim`でcoordinatorへ再公開し、dry runでは公開しない。
- dead branch GC等のmaintenance writerは同じbindingをadopting budgetへ渡す。recovery-only budget自身は従来どおりadmissionを行わず、coordinator再公開はexclusive recovery scopeが行う。
- 明示export rootsのcoordinator登録、観測走査のfail-closed、EXTRA同期と電源断検証の変更は維持する。

## 試験

`tests/test_private_volume_coordination.py`はinvalid config、独立private store 2個の共同quota、resumeでのbinding省略／置換／後付け／coordinator欠損／ledger欠損拒否、child processの異常終了後のclaim据置きとoffline再公開、maintenance binding継承、numpy oracle付きsnapshot resumeを確認する。外部budget注入ではcoordinator省略／別path／両policy不一致の拒否と、同じ接続の再利用を検証する。Linuxの実別volumeへの注入拒否も含む。実process 2個が別private ledgerを保持するケースでは、最後の24 MiBを片方だけが予約でき、予約ownerのSIGKILL後も他方が生きたままoffline精算し、解放後に他方の予約が成功する。PIDや経過時間で解放しない。

試験は`SVSIM_TEST_BACKEND=direct`で同じstore backendを選べる。coordinatorは既存node-local filesystem／host／volume検証を使い、共有filesystemへDBを置く方式へは拡張しない。
