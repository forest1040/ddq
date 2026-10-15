# Phase 4：同volumeの共有disk admissionとforeground RAM

更新日：2026-10-14。P4-009の同volume調停と、P4-007の任意metadataによる作業領域不足を追加対応した。[writer監査](phase4_writer_audit.md)で確認した境界を実装へ反映する。Macで検証し、クラスタ接続やPBS投入は行わない。全writerのfault／回収とLinux検証は残し、P4-009／007はIN_PROGRESSを維持する。

## 同じledgerを使うdataset／fork

`DistributedConfig.disk_admission_ledger`を追加した。node-localの絶対pathだけを受け付ける。同じhost／volumeのrankが異なるpathを選ぶ場合は、ledgerを開く前にcollective拒否する。hostごとのpathは異なってよい。同じpath文字列が別hostの通信や共有filesystemを要求するわけではない。

通常のbootstrapは従来のhost／volume／root集合からledgerを選び、共有policyをbindする。独立datasetで合算する場合は、同じlocal ledger pathを明示する。forkは新しいcatalogのbootstrap前から親の実ledger pathを継承する。batch／parameter-shiftの内部forkもこの経路を通る。

```python
config = DistributedConfig(
    directory="/private/tmp/ddq-job/dataset-a",
    block_bits=2,
    storage_scope="node-local",
    disk_quota_bytes=64 * 1024**2,
    disk_admission_ledger="/private/tmp/ddq-job/host-ledger.sqlite3",
)
```

quotaは共有ledgerに登録したroot集合の合算上限である。各datasetに同じquotaを別々に与えた独立上限とは扱わない。別volumeは別ledgerを使う。明示ledgerを共有しない独立jobや外部fileまで自動的に調停するものではない。

## 永続policyとroot登録

`DiskBudget(shared_policy=True)`はquota、min-free、drain、metadata pool、実page cap、record cap、device、injected accountingの有無を、同じSQLite transaction内で初回bindまたは完全一致照合する。bind済みledgerはgeneric callerがflagをfalseにしても照合を省略できない。不一致は新root／tokenを書き込まず拒否する。

旧ledgerにactive reservationがある場合は初回bindを拒否し、tokenを保護する。quiescentな旧ledgerはbindできる。旧tokenの判断をPID／期限だけで行わず、既存の明示recoveryでowner／journal／inventory／cutを確認する。

root登録後の全root使用量＋metadata pool＋未物理化予約＋drain余裕と、実空きの下限をtransaction内で確認する。不足時はroot登録もrollbackする。snapshotは共有policyの有効性と登録root数を返す。

永続root表は祖先pathが子孫pathを覆う最小集合とする。親の下のsuffixを毎回別rootへ追加しない。後から親を登録する場合も子を置き換え、実file使用量と既存tokenを保持する。pathの文字列prefixではなく`Path.is_relative_to`を使い、`Root.A`と`Root.A2`を混同しない。非重複rootは256件、呼出しごとの入力も256件以下。

suffixのcloseだけでdurable directoryを自動削除しない。閉じたbranchの実fileはquotaへ残す。任意の共有prefix／履歴／snapshotを削除して空きとして扱うことはせず、temporary branchの安全な回収は別のlifecycle残件とする。root圧縮はfile GCではない。

## 最低foreground作業領域の保護

追加したcache診断課金が、状態をRAM予算より大きくした既存MPI試験で最低bufferを圧迫する問題を検出した。予算や試験条件を緩めず、`AsyncBlockCache.ensure`で実行要求のheadroomを確保する。

trace／counterの新規予約はforegroundの要求byteを残した範囲だけで行う。cache evictionだけで満たせない場合に限り、trace→pool／counter→legacy hint→cost summaryの順で任意metadataを精算する。cost summaryを捨てた後はLRUへ戻る。削除した診断行はdrop countへ加え、metadata解放回数も有界に残す。context getterが失敗した場合もunknown／staleとして安全にfallbackする。

managed RAMのdeclare値を変えない単体試験と、stateがrank RAMを超えるMac P=1/2/4のMPI試験で確認する。managed poolの予約と全processのRSSは別scopeである。

## 検証結果

Mac／uvの最終全件回帰は650 passed／2 skipped。関連回帰107件に、実P=1/2/4のbuffered／F_NOCACHE、独立dataset／fork／template parameter-shift、既存大状態とcheckpointを含む。source／worker hash、初期failure、最終log、再現条件を[内部検証資料](internal/phase4_disk_coordination_validation/README.md)へ保存した。

## 異なるledger・外部jobの調停（追記）

独立ledger同士、宣言済み外部job、同一hostの複数ledgerをvolume単位で調停するcoordinatorは[host調停の設計と実装](phase4_disk_fault_matrix_implementation.md#host調停別ledger外部jobcross-host設計と実装2026-10-15追加)にある。上記の明示共通ledgerとは別に、ledgerを統合せずclaimの公開と検査だけを足す。

## volume coordinatorの通常runtime接続（2026-10-15追加）

`DiskBudget`のAPIだけだったvolume coordinatorを、`DistributedConfig.volume_coordinator`（node-localの絶対path、省略時はopt-out）から使えるようにした。`AsyncDistributedSimulator`など`DistributedSimulator`上のsimulatorは同じ経路を通る。pathはhost固有なので、rank間のconfig合意には含めず、「使うか否か」だけを合意する。検証は`disk_admission_ledger`と同じ規則（node-local、絶対path、4096 byte以下）に加え、ledgerと同じpathを拒否する。coordinatorのpolicy（quota、min_free、drain）はconfigの`disk_quota_bytes`／`min_free_bytes`／`drain_reserve_bytes`と同じ値を使い、最初の束縛者が決め、以後は完全一致を要求する（異なる値の別datasetは拒否される）。

```python
config = DistributedConfig(
    directory="/private/tmp/ddq-job/dataset-a",
    block_bits=2,
    storage_scope="node-local",
    disk_quota_bytes=64 * 1024**2,
    volume_coordinator="/private/tmp/ddq-job/volume/coordinator.sqlite3",
)
```

**bootstrap順序（collective）**。ledgerを選ぶ前に、同じhost／volumeのrankが同じcoordinator pathを選んでいることを全rank投票で確認する。次にcoordinatorを全rankで開き（filesystem種別、host名、policyの検査）、いずれかのrankが拒否すれば、ledgerを作る前に全rankが同じ誤りで失敗する。その後に`DiskBudget(volume_coordinator=...)`を作る。coordinatorはledgerが所有し、ledgerを閉じるときに閉じる。ledgerごとに別のcoordinator接続を持つため、2つのsimulatorが同じcoordinatorを開いてもprocess内のthread lockが共有される。

**束縛の永続化と再開**。`distributed_settings`に`volume_coordinator`（解決済みpath、無ければnull）を保存し、ledgerにも既存の`volume_binding`を持つ。`resume`は、ledgerやcatalogを開く前にrankごとの設定を読み、(1) coordinatorを指定せずに再開、(2) 別のcoordinatorで再開、(3) coordinatorなしで作ったdatasetに新たに束縛、のいずれかを拒否する。拒否はcollectiveで、ledger、coordinatorのbyteは変わらない。ledger側の既存の拒否（bind済みledgerを無束縛で開く）が二重の防御になる。

**fork／prefix fork／parameter-shift**。forkのchild configは`replace(sim.config, ...)`で作られるため、`volume_coordinator`をそのまま継承し、親のledgerと同じcoordinatorをbootstrap前に開く。parameter-batch／parameter-shiftの内部forkも同じ経路である。childのconfigからcoordinatorを落とすと、bind済みledgerの無束縛openとして拒否されるため、coordinatorなしで黙って走ることはない（試験で確認）。

**複数rank**。同じhost／volumeのrankは、そのhostのcoordinator（同じpath）を使う。pathが食い違えば`same volume coordinator`でcollective拒否する。hostごとのpathは異なってよい。`volume_coordinator`のfilesystemが許可されないrankが1つでもあれば、全rankが拒否する。

**offline回復後のclaim再公開**。bind済みledgerのtokenを回収しても、coordinatorのclaimは次の書込み可能openまで過大のまま残っていた。`recover_uncommitted_reservations`と`recover_checkpoint_reservations`は、ledgerのexclusive lease（ledger directoryのflock）の下で、ledgerの真のpending（RESERVED／WRITING行の未物理化byteの和）をcoordinatorへ再公開する。`VolumeCoordinator.reset_claim`は、claimを真の値へ無条件に置き換える（減少が目的）。epochは戻さず、ledgerを読むだけでledgerは書かない。何度適用しても同じ結果になる（`changed: false`）。報告には`volume_claim`（coordinator path、ledger path、`previous_claim_bytes`、`published_pending_bytes`、`changed`、`status`）を受領記録として含める。dry runは公開せず、現在のclaimと真のpendingを表示する。coordinatorはledgerのbindから読み、persistedなpolicyで開く（`VolumeCoordinator.open_bound`）。coordinatorが無い、読めない、別のledgerを指す場合は、tokenを変更する前に拒否する。`checkpoint_recovery`ではhost／volumeごとの最小rankが、全rankのapply完了のcollective投票の後に再公開する。他にledger directoryを開くprocessがあれば（排他leaseが取れなければ）settle前に拒否する。書込み権限を持つmaintenance scope（dead branch GC）は、bind済みledgerを同じcoordinatorで開き、claimを開く時に再公開する。

**限界**。`FileBlockStore`のstore-private ledgerとrecovery-onlyのledger自体はcoordinatorを持たない（recovery-onlyのbudgetは新しい予約を出さないため、claimの減少だけを上の再公開が担う）。coordinatorを使わない独立job、宣言を超える外部jobは従来どおり調停されない。複数host、共有filesystemは対象外である。`strict_capacity_guarantee`は立てない。

### 試験（tests/test_volume_coordination_runtime.py、16件）

- config検証：opt-in、絶対path、長さ、shared scope、非文字列、ledgerとの同一path。
- 独立な2 simulatorが別ledgerで同じcoordinatorを共有し、片方のquota内でも合計超過をmutation前に拒否する（行数不変）。精算で他方に見える。
- coordinatorなしのsimulatorがbind済みledgerを使うことを拒否する。resumeは、coordinator欠落、別path、coordinatorなしのdatasetへの新束縛をledger／coordinatorのbyte不変のまま拒否し、同じ設定では数値が一致する。
- coordinatorのfilesystemが非対応なら、ledgerやdatasetを作る前に拒否する。
- forkとparameter-shiftのchildが親と同じcoordinator・ledgerを継承する。
- 実`mpiexec`の1／2 rank：独立dataset、fork、parameter-shift（内部fork）、joint refusal、ledger分離。pathが食い違うrank、1 rankだけ非対応filesystemはcollectiveに拒否。
- offline回復：実processがkillしたwriterのclaimが、`recover_uncommitted_reservations`のapplyで真の値へ下がり、二重適用でも不変、別ledgerが再び予約でき、bind済みledgerが再openできる。1／2 rankのcheckpoint recoveryでも同様に再公開（dry runは公開しない、再適用は`changed: false`）、回復後のresumeは数値が一致する。coordinatorを移動すると回復は拒否される。維持scopeは同じcoordinatorを採用する。


## 外部jobの宣言超過の検出（2026-10-15追加）

`VolumeCoordinator.declare_external(..., roots=[専用directory, ...])`は、rootごとの初期通常file byte数、device／inode、zero-byteのincarnation markerを保存する。登録するrootは実在する同volumeのdirectoryで、既登録root（ledger、別の外部job、過去に保持したroot）と祖先／子孫関係を持ってはいけない。同じ呼出し内の重複と包含rootは最小集合へ圧縮する。coordinator／ledgerのmetadata pathを含む外部rootと、ledgerを後から外部rootへ重ねることも拒否する。これにより、他のwriterの物理化を外部jobの書込みと誤認してpendingから控除しない。

監視する量は各rootの`max(0, 現在の通常file byte数 - 登録時のbyte数)`の和（net growth）で、累積write量や実block allocationではない。初期fileの削除／置換はnet量へ反映される。観測growthは外部claimの未物理化分から控除し、全rootの実file量と二重計上しない。quotaとmin-freeの両方に、この未物理化分を使う。例：宣言60 kBのjobが60 kBを書き終えたら、物理60 kB＋pending 0で、60 kB＋宣言60 kBとは数えない。

次のledger admission、外部claim登録／増額、`check_external_usage()`、`snapshot()`で観測し、growthが宣言を超えたら`DiskAdmissionError`にclaim IDと観測値／宣言値を付けて拒否する。拒否したledgerには予約行を挿入しない。超過状態のreleaseや観測量を下回るresizeも拒否し、claimを黙って消さない。**明示的な増額resize**は、新しい宣言が観測量以上でvolume全体のquota／min-freeも満たす場合に限り、超過を解消できる。超過を永続違反履歴として記録する機構は含まない。ledgerのreleaseなど、安全な減少の公開は妨げない。

root削除／directory置換、incarnation marker欠落、root内のsymlink、別device、hardlink、特殊file、走査／stat失敗は観測不能としてfail-closedにする。markerはinode再利用だけでは同じrootと認めないために使う。全rootのquota走査も、`Path.rglob`がpermission errorを黙って飛ばす経路をやめ、`scandir`の観測失敗を拒否するため、回復後に保持した外部rootでも読めないdirectoryを空きと数えない。

root省略の既存APIと旧coordinator DBは互換で、従来どおり宣言全量をpendingに保持する。snapshotのactive外部claimは`usage_monitoring`（`net-file-growth`または`declaration-only`）、`observed_growth_bytes`、`pending_bytes`を返す。観測不能や超過時は、snapshotも推測値を返さず拒否する。新しい帰属tableは既存DBへ`CREATE TABLE IF NOT EXISTS`で追加し、旧claimを監視済みと誤表示しない。

正常releaseでは帰属とroot markerを消し、owner死亡後の明示recoveryではpendingだけを解除する。回復済みclaimの帰属とmarkerは`forget_recovered`で消す。宣言に失敗した場合も作成済みmarkerを掃除する。いずれも全体のroot表は残すため、書かれたfileはquotaに残り、同じrootの再宣言は保守的に拒否する。rootを再利用可能にする帰属GCは今回の対象外である。

これは次の観測時点での検出であり、非協力的writerを停止させる機構ではない。観測と実writeの競合、root外へのwrite、初期fileの削除と新規writeによるnet相殺、観測間の一時的超過、故意のmarker複製、実block allocationは保証しない。明示rootなしのjob、複数host、電源断model、全writerのfault／回収などの残件は継続し、`strict_capacity_guarantee`はfalseのままである。

試験は`tests/test_volume_external_usage.py`。別processによる実file write、境界での二重計上防止、超過のmutation前拒否と明示resize、baseline／重複root、削除／置換／marker／link／PermissionError、root重複拒否、失敗時rollbackとmarker清掃、旧DB互換、reopen、死ownerの回復、保持fileとmarkerのlifecycleを確認する。

検証結果：Macで上記新規14件＋既存coordinator 24件＋runtime 16件＝54 passed。`PYTHONPATH=src /Users/tmori/work3/ddq/.venv/bin/python -m pytest tests/test_volume_external_usage.py tests/test_volume_coordinator.py tests/test_volume_coordination_runtime.py -q`で確認した。runtimeの実MPIはsandbox内のsocket bind拒否を受け、許可済みのsandbox外実行で成功した。続いて統合版をLinux buffered／O_DIRECTで検証し、各105 passed／112 passed・2 skipped（Mac専用transport試験）だった。両backendで新規外部job試験と通常runtime（実MPI 1／2 rankを含む）、回収が成功した。[証跡](internal/p4_coordinator_followup_validation/README.md)に実O_DIRECT probe、source hash、条件とscopeを保存した。
