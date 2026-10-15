# P4-009 disk faultマトリクスの実装（quota境界掃引とprocess kill）

2026-10-15、writer faultの実試験を2ファイル追加した。この時点では実装コードの変更はなく、既存の予約・native VFS・回収の挙動を実processと実SQLiteで確認した（後続の追加セクションで回収コードを1件修正した）。P4-009はIN_PROGRESSのままである。

## quota境界掃引（tests/test_disk_quota_sweep.py）

catalog commit、trace追記、semantic journal追記、checkpoint sidecar（`atomic_json`）、amplitude extent（`write_version`）の5 writerについて、quotaを二分探索で「拒否される最大値」と「許可される最小値」へ絞り、境界の前後40 byteを1 byte刻みで掃引する。さらに境界から±1／2／8 page（4096 byte）離れた点も確認する。確認する不変条件は次のとおり。

- 判定は単調で鋭い。境界未満は全て`DiskAdmissionError`、境界以上は全て成功する。
- 拒否時は管理namespaceのデータbyte（hash）、durable cut（catalog行、sidecar内容、extent公開状態）、`reserved_bytes`が拒否前と同一である。
- 境界ちょうどの成功で、新しいcutだけが公開される（catalog、sidecar、extent）。

trace／journalは1 byteのopen probeを先に許可するため、拒否時に長さ0のfileが残り得る。データbyteは書かれない。試験は長さ0のfileを比較対象外とし、この点を明示している。

書込み途中の安全な失敗は書込み前の拒否と区別して別試験にした。

- extentの短いwrite後は、versionが公開されず、tokenは解放され、catalogのintegrity checkが通り、holeを残して次のextent書込みが成功する。
- sidecarはfile fsync失敗とrename失敗の両方で旧targetを保持し、temporaryを残さず、tokenを解放する。
- catalogは、予約済みenvelope内で受理されたSQLがnative page capで止まる場合に、`DiskAdmissionError`ではない`sqlite3.DatabaseError`となり、file byteが変化せずjournalが残らない。

## 実process kill matrix（tests/test_disk_kill_matrix.py）

子processが`os._exit`／SIGKILLで消えた後、親が回収手順を実行する。共通の回収確認は、生存中のtokenを1件ずつ`reconcile`し、二重`reconcile`が拒否され、`reserved_bytes`が0になることである。

| 対象 | kill位置 | 確認内容 |
|---|---|---|
| gate batch catalog commit | reserve前、reserve後BEGIN前、1／3回目のSQL後、COMMIT直前 | 旧cut。reopen後にintegrity ok、新規durable書込みが成功、kill後に残るrollback journalは次の書込みで消える |
| 同 | COMMIT直後（hook／release前） | 新cut全体が見える。tokenは明示回収まで残る |
| ledger lease append（journal） | admit直後、record途中（半行）、record全体の書込み後未確認 | committed prefixがreplayで完全。次のappendは未commit tailだけをtruncate。tokenはWRITINGで残る |
| sidecar `atomic_json` | file fsync前後、rename前後、directory fsync後 | targetは常に旧または新の全体。rename前はpartial temporaryを保持、rename後はtemporaryなし。tokenは保持 |
| catalog連続commitへのSIGKILL（6種の遅延） | 任意時点（fsync中を含む） | 確認済みrowは全て残り、rowは欠番のない接頭辞。進行中の1件だけが余り得る |
| 分散simulator gate batch（mpi4pyが必要） | batchのdurable commit直後 | token回収後、最後のsnapshotからresumeして数値一致、新snapshotを作成できる |

## 観察

- COMMIT前killでjournalが残る場合、SQLiteはsyncされていないjournalをhotとは扱わず、次の書込みtransactionが消費する。reopenと読取りだけではjournalが残る。試験はこれを仕様として固定した。
- sidecarはflush後にmaterializeするため、fsync前killでもtokenは`WRITING`のまま`reserved_bytes`が0になる。生存確認は`reserved_bytes`ではなくstate件数で行う。
- 試験が明らかにしたバグはなかった。

## 検証

- Mac：`uv run pytest -m "not slow"`で737 passed、53 skipped（mpi4py欠如のskipを含む）。新規2ファイルは単独で32 passed、2 skipped。
- Linux（ent Docker、`ddq-linux:dev-20261014`）：新規2ファイルと`test_catalog_batch`／`test_catalog_admission`で55 passed。mpi4pyを要する分散simulatorのkillもここで実行した。

## 残り（初回時点。ENOSPC、O_DIRECT、prepare／decisionの複数rank killは下の追加セクションで実施済み）

## extent segmentのstage kill（tests/test_segment_stage_kills.py、追加）

同期経路（`FileBlockStore.write_version`）と非同期経路（`AsyncStoreIO.submit_write`の完了callback、cooperative engine）について、旧extentをflush済みにしてから新extentの書込みを開始し、次の6 stageで実子processを`os._exit`した。hookは`StorageFile.write_from`／`flush`と`CatalogConnection.commit`の前後に置く。catalog commit stageは、segmentへの書込みが済んだ後の最初のcommit（extentを公開する更新）だけを対象にする。

| stage | 新extentの公開 | 新slotのbyte | tokenのmaterialize |
|---|---|---|---|
| segment write前 | なし | なし（claim済みのhole） | なし |
| segment write後（materialize前） | なし | あり | なし |
| catalog commit前（UPDATE済み、未commit） | なし | あり | あり |
| catalog commit後（token解放前） | あり | あり | あり |
| segment fsync前／後 | あり | あり | あり（tokenは解放済み） |

各killの後に、(1) 生きたtokenを確認（fsync stageは公開commit直後に解放済みのためextent tokenなし）、(2) offline reconcile（dry run→`plan_sha256`付きapply）で全tokenを1回だけ精算し再適用を拒否、(3) 観測使用量がapply前後で一致し、書込み済みslotのbyteが課金に残り、segmentが切り詰められないこと、(4) reopen後にcatalogの`integrity_check`が通り、旧extentがchecksum検証付きで読め、新extentは公開済みのstageでのみ読めて未公開では`OSError`となること、(5) claim済みslotを再利用せず次のextentが成功することを確認する。release予定のpending bytesは、materialize前はslot全体、materialize後は0であった。同期／非同期でstage別の期待は同一である。

数値の確認として、`StoredSimulator`でsnapshotをcommitした後のgateでcache flushを行い、write前／write後／catalog commit前／catalog commit後にkillし、回収後に最後のsnapshotからresumeした状態が事前に保存したnumpy oracleと一致すること（atol 1e-13）、回収後に新しいsnapshotをcommitできることを確認した。

`SVSIM_TEST_BACKEND=direct`で同じ試験を`direct` backendで実行する（Linuxでは`O_DIRECT`、alignmentは4096、Macでは`F_NOCACHE`）。

### 観察

- 同期・非同期ともextentはsegment fsyncより前にcatalogへ公開される。実processのkillではpage cacheが残るため新extentは読める。電源断で公開行だけが残りsegment bytesが失われる順序は、この試験では検証できない。snapshot commitは`store.flush()`でsegmentをfsyncしてからmanifestをcommitするため、snapshotから到達できるextentは保護される。公開済みだがsnapshot未commitのextentはresume対象外である。
- 試験が明らかにしたバグはなかった。作業中に、試験側のalignment指定誤り（direct時は4096が必要）と、tokenの寿命の想定誤り（fsync stageでは解放済み）を直した。

## 残り（更新）

残りは、別ledger／外部job／複数hostの調停と一括回収、extent公開とsegment fsyncの電源断時の順序（kill試験では検証できない）、SQLite fsync内部への決定的なstage注入、catalog tokenのfile単位の帰属、private ledgerのpolicy非永続による検査の弱さである。

## 追加分の検証

- Mac：`uv run pytest -m "not slow"`で798 passed、53 skipped、47 deselected。
- Linux（ent Docker、`ddq-linux:dev-20261014`）：本資料と回収資料の対象（segment stage、private ledger、uncommitted recovery／GC、kill matrix、quota sweep、checkpoint recovery、recovery pool、reservation ledger、catalog batch／admission）187 passedを`SVSIM_TEST_BACKEND=direct`で、同じ集合をbuffered、新規2ファイル（32件）もbufferedで再実行して成功した。

## 以前の残り（記録）


unlinked／uncommitted reservationの回収、temporary branch GC、別ledger／外部jobとのhost調停、extent writeのkernel／電源断fault、segment fsync前後のkill、prepare／decision／decision-copyのkill（実process、複数rank）、Linux O_DIRECT下の全matrix、ENOSPC実測は未完了である。SQLite内部のfsync中killはSIGKILL掃引で確率的に確認するだけで、決定的なstage注入ではない。

## 実ENOSPC、O_DIRECT、複数rank checkpoint kill（2026-10-15追加）

残りの3項目を実環境で試験した。P4-009はIN_PROGRESSのままであり、`strict_capacity_guarantee`はfalseのままである。

### 実ENOSPC（tests/test_disk_real_enospc.py）

環境変数`SVSIM_FULL_FS`が指す小さなfilesystemがあるときだけ実行する。Macの通常suiteでは全件skipする。ent Dockerでは特権を足さず、`docker run --tmpfs /full:rw,size=96m,mode=1777`で作ったsize制限のtmpfsを使った（既存のdocker profileのまま。loop mountはprivilegeが要るため使わない）。quotaは実質無制限にし、効くのは物理空きだけである。空きは別processのfillerが実blockを消費して作り、kernelがENOSPCを返す。擬似的なquota errorは使わない。

- 書込み前の拒否（catalog、trace、journal、sidecar、extentの5 writer×`min_free`／空き枯渇の2種、10件）：fillerで空きを使い切った後の書込みは`DiskAdmissionError`で拒否される。拒否前後で管理namespaceのdata byte、durable cut、`reserved_bytes`が同一である。fillerを消して空きを戻すと、同じ書込みが許可され公開される。
- admission後に外部writerが空きを消費（5件）：子processが予約を取った直後にfillerが空きを0にする。書込みは必ず例外になり、成功を装わない。例外は、catalogが`database or disk is full`（SQLITE_FULL）、trace／journalが短い書込みの検出（`short write`／`Incomplete journal write`）であり、sidecarとextentでは予約のledger更新が先にSQLITE_FULLで失敗した。例外後もtokenは残る（`live`が1または2件）。子processを後始末なしに終了させ、空きを戻してから確認した結果は次のとおり。durable cutは基底のまま（catalogのrowは旧のみでintegrity ok、sidecarは旧JSON、journalは確認済みprefixのみ、traceは先行recordのみ）。offline reconcile（`recover_uncommitted_reservations`）が全tokenを1回で精算し、artifactを削除・書換えせず、`reserved_bytes`が0になり、その後の書込みが成功する。hot rollback journalが残ったcatalogは既存と同じく、admitted reopenでreplayさせた後に回収する。
- checkpoint sidecar：intentを持たない素の`atomic_json`のtokenはoffline回収が設計どおり拒否するため、呼び出し側が`DiskBudget.reconcile`で精算する。intent付きの実checkpointは下記の複数rank試験でENOSPCを確認した。
- ledger自身も同じfilesystemにあるため、外部writerが空きを奪うとledgerの`materialize`や`release`もSQLITE_FULLで失敗し得る。これは外部jobによる空き消費が管理対象外であるという既存の限定と一致し、tokenが残ることで安全側に倒れる。metadata poolのbyteを物理的に先取りしているわけではない。

### O_DIRECT（`SVSIM_TEST_BACKEND=direct`）

quota掃引、kill matrix、未commit回収、GC、実ENOSPCの各試験は、環境変数`SVSIM_TEST_BACKEND`で`StorageConfig`／`DistributedConfig`のbackendを選ぶ。entのext4 scratch（Docker bind mount）で、`direct`の全試験を実行して成功した。O_DIRECTは4096 byte整列のaddressと長さを要求するため、試験側のextent書込みと読取りはmmapのpage整列bufferを使うように直した。sub-pageの短い書込みはO_DIRECTでは起こり得ないので、該当の1件（extentの短いwrite）は`direct`ではskipする。実際にrankがdirectで動いたことは、workerが出力するbackend名で確認した。

### 複数rank checkpoint kill（tests/test_checkpoint_stage_kills_mpi.py）

mpiexec -n 2の実processで、snapshot 1をcommit後に1 gateを足し、snapshot 2のcheckpoint中にvictim rankを落とす。落とす位置は`atomic_json`のstage（prepare、global decision、decision-copy）×位置（予約前、予約後file作成前、書込み途中13 byte、rename前、rename後）であり、`os._exit`で実際に死ぬ。別のmpiexecが回収する：offline reconcile、collectiveな`recover_checkpoint_reservations`、resume、数値一致（新規commitが公開されたcutは新、されていないcutは旧）、再度のcommit。global decisionのrename後は新cut、それ以前は旧cutである。代表4件は通常suite、全matrix（slow、30件のうちdecisionのrank 1は存在しないためskip）はslowである。ENOSPC版は、予約後にfillerが空きを消費する5件（rank 0／1、3 stage）で、commitが`database or disk is full`のCollectiveErrorで失敗することを確認した後、同じ回収で成功した。

発見したバグ：prepare／global decisionのstageで落ちると、まだ一度もcommitされていないsnapshotのintent付きtokenが残る。`recover_checkpoint_reservations`は最新committed cutのintentしか受け付けず（`Recovery intent belongs to a different dataset generation or cut`）、`recover_uncommitted_reservations`はintent付きtokenを対象外としていたため、どの手順でも精算できなかった。修正として、`recover_uncommitted_reservations`が、global decisionが存在しないsnapshotのprepare／decisionのintent付きtokenを`checkpoint-uncommitted`として扱うようにした。拒否条件は、intentのdataset rootが排他lock済みdirectoryでない、device／inodeの不一致、stage・target・temporaryがgeneration付き固定名と一致しない、directoryがsymlinkで別名化されている、targetやtemporaryが排他的な通常fileでない、decision-copyのtokenにglobal decisionがない（不整合）、のいずれかである。精算は`reconcile_checkpoint`のexact CAS（予約行とintent行の完全一致）とreceipt保存を同一transactionで行い、partial temporaryは削除しない。global decisionが存在するsnapshotのtokenは従来どおり`linked_live_tokens_deferred`として報告するだけで、checkpoint recoveryが扱う。

その他：既存のSIGKILL連続commit試験が、高負荷のhostで最初のcommit開始前にkillされると「回収するtokenが1件以上」の前提が崩れて失敗した。tokenが存在し得ない場合を許す条件に直した（実装の問題ではない）。

### 検証

- Mac：`uv run pytest -m "not slow"`で770 passed、69 skipped、47 deselected。
- Linux（ent Docker、`ddq-linux:dev-20261014`、`--tmpfs /full`、`SVSIM_FULL_FS=/full`）：buffered backendで関連11ファイル160 passed、1 skipped（負荷由来のflake 1件は上記修正後に単独で再実行して21 passed）。`SVSIM_TEST_BACKEND=direct`で同じ11ファイル160 passed、2 skipped。slowの全matrixは25 passed、5 skipped。
- 通常のentの実行は引き続き4 CPU・8 GiB・network noneである。

### 残り

store内private ledgerの回収、複数rank・複数host（各rank別root・別ledger）のoffline回収の一括操作、decision-copy tokenのうち別rank rootにあるglobal decisionを見つけられない構成、異なるledger・外部jobを含むhost調停、extent writeのkernel／電源断fault、segment fsync前後のkill、ledger自身の物理空き先取り、cross-host、電源断は未完了である。O_DIRECT下でのSQLite内部のfsync中killは確率的な確認に留まる。

## extent公開前のsegment fsync（電源断順序、2026-10-15追加）

### 順序の決定

catalogは`synchronous=FULL`でdurableにcommitされる。一方、extentのcatalog行（segment／offset／checksum）は、segmentのfsyncより前に公開されていた（同期の`write_version`と非同期の`AsyncStoreIO`の両方）。電源断では、公開行だけが残りsegment byteが失われ得た。`FileBlockStore`は、行が公開される前に、書込み済みsegmentをfsyncする順序へ変更した。

- 同期経路は`_catalog_extent`、非同期の完了callbackも`_catalog_extent`を呼ぶ。private slotの`seal_extent`（`TileExtentWriter`）も同じ関数で保護した。
- gate batchの内側では公開が`after_batch`で行われる。fsyncはそのhookの先頭で、batchが触れたsegmentごとに1回だけ行う（stageあたり1回、extent数には比例しない）。batch外では公開ごとに1回である。
- 新しいsegment fileの初回fsyncでは、`state`ディレクトリもfsyncする（file自体の存在をdurableにする）。
- fsyncは`transport_factory`で開いたhandleの`flush()`を通す。direct backendでもsize／extent割当てのfsyncが必要なため、全backendで行う。fsync失敗時は行を公開せず例外にし、tokenは回収手順で扱う。
- 約束するdurabilityは「公開された行が参照するbyteは、catalogと同じ水準でdurable」である。非公開のslotは電源断で失われてよい（resume対象外、holeとして残る）。

### 試験（tests/test_segment_power_loss.py）

試験専用のtransport層`LossyFile`が、書込みごとに旧byteを記録し、そのfileへのfsync（`flush`）で記録を捨てる。模擬電源断は、fsyncされていない書込みを全て巻戻す（短いfileは切詰める）。catalogは実SQLiteでdurableである。同期／非同期×単発／gate batchの4構成、batch内で例外を出した後の公開済み接頭辞、`TileExtentWriter.seal`で、公開された全extentがchecksum付きで読めることを確認した。負例として、公開前のfsyncを外すと公開行のbyteが失われることを確認し、模型の感度を示した。さらに、gate batchでsegment fsyncが1回、batch外で書込みごとに1回であることを数えた。

既存の`test_segment_stage_kills`は、fsync stageが公開より前になったことに合わせて期待を直した（fsync前後のkillでは新extentは未公開、tokenはmaterialize済みで残る）。

### 費用

同一のworkload（14 qubit、block_bits 8、RAM 2 MiB、3,840 extent、`materialize`＋cache flush＋store flush、5回のmedian）で、fsyncを無効にした場合（before）と有効（after）を比較した。

| 環境 | backend | before | after | 増分 |
|---|---|---|---|---|
| Mac | buffered | 11.57 s | 11.75 s | +1.6％ |
| Mac | direct（F_NOCACHE） | 11.76 s | 14.03 s | +19％（run間のばらつきが大きい） |
| ent Docker（ext4） | buffered | 168 s | 179 s | +6％ |
| ent Docker（ext4） | O_DIRECT | 182 s | 200 s | +10％ |

entの各runは142〜213 sの範囲でばらつき、before／afterの範囲は重なる。増分は数％〜20％程度の上限と読む。macOSの`fsync`はdevice cacheをflushしないため、Macの値はLinuxより電源断保護の費用を小さく見積もる。batch集約（stageあたり1回）があるため、extent数に比例する費用にはなっていない。batchの外（`write_version`の単発）では公開ごとに1回のfsyncが加わる。

## SQLite内部のxSync／xDeleteへの決定的fault注入（tests/test_vfs_fault_points.py）

native budget VFSに、`-DSVSIM_TEST_FAULTS`付きでbuildしたときだけ存在するfault pointを追加した。`SVSIM_VFS_TEST_FAULTS=1`のとき`SQLiteGuard`がflagを足し、binaryのbuild flagに記録される。通常のbuildにはこのcodeも`svsim_test_fault_arm`関数もない（試験で確認）。armは`(path部分文字列, kind, N, action)`で、kindはjournal-sync／main-sync／journal-delete（rollback journal方式のcommit point）、actionはfail（I/O error）／kill-before／kill-after（`_exit`）である。

子processでgate batchのcatalog commitにarmし、親が再openして確認する。

- killはjournal sync 1／2回目、main db sync、journal deleteの前後の8点。journal deleteの直後だけ新cut全体で、それ以前は（main dbがsync済みでも）hot journalのrollbackで旧cut全体になる。いずれもintegrity okで、新規durable書込みが成功する。
- failは4点（journal sync 1／2回目、main sync、journal delete）。commitは`OperationalError`となり成功を装わない。旧cutが残り、fault解除後の書込みが成功する。
- 観察：journal deleteの失敗後はhot journalが残る。guardが非活性の読取り（admission外のSELECT）はjournalをreplayできず（VFSがdeleteを拒否する）`database or disk is full`になる。最初のアクセスはadmit済みの書込みでなければならず、これは既存の回収手順（admitted reopenでreplay後に回収）と一致する。
- VFSが使えない環境（C compiler／`sqlite3ext.h`なし）では、理由を示してskipする。

## store-private ledgerのpolicy永続化

新規に作るstore-private ledgerに、追加tableの`private_policy`（shared_policyと同じ列）を作り、policyを保存する。FileBlockStoreが所有するledgerは`persist_private_policy=True`で開く。

- 新規作成時のみ保存する。既存ledgerは移行せず、tableがなければ従来どおり「既定値のみ受理する弱い検査」のままである（新tableは回収時のschema検査の対象外で、無くても拒否されない）。
- 通常open（FileBlockStore）は、保存済みrowと引数のpolicyが異なると`policy mismatch`で拒否する。
- recovery-only open（`store_private=True`）は、rowがあればschemaとrow（quota／min_free／drain／bootstrap／page cap／max_records／volume）を照合し、非既定値の回収を可能にする。rowがなければ非既定値のpolicyを`persisted none`で拒否する。呼出し側の`policy`が保存値と衝突すると拒否する。`shared_policy`は作らず、shared policyのlayout判定は変わらない。
- 試験：既定policyの保存、非既定のquota／min_freeでの回収（dry run→apply）、page cap／volume／schema改ざんの拒否とledger byte不変、row無し（既存ledger相当）の弱い検査と非移行、既定値での再openの拒否。既存の試験は、非既定quotaの拒否が`ValueError`から`DiskAdmissionError`（保存policyとの不一致）へ変わったため1件直した。

## 検証（この追加分）

- Mac：`uv run pytest -m "not slow"`で837 passed、69 skipped、2 failed（`test_layout_migration`の2件。このworktreeのvenvにmpi4pyがなく`ModuleNotFoundError`。本変更と無関係）。
- ent Docker（`ddq-linux:dev-20261014`、`--hostname ddq-fixed`）：disk／recovery 24ファイル（segment power loss／stage kill、VFS fault、private ledger、uncommitted recovery／GC、kill matrix、quota sweep、recovery pool、reservation ledger、catalog batch／admission／capability、checkpoint recovery、checkpoint stage kill（MPI）、stored budget／numerics／recovery、storage、async io）を、bufferedで359 passed／11 skipped、`SVSIM_TEST_BACKEND=direct`で358 passed／12 skipped。

## 残り（更新）

extent公開とsegment fsyncの電源断順序、SQLite内部のxSync／xDelete決定的注入、新規private ledgerのpolicy永続化は実装した。残りは、別ledger・外部job・複数hostの調停（下記の設計メモ）、catalog tokenのfile単位の帰属、既存private ledgerのpolicy（移行しないため弱い検査のまま）、O_DIRECTのSQLite journal自体の電源断模型、cross-hostである。

### host調停の設計メモ（未実装）

同一host上の異なるledgerや外部jobは、物理空きの観測（`min_free`）でしか互いを見ない。案は、volume単位の小さなhost coordinator ledger（`/var`相当のvolume rootに1つ、各ledgerのquota総和と各jobの宣言byteを登録）を追加し、dataset ledgerは自分のpolicyに加えてcoordinatorの空きcreditを予約することである。要点は、(1) coordinatorもbounded native VFS上のSQLiteとし、予約はdataset ledgerの予約より先に取り、解放は後にする、(2) job終了の証明は既存のdirectory flock方式を再利用する、(3) 外部jobは宣言制で、宣言を超える消費は検出のみ（strictな保証ではない）、(4) 複数hostは共有filesystemを使わず、各hostのcoordinatorを独立とし、host間の合算はscheduler側の責務にする。未実装であり、受入条件は満たしていない。
