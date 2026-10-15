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

extent公開とsegment fsyncの電源断順序、SQLite内部のxSync／xDelete決定的注入、新規private ledgerのpolicy永続化は実装した。残りは、別ledger・外部job・複数hostの調停（下記「host調停」節で同一host分を限定的に実装、配線とcross-hostは未実装）、catalog tokenのfile単位の帰属、既存private ledgerのpolicy（移行しないため弱い検査のまま）、O_DIRECTのSQLite journal自体の電源断模型、cross-hostである。

## host調停：別ledger・外部job・cross-host（設計と実装、2026-10-15追加）

### 問題

同一volume上の独立なdataset／jobが各自のledgerを使うと、各ledgerは自分のquotaと物理空き（`min_free`）しか見ず、互いの未物理化予約を見ない。各ledgerが「空きがある」と判断して予約した合計がvolumeを超え得る。外部processの予約も同様に見えない。共有ledger（上記の同volume共有ledger）は同じledger pathを明示する場合にだけ合算する。

### 設計

**volume coordinator**を1つ追加する。node-localなvolumeに置く小さなSQLite（native bounded VFS、metadata poolは固定）である。各ledgerは自分のtokenを従来どおり自分のledgerに持ち、coordinatorには次だけを持つ。

- policy（quota、min_free、drain、pool、page cap、device、host名、filesystem種別）。初回に束縛し、以後は完全一致を要求する。
- ledgerごとのclaim（metadata pool byte、未物理化pending byte、単調なepoch）。上限64。
- 参加者のrootの和集合（祖先が子孫を覆う最小集合、上限256）。観測使用量は和集合を1度だけ数える。
- 宣言済み外部jobのclaim（byte、owner、lease directory、そのidentity、generation、state、receipt）。上限256。

volume全体の検査は、全ledgerのpending＋有効な外部claim＋和集合の観測使用量＋全metadata pool＋drain余裕がquotaを超えないこと、および実空き−未割当pool−全pending−drain余裕が`min_free`以上であることとする。自ledgerのpolicy検査は従来どおり残し、両方を満たすことを要求する。

**ledgerからの接続**（`DiskBudget(volume_coordinator=...)`）。`reserve`は、自ledgerのBEGIN IMMEDIATEの中で、自ledgerの検査の後・予約行の挿入の前にcoordinatorの`admit_ledger`を呼ぶ。coordinatorの更新はledgerのcommitより先にcommitされる。ledgerのcommitが失敗するとclaimは真の値より大きく残る（過大評価）が、小さくなることはない。減少（release、materialize、reconcile）はledgerのcommit後に公開する。各ledgerは単調なepochを持ち（transactionごとに加算）、公開はepochが新しい場合だけ上書きするので、遅れて届いた古い状態が新しい状態を消さない。書込み可能なopenはledgerの真の値でclaimを権威的に再公開する。bindは永続化する（`volume_binding` table）。bind済みledgerをcoordinatorなしで開く、または別のcoordinatorで開くと拒否し、未観測のまま過剰予約する抜け道を塞ぐ。

**外部job**は宣言制である。外部processが`declare_external(bytes, owner, lease_dir, roots)`でclaimを登録する。processは`lease_dir`にshared flockを保持し続け、claim専用のmarker fileをそこに作る。claimは検査を通った場合だけ入り、生存中は容量を占める。死亡後も、証明と回収までclaimは残る（PID／時刻では解放しない）。回収は次の手順である。

1. `plan_external_recovery`（dry run）：`lease_dir`のexclusive flockが取れること（誰も保持していない）、directory identity（device／inode）とmarker fileが一致することを確認し、claim全体を含む計画とそのSHA-256を返す。何も変更しない。
2. `apply_external_recovery(plan, expected_plan_sha256)`：digestを照合し、exclusive flockを再取得して再証明し、generation・state・byteの比較交換（CAS）でclaimを`RECOVERED`に更新し、receiptを保存する。計画後にclaimが変わった場合、所有者が生きている場合、directoryが置換された場合（inode番号が再利用されても、markerで別の実体と判定する）、lease directoryが無い場合、stateが未知の場合は拒否する。二重適用も拒否する。
3. `forget_recovered`で回収済み行を消す。

回収したclaimのbyteはpending creditから外れる。外部jobが書いたbyteは、宣言したrootの観測使用量として残る。宣言を超える消費は検出しない（検出のみの対象でもない）。

**cross-host**は調停しない。coordinatorは1つのhostの、node-localなvolumeだけを対象にする。filesystem種別を`/proc/self/mountinfo`（Linux）または`statfs`（macOS）から取得し、許可リスト（ext4、xfs、btrfs、tmpfs、overlay、zfs、f2fs、apfs、hfsなど）に無いもの（nfs、cifs／smb、lustre、fuse、virtiofs、9p、判定不能など）は、ファイルを作る前に拒否する。DBにhost名を束縛し、別のhost名で開くと拒否する（共有filesystemやコピーされたDBを推測で扱わない）。coordinatorがledgerのroot volumeと異なるdeviceの場合も拒否する。複数hostの合算はscheduler側の責務のままである。

### 既存機構の拡張を選んだ理由

- 別のdaemonやlock fileを作らず、既存のbounded native VFS上のSQLite、`DirectoryLease`のflock、dry-run／plan digest／CAS／receiptの形をそのまま使った。owner死亡の証明は既存と同じくkernelのexclusive leaseである。
- 同じledgerを明示共有する方式（共有policy）は、独立ledger間の合算をledger自体の統合に頼る。ledgerを統合できない独立job、ledgerを持たない外部jobには使えない。coordinatorはledgerを統合せず、claimの公開と検査だけを足す。
- 並列の機構を作らず、`DiskBudget`のreserve／transactionに1か所ずつ接続した。

### lock順序

`thread path lock → ledger BEGIN IMMEDIATE → coordinator thread lock → coordinator BEGIN IMMEDIATE`。coordinatorは葉であり、他のledgerを開かず、ロックせず、読まない（他ledgerの状態は公開済みclaimだけを見る）。catalog commitのtoken予約はledger→coordinatorの順で、catalog lockの内側でも逆順にならない。coordinatorを保持したまま別のledger transactionを開く経路は無い。

### 試験（tests/test_volume_coordinator.py、24件）

- 独立2 ledgerがvolume予算を共有する（片方のquotaが無くても合計超過をmutation前に拒否、行数不変、releaseの減少が相手に見える、物理化済みbyteを二重に数えない）。
- bind済みledgerの未bind open／別coordinatorでのopen、policy不一致、injected accountingとの併用を拒否する。
- 外部jobは実子processが宣言する。生存中は容量を占め、回収計画は所有者生存で拒否され、SIGKILL後も自動では解放されない。dry run→digest改ざん拒否→apply→receipt→二重適用拒否→解放後の予約成功。CAS不一致、生きた保持者、directory置換（inode再利用を含む）、lease directory欠損、未知stateを拒否する。resize／release／generation。
- 競合：実process 6個（ledger A×2、B×2、外部×2）が同時に10,000 byteずつ最後まで予約し、許可量（100,000 byte）ちょうどの10個だけが受理される。killされたrace processのtokenは残り、claimの合計と一致する。
- lock順序：catalog commitを行うledger Aと、予約・精算を繰り返すledger B×2の実processが150ステップを完走する（タイムアウトなし）。終了後に再openするとclaimがledgerの真値に再公開される。
- 死んだledgerのclaimは過大評価のまま残り（相手が予約を拒否される）、tokenを`reconcile`して書込み可能な再openで再公開されて初めて解放される。pendingが残るledgerの`forget_ledger`は拒否され、全ledgerの共有flockが無いこととpending 0が揃った後にだけ成功する。
- 不対応layout：allowlist外のfilesystem種別（nfs4、cifs、smbfs、lustre、fuse、virtiofs、9p、nonlocal、空、未知）はファイル作成前に拒否。判定不能も拒否。実probeはこのhostのローカルfilesystemを受理する。別host名のDB、別deviceのledgerを拒否する。

試験が明らかにした不具合：回収の同一性確認を(device, inode)だけにしていると、Linuxではrmdir／mkdirでinode番号が再利用され、置換されたdirectoryを同じものと誤認した（ent上の試験で検出）。claim専用のmarker fileを加え、置換を拒否するようにした。

### 限界（受入条件との関係）

- `DistributedConfig`への配線は未実装である。coordinatorは`DiskBudget(volume_coordinator=...)`のAPIとして利用する。`FileBlockStore`のprivate ledger、recovery-only open、offline recovery（`uncommitted_recovery`）はcoordinatorを扱わない。bind済みledgerの書込み可能openはcoordinatorを要求するため、offline recoveryでtokenを精算したledgerのclaimは、次の書込み可能openまで過大評価のまま残る（安全側）。
- 参加者がcoordinatorを使わなければ調停されない。bind済みledgerは迂回できないが、coordinatorを使わない独立job・外部jobは従来どおり物理空きでしか見えない。外部jobの宣言超過は検出しない。
- 外部jobが宣言したrootの外へ書いたbyteは、観測使用量には入らず、物理空きの観測（`min_free`）でしか見えない。
- crashで残ったclaimは、tokenの回収（既存のexplicit recovery）と書込み可能openを経て減る。coordinatorが自動で解放することはない。ledger数は64、外部claimは256、rootは256に制限する。
- 複数host、共有filesystem、電源断でのcoordinator DB自体の耐久性（rollback journalのsynchronous=FULLに依存し、O_DIRECT下の電源断模型は未検証）は対象外である。
- `strict_capacity_guarantee`は立てない（coordinator snapshotもFalse）。

## segment fsyncの費用の再現script（examples/p4_009_segment_sync_benchmark.py）

前節の費用測定を再現するscriptを追加した（14 qubit、block_bits 8、RAM 2 MiB、gateのlayerごとに`materialize`＋cache flush＋store flush）。「fsyncなし」は、このscriptの中で`FileBlockStore._sync_segments`を差し替えるだけで、本体にswitchは無い（fsyncなしはcrash安全ではない）。上の表の数値は別のworkload（3,840 extent）で得たもので、このscriptのlayer数は小さい。arm順を交互にして5〜6回のmedianを出す。Macでは、buffered 3.96 s対3.95 s（+0.4％）、direct（F_NOCACHE）8.03 s対6.87 s（+17％）だった。run間のばらつきが大きい（並行負荷で一度-53％が出たため、静かな環境で再実行した）。

### 検証（host調停の追加分）

- Mac：`uv run pytest -m "not slow"`で861 passed、69 skipped、2 failed（`test_layout_migration`のmpi4py欠如2件、本変更と無関係）。新規`test_volume_coordinator.py`は24件で、5回連続して成功した。
- ent Docker（`ddq-linux:dev-20261014`、`--hostname ddq-fixed`、同期先`~/ddq-p4009f`）：新規＋disk／recovery／catalog／segment関連20ファイルが、bufferedで312 passed、`SVSIM_TEST_BACKEND=direct`で311 passed、1 skipped。
- Linuxのfsync費用（上のscript、`--work-dir`はhostのext4）：buffered 30.7 s対29.7 s（+3.3％）。directはrunが38〜113 sとばらつき、判断できる増分を得られなかった（共有hostの負荷による）。
