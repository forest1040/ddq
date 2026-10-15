# P4-009 disk faultマトリクスの実装（quota境界掃引とprocess kill）

2026-10-15、writer faultの実試験を2ファイル追加した。実装コードの変更はなく、既存の予約・native VFS・回収の挙動を実processと実SQLiteで確認した。P4-009はIN_PROGRESSのままである。

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

## 残り

unlinked／uncommitted reservationの回収、temporary branch GC、別ledger／外部jobとのhost調停、extent writeのkernel／電源断fault、segment fsync前後のkill、prepare／decision／decision-copyのkill（実process、複数rank）、Linux O_DIRECT下の全matrix、ENOSPC実測は未完了である。SQLite内部のfsync中killはSIGKILL掃引で確率的に確認するだけで、決定的なstage注入ではない。
