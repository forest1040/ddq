# P4-009：ledger commitの集約（append／catalog lease）

更新日：2026-10-14。P4-009の一部である。[Phase 5の時間計測](phase5_implementation.md)で、Linuxの実行時間の大半をdisk予約ledgerのdurable commitが占めていると分かった。そのcommit回数を、予約契約を変えずに減らす。P4-009とPhase 4はIN_PROGRESSのまま変えない。

## 背景

ent Docker（NVMe ext4）で、RAM storeの8Q・H×12（192 pass）を1 trial実行し、`sql_profile.py`でSQLiteの全execute／commitを計測した。SQLite時間17.2秒のうち11.7秒がledgerのcommit（2,452回）で、catalogのcommit（858回）は3.7秒だった。振幅extentの予約はない。trace 1行ごとにreserve・materialize・releaseの3回、journal 1行ごとに2回、catalog transactionごとに2回、それぞれ`synchronous=FULL`・`journal_mode=DELETE`のledger transactionを実行していた。装置のfsyncは約0.8 msまたは約5 msで、この回数がそのまま時間と変動になる（[Phase 5検証記録](internal/phase5_timing_validation/README.md)）。

## 変更

契約は「書き込む全byteを、書き込む前にlive tokenで覆う」で、これは変えない。変えるのはtokenの粒度だけである。

| writer | 以前 | 現在 |
|---|---|---|
| trace追記 | 1行ごとにreserve＋materialize＋release | `AppendReservation`：64 KiBのWRITING tokenを1回取り、使用量をlocalで数える。不足時に解放・再予約する |
| journal追記 | 1行ごとにreserve＋release | 同上（operation=`journal-append`） |
| catalog transaction | 毎回envelopeをreserve＋release | `CatalogLedgerLease`：1つのtokenを複数transactionで再利用する |

- **appendの溢れと例外。** 1行がchunkを超える場合は、その行のサイズで予約する。quotaやdrain marginで64 KiBが拒否されたら、正確な行サイズで再試行する。それも拒否されれば、1 byteも書かずに失敗する。書込みが失敗してtruncateで巻き戻せた場合は、tokenを保持して再利用する。巻き戻しに失敗した場合は、tokenを解放せずに手放し（retain）、traceをpoisonする。従来の「retained WRITING token」と同じ扱いである。
- **catalogの累積成長。** main DBの成長は累積するので、token取得時のDBサイズを記録する。`token bytes −（現在サイズ − 取得時サイズ）`が次transactionのenvelope（main成長上限＋side／journal）を覆う間は、ledgerに触れずに再利用する。覆えなくなったら、envelope＋1 MiBの余裕で再予約する。transactionごとのVFS main／side capとpage capは従来どおり毎回設定する。`admission_status()`の`reservation_count`はenvelope数、`ledger_reservation_count`はledger予約数である。
- **解放。** writer（trace／journal／catalog）のclose時に解放する。`DiskBudget.close()`は、自分に登録されたleaseを先に解放する。これでRAM store差し替えのようにbudgetが先に閉じる経路でも、live tokenを残さない。閉じたbudgetへの解放はno-opである。
- **会計。** 書いたbyteはobserved usageに入る一方、chunk全体はpendingのまま残る。過大計上はwriterごとに最大1 tokenで、安全側である。`DiskBudget.lease_reserved_bytes`と`snapshot()['lease_reserved_bytes']`で、lease由来のpendingを他と区別できる。
- **crash。** 所有processがcloseせずに消えると、writerごとにWRITING token（`trace-append`／`journal-append`／`catalog-transaction`）が1つ残る。従来もtransaction中やretained tokenで同じ状態は起き得た。精算は既存の明示recovery（`reconcile`）で行う。lease expiryで自動解放はしない。

## 検証

- Mac：全回帰は821 passed／14 skipped。ent Dockerでは、選択patternがほぼ全testに一致した回帰が812 passed／14 skipped（23分）。lease導入で意図的に変わった前提を持つtestを更新した：(1) 実行中の`reserved_bytes==0`を「lease以外のlive tokenなし」へ、(2) quotaを途中で下げた場合に、既に予約したchunk／envelope内の書込みは許し、超える最初の書込みをmutation前に拒否する、(3) 偽ledgerでextent tokenだけを数える。新規testは次を確認する：chunk共有と再予約回数、close後にlive tokenなし、crash時にWRITING token 1件、quota際のfallbackと拒否、budget closeでの解放、catalog token再利用と累積成長での再予約、実子processがcloseせず終了した後の3種token残存と一回限りの明示精算。
- ent Docker：同じcaseで、ledger commitは2,452回から16回、SQLite時間は7.6秒から2.6秒になった。速い装置状態で比べると、executeは8.2〜9.5秒から2.74秒、trialは約3倍速くなった。残るのはcatalogのdurable commit（858回）で、データ自体の永続化契約に属する。計測scriptは[内部記録](internal/phase4_ledger_lease_validation/)に置く。

## 残件

書込み途中のSIGKILL・電源断に近いfault（現在はclose無しのprocess終了→残存token→明示精算までをtestで確認）、native io_uring matrixでの再確認、catalog commit自体の集約（epoch単位のtransaction化など、durability cutの設計を伴う）が残る。P4-009のその他の受入条件（全writerのcut結合、host調停）も変わらない。
