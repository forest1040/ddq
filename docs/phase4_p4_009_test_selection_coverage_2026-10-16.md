# P4-009：高速版の除外と必須受入の対応

2026-10-16、監査基点`90b511b`（runtime coreは`9eb426f`）。[009A〜D](phase4_p4_009_acceptance_2026-10-16.md)は変更しない。新jobや重いtestは本監査では実行していない。

## Macの38 skip

[固定Macログ](internal/phase4_wave3_validation/mac-fast-final.txt)は1317 passed／38 skipped／83 deselected。`pytest --collect-only -m 'not slow'`の順序とログの1355個の結果記号を照合した。現基点で増えたcampaign incomplete-CI guard1件を除くと件数が一致する。これは省略されたskip理由を想像で埋めず、test定義のskip条件へ対応付けた分類である。

| skip群 | 件数 | 009受入への影響と別証拠 |
|---|---:|---|
| `test_disk_real_enospc` | 15 | Macにsize制限mountが無く未実行。009B/Cの実容量不足は必須。[既実施](phase4_disk_fault_matrix_implementation.md#実enospcteststest_disk_real_enoscpy)に加え、最終Linux runnerの専用tmpfs・buffered/direct各15条件で再確認する。 |
| `test_checkpoint_stage_kills_mpi`のENOSPC版 | 6 | 同じFULL未設定。実在はprepare/copy各rankとdecision rank0の5条件、decision rank1は非適用。最終6stage runnerには含まれないため、下の追加2nodeidsへ残す。 |
| native io_uring単体6＋MPI6 | 12 | `DDQ_NATIVE_URING=1` opt-inなし。native **SQLite VFS** skipではない。P4-010の別機能で、[過去のnative12 pass](internal/ent_linux_validation/README.md)と[後続checkpoint/native8 pass](internal/phase4_checkpoint_recovery_validation/README.md)を区別する。009のworker/O_DIRECT＋SQLite guard受入を、io_uringを有効化しなかったことだけで欠落としない。最新native uring全matrixを実行したとの主張もしない。 |
| Linux専用：catalog別volume、startup inode再利用、private別volume、transport O_DIRECT、buffer alignment | 5 | Macでは実行不能。最終Linux contracts/physical stageとO_DIRECT fd/alignment/write/fsync/readback/birth probeへ対応する。inode probeの例外は下記。 |

計38。MPI launcher欠落、SQLite guard unavailable、birth取得不能をこの分類で暗黙に許可しない。

## 83 slow除外

[selection定義](../tests/conftest.py)はfastをpre-commit subset、full suiteをacceptanceと明記する。slow label自体を受入免除としない。次はcollect-onlyで得た83件の全内訳であり、旧sourceの成功と最新sourceの代表回帰を区別する。

| 除外群 | 件数 | 補完と判定 |
|---|---:|---|
| checkpoint全kill matrix | 30 | 009C必須。prepare/decision/copy×5位置×2victim、実在25＋decision/rank1非適用5。[旧実施25 pass/5 skip](phase4_disk_fault_matrix_implementation.md#複数rank-checkpoint-killteststest_checkpoint_stage_kills_mpipy)はある。最新Mac fastの代表4 kill、Linuxのsegment/VFS等は全25条件を再実行した証拠ではない。下の追加実行へ残す。 |
| coordinator5、checkpoint collective recovery4、phase3 MPI2、lifecycle2 | 13 | 009A/C/D。fastは代表rank/caseを保持。[過去checkpoint/recovery全matrix](internal/phase4_recovery_pool_validation/README.md)、[coordinator Linux buffered/direct](internal/p4_coordinator_followup_validation/README.md)、最新shared/private/runtime/fork/readerと実別hostはそれぞれ別証拠。rank展開すべての最新source再実行とは記さない。 |
| async phase4 MPI policy/backend/rank、large-file | 12 | 009D。Mac fastの残存S0/S2とbounded代表、[既存Linux remaining/retry](internal/ent_linux_validation/README.md)の大状態・複数rank、最新async/segment/RAM/tileのoracleで経路を補完。Mac direct展開はF_NOCACHEで、Linux O_DIRECTはphysical stage/probeで別確認。 |
| registered root257件拒否 | 1 | 009A。同じ上限のfast `test_root_limit_is_global_across_clients_and_constructor`が256根から追加／constructor257根を拒否。除外されたnonoverlapping版も過去Linux JUnitに成功記録がある。 |
| large stored budget1、stored numerics6、tile2、Phase2 random3 | 12 | 009Dのサイズ/seed追加。fastの小さい同経路、最新slot admission/tile/RAM flush、[旧Linux remaining JUnit](internal/ent_linux_validation/remaining/remaining.xml)の実サイズoracleで補完。性能改善の証拠にはしない。 |
| followup campaign5、remaining-work MPI2、benchmark1、Phase5 timing1 | 9 | 主にP4-001/002等のcost/campaign受入。009は既存writer利用経路をfast/runtime継承監査で確認する。旧Linux campaign/remaining記録はあるが、最新ETA性能の完了へ転用しない。 |
| adaptive replan5、layout migration campaign1 | 6 | 主にP4-003/004。009Dの既存store/budget継承はfast replan fault/kill、migration/source-bound clone回収、数値oracleで確認。campaignの性能・baseline認証は未実施を消さない。 |

## Linux finalrunnerと追加必須差分

最終runner `examples/linux_phase4_lifecycle.py`は`-m not slow`を使わず、指定した6stage内の全testを実行する。native **SQLite** fault13条件、catalog/birth/bootstrap/inheritance/reader/sidecar/append/migration、private/GC/segment/quotaのbuffered/direct、実tmpfs ENOSPC15×2を担当する。これは全pytest suiteの再実行ではない。主担当が保存する最終stage log/JUnit/source manifestを成功判定の根拠とし、本監査時点の実行中stageを合格と書かない。

唯一の許可skip名は`test_reused_linux_inode_is_distinguished_by_birth_identity`。理由を区別する必要がある。512回の有限probeでinode再利用が**起きなかった**場合、通常のbirth/CAS/置換回帰は成功しても実再利用の証拠は未到達として残す。再利用したのに**birth分解能で新旧を区別できなかった**場合は、当該FSでそのidentity証明が成立しなかった観測であり、単なる未到達として安全性合格に数えない。runnerは名前によるallowlistなので、最終受入で理由を必ず読む。他のbirth/native/FULL/MPI未提供skipは不合格。

最新sourceの009Cを閉じる追加対象は、`tests/test_checkpoint_stage_kills_mpi.py::test_full_kill_matrix`と`::test_external_writer_fills_the_disk_after_admission_in_a_checkpoint_stage`。slow除外を付けずbuffered/direct各1回、専用96 MiB tmpfsの`SVSIM_FULL_FS=/full`、実2rank、非root/4CPU/8GiB/network-noneで直列実行する。既存6stageとは別containerとし、同じmountを並行して埋めない。各backendは36 collected＝30実在pass＋6非適用skipが期待値で、許可理由は厳密に`only rank 0 writes the global decision`のみ。case内run/recoverは各300秒timeout、外側有限deadlineも必要である。本監査では未実行、主担当が追加実行を進める。

実行用に[run_phase4_checkpoint_matrix.sh](../examples/run_phase4_checkpoint_matrix.sh)と[container helper](../examples/linux_phase4_checkpoint_matrix.py)を準備した。source root／空のresults／revisionを引数に取り、固定imageをimmutable IDで起動、3 source dirsだけread-only、backend別scratch、BLAS1、各backend1200秒（host側1230秒＋TERM後15秒）の上限を付ける。image ID、source全file hash、command、state/status、log/JUnitを保存する。36個のtest名集合、30成功、6個の正確な非適用名と理由を照合するため、MPI/FULL欠如やskipへの置換は合格にならない。ここでは`bash -n`とPython ASTの静的検査のみで、Docker/testの動的実行は行っていない。

実別hostの4rank/8caseと最終Linux追加結果は主担当の証拠へ照合する。skip数の少なさ、fast pass総数、過去native実績だけからP4-009をDONEにはしない。
