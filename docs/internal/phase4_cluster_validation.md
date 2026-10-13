# Phase 4 クラスタ検証記録

2026-10-13。研究室固有の接続先・保存先・job IDを含む内部資料。公開時は本資料と結果JSON、参照リンク、Git履歴を非公開化または匿名化する。

Macの回帰試験後、利用者からクラスタ使用の許可を得て実施した。単一ノードと実2ノードで、worker I/O、Linux O_DIRECT、非同期MPI、S0/S2、node-local lifecycle、batch、三方式の勾配の数値一致を確認した。PSM2、OFI/PSM2、Ethernet/TCPで専用oracleが成功した。native io_uring、大規模化学計算、一般的なtile migration、運用ETAの自動校正はこの結果に含めない。

## 許可範囲と環境

- PBS workq、各ノード最大4 CPU・mem 8gb、各job walltime 10分、環境を含むlocal disk各8 GiB以下。
- 計算はPBS割当内だけで実行。CPUcluster008とCPUcluster009が割り当てられた。停止ノードの起動、NVMeのmount/format、管理者権限での導入は行っていない。
- 各ホストの `/var/tmp/ddq-20009/phase4-dfdd4a403586` を使用。root filesystemは `/dev/sda4` のext4。コード、Python、venv、cache、cwd、TMPDIR、振幅、catalog、checkpoint、結果をlocalに配置した。
- Linux用uv 0.13.0・Python 3.12.13、numpy 2.5.4、mpi4py 4.1.2、pytest 9.1.1。`uv sync --locked --extra mpi --no-install-project --no-build --link-mode copy`、実行は `uv run --offline --no-sync --extra mpi`。
- Open MPI 5.0.6は `/opt/openmpi/gcc/8.5.0/5.0.6` のlocal root上。PRRTEのPBS/TM連携で起動した。OMP/OPENBLAS/MKLのthread数は1。rank数は単一ノード1/2/4、2ノードは各1または2 rank。`--bind-to none`であり、NUMA性能校正ではない。
- Linux実行image約69 MiBをログインノードのlocal diskからSSH転送し、checksum検査して展開した。共有 `/home` へのコード・環境・状態・結果の大量I/Oは行っていない。SSHの認証設定などsite既存の制御ファイル参照は別扱い。PBS stdout/stderrもログインノードのlocal保存先を指定した。

`mpi-selector --query` はログインノード・計算ノードとも `openmpi-5.0.6-gcc64-8.5.0`、level=userだった。selectorやshell profileの変更は不要だった。一方、計算ノードでMPIをsingleton初期化するとPSM2のPMIx transport key取得に失敗した。MPIをimportするpytestも `mpiexec -n 1` 経由にして解消した。

## PBS実行履歴

job IDのsuffixはすべて `.CPUcluster-PRISM`。時間は `qstat -fx` の `resources_used.walltime`、終了後の履歴で確認した。通常の `qstat` には短時間で終了したjobが表示されない。

| job ID | 配置 | walltime | exit | 結果 |
|---|---|---:|---:|---|
| 265084 | 008、4 CPU | 5秒 | 14 | pytestのsingleton MPI初期化でPSM2エラー |
| 265085 | 008、4 CPU | 6秒 | 0 | 起動診断。singleton失敗、mpiexec起動成功を確認 |
| 265086 | 008、4 CPU | 91秒 | 0 | 単体28件、1/2/4 rank、O_DIRECT、lifecycle、bounded state、専用oracle成功 |
| 265087 | 008+009、各4 CPU | 29秒 | 1 | 2 rankのS0/S2・buffered/direct成功後、v3共有tree前提の移行fixtureで停止 |
| 265088 | 008+009、各4 CPU | 128秒 | 1 | 2/4 rank全oracle・v4 lifecycle・bounded state・PSM2/OFI成功。最後のTCPでNIC名不一致 |
| 265089 | 008+009、各4 CPU | 25秒 | 0 | NIC指定修正後、Ethernet/TCP 4 rank成功 |

265088全体は成功扱いにせず、成功済みcaseと失敗caseを分けて記録した。最後のEthernet修正では、008のNICが `ens833f0`、009が `ens802f0` と異なるため、`btl_tcp_if_include` をNIC名からEthernet subnet指定に変更した。両者のlink speedは10000 Mb/s。TCP指定は `pml=ob1,btl=tcp,self,sm`、高速fabricは `pml=cm,mtl=psm2` または `mtl=ofi,mtl_ofi_provider_include=psm2`。物理link速度を測定帯域として扱わない。

v3移行fixtureは同一hostの同一local volumeに限定した。別hostのlocal disk間で旧共有treeを移行するadapterは未実装であり、offlineコピーが必要。cross-hostでもv4のprefix/remote lease/GC、COMMIT後のcopy failureと再開、missing copy修復、prepareと最新payloadの破損拒否の検証は実行した。このfixture修正後、Macのlifecycle回帰9件も成功した。

## 数値・資源・通信結果

専用oracleは6 qubitのrandom initial state、open/closed control、shared parameter expression、Pauli rotation、重なるphysical swap、v4 checkpoint/resume、parameter batchを検査する。期待値は **-7.773656774934246e-05**、NumPy oracleとの差は約 **1.3e-16**。三方式の勾配は **0.0816234486811098**、有限差分との差は約 **2.1e-11**。通常の回路oracleでは複数partnerの同時MPI requestも検査した。

| 保存結果 | 配置・経路 | 専用oracle秒（rank MAX） | 最大managed MiB/rank | 最大RSS MiB/rank | 1 MiB転送の中央値 ms |
|---|---|---:|---:|---:|---:|
| [single_check.json](phase4_cluster_results/single_check.json) | 1 node / 4 rank / PSM2 | 21.244 | 1.636 | 72.902 | 0.662 |
| [psm2-2.json](phase4_cluster_results/psm2-2.json) | 2 node / 2 rank / PSM2 | 10.844 | 2.737 | 85.188 | 0.690 |
| [psm2-4.json](phase4_cluster_results/psm2-4.json) | 2 node / 4 rank / PSM2 | 13.640 | 1.636 | 87.469 | 0.640 |
| [ofi-4.json](phase4_cluster_results/ofi-4.json) | 2 node / 4 rank / OFI/PSM2 | 13.180 | 1.636 | 90.523 | 0.805 |
| [ethernet-4.json](phase4_cluster_results/ethernet-4.json) | 2 node / 4 rank / TCP | 16.870 | 1.636 | 62.430 | 2.962 |

専用oracleのmanaged budgetは8 MiB/rank。別のbounded caseは4 rankで16 qubit、1 rankの状態256 KiBに対しmanaged budget192 KiB、b=6とし、global gatherなしで4つの解析的期待値に一致した。mixed operationを含む短いcorrectness caseであり、専用oracle秒からscalingや量子化学実行時間を推定しない。1 MiB転送は3回のrank MAXの中央値で、matched cold/warm条件や他job負荷を制御したNIC帯域校正ではない。

全job終了後の環境・全attempt合計のlocal使用量は008で430,783,759 bytes、009で371,903,995 bytes。空きはそれぞれ380,295,041,024 bytesと381,429,288,960 bytesで、100 GiBの空き下限を維持した。PBSの `resources_used.mem` はrank群のRSS合算を保証しないため、JSONのprocess RSSと区別する。local artifactは結果再確認用に残しており、自動cleanup寿命は未保証。以後の利用時は再確認する。

## 再現性と残る検証

- base source manifest hash: `dfdd4a4035867625869d918713fc7a8c4db8c0aaa96617c45c46ff1507dd05bd`。既存commit 8658ef6上の未commit実装を含む配布内容のhashであり、Git commit IDではない。
- Linux runtime archive SHA256: `82b47ddd376af24fb797bc6d47c8ca9c5c2243c7ab294da6fd4766b0faca67c4`。
- 専用worker SHA256: `4f9a97459651e60e807c6877a2bf9bdd61a78f69a7684ae908f76f2c93418fc4`。
- v3 fixtureの同一host限定修正はchecksum付きoverlayとして配布。SHA256: `9ecc2cd2f1ade4acdbf5ef1f1c2141669b8faa21173ad622d471f413d48f4f95`。

liburing header/libraryは既存環境に無く、`native_liburing_found=false`。新規導入は行っていないのでnative CQE/cancel/fsyncのLinux実機検証は未実施。Macのfake CQE試験をnative実測に数えない。一般tile route、別volumeへのv3 dataset移行、自動mixed-load校正・残DAG ETA、媒体喪失からの復旧は[実装資料](../phase4_implementation.md)の境界を維持する。
