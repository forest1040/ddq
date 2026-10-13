# 研究室クラスタ調査と実行計画（内部資料）

- 調査日：2026-10-13（JST）。容量・ノード状態は調査時点のスナップショット。
- 対象：`ssh lg` で接続する研究室クラスタ、PBS Professional 20.0.0。
- 状態：**計画のみ。ジョブ投入、計算負荷をかける測定、環境導入、mount、ノード起動は実施していない。**
- 取扱い：接続先、構成、運用情報を含む内部資料。将来リポジトリを共有する際は、非公開化または匿名化を判断する。公開資料の一般的な構成例とは分離する。

## 1. 方針

振幅データ、SQLite catalog、journal、大きな gate asset、作業用 checkpoint、コードと Python 環境は計算ノードのローカルディスクに置く。**他 rank の管理情報もまず MPI で交換し、共有ディスクを通常実行・再開の必須条件にしない。**

小さな manifest／結果要約を共有ディスクへ保存する方式は任意の補助とする。採用する場合も書込みは commit 時に限定し、合計 64 MiB／job を初期上限としてサイズを検査する。上限超過時に振幅・catalog を共有ディスクへ逃がすことはしない。共有ディスクの空きと quota の確認前はこの補助を使わない。

以下の段階は将来実施する計画であり、今回はジョブを投入しない。停止ノードを起動せず、利用可能なノードだけを対象にする。

## 2. ノードとスケジューラ

`pbsnodes -aS` の登録は 44 ノード。詳細の `resources_assigned` も合わせて確認した。

| 調査時の状態 | ノード | 数 |
|---|---|---:|
| free | CPUcluster008〜012、042〜044 | 8 |
| job-busy | CPUcluster041 | 1 |
| offline | CPUcluster007、039、040 | 3 |
| state-unknown/down/offline | CPUcluster001〜006、013〜038 | 32 |

`free` は全資源が空いている意味ではない。008、009、011 は各 80 CPU／110 GiB、010 は 88 CPU／142 GiB が割当済みだった。012、042、043、044 は CPU・メモリの割当が 0。実メモリの空きが多くても、他 job の PBS 予約を使うことはできない。

| queue | 既定 mem | 調査時の設定・用途 |
|---|---:|---|
| workq | 350gb | enabled／started、通常メモリノード。default queue |
| large | 1516gb | enabled／started、043／044 が所属する大容量ノード |

全調査対象ノードの PBS CPU 数は 96。通常ノードの `resources_available.mem` は 350gb、大容量ノードは 1516gb。`pbsnodes -aS` の大容量ノードの `1tb` 表示は丸められるため、詳細値を使う。sharing は `default_shared`。

将来の job は `select` の各 chunk に `ncpus`、`mpiprocs`、`mem` を明示し、walltime も設定する。queue の walltime 上限、ローカルディスク利用規約、host 固定指定の扱いは未確認。起動時に割当ノードと各 host の合計予算を再検査する。queue の既定 mem と OS の MemTotal を同一視しない。

## 3. メモリ、CPU、NUMA

008〜012、042〜044 に SSH で接続し、`/proc/meminfo`、`df`、`lsblk`、`findmnt` 等を読み取った。以下は GiB（2^30 byte）。MemAvailable は動的な OS の値で、PBS 上の予約可能量ではない。

| ノード | OS MemTotal | OS MemAvailable | PBS mem | root filesystem の空き |
|---|---:|---:|---:|---:|
| CPUcluster008 | 376.6 | 372.3 | 350gb | 354.6 |
| CPUcluster009 | 376.6 | 372.1 | 350gb | 355.6 |
| CPUcluster010 | 376.6 | 367.4 | 350gb | 356.5 |
| CPUcluster011 | 376.6 | 372.4 | 350gb | 357.0 |
| CPUcluster012 | 376.6 | 372.5 | 350gb | 364.2 |
| CPUcluster042 | 376.6 | 372.0 | 350gb | 361.4 |
| CPUcluster043 | 1510.6 | 1500.8 | 1516gb | 362.9 |
| CPUcluster044 | 1510.6 | 1501.0 | 1516gb | 362.8 |

CPU の詳細を確認した 009／043／044 は Intel Xeon Platinum 9242 @ 2.30 GHz、2 socket × 48 core、96 logical CPU、1 thread/core、4 NUMA domain。009 の CPU 配置は 0〜23、24〜47、48〜71、72〜95、各 domain のメモリは約 94 GiB。その他のノードについて同一 CPU 型番までは確認していない。

初期測定は 1 rank／node から始め、1 node の 2／4 rank と比較する。各 NUMA domain に rank を配置する場合は domain 内のメモリ容量にも収まる予算を使う。1 rank で 256 GiB 等を確保する実験は複数 domain を使うため、単一 domain にメモリを固定しない。PBS の CPU 割当・cpuset と MPI binding を優先し、thread 数を抑えてから増やす。

`managed RAM` は process RSS と異なる。NumPy、MPI 内部 buffer、Python metadata、SQLite、OS cache、staging と出力 buffer を別に観測し、swap は計算用メモリに数えない。

## 4. ローカルディスクと共有ディスク

| 場所・デバイス | 実体 | 判断 |
|---|---|---|
| 計算ノード `/` | `/dev/sda4`、ext4 | ローカル SATA SSD。ただし OS と共用 |
| 計算ノード `/tmp`、`/var/tmp` | root と同じ ext4 | ローカル。tmpfs／共有 `/home` ではない |
| 計算ノード `/home` | ログインノードが export する NFS | 共有。通常実行のデータ置場にしない |
| ログインノード `/home` | `/dev/sda1`、XFS | compute 側からは NFS となる共有元 |
| 043／044 `/dev/nvme0n1` | TOSHIBA KXG60ZNV1T02、ext4、未マウント | 専用 scratch 候補。現状の利用可能容量には算入しない |
| `/opt/intel`、`/opt/singularity` | NFS mount | 環境配布元でも共有 I/O が発生する。大きな作業領域にしない |

調査対象の SATA SSD は Intel SSDSCKKB48、480,103,981,056 byte（447.1 GiB）、回転なし。root の filesystem 容量は約 418.5 GiB、空きは上表の値。専用 `/scratch`、`/local`、`/work`、`/data` は今回確認できなかった。009 では `/local_scratch` も存在しなかった。

043／044 の NVMe は 1,024,209,543,168 byte（953.9 GiB）。`lsblk` ではディスク全体に ext4 が表示され、mount point は空欄。既存データの有無、利用者、mount 方針、filesystem 内の空きは不明である。**format・mount はせず、管理者が利用可とした mount point と権限が確認できてから計画の容量を更新する。**

共有 `/home` は総容量約 49.1 TiB、空き約 129.9 GiB、使用率表示は 100%。表示の丸めがあるため空き 0 ではないが、大きな新規データを書ける余裕とは扱わない。個人 quota は今回確認できなかった。ログインノードの root 空きは約 135.1 GiB、RAM は約 187.6 GiBだが、計算資源として使わない。

ローカル作業領域案は `/var/tmp/ddq/<uid>/<job-id>/`。最終的な許可された場所は管理者と確認する。private directory を用い、job ごとに一意にし、既存ディレクトリを上書きしない。`/tmp` は tmpfiles の age 10 日、`/var/tmp` は 30 日という削除設定がある。永続保存期間の保証ではなく、再起動・運用削除も含め checkpoint の保存寿命を確認する。

root 共用 SSD は最初 8 GiB／job、校正段階で 64 GiB／node を上限とする。容量拡大は 128 GiB、最大 200 GiB／node を暫定上限にし、**root 空き 100 GiB を常に残す**。ソフトウェア環境・metadata・全 rank のデータ・失敗残骸を合算し、起動前と実行中に確認する。PBS の CPU／メモリ予約は disk 容量の予約ではない。停止時の flush 用余裕を先に確保し、空きを使い切ってから止めない。

## 5. ネットワークと MPI

009 の PCI 情報では Intel Omni-Path HFI Silicon 100 Series、driver `hfi1`。調査した 8 ノードの HFI port は ACTIVE、100 Gb/sec と表示された。009 の Ethernet は Intel X540-AT2、`ens802f0` が 10 Gb/s で link up。009 の `ib0` も up だが、これを Ethernet 比較の経路として使わない。

このクラスタの高速網は **Omni-Path 100** として計画する。汎用構成例にある InfiniBand 200 Gb/s 等の仮定値を転用しない。100 Gb/s の物理上限は 12.5 GB/s、10 Gb/s は 1.25 GB/s。どちらも protocol／packing／MPI／競合を含む実効速度ではない。

インストール済み Open MPI は `/opt/openmpi/gcc/8.5.0/5.0.6`、5.0.6。`ompi_info` では PML `cm`／`ob1`、MTL `psm2`／`ofi`、BTL `tcp`／`self`／`sm` が存在した。UCX PML は一覧になかった。PRRTE 3.0.7 の `prte_info` に `ras: pbs` と `plm: tm` があり、PBS 対応コンポーネントは確認できた。ただし PBS 割当内での起動はまだ検証していない。

将来の比較条件は以下とする。component が存在することと実際にその経路で通信できることは分け、起動時の選択ログと peer 測定で確認する。

| 条件 | MPI 設定候補 | 確認すること |
|---|---|---|
| Omni-Path | `--mca pml cm --mca mtl psm2` | HFI／PSM2 経路、rank placement、実効速度 |
| OFI 比較 | `--mca pml cm --mca mtl ofi --mca mtl_ofi_provider_include psm2` | provider が psm2、TCP に落ちていないこと |
| Ethernet | `--mca pml ob1 --mca btl tcp,self,sm --mca btl_tcp_if_include ens802f0` | 全割当 node の interface 名・link、10 Gb/s の経路 |

OFI／PSM2 の関係は [Open MPI 5.0 公式資料](https://docs.open-mpi.org/en/v5.0.0/tuning-apps/networking/ofi.html)、PBS 起動の確認方法は [Open MPI 5.0 公式資料](https://docs.open-mpi.org/en/v5.0.x/launching-apps/tm.html)を参照する。PBS 連携を使い、割当外の host を MPI に指定しない。`PBS_NODEFILE` は変更しない。

## 6. uv 環境とローカル配置

既定 `python3` は共有 `/home` の Miniconda Python 3.9.12。プロジェクトの Python >=3.12 条件を満たさない。`uv` は調査時の PATH に見つからなかった。OS kernel は 4.18.0-477.10.1.el8_8、glibc 2.28。

将来の導入では、コードの確定 revision と `uv.lock`、uv、Python 3.12、必要 wheel をローカル環境で準備し、各 node のローカルディスクへ明示的に配布する。原則として共有 `/home` の `.venv` や package cache を実行時に参照しない。ローカル `UV_CACHE_DIR`、`UV_PYTHON_INSTALL_DIR`、`.venv` を用い、`uv sync --locked --extra mpi` のオフライン再現性を確認する。

MPI は 5.0.6 に統一し、mpi4py が同じ MPI ABI／libmpi を使うことを `MPI.Get_library_version()` で確認する。必要なら site の MPI で mpi4py を build する。MPI 実体と依存 library の mount 元も確認し、共有 mount にある場合は管理者の配布手段または互換性を確認したローカル配布を使う。別の Intel MPI と Open MPI を混ぜない。ローカル環境の転送・展開・cache も SSD 上限に含める。

Linux `O_DIRECT` は小さな correctness job で alignment／read-write／fsync を確認する。Phase 4 は未実装であり、この kernel で io_uring を利用可能とは確認していない。初期案は同期 I/O、後続は worker I/O とし、io_uring は機能検査に成功した場合だけ追加する。

## 7. 複数ノードの保存方式：通信を基本にする改修

現在の [distributed.py](../../src/svsim/distributed.py) は `directory/rank-NNNN` に rank-local store を作り、振幅は owner rank から MPI で取得する。同じ `/var/tmp/...` という文字列でも別 host の filesystem は別物である。

[distributed_checkpoint.py](../../src/svsim/distributed_checkpoint.py) の `load_global()` は、その directory の global manifest と**全 rank の prepare ファイルを直接開く**。`examples/phase3.py` も checkpoint／resume を実行する。このため、単に保存先を node-local に変えて複数ノードで既存デモを動かす計画にはしない。Luna に読み取りレビューを委譲し、resume／fork／GC の依存も確認した。

| 対象 | 改修案と検証条件 |
|---|---|
| 配置情報 | store-id、owner-rank、host／volume-id、local-path を記録。path は owner 側で解決し、他 host が直接 open しない |
| prepare／commit | 各 rank がローカル durable prepare を書き、hash と必要な小さな管理情報を MPI で交換。全 rank の成功を合意後、global commit を配布し、各 node に保存する |
| resume | communicator を作ってから各 rank が自分の prepare／journal／extent を検査し、collective vote と manifest hash の一致を確認。root のローカルファイルだけに依存しない |
| fork／prefix | catalog のコピーはローカル、immutable extent の読み取りは source owner との通信。共有 prefix の lease を collective に管理する |
| GC | 全 rank の committed root と lease を合意し、遠隔 host にしかない参照を見落とさない。承認した集合に対して owner がローカル extent を回収する |
| 再配置 | 同じ rank 数・配置での再開を先に検証。別 host への移行は SSH／MPI 等の明示転送、checksum、容量予約を伴う別工程。rank 数変更は現状未対応 |

大きな IR／asset／manifest は「metadata」という理由だけで共有へ置かない。必要な owner への通信・分割転送と local 保存にする。通信する管理情報にもサイズ上限と backpressure を設ける。

任意の共有 manifest mirror はこの改修の代替ではない。小さい管理ファイルだけ共有にしても、大きな catalog／extent の場所と参照寿命を解決する必要がある。symlink の付け替えで既存 directory 全体を共有とローカルに分割する運用は採用しない。

node-local checkpoint は、全必要 shard が残っている間の process 再起動に使える。**node／SSD が失われた場合の復旧には shard の別 node への明示複製が必要**であり、manifest の複製だけでは復旧できない。最初は node 障害時は再計算とし、複製を追加する場合は通信量と disk 予算を別途見積もる。共有ディスクには振幅を退避しない。

## 8. 容量見積もりと構成候補

complex128 状態のサイズは `S(n) = 16 × 2^n byte`。以下は**容量だけの候補**であり、現実装の計算時間・metadata 数・回路の更新量を保証しない。QPE の qubit 数には逐次位相推定の ancilla 等も含める。

RAM の仮予算は、均等分割された node 当たり状態 `Snode` に対して forward `1.25 × (2 Snode + 16 GiB)`、adjoint `1.25 × (4 Snode + 16 GiB)`。複数 rank の予算は host 合計で評価し、16 GiB は host ごとの暫定 overhead。偏った ownership、large gate asset、増える metadata は実測で加算する。

| 候補 | PBS mem／node の計画値 | forward 候補 | adjoint 候補 | node 当たり推定ピーク |
|---|---:|---|---|---:|
| 通常 node ×1、RAM 振幅 | 256gb | 32Q、状態 64 GiB | 31Q、状態 32 GiB | いずれも 180 GiB |
| 大容量 node ×1、RAM 振幅 | 1280gb | 34Q、状態 256 GiB | 33Q、状態 128 GiB | いずれも 660 GiB |
| 通常 node ×2、通信＋RAM | 各 256gb | 33Q、総状態 128 GiB | 32Q、総状態 64 GiB | 各 180 GiB |
| 大容量 node ×2、通信＋RAM | 各 1280gb | 35Q、総状態 512 GiB | 34Q、総状態 256 GiB | 各 660 GiB |

大容量 node の PBS 表示 1516gb は OS MemTotal 約1510.6 GiBより大きいため、上限いっぱいの申請はしない。RAM 振幅でも現実装の durable snapshot は振幅を file に出す可能性があり、上表の大容量状態を root SSD に checkpoint できるとは限らない。通常 32Q 状態だけでも 64 GiB、大容量 34Q は 256 GiB。最初の大容量 RAM 試験は非永続・短い実行を前提にし、checkpoint を使う場合は別の disk admission とライフサイクル対応が必要である。

file store は append-only 更新のため `Snode` だけを確保しない。計画式を `Dnode = 1.25 × K × Snode + 環境・catalog・journal・segment slack` とし、K は初期版を含む状態全体相当の保存量。既存 Phase 3 の短い例の書込みは約 9S だったが、一般の回路で K=9 を上限とはしない。下表は仮に K=9 とした振幅部分だけの値。

| root SSD の仮データ予算／node | 1 node の候補 | 2 node 均等分割の候補 | 振幅部分／node | 残りの用途 |
|---|---|---|---:|---|
| 64 GiB | 28Q、状態 4 GiB | 29Q、総状態 8 GiB | 45 GiB | 19 GiB以内で環境・metadata・slack |
| 128 GiB | 29Q、状態 8 GiB | 30Q、総状態 16 GiB | 90 GiB | 38 GiB以内で同上 |
| 200 GiB | 30Q、状態 16 GiB | 31Q、総状態 32 GiB | 180 GiB | 20 GiB以内。拡大前の測定必須 |

短い回路で収まっても、トロッター反復や QPE の controlled evolution で K は増える。version 回収／working store の実装と実測なしで長い量子化学計算をこの SSD に収容できるとは判断しない。1 mHa・2次対称トロッター・逐次位相推定の条件は [既存見積もり](../slides/resource_estimates.md)を引き継ぎ、実効性能と保存量が分かってから研究室構成用の値を再計算する。

## 9. 将来の実施順序と合格条件

| 段階 | 内容 | 資源と合格条件 |
|---|---|---|
| 0：今回 | 読み取り調査と計画書 | 完了。ジョブ未投入、実効帯域未測定 |
| 1：運用確認・準備 | scratch 許可、NVMe、quota、walltime、local software 配布、checkpoint 寿命を確認 | mount／導入は別作業。共有 I/O の実体と転送量を確認 |
| 2：単一 node correctness | buffered→O_DIRECT、1／2／4 rank、期待値・勾配・checkpoint／resume | 最初 1 node、4 CPU以下、mem 8gb、walltime 10分、disk 8 GiB以下。8Qで開始し16Qへ拡大 |
| 3：local 保存対応 | §7 の collective commit／resume／fork／GC 改修 | 全 rank が別 local filesystem でも再開一致。欠落 shard／prepare／破損 hash は全 rank が失敗する |
| 4：2 node correctness | workq の通常 node 2台で小規模ケース | 各4 CPU以下、mem 8gb、walltime 10分、disk 各8 GiB以下。共有ディスクへの振幅・catalog I/Oなし |
| 5：校正 | local SSD、Ethernet／OPA、NUMA、cache、mixed I/O | disk 各64 GiB以下、テストファイル最初1 GiB以下。段階的にサイズを増やし同条件3回以上 |
| 6：容量拡大 | RAM／SSD の §8 候補を小さい n から検証 | RSS、managed peak、disk peak、残存版、fsync時間、実行時間を記録。次のサイズは admission 後 |
| 7：Phase 4 評価 | worker I/O、MPI progress、prefetch と guard | 同期経路との数値一致を先に確認し、overlapとwall時間の改善を測る |

012／042、043／044 は調査時に未割当で、通常／大容量の 2 node 比較候補。将来も空いている保証はなく、実施時の PBS 資源を優先する。特定ノードへの直接 SSH は今回の読み取り調査に限定し、計算測定は PBS の割当内で行う。

単一 node の段階2では `examples/phase3.py` に `--num-qubits 8 --block-bits 2 --budget-kib 512` を渡し、保存先・JSON 出力・cwd を local にする。既存デモの checkpoint／resume を含む動作を検証する。16Q の容量例は b=8／managed RAM 256 KiB を基準にする。複数 node の段階4では、保存方式改修後の専用ケースを使う。

性能校正では、SSD の sequential read/write と fsync、実際の Block サイズの I/O、MPI latency と bandwidth、packed/full、同期/overlap、1/2/4 rank を比較する。send と recv を二重計数せず、wall は rank elapsed の MAX、通信は合計 send payload とする。cold／warm cache、backend、NUMA binding、MPI component、他 job の負荷を記録する。

実行時間はまだ測っていない。校正後に `T ≈ 計算 + local I/O + 通信 + metadata/commit` の同期モデルから始め、overlap は実測した範囲だけ差し引く。物理 link rate や以前の native kernel 目標値をそのまま ETA に使わない。逐次位相推定・途中測定と誤差認証は現実装の完成済み機能ではないため、初回は短いトロッター片／期待値計算で評価する。

## 10. 未確認事項と次回の読み取り確認

- NVMe の利用目的・既存データ、許可される mount point と空き容量。
- local scratch と cleanup の規約、PBS job の TMPDIR、epilogue による削除、disk reservation の有無。
- queue の walltime／fair-use、exclusive 配置と NUMA binding の site 方針。
- 各 node の Ethernet interface と fabric 選択、MPI library のローカル配置、compute 側の環境配布方法。
- 個人 quota と小さな結果・任意 manifest mirror の保存先。大量出力を PBS stdout に流さず、local report を保存して必要な要約だけ回収する。
- 調査 SSH では一部 host に hostname と IP address の known_hosts 不一致警告があった。batch 実施前に管理者と host key を照合し、検証を無効化する回避はしない。

SSH 認証や PBS 自体が参照する site 管理ファイルと、アプリケーションの I/O は区別する。既存の共有 `/home` を廃止する計画ではなく、アプリの大量アクセスと実行時依存を取り除く。PBS spool／stdout の site 配置も実施前に確認する。

今回用いた主な読み取りコマンドは `pbsnodes -aS` と各 node の詳細、`qstat -Qf`／`-Bf`、`findmnt`、`df -B1`、`lsblk -b`、`lscpu`、`/proc/meminfo`、`numactl --hardware`、HFI port 情報、`ompi_info`／`prte_info`。他利用者の job ID、内部 IP、鍵情報はこの資料に保存していない。
