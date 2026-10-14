# Linux開発環境とnative I/O検証

2026-10-14。Linux x86_64サーバのDocker上に、uv・OpenMPI・liburing・SQLite開発ヘッダーを揃えた開発環境を追加した。環境構築と実行コマンドは[docker/linux/README.md](../docker/linux/README.md)、定義は[Dockerfile](../docker/linux/Dockerfile)、証跡を保存する実行入口は[run_validation.sh](../docker/linux/run_validation.sh)を参照する。

## 環境と実行範囲

Python／uvのbase imageをdigestで固定し、Python依存は`uv.lock`から導入する。mpi4pyはコンテナの`mpicc`でsource buildし、同じOpenMPIへリンクする。実行はnon-root、ネットワークなし、4 CPU・8 GiB RAM・512 PID・256 MiB shared memoryを上限とする。ホストのローカルext4をscratchに使い、コードはread-only bind mount、依存はイメージ内の`.venv`に置く。ホストのMPIやPython環境へは変更を加えない。

通常テストはDockerの既定seccompで実行する。native io_uring検証時だけ、固定したMoby default profileに`io_uring_setup`／`io_uring_enter`／`io_uring_register`の許可を追加した[限定profile](../docker/linux/seccomp-uring.json)を明示する。[出典とhash](../docker/linux/seccomp-provenance.json)、[ライセンス](../docker/linux/LICENSE.moby-profiles)も保存した。既定profileでの拒否と限定profileでの成功は別のscopeで記録する。

検証対象は単一Linuxホスト内のMPIであり、研究室クラスタや複数ホストの検証とは分ける。接続先、絶対パス、実際のtoolchainとsource hash、ログは[内部検証記録](internal/ent_linux_validation/README.md)に保存する。

## 追加した検証と計測修正

`DDQ_NATIVE_URING=1`を明示した場合だけ、[nativeテスト](../tests/test_native_uring_linux.py)を実行する。実kernelのbuffered／O_DIRECT read・write・fsync、短いEOF、閉じたFDのnegative CQE、cancel競合、request上限とpin／RAM解放を検査する。明示要求時のcapability不足は失敗となり、skipやworkerへのfallbackにはならない。

[native MPIテスト](../tests/test_native_uring_mpi_linux.py)はbuffered／O_DIRECTと1／2／4 rankの6構成を使う。全rankでio_uringの実体と`MPI_THREAD_SINGLE`を検査し、状態のeager reference一致、期待値、adjoint勾配、物理SWAP、prefix fork／lease／GC、checkpoint／resumeを確認する。fake CQEテストは引き続きprotocolの決定的な回帰用であり、native実測に数えない。

native requestにはbackend、device、alignment、実際の転送byte、発行時のin-flight数を記録し、callback終了までのwall時間を補った。submissionからCQEまでの時間にはqueue時間も含まれるため、queue／純serviceの内訳は`null`とし、分離した計測値を捏造しない。probeにはadapterのC sourceとcompiled binaryのSHA-256を含める。

## 初期検証

初期native実機検証は12件すべて成功した。通常のLinux回帰は分割実行のnode IDを全656件と照合し、642成功・14 skip・失敗0を確認した。Macの関連63件も成功した。Linux回帰で判明したpage-size依存のcache fixture、writable cwdを前提とするrunner fixture、Mac固定のtile backend／alignmentを修正した。correctness campaignとMPIの有限時間上限をLinux環境に合わせ、本番のRAM／SSD予算・制限処理・数値検証は維持している。詳細とscope、元の失敗logは[内部記録](internal/ent_linux_validation/README.md)を参照する。

## 残る範囲

この環境と初期native検証によって[P4-010](phase4_tasks/P4-010_native_uring.md)の環境不足は解消するが、全受入matrixの完了とは扱わない。cancel勝敗の個別証跡、aligned short I/Oの再発行、fsync失敗とauthoritative checkpoint cut、全CQE順序・lifetime・shutdown、workerとの同条件性能比較、native大状態とcross-host検証は引き続き必要。Phase 4全体の残件は[backlog](phase4_backlog.md)で管理する。
