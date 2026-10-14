# 研究室クラスタでの複数ホスト校正（P4-001／P4-002）

実施日：2026-10-15（JST）。利用者が許可した範囲（ジョブが空いている場合のみ）で実施した。投入前に`pbsnodes`と`qstat`で空きを確認した（workqの009、010、012がfree。各96 CPUのうち80 CPU・110 GiBが割当済みで、残りは16 CPU・約240 GB）。

## 資源と配置

- PBS workq、`select=2:ncpus=4:mpiprocs=1:mem=8gb`、`place=scatter`、walltime 10分。前回（[Phase 4 cluster validation](../phase4_cluster_validation.md)）と同じ範囲である。
- 割当：CPUcluster009＋CPUcluster010。Open MPI 5.0.6、PSM2（`pml=cm`、`mtl=psm2`）、`--bind-to none`、1 rank／node。
- Python環境：前回のruntime image（SHA256 `82b47ddd…`）を元のpath `/var/tmp/ddq-20009/phase4-dfdd4a403586`に置いて使った。venvは絶対pathのsymlinkでinterpreterを指すので、別pathへ展開すると起動できない。
- コード：checksum付きのoverlayを`/var/tmp/ddq-20009/phase5-dc76b401`に置き、`PYTHONPATH`で指した（SHA256 `dc76b40132403e397b0aeab8e373c7a5afb45a3ce3b6083f728e5909c0e0dc82`。HEAD `be3913c`に、複数ホスト対応とVFS修正の未commit差分を加えたもの）。
- 配置先：環境、コード、振幅、catalog、scratch、結果はすべて計算nodeのlocal（ext4）に置いた。job後に作業領域を削除し、結果だけをtarでログインnodeのlocalへ回収した。共有`/home`は使っていない。
- local使用量：envとcodeで約372 MB（009）と約215 MB（010）、空きは約365 GB／383 GB。100 GiBの空き下限を維持した。

## 試行と結果

| job | 結果 | 原因と対応 |
|---|---|---|
| 265148 | 14秒、計算前に停止（127） | runtime imageを別pathへ展開したため、venvのinterpreter（絶対pathのsymlink）が切れた。→ 元のpathで環境を用意する形に変更 |
| 265149 | 15秒、計算前に停止（4） | P4-009のnative SQLite VFS拡張が、OS付属ヘッダ（SQLite 3.26）でコンパイルできなかった（`SQLITE_DIRECTONLY`は3.30で追加）。実行時のSQLiteはPython同梱の3.53.1。→ 未定義なら値を補い、実行時に3.30未満なら拒否するよう修正（[run2](run2/)） |
| 265150 | 1分28秒、ETAは成功 | 下記（[run3](run3/)） |

**ETA（2 node×1 rank、async、file store、O_DIRECT、6〜7 qubit、b=4）**
- 校正：22件、すべて完了・oracle合格。
- held-out：4 workload×3回×2点で24 check。
- 精度：median ｜相対誤差｜ 7.8%、p95 9.8%、worst 10.3%、interval coverage 95.8%。校正median syncで評価してもmedian 8.5%、worst 11.4%。
- syncの安定性：この装置のfsyncは0.13〜0.24 msで安定しており、不安定trialの除外は0件だった。

**runtime校正（`svbench runtime`、2 node、worker、O_DIRECT、8 qubit）**
- 3 suite（kernel-storage、memory-mpi、storage-mpi）をそれぞれ5回測定し、数値検証に合格した。median wallは1.109 s／0.499 s／1.299 s。
- statusは`partial`。候補用envelopeを作るheld-out数が無く、品質が「finite diagnostics only」だったためで、測定の失敗ではない。
