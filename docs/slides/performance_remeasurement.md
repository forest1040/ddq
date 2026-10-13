# 実行速度の再計測と見積もりへの反映

2026-10-13、Mac Studio（Mac15,14）、RAM 512 GiB、32 physical cores（Performance 24＋Efficiency 8）。Pythonはuv環境。ほかの利用者のjobは停止せず、自分の計測を順番に実行した。

## 旧計測と今回の条件

旧VQE資料のPython `pauli_inner` はb=16、mask X/Z=3/5、warmup後3回、約1.563百万振幅項/秒だった。今回の同条件7回は **1.536百万/秒（旧値比0.983）**。単一processでは大きな短縮は確認できなかった。

開始／終了のCPU idleは87.93%／88.14%。7試行のprocess CPU時間／wall時間は中央値約1.0。16 processの測定中は自分のCPU消費でidleが下がる。OS負荷とraw trialは[JSON](performance_remeasurement.json)に記録した。専用機として負荷を完全隔離した測定ではない。

旧QPE表の1億更新/秒はこのPython実測から算出した値ではなく、未測定のnative目標だった。したがって再計測でその表を比例短縮せず、以下に実測を使う別の条件付き推定を追加する。元のVQEスライドも保持する。

## Python kernelのコア利用

BLAS/OMP/MKL threadを1にし、1/2/4/8/16の独立processをbarrierで揃えた。各workerでwarmup1回＋測定7回。表は各試行の最遅workerから求めた合計速度の中央値。単位は百万振幅項／更新/秒。

| 独立process | Pauli内積 | local Pauli回転 | 2 block pair回転 |
|---:|---:|---:|---:|
| 1 | 1.536 | 2.909 | 2.894 |
| 2 | 3.287 | 6.182 | 6.083 |
| 4 | 6.761 | 12.658 | 12.280 |
| 8 | 13.175 | 25.334 | 24.626 |
| 16 | 26.142 | 50.582 | 49.031 |

b=20の単一processでも内積1.545百万、local回転2.937百万、pair回転2.868百万で、b=16と同水準だった。pairの更新数は2 block分を数え、pair数を振幅更新数として扱わない。kernelごとに内積oracle・inverse・pair出力を照合した。

この独立processの速度を現行MPI実行器の速度と同一視しない。1 node当たり16 workerが均衡して動く場合の感度は[集計script](reestimate_performance.py)で出力するが、Frame、metadata、I/O、通信、実回路の依存を含む実行保証ではない。

## Mac MPI実行器の再計測

[旧計測](../phase4_benchmark.md)と同じ8Q、b=4、buffered、managed RAM 256 KiB/rank、同じ回路を、ほかの自分の試験と重ねずwarmup1回＋測定5回で実行した。[新raw JSON](phase4_remeasurement.json)。executeはrank MAXの中央値。

| rank | 実行器・policy | 旧 秒 | 新 秒 | 旧/新 |
|---:|---|---:|---:|---:|
| 1 | 同期 S0 | 0.5117 | 0.5109 | 1.002 |
| 1 | 非同期 S0 | 0.5827 | 0.5742 | 1.015 |
| 1 | 非同期 S2 | 0.0909 | 0.0873 | 1.042 |
| 2 | 同期 S0 | 0.6626 | 0.6506 | 1.018 |
| 2 | 非同期 S0 | 0.6966 | 0.6938 | 1.004 |
| 2 | 非同期 S2 | 0.1100 | 0.1095 | 1.004 |
| 4 | 同期 S0 | 1.2332 | 1.2238 | 1.008 |
| 4 | 非同期 S0 | 1.3029 | 1.2241 | 1.064 |
| 4 | 非同期 S2 | 0.2616 | 0.2155 | 1.214 |

各試行で期待値とcheckpoint再開が一致した。差のすべてをCPU競合の影響と断定しない。計測を独立したセッションで反復した比較であり、この小回路の秒数から量子化学QPEの時間を外挿しない。

## Qulacsの単一ノード・RAM比較

Qulacs 0.6.14をuv一時環境で使用する。配布macOS arm64 wheelではlibompへのリンクがなく、1/8/16/24 thread指定時のprocess CPU/wallは約1.0で、実際の回転はほぼ単一threadだった。指定thread数だけで並列速度を判断しない。

既存のコンパイラ・Boost・libompを使うOpenMP source buildをLunaに移譲して準備した。[再現手順](qulacs_openmp_build.md)。リポジトリの依存関係とlockは変更していない。[Qulacs公式README](https://github.com/qulacs/qulacs/blob/v0.6.14/README.md)にはOMP／QULACSのthread数指定が説明されている。

QulacsのPauliRotationは `exp(+i angle P/2)`、svsimは負符号なので角度を反転して照合する。[公式APIの回転規約](https://docs.qulacs.org/en/latest/guide/2.0_python_advanced.html#multi-bit-pauli-rotating-operation)。制御付きfactorは次の2回転で実装し、小さいrandom stateで両ancilla branchを独立oracleと比較した。

```text
exp(-i theta |1><1| P/2)
  = exp(-i theta P/4) exp(+i theta Za P/4)
```

8種類の合成Pauli項を同じ係数で前進／逆順に並べた2次対称列を使う。1片は16 controlled factor＝32 native rotation。full stateはRAM上でin-place更新する。計測内にSSD/MPIはない。25Qは512 MiB、29Qは8 GiBの状態であり、IPEAのancilla 1個込み。合成項は分子積分ではなく、特定の化学Hamiltonianの速度・誤差を保証しない。

### 同じOpenMPビルドの単一thread・複数thread

25Qは[単一thread raw](qulacs_omp_single_measurement.json)、29Qは[単一thread raw](qulacs_omp_single_29_measurement.json)、並列は[raw](qulacs_omp_ram_measurement.json)。warmup1回、測定は25Q単一／並列が5回、29Q単一のみ3回。CPU/wallはprocess CPU時間のwall時間に対する比で、指定thread数とは別に記録する。

| 全状態 | 指定thread | 32回転片 中央値 秒 | CPU/wall | 単一threadに対する速度比 |
|---|---:|---:|---:|---:|
| 25Q / 512 MiB | 1 | 2.481530 | 1.00 | 1.00 |
| 25Q / 512 MiB | 8 | 0.289902 | 7.84 | 8.56 |
| 25Q / 512 MiB | 16 | 0.145404 | 15.57 | 17.07 |
| 25Q / 512 MiB | 24 | 0.154099 | 16.06 | 16.10 |
| 29Q / 8 GiB | 1 | 39.604449 | 1.00 | 1.00 |
| 29Q / 8 GiB | 8 | 4.593090 | 7.91 | 8.62 |
| 29Q / 8 GiB | 16 | 2.298979 | 15.82 | 17.23 |
| 29Q / 8 GiB | 24 | 2.048514 | 19.29 | 19.33 |

25Qは16 threadが最速、29Qは24 threadが最速だった。OpenMP並列の開始／終了idleは87.80%／88.23%、測定中の最低idleは26.67%。ほかのjobは存在するため、Performance coreを全て専有した測定とは扱わない。配布wheelの25Q単一thread片は2.228654秒、29Qは35.801904秒だった。OpenMPの有無によるbuild差もあるため、純粋なthread比較は上表の同じsource buildで行う。

### 単一ノードRAMでのQPE時間の条件付き更新

元の **1 mHa、2次対称トロッター、IPEA** を保持する。Λ=100 Ha、phase budget 0.5 mHa、m=20、delta_max=0.01 Ha^-1、base_steps=2、Trotter budget 0.5 mHaは未認証。Hamiltonian項数もAの5,000、Bの20,000という入力仮定を保持する。Bは元のSSD構成と同じ問題サイズを、今回のMacのRAMだけで実行する比較である。

```text
native_rotations = 2 * (2*M) * 2 * (2^20-1)
A: 41,943,000,000 native rotations / trajectory
B: 167,772,000,000 native rotations / trajectory
Ttrajectory = 1.05 * native_rotations * (measured_t32 / 32)
T32_trajectories = 32 * Ttrajectory
```

計測時間にRAMアクセスは含まれるので、別のRAM帯域費用を重複加算しない。SSD/MPI項は0。5%marginは旧モデルの仮定であり、未計測の固有状態準備、IPEAのmeasurement/reset/feedforward、古典制御を保証する上限ではない。

| 問題サイズ / RAMのみ | thread | 1 trajectory 外挿 年 | 32 trajectory 外挿 年 |
|---|---:|---:|---:|
| A / 25Q | 1 | 108.22 | 3,463.09 |
| A / 25Q | 8 | 12.64 | 404.57 |
| A / 25Q | 16 | 6.34 | 202.92 |
| A / 25Q | 24 | 6.72 | 215.05 |
| B / 29Q | 1 | 6,908.74 | 221,079.66 |
| B / 29Q | 8 | 801.23 | 25,639.52 |
| B / 29Q | 16 | 401.04 | 12,833.34 |
| B / 29Q | 24 | 357.35 | 11,435.20 |

Aの16 threadは旧native目標表の約468年より約74倍短い。これはQulacsのnative速度・並列化を反映した別の推定で、旧Python計測のCPU競合を補正した倍率ではない。Bの時間がAより長い主因は状態16倍・項数4倍。単一threadからの実測差、build差、問題サイズ差を混同しない。

いずれも少数の合成項から、元の「全Pauliを別passで処理する」演算回数への線形外挿である。化学積分・Trotter bias・長い回路の数値誤差は未認証。実際の分子のQPEを最後まで実行した結果ではない。grouping、diagonal fusion、layout、controlled branch処理、step-size最適化の利益は先取りしていない。独立32 trajectoryを逐次実行する場合、状態容量は32倍にしない。

[再見積もりJSON](performance_estimates.json)にはsample min/maxに対応する外挿も保存する。この範囲はconfidence intervalや誤差保証ではない。C/Dは今回のQulacs単一ノード測定から分散時間へ直接置換しない。Pythonの測定rateを使うVQE／QPEの感度値は別フィールドに保存し、目標I/O/networkと均衡並列の仮定が残ることを明記した。

## クラスタ結果の利用範囲

既存の実2ノード結果から、1 MiBの単一転送のrank MAXを3回測った中央値を匿名化して掲載する。専用warmupなし。bounded simulator protocolのcontrol／ack費用を含む。

| 経路 | rank | 1 MiB転送の中央値 ms |
|---|---:|---:|
| PSM2 | 2 | 0.690 |
| PSM2 | 4 | 0.640 |
| OFI/PSM2 | 4 | 0.805 |
| Ethernet/TCP | 4 | 2.962 |

実環境は10 Gb EthernetとOmni-Pathであり、元の構成例の100 Gb Ethernet／200 Gb InfiniBandとは違う。これは経路比較に利用し、8 nodeで数GiBを流す帯域へ比例外挿しない。Omni-PathをInfiniBandの実測と呼ばない。したがってC/Dの元の目標帯域は置換しない。単一ノードRAMのQulacs推定には通信項を入れない。

## 全ケースの一覧表

スライド35／36に、VQE／QPEそれぞれ一つの表を追加した。既存JSONの値を表示単位へ換算したもので、新しい実機計測や構成間の性能保証ではない。

### VQE：100反復（energy＋Adjoint）、単位は日

RAM／SSDはnode当たりの予約例（GiB／TiB）、状態はnode当たりGiB。Python列の1／16は独立process数/nodeで、均衡並列とMac等級CPUの複製を仮定する。

| 構成 | system Q / node | 状態/node GiB | RAM/SSD予約 | native目標 日 | Python 1 process 日 | Python 16 process 日 | Qulacs VQE全体 |
|---|---|---:|---|---:|---:|---:|---|
| A RAMのみ | 24 / 1 | 0.25 | 512 / 0 | 2.07 | 134.4 | 7.91 | 未計測 |
| B SSDあり | 28 / 1 | 4 | 64 / 32 | 133.7 | 8,523 | 503.5 | 未計測 |
| C Ethernet | 32 / 8 | 8 | 64 / 64 | 663.2 | 42,530 | 2,508.9 | 未計測 |
| D InfiniBand | 32 / 8 | 8 | 64 / 64 | 662.5 | 42,529 | 2,508.2 | 未計測 |

native目標は未測定の1億項/s/node、Python列は内積の再計測rate（約154万／2,614万項/s）による感度。元のSSD/network帯域、partner reuse、5%marginの仮定を維持する。Qulacs回転の速度から、未測定のobservable/action/reverse/checkpointを含むVQE全体を導出しない。native列の根拠は [vqe_estimates.json](vqe_estimates.json)の **cases[].total_100_iterations_seconds / 86400**、Python列は [performance_estimates.json](performance_estimates.json)の **vqe_cpu_sensitivity[].total_100_iterations_days**。

### QPE：独立32 trajectory、単位は年

全Qはsystem＋IPEA ancilla 1個。1 mHa設定、2次対称トロッター、Lambda=100 Ha、m=20、delta_max=0.01 Ha^-1を維持し、誤差は未認証。全列を32 trajectoryの合計へ揃え、1 trajectoryと混在させない。

| 構成 | 全Q / node | native目標 年 | Python 1 process 年 | Python 16 process 年 | Qulacs配布版 実質1 thread 年 | OpenMP 1 thread 年 | OpenMP 8 thread 年 | OpenMP 16 thread 年 | OpenMP 24 thread 年 |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| A RAMのみ | 25 / 1 | 1.498万 | 51.78万 | 3.056万 | 3,110 | 3,463 | 405 | 203 | 215 |
| B SSDあり | 29 / 1 | 95.90万 | 3,314万 | 195.6万 | — | — | — | — | — |
| C Ethernet | 33 / 8 | 658.1万 | 1.675億 | 1,157万 | — | — | — | — | — |
| D InfiniBand | 33 / 8 | 568.8万 | 1.666億 | 1,067万 | — | — | — | — | — |
| B RAM比較 | 29 / 1 | — | — | — | 19.99万 | 22.11万 | 2.564万 | 1.283万 | 1.144万 |

「—」はその構成で未計測／未見積もり。B RAM比較は元のBと同じ29Qを512 GiB MacのRAMだけに載せる別構成であり、元のSSDモデルの数値をコピーしない。Qulacsの単一ノード実測をC/DのSSD／8 node構成へ転用しない。配布版は1 thread指定の代表値を載せ、8／16／24指定時も実効CPUはほぼ1 coreであることを前節に記載した。OpenMPの全thread条件は同じ表で比較する。

native列の根拠は [chemistry_estimates.json](chemistry_estimates.json)の **cases[].all_runs_years**、Python列は [performance_estimates.json](performance_estimates.json)の **qpe_cpu_sensitivity[].all_runs_years**、Qulacs列は同JSONの **qulacs_one_node_ram_qpe[].all_runs_years**。Qulacs列は合成項からの外挿で、固有状態準備、IPEA measurement/reset/feedforward、分子積分、Trotter biasの実測・認証を含まない。年は365.25日、表は丸め値。

## 再現

計測を同時に起動せず順に実行する。

```sh
OMP_NUM_THREADS=1 OPENBLAS_NUM_THREADS=1 MKL_NUM_THREADS=1 \
  uv run --locked --extra mpi python docs/slides/recalibrate_performance.py \
  --output docs/slides/performance_remeasurement.json

uv run --locked --extra mpi python examples/phase4_benchmark.py \
  --repetitions 5 --backend buffered --output docs/slides/phase4_remeasurement.json

OPENBLAS_NUM_THREADS=1 uv run --locked --extra mpi --with qulacs==0.6.14 \
  python docs/slides/benchmark_qulacs_ram.py \
  --output docs/slides/qulacs_ram_measurement.json

# Source buildを準備した後。同じuv隔離環境で単一／並列を順に測る。
VIRTUAL_ENV=/private/tmp/ddq-qulacs-omp/venv PYTHONPATH="$PWD/src" \
  uv run --active --no-project python docs/slides/benchmark_qulacs_ram.py \
  --threads 1 --qubits 20 25 --output docs/slides/qulacs_omp_single_measurement.json
VIRTUAL_ENV=/private/tmp/ddq-qulacs-omp/venv PYTHONPATH="$PWD/src" \
  uv run --active --no-project python docs/slides/benchmark_qulacs_ram.py \
  --threads 1 --qubits 29 --repetitions 3 --output docs/slides/qulacs_omp_single_29_measurement.json
VIRTUAL_ENV=/private/tmp/ddq-qulacs-omp/venv PYTHONPATH="$PWD/src" \
  uv run --active --no-project python docs/slides/benchmark_qulacs_ram.py \
  --threads 8 16 24 --qubits 25 29 --output docs/slides/qulacs_omp_ram_measurement.json

uv run --locked --extra mpi python docs/slides/reestimate_performance.py \
  --output docs/slides/performance_estimates.json
```
