# 分散量子回路シミュレータ

`docs/slides/distributed_quantum_circuit_simulator_slides.tex` は、`plan/distributed_quantum_circuit_simulator_development.md` の開発仕様に沿った、日本語・16:9・18枚のBeamer資料です。共通テンプレート `docs/preamble.tex` を読み込みます。発表者向けの補足と出典節は各フレームの `\note{...}` に記載しています（通常のPDFには表示しません）。

## ディレクトリ構成

```text
docs/preamble.tex  共通TeXテンプレート
docs/slides/    スライドのTeXソースと生成PDF・補助ファイル
plan/           開発仕様
ref/            参考資料・参照実装
src/svsim/      Phase 0 の参照計算と Phase 1 の外部記憶実装
tests/          数値一致・ライフサイクル・OS別 I/O テスト
pyproject.toml  Pythonプロジェクト設定
uv.lock         uvのロックファイル
```

## 研究室クラスタの内部計画

[研究室クラスタ調査・実行計画](docs/internal/lab_cluster_execution_plan.md)には、研究室固有の接続先、ノード構成、容量、運用情報を記載しています。将来リポジトリを共有する際は、この資料を非公開にするか匿名化する可能性があります。現在はリポジトリ内に保存しており、`docs/internal/` という配置だけでアクセスを制限するものではありません。公開前には資料・参照リンクと Git 履歴に含まれる情報も確認してください。

## Python環境

Python 3.12とuvを使用します。リポジトリ直下で実行します。

```sh
uv sync
uv run python --version
```

仮想環境は`.venv/`に作成されます。NumPy と pytest をロックファイルから導入します。

## Phase 0 実装

意味保存 Gate IR、共有 Block ログ、局所 Clifford Frame、Joint materialization、任意 U(2)/U(4)、symbolic parameter binding、期待値・随伴勾配の RAM 参照実装です。Linux の `O_DIRECT` と macOS の `F_NOCACHE` を共通の同期ストレージ API から使えます。

```sh
uv sync
uv run pytest
uv run python examples/phase0.py
```

[Phase 0 設計書](docs/phase0_design.md) に API・不変条件・検証範囲を記載しています。[I/O 資料](ref/direct_io_linux_macos.md) に OS ごとの相違点があります。外部記憶との接続は以下の Phase 1 実装で提供します。MPI と非同期実行は後続 Phase の範囲です。

## Phase 1 実装

RAM＋SSD/HDD の `StoredSimulator`、RAM 予算付き Block Cache、COW snapshot、物理レイアウト交換、全状態を集めない期待値計算、I/O 性能監視を追加しました。[Phase 1 設計書](docs/phase1_design.md)、[実装と検証](docs/phase1_implementation.md)、[実測比較](docs/phase1_benchmark.md) を参照してください。

```sh
uv run python examples/phase1.py --storage /tmp/svsim-phase1-demo --backend direct
uv run python examples/phase1_benchmark.py --output /tmp/svsim-phase1-report.json
```

保存先は新しいディレクトリを指定します。`direct` は Linux の `O_DIRECT`／macOS の `F_NOCACHE` を選びます。managed RAM 上限と process RSS の違いは実装資料に記載しています。

## Phase 2：位相を保持する Frame

[Phase 2 設計書](docs/phase2_design.md)と[実装・検証資料](docs/phase2_implementation.md)に、Clifford tableau と相対位相、Pauli 回転の遅延ログ、Joint の Frame 整合、ExactPi と Binding guard、仮想 Block 置換、v1 snapshot 互換性を記載しています。

```sh
uv run python examples/phase2.py /tmp/svsim-phase2 --backend buffered
uv run python examples/phase2_benchmark.py --num-qubits 16 --output /tmp/svsim-phase2-report.json
```

`frame_policy='keep'` で対応する局所回転を Frame の内側へ遅延できます。`auto` は校正がない局所回転を具体化し、校正済みの `FrameCalibration` を渡すと費用推定で選択します。[比較実験](docs/phase2_benchmark.md)は状態 1 MiB／managed RAM 256 KiB の例を含みます。

## Phase 3：MPI とストレージ階層

[Phase 3 設計書](docs/phase3_design.md)、[実装・検証](docs/phase3_implementation.md)、[実測比較](docs/phase3_benchmark.md)に、rank ごとの RAM／SSD、MCX packed exchange、分散 checkpoint／prefix、S0/S1 physical SWAP、全状態を集めない期待値・随伴勾配、MPI 性能観測を記載しています。1/2/4 rank の実 MPI と macOS F_NOCACHE 経路を検証しました。

```sh
brew install open-mpi
uv sync --locked --extra mpi
uv run --locked --extra mpi pytest -q
mpiexec -n 4 .venv/bin/python -m mpi4py examples/phase3.py \
  --directory /tmp/svsim-phase3 --gradient --backend direct
```

MPI は optional dependency です。全 rank が同じ collective API 順序で実行し、ParameterSymbol には安定した parameter_id を指定します。

## Phase 4：非同期実行とnode-local保存

Phase 4は主要機能の実装・correctness検証まで進んでいます。全体の完了に必要な残件は[Phase 4 backlog](docs/phase4_backlog.md)で管理しています。

[Phase 4 設計書](docs/phase4_design.md)と[実装・検証資料](docs/phase4_implementation.md)に、worker I/O、nonblocking MPI、先読み・書き戻し、S2／local fusion／FusedSWAP、Parameter batch、期待値・三方式の勾配、Runtime Guardとpolicy変更、node-local checkpoint／resume／GCを記載しています。[Mac実測比較](docs/phase4_benchmark.md)では同期／非同期／S2を同一回路で比較しています。

Mac上のMPIとlocal scheduler、および許可後のPBS割当内で単一ノード・実2ノードの2／4 rankを検証しました。Linux O_DIRECT、PSM2／OFI／Ethernetで数値一致を確認しています。振幅・catalogはnode-localに保存し、checkpoint／resume／prefix lease／GCはMPI通信で管理します。[クラスタ検証記録](docs/internal/phase4_cluster_validation.md)は内部資料です。optional Linux io_uring adapterはliburingが既存環境になく、native Linux実機では未検証です。運用ETAの自動校正・一般的なtile migration等の拡張範囲は実装資料に明記しています。

```sh
uv sync --locked --extra mpi
uv run --locked --extra mpi mpiexec -n 4 .venv/bin/python -m mpi4py examples/phase4.py \
  --directory /private/tmp/ddq-phase4-new-run --backend direct --gradient adjoint
uv run --locked --extra mpi pytest -q
```

保存先は毎回新しいディレクトリを指定します。`LocalDevelopmentRunner`はMacの有限job queue、`PBSAdapter`はscript生成だけを提供し、クラスタへの自動投入は行いません。

## Phase 5：計測・静的推定の基盤

[Phase 5設計書](docs/phase5_design.md)に沿って、`svbench probe/all`、`svsim register/estimate`、条件付きprofileとraw trial、RAM／disk／trafficの静的reference、40／50Qの状態を確保しない解析を実装しました。[使い方と実装範囲](docs/phase5_implementation.md)、[Mac計測](docs/phase5_benchmark.md)を参照してください。未校正の総実行時間はnullで返し、容量・実行対応・coverageを別々に表示します。統合時間評価、大規模実測、Phase 4のETA・適応制御への接続は残件です。

Phase 5の詳細profile／rawにもlocal path・ソフトウェア構成が含まれます。`docs/internal/phase5_mac_calibration/`は内部資料として扱い、リポジトリを外部共有する際には非公開化または匿名化exportの対象にします。

## スライドのビルド

リポジトリ直下で実行します。LuaLaTeX、LuaTeX-ja、原ノ味フォント、Beamer、およびテンプレートで使用するパッケージ（quantikz等）が必要です。

```sh
latexmk -lualatex -interaction=nonstopmode -halt-on-error -outdir=docs/slides docs/slides/distributed_quantum_circuit_simulator_slides.tex
```

出力：`docs/slides/distributed_quantum_circuit_simulator_slides.pdf`。補助ファイルも`docs/slides/`に生成し、Gitでは無視します。

スライド17の帯域・再生時間は仕組みを説明するための仮定値です。性能の実測値ではありません。

スライド18〜23に、RAMのみ／Local SSD／複数node Ethernet／複数node InfiniBandの構成例と、Forward・AdjointのRAM/SSD確保量を追加しています。[計算条件](docs/slides/resource_estimates.md)に、状態サイズ、append-onlyの更新回数、容量余裕、生存版GCモデルとの違いを記載しています。

スライド24〜26のVQE見積もりを保持し、27〜29に1 mHa・2次対称トロッター・逐次位相推定（QPE）、30に最適化検討を追加しています。QPE時間は未最適化の全Pauli passとnative並列化目標を使う条件付き計画値で、誤差認証・working store・途中測定は未実装です。[Markdown](docs/slides/resource_estimates.md)に両方の条件を記載し、[VQE script](docs/slides/estimate_vqe.py)・[JSON](docs/slides/vqe_estimates.json)、[QPE script](docs/slides/estimate_chemistry.py)・[JSON](docs/slides/chemistry_estimates.json)を分けて保存しています。[別の検討資料](docs/quantum_chemistry_optimization.md)に、直接期待値、逆ゲート／bra-ket、トロッターのfusion／配置／回収の最適化候補を記載しています。

スライド3〜5は `ref/03-2.tex` のクロネッカー積の図と `ref/10.tex` のストライドアクセスの図をもとに、全体行列・振幅ペア・Block境界・Rank配置の対応を説明しています。

スライド11〜13は、遅延Log・Global DAGの依存からRAM振幅へ反映するタイミング、F_3 frontierの操作例、RAM再利用・SSD書き戻し・Checkpointの耐久commitを図示しています。

スライド9〜10は、Pauli FrameへのX/Z吸収、H越しのPauli伝播、Block内HのClifford Frame追跡、Block間HのJoint実行を比較しています。Pauli Frameの定義・伝播は [PennyLane公式解説](https://pennylane.ai/compilation/pauli-frame-tracking/details) も参照しています。Block間の相対位相は保持する設計です。
