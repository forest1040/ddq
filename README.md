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

## スライドのビルド

リポジトリ直下で実行します。LuaLaTeX、LuaTeX-ja、原ノ味フォント、Beamer、およびテンプレートで使用するパッケージ（quantikz等）が必要です。

```sh
latexmk -lualatex -interaction=nonstopmode -halt-on-error -outdir=docs/slides docs/slides/distributed_quantum_circuit_simulator_slides.tex
```

出力：`docs/slides/distributed_quantum_circuit_simulator_slides.pdf`。補助ファイルも`docs/slides/`に生成し、Gitでは無視します。

スライド17の帯域・再生時間は仕組みを説明するための仮定値です。性能の実測値ではありません。

スライド3〜5は `ref/03-2.tex` のクロネッカー積の図と `ref/10.tex` のストライドアクセスの図をもとに、全体行列・振幅ペア・Block境界・Rank配置の対応を説明しています。

スライド11〜13は、遅延Log・Global DAGの依存からRAM振幅へ反映するタイミング、F_3 frontierの操作例、RAM再利用・SSD書き戻し・Checkpointの耐久commitを図示しています。

スライド9〜10は、Pauli FrameへのX/Z吸収、H越しのPauli伝播、Block内HのClifford Frame追跡、Block間HのJoint実行を比較しています。Pauli Frameの定義・伝播は [PennyLane公式解説](https://pennylane.ai/compilation/pauli-frame-tracking/details) も参照しています。Block間の相対位相は保持する設計です。
