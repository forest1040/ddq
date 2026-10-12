# 分散量子回路シミュレータ

`docs/slides/distributed_quantum_circuit_simulator_slides.tex` は、`plan/distributed_quantum_circuit_simulator_development.md` の開発仕様に沿った、日本語・16:9・18枚のBeamer資料です。共通テンプレート `docs/preamble.tex` を読み込みます。発表者向けの補足と出典節は各フレームの `\note{...}` に記載しています（通常のPDFには表示しません）。

## ディレクトリ構成

```text
docs/preamble.tex  共通TeXテンプレート
docs/slides/    スライドのTeXソースと生成PDF・補助ファイル
plan/           開発仕様
ref/            参考資料・参照実装
src/svsim/      Phase 0 の Python 実装
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

[Phase 0 設計書](docs/phase0_design.md) に API・不変条件・検証範囲を記載しています。[I/O 資料](ref/direct_io_linux_macos.md) に OS ごとの相違点があります。ストレージ API の Block 自動退避への接続、MPI、非同期実行は次段階の範囲です。

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
