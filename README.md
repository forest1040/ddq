# 分散量子回路シミュレータ：開発者向けスライド

`distributed_quantum_circuit_simulator_slides.tex` は、`ref/` の開発仕様とスライド構成案に沿った、日本語・16:9・18枚のBeamer資料です。共通テンプレート `ref/preamble.tex` を読み込みます。発表者向けの補足と出典節は各フレームの `\note{...}` に記載しています（通常のPDFには表示しません）。

## ビルド

リポジトリ直下で実行します。LuaLaTeX、LuaTeX-ja、原ノ味フォント、Beamer、およびテンプレートで使用するパッケージ（quantikz等）が必要です。

```sh
latexmk -lualatex -interaction=nonstopmode -halt-on-error -outdir=build distributed_quantum_circuit_simulator_slides.tex
```

出力：`build/distributed_quantum_circuit_simulator_slides.pdf`

スライド17の帯域・再生時間は仕組みを説明するための仮定値です。性能の実測値ではありません。

スライド3〜5は `ref/03-2.tex` のクロネッカー積の図と `ref/10.tex` のストライドアクセスの図をもとに、全体行列・振幅ペア・Block境界・Rank配置の対応を説明しています。

スライド11〜13は、遅延Log・Global DAGの依存からRAM振幅へ反映するタイミング、F_3 frontierの操作例、RAM再利用・SSD書き戻し・Checkpointの耐久commitを図示しています。

スライド9〜10は、Pauli FrameへのX/Z吸収、H越しのPauli伝播、Block内HのClifford Frame追跡、Block間HのJoint実行を比較しています。Pauli Frameの定義・伝播は [PennyLane公式解説](https://pennylane.ai/compilation/pauli-frame-tracking/details) も参照しています。Block間の相対位相は保持する設計です。
