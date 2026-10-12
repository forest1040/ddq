# 分散量子回路シミュレータ：開発者向けスライド

`distributed_quantum_circuit_simulator_slides.tex` は、`ref/` の開発仕様とスライド構成案に沿った、日本語・16:9・10枚のBeamer資料です。共通テンプレート `ref/preamble.tex` を読み込みます。発表者向けの補足と出典節は各フレームの `\note{...}` に記載しています（通常のPDFには表示しません）。

## ビルド

リポジトリ直下で実行します。LuaLaTeX、LuaTeX-ja、原ノ味フォント、Beamer、およびテンプレートで使用するパッケージ（quantikz等）が必要です。

```sh
latexmk -lualatex -interaction=nonstopmode -halt-on-error -outdir=build distributed_quantum_circuit_simulator_slides.tex
```

出力：`build/distributed_quantum_circuit_simulator_slides.pdf`

スライド9の帯域・再生時間は仕組みを説明するための仮定値です。性能の実測値ではありません。
