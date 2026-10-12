# Phase 2 比較実験

2026-10-13 の単一実行。対象は同じ seed・初期状態・回路に対する常時具体化、KEEP＋input-a/input-b、校正済み auto。全体位相を合わせず、complex amplitude を直接比較した。

## 条件

- macOS 26.7.1 / arm64、Python 3.12.13、NumPy 2.5.4、OS page 16,384 bytes。
- 16 qubit、b=6、状態 1 MiB、managed RAM 256 KiB、metadata reserve 128 KiB。
- buffered backend、alignment=64、slot=1,024 bytes。初期 import の dirty cache を flush/clear してから各 policy を実行。
- seed=20261013。全 case の回路 hash は `4afb9b94868420d455e7da7e8035165002083fbe3bb2128622494f92afe495eb`、初期状態 hash も一致。
- H/S/T/Rz、条件付き局所 H、上位 H、局所/Joint Pauli 回転、上位 X/CX/SWAP を含む 11 gate。
- calibration は同じ b=6、各 3 回。Frame gate 約 41.8 µs、Pauli rotation 約 24.8 µs、inverse 共役約 50.2 µs、synthesis 約 188.6 µs、Frame 左更新約 268.2 µs。これらは小規模単発 CPU 費用で、wall time の加算内訳ではない。

```sh
uv run --locked python examples/phase2_benchmark.py --num-qubits 16 --output /tmp/phase2.json
```

JSON と `<output-stem>.traces/` の policy 別 JSONL を出力する。今回の原データは実行環境の `/private/tmp/ddq-phase2-final16.json` と隣接 trace directory にある。

## 実測

| policy | execution 秒 | end-to-end 秒 | execution read | execution＋snapshot write | 最大 complex 誤差 |
|---|---:|---:|---:|---:|---:|
| materialize / identity | 14.349 | 17.798 | 7.5 MiB | 7.5 MiB | 3.51e-18 |
| keep / input-a | 4.851 | 8.251 | 2 MiB | 2 MiB | 7.76e-18 |
| keep / input-b | 4.829 | 8.316 | 2 MiB | 2 MiB | 9.35e-18 |
| auto / auto | 12.706 | 17.828 | 2 MiB | 2 MiB | 5.49e-18 |

全 case の managed peak は 262,144 bytes。import write は各 1 MiB、export read は各 1 MiB。snapshot 時の残り dirty write 4,096 bytes を実行段階の write と合算した。export は別 interval で数え、表の execution read に含めない。

end-to-end は import/flush、gate 登録、eager reference、AccessPlan、execution、snapshot、export/readback と検証を含む。simulator close、保存 trace のコピー、CLI JSON 書き出しは含まない。初期状態・expected・readback の三つの 1 MiB 配列は呼び出し側の参照配列で managed budget の外にある。process-global RSS high-water は約 59 MiB で、case 単独の RSS 測定ではない。

この回路では KEEP の二つの anchor が振幅転送と実行時間を削減した。一方、auto は転送を削減しても end-to-end で常時具体化を改善していない。差分 Frame、tableau、catalog lookup、trace、読み出し時の具体化の CPU 費用が残り、初期 heuristic の精度には制約がある。

結果は特定回路・単一実行・3 回の小規模校正に対する参考値である。平均・信頼区間や全回路での高速化を示す値ではない。Phase 1 の過去の wall time とは回路・環境・計測区間が異なるため比較しない。

## direct backend と API 例

8 qubit の全 policy を `--backend direct` で実行し、macOS F_NOCACHE を利用した。最大 complex 誤差は 7.08e-17。Linux O_DIRECT の実行結果として解釈しない。

`examples/phase2.py ... --backend direct` では 16Q・1 MiB 状態を 256 KiB の managed budget で扱い、未消化 log を持つ prefix を fork した。ExactPi(1,2) と float 0.3 の suffix の `<Z0 Z15>` はそれぞれ約 0、0.944609090144。両方の managed peak は 246,144 bytes。snapshot v2 の commit も実行した。
