# runtime校正のheld-out envelope（ent Linux、O_DIRECT）

2026-10-15。ent Docker（`ddq-linux:dev-20261014`、`--network none --cpus=4 --memory=8g`）で次を実行した。

```sh
svbench runtime --output runtime-<engine>-p<ranks>.json --scratch <local ext4> \
  --ram-budget 268435456 --disk-budget 1073741824 --wall-seconds 600 \
  --backend direct --engine <worker|cooperative> --ranks <1|2> --repetitions 10
```

2 rankは`mpiexec -n 2`で起動した。各`*.json`の`observations_file`にあるsha256は、展開後のJSONL（`gzip -dc *.observations.jsonl.gz`）の値である。保存時に`gzip -n -9`で圧縮した。結果の解釈は[cost registry資料](../../../phase4_cost_registry_implementation.md)にある。
