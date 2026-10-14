# Phase 5 計測・時間推定・比較評価の検証記録

2026-10-14。`flow.sh`（Mac、zsh）と`flow_linux.sh`（ent Docker、bash）は、probe → `calibration-suite` → `benchmark` → `calibrate-time` → held-out 4件の`estimate`／`benchmark` → `report`を順に実行する。raw run・plan・model・campaign・reportを保存する。probeとrunにはpathやhost情報が含まれるため内部資料とする。

## Mac（`mac/`）

1 rank、b=4、RAM store、buffered、OMP等1 thread。suite 52件（5 class × n=5..8 × count 1/4/12、空回路を含む）を各3回、held-out 4件を5回測った。全run completedでoracleに合格した。held-out 3件は|相対誤差|0.65〜2.5%（median 0.71%）、interval coverage 3/3。`held-c`はjoint RXが未校正classなので`NO_TIME_PREDICTION`となった。`held-b`はgate数が校正範囲外で、extrapolatedと表示される。

## Linux（`linux/`）

ent Docker（4 CPU、8 GiB、network none）。最初にMacと同じsuiteを600秒の上限で実行したところ、cooperative deadlineで全caseが`partial`になり、model入力から正しく除外された。同じphaseでMacと比べると、executeが約24倍遅かった（pass当たり約160 ms、Macは約6.6 ms）。ext4上で、catalog／storeのcommitごとのdurability syncが原因と推測しているが、未検証である。縮小suite（n=5..7、count 1/4）で再実行した結果は下記に追記する。
