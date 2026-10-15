# P4-004 layout migration 候補の実測（Mac・Linux、P=2）

2026-10-15。HEAD `65f268f37e49df817ac38d6a18edfd62c8f5e282`（作業ツリーはclean、`src/`と`examples/`は無変更）。P4-004の`layout_swap`候補を、事前校正したexecutor-classプロファイルで価格付けし、固定（observe）とauto の対比較を行った。設計は[phase4_layout_migration_design.md](../../phase4_layout_migration_design.md)、キャンペーン形式は[phase4_adaptive_replan_implementation.md](../../phase4_adaptive_replan_implementation.md)を参照する。

## 環境

- Mac: Apple silicon arm64、macOS 26.7.1、Open MPI 5.0.11、Python 3.12.13、`.venv`、`mpiexec -n 2`、`PYTHONPATH=src OMP_NUM_THREADS=1`。
- Linux: ssh `ent`（x86_64、i9-12900H、kernel 6.8）、Docker image `ddq-linux:dev-20261014`、`--cpus=4 --memory=8g --network none`、コンテナ内で`mpiexec -n 2`。scratchはホストのローカルext4をbind mountした`/scratch`。repoは`~/ddq-p4004`へrsync（`.venv`と`.git`を除外）。imageにgitが無いため`DDQ_GIT_REV=65f268f...`を渡した。
- 共通: 4qubit、32個のRX(q3)、2 rank、`--resource mpi`、`--repetitions 7 --warmup 2`、injectionは`--injection-delay D`（実際のper-call遅延は`D/2`）。

## 手順

```sh
# Mac（校正は1回、約3秒）
mpiexec -n 2 .venv/bin/python examples/p4_004_layout_migration.py --output mac/profile
mpiexec -n 2 .venv/bin/python examples/phase4_remaining_campaign.py --action adaptive --resource mpi \
  --scenario SCEN --injection-delay D [--executor-profile mac/profile] --repetitions 7 --warmup 2 --output OUT.json --scratch DIR
# Linux（コンテナ内、詳細はlinux/commands.txt）
mpiexec -n 2 python examples/p4_004_layout_migration.py [--backend direct] --output /output/linux/profile
mpiexec -n 2 python examples/phase4_remaining_campaign.py ... --executor-profile /output/linux/profile
```

`summ.py`は表の数値をJSONから集計したスクリプトである。Mac用の実行スクリプトは上記コマンドの繰り返しである。

## 結果

wall中央値は秒。切替/VERIFYは「auto 7 trial中の切替数/VERIFY成功数」。差のCI95はauto−fixedのpaired wall差（負ならautoが速い）。predicted gainはauto trialの`predicted_gain_lower_seconds`の中央値で、実測gainは同じtrial対のexecute_seconds差（fixed−auto）の中央値である。

| 環境 | scenario | D | profile有無 | fixed中央値 | auto中央値 | 切替/VERIFY | 差のCI95 | predicted gain lower | 実測gain | 判定 |
|---|---|---|---|---|---|---|---|---|---|---|
| Mac | sustained | .1 | あり | 6.365 | 1.726 | 7/7 | [-4.681, -4.621] | 2.32 | 4.62 | 陽性（期待通り） |
| Mac | sustained | .05 | あり | 3.568 | 1.260 | 7/7 | [-2.360, -2.269] | 1.12 | 2.31 | 陽性 |
| Mac | sustained | .1 | なし | 6.290 | 6.328 | 0/7 | [+0.026, +0.057] | なし | なし | 切替なし（期待通り）。autoの観測overheadのみ |
| Mac | transient | .1 | あり | 1.056 | 1.101 | 7/7 | [+0.037, +0.054] | 2.30 | -0.05 | 期待と相違。全trialで切替し、わずかに遅い |
| Mac | missing-baseline | .1 | あり | 7.160 | 7.158 | 0/7 | [-0.024, +0.010] | なし | なし | 切替なし（期待通り） |
| Mac | recovery | .1 | あり | 2.296 | 1.731 | 7/6 | [-0.571, -0.535] | 2.31 | 0.64 | 切替は正当。gainは正だが下限を下回る。1 trialがVERIFY失敗 |
| Linux | sustained | .1 | あり | 8.564 | 3.965 | 7/7 | [-4.635, -4.558] | 1.98 | 4.56 | 陽性（期待通り） |
| Linux | sustained | .1 | なし | 9.827 | 9.755 | 0/7 | [-0.072, +0.121] | なし | なし | 切替なし（期待通り） |
| Linux | transient | .1 | あり | 4.659 | 4.795 | 7/1 | [-0.033, +0.183] | 2.49 | -0.19 | 期待と相違。全trialで切替し、VERIFY 6件失敗 |
| Linux | sustained | .2 | あり | 14.548 | 5.401 | 7/5 | [-9.323, -8.697] | 4.64 | 9.23 | gainは大きいがVERIFY 2件失敗のため陽性規則は不成立 |
| Linux | transient | .2 | あり | 4.856 | 5.056 | 7/3 | [+0.183, +0.294] | 4.40 | -0.20 | 期待と相違。有意に遅い |
| Linux（O_DIRECT） | sustained | .1 | あり | 21.661 | 18.165 | 6/3 | [-4.953, -3.015] | 2.64 | 4.11 | 陽性規則は不成立（切替6、VERIFY成功3） |

Linuxのsustained D=.1は保守的な下限でも拒否されず、D=.2は追加の感度確認として実施した。Mac profileをLinuxへ付けた場合は、後述のとおり設定不一致で拒否された。

## 判断理由（`replan_decisions`で確認）

- profileなしのsustained（Mac・Linuxとも）: `layout_swap`候補は全評価で`UNKNOWN_COST_COMPONENT`となり拒否された。他の候補は`INSUFFICIENT_GAIN`、`swap_policy S1`は従来どおり`UNKNOWN_COST_COMPONENT`であった。profileが無い間は価格の不明な候補を選ばない、という設計どおりの挙動である。
- profileありのsustained: `layout_swap [3,0]`が`MEASURED_POSITIVE_GAIN`で唯一のeligible候補となり、`apply`の後に`VERIFIED_AGAINST_PREDICTED_UPPER`（STABLE）で検証された。VERIFYは2 callで成功し、`send_bytes_sum`は8192から2304へ減った（Mac）。
- missing-baseline: `replan_decisions`は空であった。baseline群が無いため評価自体が行われず、切替しない。
- Mac recovery: 注入窓が短く（end=12）、判断時の劣化が窓の終了後に続かないため、実測gain（0.64秒）は予測下限（2.31秒）を下回った。1 trialは`CALL_EXCEEDED_PREDICTED_UPPER`でVERIFY失敗となった。
- transient（Mac・Linux）: 劣化窓（end=6）が判断時点（frontier 7、超過サンプル3件）の直後に終わる。予測下限は観測した超過コストが残り24 callにわたり続くと仮定するため、gain 2.3秒と見積もるが、実測は負となる。この下限は未来の回復を見込めない。これはキャンペーンの期待（切替なし）と異なる結果であり、設計上の限界か要修正かは判断を要する。
- Linuxの`VERIFY_FAILED`（`CALL_EXCEEDED_PREDICTED_UPPER`）: 切替後の1 callが0.083〜0.102秒で、profileのmigrated-layout上限（0.0733秒）を超えた。durable commitの揺らぎで、校正時より遅いcallが出る。sustained D=.1は7/7成功、D=.2は5/7、transientは1/7と3/7であった。
- O_DIRECT: 7 trial中1件（trial 4）は評価がeligible候補を出さず`observation-or-safe-refusal`であった。切替した6件のうち3件はbaselineが`INSUFFICIENT_BASELINE`となり、予測下限が9.52・8.68・3.52秒と大きく揺れた。実測gainはそれぞれ3.05・5.11・1.47秒で、2件が下限を下回った。O_DIRECTのservice揺らぎが下限推定を過大にする。
- 予測下限と実測の関係: sustained（Mac・Linux通常I/O）は全trialで下限が実測gainより小さい（約半分）。transient、Mac recovery、O_DIRECTの一部では下限が実測を上回った。

## profileの束縛に関する検証

- Mac profileをLinuxへ付けた`linux/macprofile_refusal.json`は、全trialが`Executor profile configuration differs from the current runtime allocation/build`で失敗した（fixed・autoとも）。
- 手順上の注意: `docker run --rm`はコンテナごとにhostnameが変わり、probeの`host_id`（hostnameのhash）が変わる。このため初回のLinux校正profileは、同じLinuxでも別コンテナからは同じ理由で拒否された（初回の失敗runは破棄し、再実行した）。以降の校正とキャンペーンは`--hostname ddq-p4004`を固定して実行した。Mac拒否は固定hostnameで再確認した。コンテナ運用ではhostnameの固定が必要である。`src/`は変更していない。
- `examples/`に修正した箇所は無い。

## ファイル

- `mac/`: `profile/`、`calibrate.log`、各runの`*.json`と`*.log`（`sustained_d.1_profile`、`sustained_d.1_noprofile`、`sustained_d.05_profile`、`transient_d.1_profile`、`missing-baseline_d.1_profile`、`recovery_d.1_profile`）。
- `linux/`: `profile/`、`profile-direct/`、`calibrate*.log`、各runの`*.json`と`*.log`、`macprofile_refusal.*`、`commands.txt`（実行スクリプトと`dk.sh`）。
- `summ.py`: 集計スクリプト。
