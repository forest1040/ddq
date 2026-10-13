# Phase 4：回収・cache・勾配資源・collector費用のMac検証

2026-10-14。対応は[追加実装](../../phase4_safety_monitoring_implementation.md)。cluster／SSH／PBSは使用していない。Luna担当の回収・cache・勾配資源検証を統合した最終関連回帰は **132 passed、92.37秒**：[log](pytest-related.txt)。前回の615件全回帰とは実装時点が異なり、今回のtreeの全件総数と呼ばない。

## 再現条件

n=4、b=2、complex128、P=1/2/4、要求buffered/direct。Mac directのresolved identityは全本測定rankから取得したtraceでmacos-f-nocache。host予約RAM512 MiB、managed RAM8 MiB／rank、SSD64 MiB／rank、OMP／OPENBLAS／VECLIB=1、cooperative I/O・worker1、fixed S2・prefetch0。各jobはhard wall180秒、最大同時job1。独立scratchをtrial close後に削除する。外部job／OS cache／background loadは強制制御していない。

```sh
UV_CACHE_DIR=/private/tmp/ddq-uv-cache uv run --offline --no-sync python \
  examples/phase4_followup_matrix.py --action monitor \
  --output-dir docs/internal/phase4_safety_monitoring_validation/mac-monitor-20261014 \
  --scratch /private/tmp/ddq-monitor-20261014 \
  --repetitions 7 --warmup 2 --wall-seconds 180
```

source／worker／lock／Python／MPI／threadのidentityをrawへ保存した。revisionは開始時HEADで、実行時の未commit source／worker byte hashで測定対象を固定する。全6caseとcommit対象のsource／worker hashを照合した。[matrix](mac-monitor-20261014/matrix.json)、[機械可読結果](results.json)、[job進行log](matrix-log.txt)。

## 結果

warmup24 trial、本測定84 trial、合計108 trial。全caseでeager state検証、rank wall MAX、raw hash、source一致、RAM／SSD上限、on/offのcollector pool一致を再照合した。disabledはobservation0、enabledは実observationを持つ。primaryはconstruct・forward・state検証・checkpoint・collector snapshot・close・cleanup込み、executeはforwardのみ。差はon − off。7 pairのbootstrap CIは当該sampleの診断値で、将来の予測区間ではない。

| 構成 | resolved backend | off median (s) | on median (s) | paired差 CI95 (s) | 判定 |
|---|---|---:|---:|---|---|
| buffered-p1 | buffered | 0.276 | 0.278 | [0.0008, 0.0032] | onが遅い方向 |
| buffered-p2 | buffered | 0.664 | 0.679 | [0.0057, 0.0232] | onが遅い方向 |
| buffered-p4 | buffered | 2.412 | 2.438 | [-0.0053, 0.0648] | 未確認 |
| direct-p1 | macos-f-nocache | 0.299 | 0.293 | [-0.0087, 0.0136] | 未確認 |
| direct-p2 | macos-f-nocache | 0.705 | 0.702 | [-0.0140, 0.0043] | 未確認 |
| direct-p4 | macos-f-nocache | 2.452 | 2.456 | [-0.0166, 0.0518] | 未確認 |

| 構成 | collector pool／rank (bytes) | managed peak／rank MAX (bytes) | raw |
|---|---:|---:|---|
| buffered-p1 | 73728 | 550144 | [JSON](mac-monitor-20261014/buffered-p1.json) |
| buffered-p2 | 73728 | 419072 | [JSON](mac-monitor-20261014/buffered-p2.json) |
| buffered-p4 | 73728 | 337152 | [JSON](mac-monitor-20261014/buffered-p4.json) |
| direct-p1 | 73728 | 550144 | [JSON](mac-monitor-20261014/direct-p1.json) |
| direct-p2 | 73728 | 419072 | [JSON](mac-monitor-20261014/direct-p2.json) |
| direct-p4 | 73728 | 337152 | [JSON](mac-monitor-20261014/direct-p4.json) |

on/off両側で同じcollector poolを予約するため、collector無効化によるcache容量増加を差へ混ぜない。trace、旧Guard、I/O／MPI progressとdurable checkpointは維持する。これはcompletion収集とsnapshotの費用対照で、全monitor／auto探索を無効化した対照ではない。execute medianとpaired CIは機械可読結果およびrawへ保存した。

## 回帰の内容と残件

実子processのSQLite予約／partial file fsync後の異常終了、再openの課金維持、二重回収・既記録byte減算拒否、cacheのcapacity refusal／stale cleanup／close精算、shared budgetのbatch／parameter-shift、unknown resource拒否／undersized upper拒否／report改変、collector無効時のvalidator非実行、Mac MPI P=1/2/4のstate／勾配を確認した。

writerの予約／VFS／回収境界は[監査資料](../../phase4_writer_audit.md)に記録する。tokensとauthoritative cutの結合、独立datasetのhost調停、全writer fault、full future plan自動抽出、認証済みauto性能、全stage registry、全monitor overhead、Linux／cross-hostは残る。strict_capacity_guarantee=false、P4-001／007／008／009はIN_PROGRESS。
