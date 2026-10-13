# Phase 4 ETA／候補／tile／cache：Mac検証

日付：2026-10-14。実装範囲は[先行実装資料](../../phase4_remaining_implementation.md)。cluster／SSH接続、PBS job投入は行っていない。公開時の扱いはREADMEのinternal資料方針に従う。

## 再現

Macの既存uv lock環境、Python／mpi4py／local MPIを使う。追加MPI oracleはn=6、b=2、P=1/2/4、FILE buffered、cooperative、S2、lookahead4、managed RAM4 MiB／rank、SSD quota64 MiB。stateはeager referenceとrtol／atol 2e-11で照合する。各rankは別owner directoryを使う。性能校正や科学計算の所要時間の測定ではない。

```sh
UV_CACHE_DIR=/private/tmp/ddq-uv-cache uv run --offline --no-sync pytest -q

UV_CACHE_DIR=/private/tmp/ddq-uv-cache uv run --offline --no-sync pytest -q \
  tests/test_runtime_eta.py tests/test_remaining_work.py tests/test_runtime_control.py \
  tests/test_remaining_work_mpi.py tests/test_replan_candidates.py tests/test_replan_cost.py \
  tests/test_cache_identity.py tests/test_cache_cost.py tests/test_tile_transpose.py \
  tests/test_tile_extent_writer.py tests/test_parameter_batch.py tests/test_phase4_gradient.py
```

MPI workerは[test worker](../../../tests/mpi_remaining_work_worker.py)、launchは[test](../../../tests/test_remaining_work_mpi.py)。whole subprocess timeoutは120秒。RAM／quotaをperformance適合の証拠とはしない。

## 確認した動作

- finite window／incremental suffix scan、S2 fusionのsource coverage、counted DAGのdependency／resource／巨大countとoverflow。
- STAGEDでもpublished frontierを進めずpublicationを残す。stale、unknown、dirty、未完了request、byte不足とprivate deltaの増加で偽の0秒を返さない。
- injected costを使うrank MAX arithmetic、全rank同じproposal ID、identity候補、単一rankのETA解析失敗をordered voteしてdeadlockを避ける。これらのcostは診断用で、production profileと自動gainには使わない。
- native I/O／MPI tag／queue RAM／private／restart-required capability拒否、candidate数／deadline、全transition費用の一回加算、unknown／quality／context／margin／overflowによる拒否。
- tileのordered／overlapping pair、inverse、logical checksum／padding、hole／overlap／順序逆転／short I/O、RAM不足と旧cut保持。Mac F_NOCACHEも対象testで確認。P=1のactual state／expectation、full blockが入らないb=13/14をsource page＋destination tileで実行する。
- P>1 tileのguardはCOMM_SELFでsize capabilityを変える限定oracle。実P=2/4 tile fragment通信の証拠とはしない。
- cache identityのdependency差／非依存差／ExactPi／PhaseValue、validated costの比較、stale／unknown／unit／overflow fallback、pending I/O pinと失敗writebackのbuffer保持。
- 既存手動replan、batch／prefix／shared budget、三方式のgradient／lifecycle／実Mac MPIの回帰。

## 結果と境界

全回帰は **557 passed、2 skipped、456.41秒**。[log](pytest-full.txt)。このrunの開始後に監視metadata境界、strict bool、PhaseValue identityと関連テストを追加したため、最終変更は別の対象回帰で再検証した。最終対象は **104 passed、36.89秒**：[log](pytest-focused.txt)。Mac内のP=1/2/4、batch、gradientを含む。全回帰557件を最終treeの全テスト件数と混同しない。再現用のcode／lock／log hashは[validation.json](validation.json)。

P4-002／004／006／007はIN_PROGRESS。細粒度stageとheld-out ETA誤差、monitor overhead、candidate automatic cost／durable safe切替／VERIFY、P>1／一般Frameのtile、tile後の勾配と実費用、cache hint／trace RAM課金、reader lifetime／共有／fairnessとLRU paired比較は未実施。Linux O_DIRECTとcross-hostの校正・fault検証も今回の対象外。

過去の[基盤matrix](../phase4_foundation_validation/README.md)はその記録済みbuildの結果であり、今回のbuildのprofile一致や性能改善へ自動流用しない。クラスタで追加検証する際はjob／資源／scratchを具体化して許可を確認する。
