# Phase 4 基盤：Mac内部検証

日付：2026-10-14。公開時の扱いはrepositoryのinternal資料方針に従う。実装と残件は[基盤実装資料](../../phase4_foundation_implementation.md)。今回クラスタには接続せず、PBS jobは投入していない。

## 再現条件

Mac Studio Apple M3 Ultra、32 CPU、512 GiB RAM。Pythonは既存uv lock環境、mpi4py／local Open MPI。bufferedと実F_NOCACHEを確認する。source／uv lock／dirty buildのhash、softwareとthread環境は各raw reportに記録する。

最終matrixは `mac-matrix-final-20261014/` に保存する。n=4、b=2、complex128、seed=1、P=1/2/4、worker／cooperative、buffered／direct、queue上限1/2の24case。caseを逐次実行し、各suiteはwarmup1回＋測定5回。warmupは集計から除く。小規模のcorrectness／計測接続の検証であり、SSD帯域や量子化学の長時間実行を代表しない。

```sh
UV_CACHE_DIR=/private/tmp/ddq-uv-cache uv run --offline --no-sync python \
  examples/phase4_foundation_matrix.py \
  --scratch /private/tmp/ddq-p4-final-matrix-20261014 \
  --output-dir docs/internal/phase4_foundation_validation/mac-matrix-final-20261014 \
  --ram-budget 268435456 --disk-budget 268435456 --wall-seconds 120 \
  --repetitions 5 --num-qubits 4
```

RAM／SSDはcampaign全体で各256 MiB。SSD quotaはPで割って同host ledgerに適用する保守的設定。ledger poolは8 MiB、drain余裕も含める。callback・collectorの費用はexecute wallに含め、startup、eager oracle、state gather／検証は別区間とする。whole-group wallはそれらとcloseを含む。実空きや他jobの占有を制御した性能比較ではない。

FILE suiteは実I/O、MPI suiteはowner bitを使う実payload通信を確認する。P=1のMPI suiteはnot-applicable。eager stateとのrtol／atol 1e-10、norm誤差1e-10以内を検証する。転送量・managed peak・actual backendはtrial別に保存する。group wallはrank MAXで、sample extremaを予測区間にしない。

## pilotと失敗の扱い

最終matrixは全24構成・336 trial（measured 280）で検証成功、collector LOSSゼロ、build source hash一致。最大norm誤差は2.220446049250313e-16。case別の診断値は[結果表](results.md)、raw hashの検査結果は[validation.json](mac-matrix-final-20261014/validation.json)。保存したbuildを現tree／別allocationへ自動一致させない。

`p1-*.json` は初期P=1の4構成、warmup1＋測定2回のsmoke資料で、最終matrixとは集計しない。

`mac-matrix-20261014/` はn=6のpilot。数値検証は通ったが、64 KiB collectorの容量不足でイベントLOSSが発生した。raw資料の増加により旧snapshot取得順ではsource hashの一部もomitされた。pilotは途中で停止し、成功labelにかかわらずprofile／自動cost／最終検証へ採用しない。rawは原因確認用に保持する。

collectorを128 KiBへ増やしてmanaged RAMへ予約し、snapshotはsource／pyproject／uv.lockを先にhashするよう修正した。LOSSがあるcampaignは今後partialとなる。再計測では全caseのbuild source hash一致とLOSSゼロを別途確認する。

初期の回帰ではresume／prefixの同catalog再open、共有observer予算の二重解放、P=4の既存Frame再書込みによるtimeoutを検出した。connection guard、batch／shiftの精算、idempotent Frame internと予約の原子的WRITING保存を修正した。P=4 out-of-coreは強制停止せず診断runでも数値検証まで終了し、修正後は既存120秒のテスト上限でP=1/2/4が通った。

## 受入の境界

最終回帰：`UV_CACHE_DIR=/private/tmp/ddq-uv-cache uv run --offline --no-sync pytest -q` で **469 passed、2 skipped**、438.28秒。[実行log](pytest-final.txt)。Mac内の実MPIも含む。文書18件のlocal参照124箇所とgit diffの空白も確認した。

quota不足のmutation前拒否、native page／side-file cap、rollback、cursor／script／backup、hot journal、取消／short read／callback error、failed group、warmup、profile不一致、observer予算、parameter batch／三方式の勾配をテストする。

Linuxでの同VFS／O_DIRECT、別host校正、全writerのkill／reconcile matrix、独立dataset間のhost coordinator、Frame／packing／metadata別cell、監視overheadの対照は未実施。P4-001／009はIN_PROGRESS、全jobのstrict capacity保証はfalse。クラスタを使う前に具体的なjob・資源・scratchを提示して利用許可を確認する。
