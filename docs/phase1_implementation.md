# Phase 1 実装と検証

[設計書](phase1_design.md) に対応する実装を `src/svsim/` に追加した。Phase 0 の `DeferredSimulator` は RAM 参照実装として維持し、外部記憶実行は `StoredSimulator` から利用する。

## 利用方法

```sh
uv sync
uv run pytest

# 新しい保存先を指定する。既存 state catalog は上書きしない。
uv run python examples/phase1.py --storage /tmp/svsim-phase1-demo --backend direct

# 同じ入力で実行モードと任意 3Q/4Q の分解を比較する。
uv run python examples/phase1_benchmark.py \
  --num-qubits 14 --block-bits 6 --ram-budget 131072 \
  --output /tmp/svsim-phase1-benchmark/report.json
```

`backend='direct'` は Linux では O_DIRECT、macOS では F_NOCACHE。`buffered` も選択できる。Linux の整列条件は呼び出し側設定で検証し、対応していない filesystem では例外を返す。

## 追加した実装

| モジュール | 実装 |
|---|---|
| `storage.py` | alignment/padding を持つ append-only segment、payload checksum、SQLite version/root/frame/snapshot catalog、fork 間で共有する extent allocator |
| `cache.py` | version-keyed LRU、pin、dirty COW write-back、所有権を移す output lease、MemoryBudget |
| `transport.py` | 予約済み整列 buffer に対する read_into/write_from、short transfer、OS 別契約 |
| `journal.py` | canonical Gate/Expr serialization、安定した symbol ID、checksummed JSONL、committed cursor、未完成 tail 修復 |
| `stored_runtime.py` | 局所 epoch、Frame/phase 蓄積、Joint、fusion、layout 転置、snapshot/prefix fork/resume、streaming export、保守的 GC |
| `streaming_kernels.py` | caller-owned buffer に対する 1Q/対角/MCX/小規模 Dense/Joint kernel。状態全体の添字配列・結合を作らない |
| `lowering.py` | 任意 U(2)..U(16) の Givens/Gray-path structured-exact lowering、再構成 residual、source/error 保存 |
| `observable_plan.py` | upper X mask による Pauli grouping、unordered partner pair の両方向の寄与、compensated sum |
| `planner.py` | 規則による partner group、task の RAM 必要量、cold-cache byte 範囲の見積り |
| `telemetry.py` | 逐次 JSONL trace、負荷条件付き帯域比較、継続性・回復条件を持つ observe guard |
| `benchmark.py` | caller-owned buffer での RAM/順次・ランダム I/O 測定、仮想時間の MockTransport |

## 保存・版公開・回復

snapshot で使われる root と extent は immutable。局所 epoch と Joint は子 root に結果を作り、全対象の処理が成功してから実行中の root を切り替える。materialization 中に失敗した場合は、呼び出し前の root/frontier/binding に戻す。

dirty Block を eviction 時に書き戻しても、それだけで durable snapshot とはしない。`commit_snapshot()` は Cache write-back、segment/asset/journal の fsync と directory の同期の後に、SQLite の一つの transaction で manifest を公開する。JSON manifest は export で、再開の正本は catalog の committed row。

`resume(config, snapshot)` は saved cursor までの journal と asset 内容を検証し、保存された Frame/phase を含む cut を再構成する。未実行 suffix は元の semantic gate から再生する。checksum 不一致、短い slot、壊れた committed journal は拒否する。

`commit_prefix(variable_parameters, binding=...)` は、指定 parameter に依存する最初のゲートの直前で停止する。固定 parameter 値、initial state、circuit prefix、layout、dtype、semantics version を fingerprint に含める。`fork(prefix, binding=...)` は元の registered circuit を維持し、共有 prefix を変更せず suffix を別 root と extent に書く。

`collect_garbage()` は他の active runtime/fork がない場合にだけ実行できる。現 run と committed snapshot の root chain を保守的に保持し、未参照の whole segment を削除する。live segment 内の穴の圧縮や、live snapshot の移動は行わない。プロセス crash で残った active-run row の自動清掃は未実装で、通常の resume は可能だが GC は保守的に拒否する。

## RAM 予算の意味

予算は runtime の amplitude buffers、予約済み kernel 入出力、matrix/semantic metadata reserve、観測 accumulator、明示的な全状態出力 allocation を管理する。BufferLease は alignment の余裕に加えて **OS の mmap page 単位への切り上げ**を計上する。macOS arm64 の 16 KiB page で小さな Block を多数持つ場合にも、Block payload のサイズだけで上限を見積もらない。

Python interpreter、NumPy/BLAS の内部領域、SQLite/オブジェクトの allocator 誤差、OS page cache を含む process RSS のハード制限とは区別する。semantic metadata は canonical serialization の保守的な容量見積りと asset bytes で admission を制限する。structured lowering の Python task objects も最大幅 4Q、最大 4096 operation に制限するが、process RSS の精密な上限制御は行っていない。

`state()` は参照比較用で、全振幅と streaming 読み出しの必要量が予算を超えると拒否する。`iter_blocks()` は Block 単位の読み取り専用コピーを返す。返却された配列を呼び出し側が保持するメモリは、runtime の Cache 所有から外れる。全状態を保持したくない処理では `export_state()` と `expectation()` を使う。

## 設計から具体化した選択

- Frame sequence は SQLite の interned registry に保存し、Frame ID を共有する。membership は現在、Block view の disk index と journal の control predicate から解決する。metadata-only epoch でも Block view を逐次更新するため、40/50 qubit 相当の疎索引最適化は今後の範囲。
- 大きな Joint working-set の任意 streaming 分割は行わない。必要な予約量を materialization 前に計算し、収まらなければ状態変更前に拒否する。
- `eager` は source gate ごとの amplitude materialization、`batching` と `deferred-no-frame` は局所 epoch 内の振幅再利用、`per-block-frame` は Frame/Block phase 蓄積を使う。現在 batching と deferred-no-frame の局所実行規則は共通。
- generic Dense が上位 bit を含む場合は partner group、diagonal と CZ は Block-local/phase 経路。MCX の partial exchange は選択振幅だけを計算し、初期版では full slot を搬送する。
- `fallback` は Phase 0 の named exact basis。任意 3Q/4Q の比較には Controlled-U(2)/MCX への `structured-exact` を使う。H/S/T/CNOT/RZ だけの任意 U synthesis ではない。
- `AccessPlan` は amplitude I/O の保守的な cold-cache 範囲。pending dirty Cache の書き出しも upper write bound に含める。comparison report は materialization＋snapshot の実測 byte と照合し、初期 import/export/observable の I/O をこの照合範囲から分ける。
- I/O baseline と runtime は同じ backend identity、device、operation、pattern、size、alignment、concurrency、flush 方針を比較する。未分類の初回 access、基準がない条件、Cache hit は SSD 劣化判定へ混ぜない。cache warmth は不明と記録し、warmup 回数は別の測定条件として報告する。
- observe guard は既定 8 sample/window、3 窓連続で基準比 0.8 未満を劣化、3 窓連続で 0.9 以上を回復とする。計画や layout の変更は行わない。

## 検証

既存 Phase 0 のテストを維持し、以下を追加した。

- 8/10/12 qubit の混合回路、open controls、任意 U、symbolic Binding、各実行 mode の参照一致。
- MCX controls 0..6 と local/upper target、3Q/4Q native/structured-exact、layout 変更後の追加ゲート。
- Cache pin/evict/dirty failure、caller-owned buffer、checksum/短い slot、immutable root/extent。
- Frame と相対位相を残した snapshot 再開、異なる Binding の interleaved COW fork、固定値 mismatch と foreign snapshot 拒否。
- write/flush failure で旧 snapshot を維持、materialization rollback、gate 登録失敗の ID 重複防止、未完成 journal tail の修復、GC の quiescence。
- gather API を禁止した期待値、同じ partner mask の読み出し再利用、Y 位相、layout・Frame・phase の一致。
- 状態 256 KiB / managed budget 96 KiB の実行・export・期待値・snapshot。全状態 `state()` は予算超過を拒否。
- MockTransport による継続低下と observe/plan 不変、calibration と runtime condition の一致。

macOS の実 F_NOCACHE で caller-owned buffer と COW suffix の実行例を確認した。Linux 実 O_DIRECT のテストは Linux で実行する。同期 API の失敗時契約は模擬テストでも検証する。

実測比較は [Phase 1 ベンチマーク](phase1_benchmark.md) に記載する。性能差はここに記録した小規模条件に限る。MPI、非同期処理、tableau Frame、FusedSWAP、分散観測・勾配、自動再計画は後続 Phase の範囲。
