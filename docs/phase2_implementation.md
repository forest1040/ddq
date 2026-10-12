# Phase 2 実装・検証

2026-10-13。対応仕様は[Phase 2 設計書](phase2_design.md)。単一プロセスで、位相を保持する Clifford Frame と Non-Clifford 回転を統合した。Python は uv 環境を使用し、新しい runtime 依存は追加していない。

## 実装した経路

| 機能 | 実装と動作 |
|---|---|
| Clifford | `clifford.py` の immutable bit-packed tableau。符号付き forward/inverse 共役、合成、逆、canonical bytes、symplectic 検証 |
| Frame と位相 | `frame.py` の deterministic synthesis と exact Z8 anchor。tableau が同じでも scalar を保持する。`phase.py` の有理 π 位相と legacy complex multiplier |
| Non-Clifford | T/T† の scalar を分離し、Rx/Ry/Rz・PAULI_ROT を inverse conjugation で base 座標へ移す。局所 Clifford の左合成後も既存 log を再共役しない |
| カーネル | `pauli_rotation.py` の局所 offset pair と上位 XOR partner pair。Y・負符号・source parity を含む。状態全体の添字配列や dense Pauli 行列を生成しない |
| Joint 整合 | 上位だけの H/U 等で input-a/input-b の共通 Frame、相対 phase ratio、差分 Clifford を適用。一般 Joint Dense・下位 control は identity 化した native 経路 |
| 仮想置換 | 上位だけの X/CX/MCX/SWAP を root edge として保存。Frame、phase、log、cursor を入力 label と一緒に移す。QubitLayout は変えない |
| 正確な角度 | `ExactPi(n,d)`、限定した rational arithmetic と既知 sin/cos の証明。π/2 の float を exact witness とみなさない。局所 quarter-turn と上位 Z parity を scalar 込みで Frame 化 |
| Binding guard | 実行済み dependency の値と witness を比較。prefix fingerprint に固定 witness を含める。witness のない Binding hash は旧方式を維持 |
| 保存・再開 | snapshot schema/semantics v2。旧 integer Frame の adapter、immutable extent の共有、v1 circuit hash の維持。root rule と v2 view の checksum を検証 |
| 観測・出力 | 借りた buffer で base log→canonical Frame→scalar の順に解消し、既存 streaming expectation/export を使用。snapshot の保存振幅を観測のために変更しない |

状態表現は `psi_h = beta_h C_h L_h a_h`。`L_h` は局所演算のみで、上位 X を持つ回転は独立した Joint task として一組ずつ処理する。振幅の具体化・Frame 整合では新しい coordinate ID を作り、部分出力の root を公開しない。materialization 失敗時は root/frontier/binding を元の cut へ戻す。

## 位相補助表現の変更

設計の CH-form に代えて、**affine-quadratic path sum と厳密な Z4 Gaussian elimination** を採用した。補助変数の GF(2) support equation と Z4 quadratic form を使い、最小非ゼロ計算基底添字の位相を integer mod 8 で求める。float の atan、近似的なゼロ判定、2**b の補助状態を使わない。

補助計算へ渡す synthesis word は tableau から作り直す多項式サイズの列であり、登録済み gate の履歴長を Frame word として保持しない。`W = omega**q C0` の補正は `omega**(-q)`、左合成で Block phase へ移す値は別の Z8 delta として扱う。`(HS)^3` の identity tableau が持つ scalar も保存する。

## metadata、予算、compaction

共有更新は `root_edges` の条件付き規則で保存する。n=40/50、b=24 相当の H と条件付き T を materialize しても、Block 数分の view row や amplitude buffer を作らない。過去の predicate/parity は edge を遡った **source label** で評価する。

Frame registry は SQLite の `frames_v2` に canonical tableau を intern する。lookup 用 Frame/synthesis cache と共役・更新 memo は byte 制限付き。word と tableau の Python object の大きさも cache admission に含める。lookup は root ID/label だけを待ち列に保持し、parsed rule は一つずつ処理する。

base log は共有 root rule から派生する bounded record 列として保持する。独立した `BaseLogRef` DAG registry には分けていない。隣接する同じ base Pauli を fusion し、source provenance と expression hash を保存する。log 上限は metadata reserve から決めた 1〜64 件。conservative な root-level 上限を保存し、compaction・snapshot 後も延長時に安全な flush を行う。

- `flush_base_logs()` は明示的に Block を走査し、Frame を保持して base log を消化する。
- `compact_metadata()` は一つずつ view を解決して disk catalog を平坦化する。振幅 I/O は不要だが、metadata scan の費用は発生する。
- rule depth は metadata reserve に応じた 8〜256 の制限を持つ。超える前に明示 compaction が必要。Phase 1 の自動 `flatten()` は Phase 2 で行わない。
- 大きな synthesis asset が metadata working space に収まらない場合は拒否する。大きな b の全 Clifford を固定の小さな metadata reserve で処理できるとは保証しない。

managed RAM は予約済み metadata と mmap page を含む amplitude buffer の上限。Python allocator、SQLite 内部、呼び出し側が保持する入力・参照・出力配列を含む process RSS の厳密な上限ではない。比較 CLI はそれらの参照配列の容量と process-global RSS を別に報告する。

## policy と費用モデル

`frame_policy='auto'/'keep'/'materialize'`、`alignment_policy='auto'/'input-a'/'input-b'/'identity'` を追加した。KEEP の対象は対応する局所 Pauli rotation と Frame 更新。下位 control projector の共役を computational predicate のまま扱わず、そのような gate は native fallback を使う。合法でない強制 ALIGN は拒否する。

`calibrate_frames(block_bits=b)` は小規模 b=1〜8 で Frame gate、Pauli rotation、共役、synthesis、tableau update を計測し、`FrameCalibration` を返す。`StoredSimulator(..., frame_profile=profile)` は実行 Block 幅との一致を検査する。

校正がない `auto` は局所回転を安全な native materialization へ送り、Joint では差分 Clifford がゼロと証明できる共通 Frame だけを保持する。校正がある場合は、gate-count、支持の保守的な倍率、共役費用を使って候補を比較する。候補、費用、model/profile version、選択理由を trace に記録する。

これは初期の **CPU 費用 heuristic** であり、設計で列挙した cache warmth、将来 barrier の全 I/O、group fragmentation を精密に予測する最適 planner ではない。I/O の AccessPlan は引き続き保守的な上限を提供する。performance guard は observe-only のまま。

## snapshot と互換性

新規 catalog は `user_version=2` を使う。未知 schema は DDL の前に拒否する。旧 `views`/`frames` に、`views_v2`/`frames_v2`/`root_edges`/log-bound/checksum table を追加する。v1 snapshot は旧 word の位相を exact anchor へ移して読み、元 snapshot と amplitude extent を上書きしない。

新しい `StoredBlockView` は `frame_asset_id`、`phase_value`、`base_log`、`base_coordinate_id` を持つ。`frame_id` は旧 integer Frame 用で、新しい Frame の比較には `frame_asset_id` を使う。Block phase は正規化せず、旧 complex multiplier も近い root-of-unity へ置換しない。

commit は参照 metadata を検証し、segment/cache と journal の sync 後に snapshot を catalog transaction で公開する。古い cut の resume、prefix COW、write/flush fault、checksum fault、virtual permutation 後の GC を検証した。

## 実行・検証

```sh
uv sync --locked
uv run --locked pytest -q
uv run --locked python examples/phase2.py /tmp/svsim-phase2 --backend direct
uv run --locked python examples/phase2_benchmark.py --num-qubits 16 --output /tmp/svsim-phase2-report.json
```

試験は complex amplitudes をそのまま比較し、global phase を合わせない。1〜4Q operator oracle、8/10/12Q native 回帰、ランダムな mixed 8Q、Y・複数上位 X・相対位相、ExactPi/float 区別、log flush/compaction、snapshot v1/v2、観測・物理 layout swap、atomic rollback と metadata 規模を対象とする。

最終の `uv run --locked pytest -q` は **244 passed、2 skipped**。skip は Linux 専用の direct I/O 試験で、今回の macOS 環境では対象外。

Phase 1 の I/O 障害注入・extent allocator 試験は `frame_policy='materialize'` を指定して実書き込みを起こす。遅延操作で不要になった書き込みを仮定せず、従来の障害・独立 suffix の検証を維持した。

Linux `O_DIRECT` と macOS `F_NOCACHE` の抽象化は継続利用する。macOS の direct CLI と、状態 1 MiB／managed RAM 256 KiB の実行を確認した。実測比較とその制約は[比較実験](phase2_benchmark.md)に記載。
