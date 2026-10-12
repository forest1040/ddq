# 分散量子回路シミュレータの開発

**開発者向け説明資料：要点・スライド構成案**  
作成日：2026-10-12  
文書の目的：実装詳細の議論に入る前に、開発目的・主要技術・アーキテクチャ・開発優先順位を共有する。

---

## 1. このプロジェクトの基本的な考え方

> **大規模量子回路シミュレーションを、ゲートの数値演算だけでなく、状態ベクトルの配置・移動・遅延実行を統合して最適化する問題として捉える。**

目指すのは、**大きな状態ベクトルを扱い、実行環境に応じてI/O・MPI通信・演算・メモリを最適化できる分散量子回路シミュレータ**である。

- 単一ノードのメモリに収まらない大規模回路を実行する。
- 状態ベクトルをBlockに分割し、RAM・SSD/HDD・リモートノードのRAMを統一的に管理する。
- 可能な限りBlock内の局所演算として処理し、データの読み書きとネットワーク転送を削減する。
- 回路の演算を即時に実行せず、ログと依存関係として保持して、必要時にまとめて適用する。
- ハードウェア構成、回路構造、実行時の実効性能の3段階で実行計画を最適化する。
- 回路実行だけでなく、期待値計算・パラメータ最適化・勾配計算も対象とする。

## 2. 重要なポイント（開発方針）

### 2.1 大規模化とI/O最適化

1. **大きい量子回路を実行したい。** 単一ノードのRAM容量を超える状態ベクトルを扱う。
2. **支配的なコストの一つはデータ移動。** RAMアクセス、SSD/HDD I/O、MPI通信、同期、および振幅演算の総時間を最適化する。
3. **状態ベクトルをBlock化する。** 必要なBlockだけをRAMに配置し、連続するゲートをまとめて実行する。
4. **単一ノードのout-of-coreとマルチノードMPIを共通モデルで扱う。** いずれも「必要な振幅を、必要な計算場所へ集める」問題として考える。
5. **MPI＋RAM＋SSD/HDDの統合構成を基本とする。** ローカルSSDから読むか、リモートRAMから受信するか等をコストで選ぶ。

### 2.2 回路・ゲート・状態表現の最適化

6. **Per-Block Operation Log＋Global Dependency DAG。** Block局所演算は独立に遅延し、Block間ゲートは全体DAGで順序・依存関係を保証する。
7. **Clifford Frameを追跡する。** BlockごとのFrame、共有Frame Group、相対位相を保持し、必要時にFrame整合・materializationを行う。
8. **Native-firstゲート実行。** 全ゲートを一律に分解せず、局所行列、対角演算、置換、MCXなどは専用Kernelを優先し、分解・融合・Joint Kernelを比較する。
9. **Qubit LayoutとSWAP/FusedSWAPを最適化する。** MPI-globalな量子ビットを局所化し、後続ゲートを含めた総通信量・ディスク再配置量を比較する。
10. **パラメータ回路を効率化する。** 記号のまま最適化できる部分を見つけ、値の変更時は共通計画や依存しない計算結果を再利用する。

### 2.3 実行・検証の最適化

11. **ハードウェア・ネットワークを実測して計画する。** CPU/RAM/NUMA/SSD/HDD/NIC/MPIの性能・競合をHardware Profileとして保存する。
12. **実行中も性能を監視して再計画する。** 初期想定よりI/OやMPI性能が低下したら、残りの処理について安全にスケジュールを調整する。
13. **期待値とBackpropagationをBlock並列化する。** Pauli期待値・Blockペアの内積・Adjoint勾配計算を共通Kernelで支える。
14. **実行時間と必要資源を事前推定する。** dry-runでRAM/ディスク容量、I/O、MPI量、Checkpoint、実行時間、ボトルネックを予測する。
15. **正しさ・再開性・実測性能を検証する。** Block版、演算frontier、Frame、Layout、Checkpointの整合性を保証する。

---

## 3. 3段階の最適化

| 段階 | タイミング | 主な処理 | 成果物 |
|---|---|---|---|
| **Stage 1：環境解析** | システム初期化・構成変更時 | ハードウェア発見、RAM/SSD/HDD性能、MPI帯域と遅延、NUMA/PCIe競合、複合負荷計測 | `hardware-profile.json` |
| **Stage 2：静的最適化** | 回路登録時 | 回路DAG、Blockの局所性、Frame/Log、Nativeゲート、SWAP、Checkpoint候補、資源・実行時間推定 | `plan-template.json` |
| **Stage 3：動的最適化** | 回路実行中 | 実効I/O/MPI性能監視、先読み、Block配置、実行順、materialization、代替Plan選択 | `runtime-trace.jsonl` |

**重要な設計方針：** Stage 2では実行順序を一つに固定しすぎず、等価性を保証できる実行候補を複数保持する。Stage 3では、性能変化と再計画コストを比較し、Block版・DAG・Frame・Layoutの整合を維持できる境界で切り替える。

- 想定帯域と実測帯域は**同じI/Oサイズ・キュー深度・通信パターン・並行負荷条件**で比較する。
- 一時的なノイズを誤検出しないよう、移動平均・継続時間・ヒステリシスを使用する。
- 変更候補：先読み深さ、I/O並列度、Block取得経路、Cache退避、ログの具体化時刻、SWAP/Layout候補。
- 実行中のMPI rank数変更など、実行環境により即時変更が難しいものは初期対象としない。
- 実測結果をStage 1のプロファイル更新や次回のStage 2へフィードバックする。

## 4. 統一アーキテクチャの概念

```text
Circuit / Parameterized Circuit
            |
      Semantic Gate IR
            |
   Global Dependency DAG
            |
  Per-Block Operation Logs
  Per-Block Clifford Frames
  Shared Frame Groups / Phase
            |
  Static Planner (Stage 2)
      +-----+------+--------------------+
      |            |                    |
 Native/Fused     SWAP/Layout       Deferred Execution
 Gate Selector     Planner          + Materialization
      +------------+--------------------+
                   |
           Runtime Scheduler
                   |
       Performance Guard (Stage 3)
                   |
         Block Placement / Cache
                   |
   +---------------+-----------------+
   |               |                 |
 Local RAM      SSD / HDD        Remote RAM
                                   (MPI)
   +---------------+-----------------+
                   |
       Block/Joint Gate Kernels
                   |
    Expectation / Adjoint / Output
                   |
      Checkpoint / Trace / Estimator
```

**取得方法の例：** あるBlockの正しい版が必要な場合、(a) local RAM再利用、(b) local SSD/HDDから読み込み、(c) remote RAMからMPIで取得、(d) 同じbase版があればOperation Logの再生、を比較する。これらは**版と依存関係が成立する場合のみ**選択可能。

## 5. 規模と性能について伝えること

`complex128`のdense状態ベクトル1本のサイズは次の通り。

$$
S(n)=16\times 2^n\ \mathrm{bytes}.
$$

| 量子ビット数 | 状態容量（1コピー） |
|---:|---:|
| 32 | 64 GiB |
| 40 | 16 TiB |
| 50 | 16 PiB |

- これ以外に、作業用Block、Forward/Adjoint、Checkpoint、ログ、状態の複製が必要になる場合がある。
- **保存できることと、実用的な時間内に計算できることは別。** 全状態走査回数、MPI転送量、実効帯域がボトルネックとなる。
- データ移動の最小化を主眼とするが、演算時間・同期・スケジューラ自身のオーバーヘッドも含むEnd-to-Endで評価する。

---

## 6. 開発者向けスライド構成案（10枚）

### Slide 1：開発目的 — 大規模量子回路を実行したい

**伝えたいこと：** メモリ容量の壁を越えて大規模な状態ベクトルを扱う。そのためには保存容量だけでなく、データ移動を抑える設計が必要。

- 32/40/50量子ビットの状態容量を示す。
- 主なコストはRAM帯域、SSD/HDD I/O、MPI通信、同期、演算。
- 「最大量子ビット数」だけでなく「時間内に実行できる量子ビット数」を重視。

### Slide 2：基本戦略 — State VectorのBlock分割

**伝えたいこと：** 状態をBlockに分け、BlockをRAMに置いている間に局所演算をまとめて処理する。

- Block-localとBlock間ゲートを区別する。
- 同じBlockの再読み込み・書き戻しを減らす。
- Block間ゲートは必要な複数Blockの共同Taskで扱う。

### Slide 3：MPI＋RAM＋SSD/HDDの統一アーキテクチャ

**伝えたいこと：** Out-of-coreとMPI分散は「Blockの配置・移動」という同じ抽象化で扱える。

- RAM、SSD/HDD、remote RAMを移動コスト付きの配置先として扱う。
- local SSDから取得するか、remote RAMからMPIで取得するかを選択する。
- Blockの版・レプリカ・RAM予約を明示的に管理する。

### Slide 4：Qubit Layout・SWAP/FusedSWAP最適化

**伝えたいこと：** Global qubitを局所化できれば後続ゲートの通信を減らせる。ただし配置変更そのものにもコストがかかる。

- local / disk-crossing / MPI-global bitの3階層。
- SWAP・FusedSWAPによる実データ再配置と論理ワイヤの置換を区別する。
- SWAP後のLayoutを保持し、将来のアクセス削減で再配置費用を回収する。

### Slide 5：遅延演算ログとClifford Frame

**伝えたいこと：** ゲートを即時に振幅へ適用せず、依存関係を保ちながら必要時にまとめて具体化する。

- Per-Block Operation LogとGlobal Dependency DAGを分離。
- Per-Block Clifford Frame / Frame Group / 相対位相を管理。
- Block間演算では共同Task、Frame整合、正しい版の確定が必要。
- **Clifford追跡は演算を遅延できるが、全振幅の具体化コストが消えるわけではない。**

### Slide 6：ゲート実行の最適化 — Native-first

**伝えたいこと：** すべてを小さなユニバーサルゲートへ先行分解せず、ゲートの構造と配置から実行方法を選ぶ。

- Native 1Q/2Qユニタリー、対角演算、置換、MCX（CCX/CCCX以上を統一）、Pauli回転。
- Block-localなら直接実行・融合を優先候補とする。
- 非局所ゲートはJoint Kernel、SWAP局所化、分解のコストを比較する。
- パラメータは記号のまま保持し、値を入れ替えて実行計画を再利用する。

### Slide 7：3段階の最適化

**伝えたいこと：** 環境解析・静的計画・実行時適応を役割分担させる。

- Stage 1：実効帯域・レイテンシ・混合負荷測定。
- Stage 2：回路から複数の合法なPlanを作成し、必要資源を推定。
- Stage 3：実測I/O・MPI帯域と比較して、効果のある範囲で再スケジューリング。
- Runtime Performance Guardによる性能低下検知と安全なPlan変更。

### Slide 8：Block単位の期待値計算・Backpropagation

**伝えたいこと：** 状態ベクトルの実行だけでなく、量子化学・VQE向けの期待値と勾配も分散実行する。

- Pauli期待値：Block局所またはBlockペアごとの部分和＋MPI集約。
- Adjoint勾配：Forward状態とAdjoint状態をBlock分散し、局所内積を集約。
- Pauli作用・Block内積・Partner Blockの読み込みを共通Kernel化。
- 逆伝播の追加I/O・Checkpoint・再計算コストを最適化する。

### Slide 9：Resource Estimator — 実行できるかを事前に判断

**伝えたいこと：** ハードウェア構成と回路から、必要容量・通信・I/O・実行時間の見込みを示す。

- 必要RAM・ストレージ・ノード・NIC帯域。
- SSD/HDD read/write、MPI送受信、全状態走査換算数。
- 代替Planの比較：例えばSSD読み込み vs MPI取得 vs ログ再生。
- 実測値と見積り誤差を記録し、モデルを校正。

### Slide 10：開発ロードマップと検証方針

**伝えたいこと：** 高性能化より先に論理的正しさを担保し、段階的に分散・階層化・実行時最適化を進める。

1. 小規模NumPy参照＋Global DAG / Block Log / Frame / Layoutの意味検証。
2. 単一ノードRAM＋SSDでBlock I/Oと遅延具体化を検証。
3. MPI＋SSDの統合、SWAP、Joint Kernel、State/Log取得選択。
4. 期待値・Adjointとパラメータ回路の反復実行。
5. 実行時Performance Guard、Resource Estimator、32〜40量子ビット級の評価。

**検証指標：** 参照実装との数値一致、全実行時間、SSD/HDD I/Oバイト、MPI通信量、Blockヒット率、再計画費用、Checkpoint再開性、推定誤差。

---

## 7. スライド全体で強調する3つのメッセージ

1. **データ移動を最適化することが、巨大な量子回路シミュレーションの中心課題。**
2. **RAM・SSD/HDD・MPIを統一し、Block分割・遅延実行・Qubit配置・Nativeゲート選択を共同最適化する。**
3. **ハードウェアに合わせた実行前計画だけでなく、実行中のI/O・ネットワーク性能に適応できるシミュレータを目指す。**

## 8. 開発上の留意点

- 遅延実行やClifford Frameは、任意の状態ベクトルの情報量そのものを圧縮しない。
- Blockは一般に独立した量子部分系ではないため、BlockごとのFrameやLogだけで全体系を勝手に独立更新しない。
- 非局所ゲート・観測・Adjointでは依存する振幅のBlockペア／Block群と版の整合を保証する。
- SWAPやゲート分解の最適性は、MPIだけでなくSSD/HDD・RAMのI/Oを含めた総時間で判断する。
- 事前見積りの帯域が実行中も出るとは限らない。動的変更の効果が費用を超えるときだけ切り替える。
- 既存手法と比較し、**通常のBlock batchingに対して追加機能が本当に改善するか**を必ず検証する。

---

## 9. 次の作業

このMarkdownを基に、10枚のスライドへ展開する。視覚素材としては、**Block配置図、Local/Global qubitとSWAPの模式図、遅延ログからmaterializationへの処理図、3段階最適化の循環図、SSD対MPI取得コストの比較例**が有効。
