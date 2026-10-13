# 分散量子回路シミュレータの開発

- **プロジェクト名（仮）**：Deferred Hierarchical Distributed State-Vector Simulator（DH-SVS）
- **更新日**：2026-10-12
- **状態**：実装前の設計仕様。性能向上・研究上の新規性は未検証
- **最新改訂事項**：任意行列ユニタリーのユーザー分解不要なNative-first受付、構造化ゲート分類、シンボリックParameter IR、パラメータ束縛別のPlan特殊化、反復実行での安全な共通prefix再利用、Parameter-shift／Adjoint勾配のI/O-aware選択を初期アーキテクチャへ統合。従来のMCX、Clifford Frame、SWAP、三段階最適化、Performance Guardも維持する
- **主用途**：任意の状態を保持するdense state-vector量子回路シミュレーション
- **推奨構成**：Python 3.12+ / `uv` / `mpi4py`（制御・通信）、NumPy（参照）、必要に応じてC/C++カーネル、Linux `O_DIRECT` / `io_uring`
- **基本方針**：**意味保存Gate IR、任意ユニタリー行列とパラメータ化Gateの受付、Native-first実行（MCX・対角・置換・小規模DenseMatrix）、条件付き分解、シンボリックParameter DAG／値のlate binding、Per-Block Operation Log／Clifford Frame、Global Dependency DAG、Qubit Layout・SWAP Planner、三段階最適化（Hardware→Static→Runtime）、実行中性能検証・安全な再計画は初期設計から必須**。高度な勾配最適化は段階的に実装する
- **数式表記**：すべて `$$ ... $$` のディスプレイ数式で記述

---

## 0. 開発方針の要約

RAMを超える巨大な状態ベクトルを固定サイズのBlockへ分割し、RAM・SSD/HDD・MPI越しのremote RAMに分散配置する。各Blockは**独立した論理演算ログと局所Clifford Frame**を持ち、ゲート適用を可能な限りメタデータ上で遅延する。計算が必要なときだけ、必要なBlock集合をRAMへ揃え、依存関係に沿って一括実行（materialization）する。

本方式の核は以下の11要素である。

1. **Per-Block Operation Log**：Blockごとの局所演算・将来アクセスを記録する。物理的には共有演算や参照を圧縮して保持する。
2. **Per-Block Clifford Frame + Frame Group**：Block内部のClifford作用を独立追跡。共通Frameは共有しCopy-on-Writeで分岐する。
3. **Global Dependency DAG / Cross-Block Task Log**：複数Blockに跨るゲート、厳密な論理順序、版整合、MPI同期を管理する。
4. **Deferred Materialization + Placement Scheduler**：Frameを揃えるか、振幅を実体化するか、ログを送るか、SSD/ネットワーク経由でBlockを転送するかを共同計画する。
5. **Native-first Gate Planner + Fallback Lowering**：入力ゲートの意味を高水準IRに保持し、局所/対角/置換/Pauli/MCX（Toffoli/CCCXを包含）等は専用Kernel・Frame・位相・Block置換として実行する。利用不可または高コストの演算だけをユニバーサル基底へ分解する。
6. **Hierarchical Layout / SWAP Planner**：局所bit、ディスク跨ぎbit、MPI跨ぎbitの配置を変更し、SWAP/FusedSWAPの実データ移動と将来のglobal gate削減を比較する。
7. **Hardware-aware Estimator**：実行DAG、SSD/HDD/RAM/NIC帯域、NUMA/PCIe競合から資源と実行時間を見積もる。
8. **Distributed Observable / Adjoint Engine**：期待値と随伴微分を共通のBlock-pair Pauli内積に集約し、Pauli項・Block・MPI rank・パラメータ反復の4軸でI/Oと通信を再利用する。
8. **Three-Stage Optimization Lifecycle**：初期のハードウェア確認・実測、回路登録時の静的最適化、実行中の動的調整を同一の版付きコストモデルに接続する。
9. **Runtime Performance Guard & Safe Replanning**：実行時のSSD/HDD・MPI・NUMA等の実効性能を初期想定と継続比較し、低下時には予測改善額と安全性を確認して自動／手動で実行計画を変更する。
10. **Arbitrary Unitary Support**：利用者から任意のユニタリー行列を分解せず受理し、形状・ユニタリー性・bit順序を検証する。Block局所・構造化ならnative、必要ならJoint／SWAP／分解を自動比較する。
11. **Parameter-Aware Circuit Template**：ParameterExpressionと依存DAGを保持し、値のlate binding、パラメータ別の安全な特殊化、VQE等の反復実行における共通prefix再利用、勾配手法の選択を行う。

**根本的な注意**：状態ベクトルのBlockは一般に独立した量子部分系ではなく、同一の純粋状態を表す振幅の区間である。Blockごとに独立した**表現・遅延ログ**を持てても、論理状態の独立性やBlockごとの正規化が成立するわけではない。特に非局所ゲートはGlobal DAGに従う共同処理になる。

## 1. 目的・範囲・成功条件

### 1.1 目的

- 単一ノードRAMより大きい量子状態をSSD/HDDで扱う。
- 同一の実行基盤でRAMのみ、RAM+SSD/HDD、MPI分散RAM、MPI+RAM+SSD/HDDを扱う。
- **Block固有の演算列はBlockごとに蓄積・融合・具体化**し、非局所ゲートは依存グラフで安全に制御する。
- Clifford FrameをBlockごとに追跡し、Frame共有・Frame整合・materializationコストを比較する。
- State shippingとOperation/Frame shippingを、版整合・再生費用込みで選択する。
- 入力ゲートの意味と構造属性を保持し、**局所性・ゲート融合・Frame吸収・SWAP配置・I/Oを考慮してnative実行か分解かを選択**する。MCX（0制御=X、1制御=CNOT、2制御=Toffoli、3制御=CCCX、以降も同一IR）は計算基底の条件付き置換として扱い、制御数に応じた巨大な密行列へ変換せずに実装する。未対応の場合は限定ユニバーサル基底にloweringする。
- mpiQulacsのSWAP/FusedSWAP着想を多段構成へ一般化し、global qubitを局所化するための**実際の振幅再配置**と将来の通信削減を共同最適化する。
- ハードウェア構成ごとの必要RAM、SSD本数、NIC帯域、I/O・MPI量、推定時間をdry-runで出す。
- **実行中にSSD/HDD I/O・MPI通信の実効性能が初期Profileや登録時の予測を満たしているか監視し、持続的に悪化した場合は自動または手動で安全な実行計画へ切り替える。**
- **ユーザーへ入力前の任意ユニタリー分解を原則要求せず**、Block-localな小規模DenseMatrixはnativeで処理する。
- **パラメータを数値化する前から代数最適化**し、束縛ごとの適法な特殊化と共通状態の再利用によって、反復実行・勾配評価の総I/O時間を抑える。

### 1.2 主対象と対象外

初期対象はdense state-vector。入力の`H/S/X/Z/CX/CZ/T/Rz/Rx/Ry/SWAP/MCX`（CCX・CCCXもMCXとして正規化）、小規模の任意1Q/2Qユニタリ等について、**高水準のゲート意味を保存してから実行方式を選ぶ**。Block-localで専用Kernelが使えるゲートは原則分解せず、Block外に作用しても対角・置換・条件付き局所作用で済むゲートはその構造を維持する。未対応または分解が有利なゲートは`H/S/T/CNOT`（必要に応じ`Rz(θ)`）へloweringする。`SWAP`の論理作用と物理的なbit配置変更は別扱いとする。任意ユニタリー行列 `U(2^k)` と`Rz/Rx/Ry/PauliRot`等のSymbolic Parameterを追加受付する。密行列はまず1Q/2Q nativeを必須とし、3Q/4Qは閾値付きでnative・分解の両候補を比較する。大きなUは構造化形式（対角・置換・Pauli指数）を優先し、密行列の許容サイズとRAM制限を明示する。固定電子数・固定スピンの量子化学専用表現、テンソルネットワーク、Cliffordのみのstabilizer専用表現は初期対象外（将来のStateSpace backend候補）。

容量は`complex128`なら次の通り。

$$
S(n)=16\,2^n\ \mathrm{bytes}.
$$

| 量子ビット数 | 状態1本 |
|---:|---:|
| 30 | 16 GiB |
| 32 | 64 GiB |
| 36 | 1 TiB |
| 40 | 16 TiB |
| 50 | 16 PiB |

**遅延実行はdense状態の情報量を減らさない。** 最終的に全振幅を出力する場合には、未実行演算・Frameの作用を解消する必要がある。

### 1.3 成功条件

- 8〜16量子ビットではeager参照と状態ベクトル・観測量が許容誤差内で一致する。
- Blockごとに異なるFrame、相対位相、Cross-Block依存を正確に処理できる。
- バッチ化だけの場合と比較し、I/O、MPI通信、全実行時間の改善条件・悪化条件を数値で示せる。
- `SWAPなし / order-preserving SWAP / lookahead＋FusedSWAP` の各モードで、追加した再配置コストを含む正味の速度差を評価できる。
- 強制分解モード（Clifford+T）では合成近似誤差を予算化し、**auto-native（既定）と同じ入力回路の出力精度・通信量・ディスクI/O・総時間**を比較できる。MCXについては制御数`k=0..6`と0/1の制御条件、local／upper-control／upper-target／disk／MPI-crossingの各ケースで、Nativeと参照計算・可能な分解の正確な結果・移動量を比較する。
- 予測したBlockアクセス・通信量と実測traceが対応し、推定実行時間を校正できる。
- **SSD帯域低下・NIC混雑・1 rankだけの性能悪化を意図的に再現し、誤検出を抑えながら性能低下検出→代替計画の選択→安全な切替→改善確認まで自動化できる。**
- **任意U(2)/U(4)のBlock-local／Cross-Block実行と、3Q/4Q小行列のnative-vs-decomposition**で参照一致・行列の妥当性検証・I/O費用を比較できる。
- **同じパラメータ回路を複数回再利用**し、symbolic fusion、parameter binding、依存状態の再利用可否、勾配手法の推定I/Oを計測できる。

## 2. 数理モデル：Block独立Frameはどう成立するか

### 2.1 Block分解

量子ビット数を`n`、Block内ビット数を`b`、Block番号に使うビット数を`k=n-b`とする。計算基底順序を適切に選び、状態を次のように分解する。

$$
|\Psi\rangle=\sum_{h=0}^{2^k-1}|h\rangle\otimes|\psi_h\rangle,
\qquad |\psi_h\rangle\in\mathbb C^{2^b}.
$$

ここで`|ψ_h>`は**未正規化の部分ベクトル**であり、Block間の相対位相を含む。Blockごとに規格化してはいけない。

各BlockへClifford Frameを持たせる場合、整合した論理的なカット（consistent cut）において、次の表現を採用する。

$$
|\psi_h\rangle=e^{i\theta_h}C_h|\phi_h\rangle,
\qquad C_h\in\mathcal C_b.
$$

- `φ_h`：保存されたBlockの基底状態（RAM/SSD/HDDの振幅）
- `C_h`：Block内部の`b`量子ビットのClifford
- `θ_h`：Block間の**相対位相**を保持する値
- `π`：必要なときのBlock番号の論理的な置換（上位ビットのX/CNOT/SWAP等）

Block番号の置換もある場合、整合したカットでの状態は、例えば次の形になる。

$$
|\Psi\rangle=\sum_h|\pi(h)\rangle\otimes e^{i\theta_h}C_h|\phi_h\rangle.
$$

**これは任意のグローバルCliffordがBlock局所演算へ分解できるという主張ではない。** 表現できない非局所ゲートはCross-Block TaskとしてDAGに保持し、実行するときに共同で振幅を更新する。

**非同期実行中はBlockごとの適用済みfrontierが異なる。** 上式は統一された論理的なconsistent cutについて定義し、実行途中の正しい状態は「版付きBlock＋Frame＋位相＋未実行DAG」全体で一意に定義する。あるBlock単独の最新版だけを全体状態の最新版と見なさない。

### 2.2 Clifford FrameがBlock-localである条件

`C_h`が作用するのは**Block内部の下位`b`ビットのみ**。下位ビットへ作用するCliffordはFrameの合成で追跡できる。上位ビットへの対角CliffordはBlock別位相、上位ビットの可逆古典演算はBlock番号の置換として扱えることがある。

| ゲート例 | 正しい遅延表現／実行方法 |
|---|---|
| 下位ビット `H, S, X, Z, CX, CZ` | 各Blockの`C_h`を更新。共通演算はFrame Groupの一括更新 |
| 上位ビット `Z, S, Rz` | Block番号に応じて`θ_h`を更新。任意角Rzは一般にはグローバルCliffordではないが位相管理は可能 |
| 上位ビット `X, CNOT, SWAP`（対象がすべて上位） | Block番号の論理置換`π`として管理 |
| 上位制御→下位標的の`CX` | 条件を満たすBlockの局所`C_h`を更新 |
| 上位ビットの`H` | Block間の振幅混合。単純なBlock-local frame更新では表せない |
| 下位制御→上位標的の`CX` | 下位振幅を条件にBlock間で移動。単純なBlock番号置換ではない |
| Block間依存を持つ一般ゲート | Global DAG上のJoint Block Taskとして遅延・実行 |

`Rz`等の任意角位相は`θ_h`へ吸収できる場合があるが、**Clifford Frameそのものに任意角演算を入れてはならない**。これらは位相メタデータやNon-Clifford Logとして別管理する。

上位ビットのX/CNOTをBlock番号の置換にする場合も、`QubitLayout`と`π`の意味を区別する。ビット配置の再解釈だけで物理ストレージ上の配列が勝手に並べ替わるわけではない。

### 2.3 非局所HadamardでFrameが異なる場合

上位ビットのHadamardで、Block A/Bが振幅混合する場合、

$$
|\psi_A\rangle=e^{i\theta_A}C_A|\phi_A\rangle,\qquad
|\psi_B\rangle=e^{i\theta_B}C_B|\phi_B\rangle.
$$

まずBをAのFrame座標系へ整合させる。

$$
|\widetilde\phi_B\rangle
=e^{i(\theta_B-\theta_A)}C_A^\dagger C_B|\phi_B\rangle.
$$

`C_A† C_B`はBlock内Cliffordなので表現できる。ただし**振幅に作用させる計算・I/Oは必要**となる。その後、

$$
|\phi'_A\rangle=\frac{|\phi_A\rangle+|\widetilde\phi_B\rangle}{\sqrt2},
\qquad
|\phi'_B\rangle=\frac{|\phi_A\rangle-|\widetilde\phi_B\rangle}{\sqrt2}.
$$

として、出力側の共通Frameを`C_A`、共通基準位相を`θ_A`にできる。**演算前の入力Bufferを破壊せずに両出力を計算する**点に注意。

この手順は一つの正しい実装例であり、Schedulerは`C_A`へ揃える／`C_B`へ揃える／双方をidentityへmaterializeする方式をコストで比較してよい。依存関係を含むBlock群を同時確保できなければ、合法な分割実行・ストリーミング方式を計画する。

### 2.4 Frameの独立性と全体Cliffordの違い

各Blockの`C_h`がCliffordでも、全体系の制御演算

$$
\sum_h |h\rangle\langle h|\otimes C_h
$$

がCliffordであるとは限らない。例えば上位ビットによって下位`H`を適用するcontrolled-Hは一般にCliffordではない。

そのため、**Per-Block Frameの集合を全体系の単一Clifford tableauだと見なしてはならない**。初期仕様ではPer-Block Frameを基本とし、Blockに収まらないClifford操作はGlobal DAGに置く。将来Global Residual Clifford Frameを追加する場合は、作用順序・書換え・二重適用防止を別のADRで定める。

## 3. Native-first Gate IRと条件付きUniversal Lowering（初期コア機能）

### 3.1 設計原則：入力ゲートをすぐ分解しない

今回の実行基盤のコストは、単なるゲート数ではなく**RAM上の振幅再利用回数、SSD/HDD I/O、MPI交換、Frame整合、物理bit再配置**で決まる。したがって、入力直後にすべてを`H/S/T/CNOT`へ分解する設計は採用しない。

論理IRでは、入力演算の意味と次の属性を保持する。

```text
High-level GateOp
  op_id, targets, controls, parameters / small matrix
  exact_semantics, decomposition_error_budget
  diagonal, permutation, controlled_permutation, Clifford, Pauli_rotation
  control_values, target_bit, predicate_locality / remote_mixing
  can_apply_block_local, can_apply_joint_block
  dependency_frontier, layout_id
```

`can_apply_block_local`等は固定属性だけではなく、**現在の物理QubitLayoutとPer-Block Frameを反映した後**に再評価する。特にFrameの共役によって1Q回転が複数量子ビットのPauli rotationへ変わり、局所性を失う場合がある。

```text
Circuit / QASM
     |
Semantic Gate Normalizer（入力構造は保持）
     |
Global DAG / Per-Block Operation Log（論理ゲートが正本）
     |
Frame / Phase / Permutation / Layout Analysis
     |
Native-first Execution Planner
     +-- FRAME_UPDATE / PHASE_UPDATE / VIRTUAL_PERMUTATION
     +-- NATIVE_LOCAL_GATE / NATIVE_DIAGONAL / NATIVE_JOINT_GATE
     +-- FUSED_LOCAL_KERNEL / NATIVE_MCX_CONDITIONAL_SWAP
     +-- PHYSICAL_INDEX_BIT_SWAP → NATIVE_LOCAL_GATE
     `-- UNIVERSAL_BASIS_LOWERING（最後の手段）
     |
RAM / NVMe / HDD / MPI + Kernel
```

**演算ログの一次情報は高水準GateOpと依存関係**であり、具体的な物理演算へのloweringはmaterialization計画時に行う。lowering後のExecutionTaskは元の`op_id`から派生した別版とし、**元の論理演算と実行用分解列を二重に実行しない**。

### 3.2 実行候補と優先順位

| 条件とゲート例 | 候補 | 主な理由 |
|---|---|---|
| Block内`H/S/X/Z/CX/CZ` | `UPDATE_LOCAL_FRAME` / 専用native Kernel | CliffordならFrameを更新可能。実体化時も専用演算が有利なことがある |
| Block内の任意`U(2)`、`Rx/Ry/Rz` | `NATIVE_1Q`（2×2）/ 連続ゲート融合 | 1回の振幅ペア更新で済み、Clifford+T合成誤差を避ける |
| Block内の`CNOT/CZ/SWAP` | 構造専用Kernel / Frame / 配列index変換 | 一般の4×4 DenseMatrixより軽いことがある |
| Block内の2Q一般unitary | `NATIVE_2Q`（4×4）/ 小規模融合 / 分解を比較 | 密行列は常に最速ではない。局所性と演算量を測る |
| **MCX、標的・残りの制御がBlock内** | **`NATIVE_MCX_LOCAL`：制御マスクで選んだ振幅ペアだけ交換** | 制御数を問わず小さな条件付き置換Kernelで済む。0/1制御はClifford経路も候補 |
| **MCX、標的がBlock内・一部の制御がupper** | `MCX_CONDITIONAL_LOCAL`：upper制御条件に合うBlockだけ実行 | 残ったlocal制御数が0/1ならX/CNOTとしてFrame更新できる場合もある |
| **MCX、標的・制御がすべてupper** | `VIRTUAL_MCX_BLOCK_PERMUTATION` | 可逆なBlock ID置換として遅延可能。全Blockの置換表は列挙しない |
| **MCX、標的がupper・一部の制御がlocal** | `NATIVE_MCX_PARTIAL_EXCHANGE` / Joint Task | 制御条件に合うoffsetだけBlock間交換し、MPI部分転送・全Block転送・SWAPを比較 |
| Block外・rank跨ぎの`CZ/Z/S/Rz` | 対角位相・Block相対位相メタデータ | 非対角ゲートへ分解すると不要な振幅混合が生じる |
| 上位bitの`X/SWAP/可逆置換` | Block番号・論理ワイヤの置換 | 大きい状態転送を避けられる場合がある |
| Block外の`H`、振幅混合する一般1Q | 対応Block集合の`NATIVE_JOINT_GATE` / SWAP局所化 / 分解 | 分解では通信・I/Oが増える場合もある |
| Pauli-string rotation | 専用bit-mask/位相Kernel / Frame書換え | 巨大な密行列を生成しない |
| 多qubit DenseMatrixや未対応ゲート | `UNIVERSAL_BASIS_LOWERING` / 代替構造化Kernel | 行列のサイズが急増し、分解の方が有利な場合がある |

実行時の選択順序の初期heuristicは次を基本とする。

1. **Frame・位相・論理置換だけで表現可能**かを確認。
2. **Block内で完結する専用native Kernel**または小さな融合Kernelを優先候補にする。
3. 複数Blockなら、**native joint実行／FusedSWAPで局所化／構造化ゲート**を比較する。
4. native未対応、精度条件不適合、または実測コストで有利なときだけ**ユニバーサルゲートに分解**する。

これは無条件の優先順位ではない。**nativeな巨大密行列は小ゲート列より遅い**ことがあり、特に3量子ビット以上の任意行列は慎重に扱う（**MCXのような制御数に依存しない構造化置換ゲート**は例外的にnative化する）。Qulacsも特化型のゲートKernelと一般密行列ゲートを区別し、前者の最適化を行っている。

### 3.3 非局所`CZ`は分解を避けるべき典型例

離れた2量子ビット間の`CZ`は計算基底で対角であり、

$$
CZ\,|x\rangle=(-1)^{x_c x_t}|x\rangle.
$$

従って、制御/標的がMPI-globalなbitでも、ローカルに保持する振幅インデックスから位相だけを決定できる場合、**他rankから振幅を受け取る必要はない**。遅延ログの位相更新として保存してもよい。

これを形式的に

$$
CZ_{c,t}=(I\otimes H_t)\,CNOT_{c,t}\,(I\otimes H_t)
$$

へ先行分解すると、ターゲットのbit配置によっては**本来不要なMPI交換やディスク跨ぎの振幅混合**が発生する。分解後の最適化器が元の構造を必ず復元できるとは限らない。

したがって、**局所ゲートに限らず、対角・permutation・条件付き局所作用という構造情報を失わない**ことが重要である。

### 3.4 MCX（Multi-Controlled X）のNativeサポート方針：CCX/CCCXを一つの演算へ統合

**決定事項：任意個の制御量子ビットを持つ`MCX`を標準Semantic Gate IRおよびNative-first Plannerへ組み込む。** 個別に`CCX`、`CCCX`、`CCCCX`の行列Kernelを増殖させない。入力名は別名として受け付けるが、正規化後は次の対応へ統一する。

| 制御数 `k` | 入力の別名 | 内部表現 | 特徴 |
|---:|---|---|---|
| 0 | X | `MCX(controls=(), target=t)` | Clifford、全振幅対の交換 |
| 1 | CX / CNOT | `MCX(controls=(c,), target=t)` | Clifford。Frameへ吸収できる場合がある |
| 2 | CCX / Toffoli | `MCX(controls=(c1,c2), target=t)` | 非Clifford、条件付き振幅交換 |
| 3 | CCCX / C3X | `MCX(controls=(c1,c2,c3), target=t)` | 非Clifford、同じKernel |
| 4以上 | C4X、C5Xなど | `MCX(controls=..., target=t)` | 任意制御数。Nativeでは追加ancilla不要 |

**0制御と1制御も同一IRに正規化するが、実行Backendまで強制的に共通Kernelにする必要はない。** X/CNOTの高速KernelとClifford Frame更新は有力な特殊化であり、Plannerが選択する。

#### 3.4.1 制御値0/1と正確な数学的意味

各制御ビットが`0`または`1`のどちらで発火するかを`control_values`で保持する（open/negative controlも可）。`controls`と`control_values`は同じ長さ、制御・標的はすべて重複なし、配置先bitも一意とする。`QubitLayout`を通じて物理bit位置`p(c)`へ写した後、次のマスクを構築する。

$$
M=\sum_{j=1}^{k}2^{p(c_j)},\qquad
V=\sum_{j=1}^{k}v_j2^{p(c_j)},\quad v_j\in\{0,1\}.
$$

計算基底の添字`i`に対する条件を次のように定義する。

$$
\chi(i)=\begin{cases}1,&(i\mathbin{\&}M)=V,\\0,&\text{それ以外}.
\end{cases}
$$

すると標的の物理bitを`t`として、正確な作用は次の置換で表せる。

$$
\mathrm{MCX}\,|i\rangle=\left|i\oplus\bigl(\chi(i)2^t\bigr)\right\rangle.
$$

`i`の標的bitが0のときだけ振幅ペアを列挙して交換すれば、**すべての交換が重複なく一度だけ**行われる。制御集合は標的を含まないため、対になる二つの添字で`chi(i)`は同じである。独立制御が`k`個ある`n`量子ビットのdense stateでは以下が厳密に成り立つ。

$$
N_{\mathrm{pairs}}=2^{n-k-1},\qquad
N_{\mathrm{touched}}=2^{n-k},\qquad
f_{\mathrm{touched}}=2^{-k}.
$$

**この`2^{-k}`は演算対象振幅の割合であって、SSD/HDDの読書き量やMPI通信量の削減率ではない。** 読み込むBlockの粒度、packing、scatter/gather、未実行Frameの具体化で総コストは変化する。

#### 3.4.2 Native IR / ビットマスクのデータモデル

```python
@dataclass(frozen=True)
class MCXOp:
    op_id: int
    controls: tuple[int, ...]       # logical qubit IDs
    control_values: tuple[int, ...] # 0/1; controlsと同順
    target: int
    layout_id: int
    dependencies: tuple[int, ...]

@dataclass(frozen=True)
class CompiledMCXPredicate:
    target_bit: int                  # physical index bit
    control_mask: int               # physical bit mask M
    control_value: int              # physical bit pattern V
    local_mask: int
    local_value: int
    upper_mask: int
    upper_value: int
    k_local: int
    k_upper: int
```

元の`MCXOp`は論理回路の**正本**として保存し、物理`QubitLayout`を変更するたびに`CompiledMCXPredicate`を再構築する。`control_values`を整数文字列で受け取る際のbit順はパーサで正規化し、Kernelへ曖昧なendian表現を持ち込まない。Pythonの任意精度整数をメタデータに使い、ネイティブKernelのbit幅制限は明示する。`k>=n`、同じ制御の重複、標的への制御指定は拒否する。

#### 3.4.3 Block内・Block間・MPI間の実行経路

`b`をBlock内bit数とし、物理添字を`i=h*2^b+ell`に分解する。`k_local`は制御のうち下位`b`bitにある個数、`k_upper`は上位bitにある個数とする。

| 位置関係 | 標準候補 | 重要な条件・効果 |
|---|---|---|
| `t<b`、全制御local | `NATIVE_MCX_LOCAL` | 単一Block内の振幅交換。Blockロード後は外部通信なし |
| `t<b`、upper制御あり | `MCX_CONDITIONAL_LOCAL` | Block IDのupper条件を先に判定し、該当BlockだけLocalOpとして処理 |
| `t>=b`、全制御upper | `VIRTUAL_MCX_BLOCK_PERMUTATION` | Block全体を条件付きで入れ替える可逆関数として保持。直ちに振幅I/Oしなくてよい |
| `t>=b`、local制御あり | `NATIVE_MCX_PARTIAL_EXCHANGE` | partner Block間の条件付きoffsetだけ交換。Joint Taskと版同期が必須 |
| `t`がMPI rank bit | `MCX_MPI_PARTIAL_EXCHANGE`/`FULL_BLOCK_EXCHANGE`/`SWAP_TO_LOCAL` | partner rankへの必要な振幅転送。転送規模とpacking費用で候補を選ぶ |
| `t`がSSD/HDD跨ぎbit | `MCX_DISK_JOINT_EXCHANGE`/`SWAP_TO_LOCAL` | 非連続更新のため、論理上疎でも全Block入出力が必要になることがある |

**upper制御の先行フィルタ**：上位制御に一致しないBlockはログ上でno-opとなる。該当Blockでは残った`k_local`制御のMCXだけを実行する。`t<b`で`k_local=0`なら局所X、`k_local=1`なら局所CNOTになるため、Frameが整合する限りClifford Frame更新へ吸収できる。`k_local>=2`は非CliffordのNative条件付き置換を選択する。

**upper targetとpartial exchange**：ターゲットbitが上位の場合、パートナーBlock番号は以下となる。

$$
h'=h\oplus2^{t-b}.
$$

各Block対について、`target=0`側のBlockを一度だけ処理し、upper制御条件に一致する場合に限り、両Blockの同一offset`ell`でlocal制御条件を満たす振幅を交換する。Blockあたりの対象offset数は次のとおり。

$$
N_{\mathrm{active\ offsets\ per\ block}}=2^{b-k_{\mathrm{local}}}.
$$

そのBlock対が別MPI rankにある場合、`complex128`の理論的な**双方向転送ペイロード合計**は次のように見積もれる（通信ヘッダ、packing、alignment、追加転送を除く）。

$$
D_{\mathrm{MPI,pair}}^{\mathrm{packed}}=2\cdot16\cdot2^{b-k_{\mathrm{local}}}\ \mathrm{bytes}.
$$

全Block転送との比較、連続したoffsetのrunsへまとめるか、送受信を集約するかはHardware Profileで校正する。Local制御が多数でもディスク上では**Block全体の読み書きが下界になる実装があり得る**。`k_upper`が多いほど「活動Block対の数」も減るため、Stage 2は単なる`2^{-k}`ではなく**対象Block集合と該当rank対**をsymbolic mask/rangeで評価する。

**全てupperの場合**：条件付きBlock ID置換をビット述語を持つ可逆関数として遅延する。数千万Blockの`block_id -> block_id'`一覧表を構築しない。Frame Group、相対位相、ログfrontier、BlockVersionは**振幅ペイロードと一緒に論理Block参照先が切り替わるよう**管理する。**Block番号置換と物理Index-bit SWAPは別操作**である。

#### 3.4.4 正しさとPer-Block Clifford Frame

MCXは**0制御（X）と1制御（CNOT）だけがClifford**。2制御以上は原則Clifford tableauへ直接吸収しない。ただし、upper制御条件によるBlock選別後、local側の残り制御数が0/1なら、その**選択Blockに限り**Frame更新へloweringできる。

保存している振幅を`phi_h`、論理振幅を`exp(i*theta_h) C_h phi_h`と表す場合、一般のMCXの述語は**論理状態の計算基底**に作用する。`C_h`を無視して`phi_h`へ生のMCXペア交換を適用してはならない。基本fallbackは対象Blockを必要なfrontierまでmaterializeしてFrameを揃えてから交換する。`C_h^dagger MCX C_h`を表現・実行できる場合のみ共役変換を許可するが、変換後が単純な条件付き置換であると仮定しない。

異なるFrameを持つBlock間のpartial exchangeは、**片側だけFrameを解消して同一offsetを交換する設計を禁止**する。対象Block集合を整合したfrontierへ揃え、元の論理基底で条件を判定してから両出力版を公開する。Open controlの判定も必ずレイアウト変換後のbit意味で行う。

#### 3.4.5 Operation Log、SWAP、三段階Plannerとの連携

- **Per-Block Operation Log**：`t<b`で完結するMCXは`LOCAL_MCX`として遅延可能。upper制御に一致しないBlockを個別列挙せず、共有のpredicate付きログ参照で表す。
- **Global Joint DAG**：`t>=b`かつlocal制御がある場合は両Block（rank）の入力版を要求する`JOINT_MCX_EXCHANGE`を一意に登録し、各BlockはTaskへの参照だけを保持する。
- **Stage 1（Hardware）**：`k_local`、Blockサイズ、contiguous-run長、MPIメッセージ長、disk read/writeとpackingが重なった実効帯域をベンチマークする。
- **Stage 2（登録時）**：`NATIVE_LOCAL`、`VIRTUAL_PERMUTATION`、`PARTIAL_EXCHANGE`、`FULL_BLOCK_EXCHANGE`、`SWAP_TO_LOCAL`、`FALLBACK_DECOMPOSE`の合法候補と、将来ゲートを含む費用を見積もる。
- **Stage 3（実行時）**：SSD/NICの実効速度を観測し、未着手Taskについてpartial/full/swap候補を再比較。途中まで実行したJoint交換に別実装を二重適用しない。切替はsafe frontierと版検証が条件。

MCXの標的bitを局所化する物理SWAPは、後続の同じ標的への演算が多いと有利になることがある。一方、`k_local`が大きく活動振幅が少ない単発MCXでは、**全状態を移動するSWAPよりpartial exchangeが安価**な場合がある。MPIバイト数だけでなく、SSD/HDDの全走査・Frame整合・送受信packing・元Layoutへ戻す費用まで比較する。

$$
\widehat T_{\mathrm{MCX}}=\min_{p\in\mathcal P_{\mathrm{legal}}}\widehat T_{\mathrm{critical\ path}}(p;\,\mathrm{I/O,MPI,Frame,Layout,replay,future}).
$$

これは実行DAGの依存とMixed Benchmarkを使った推定であり、同時実行できるディスク時間・MPI時間・CPU時間を無条件に単純加算してはいけない。**追加ancillaを必要とする分解は状態容量を指数的に増やし得る**ので、許可されたworkspace/ancilla予算を超える分解候補を除外する。MCX自体はNative実行なら追加ancilla不要。

#### 3.4.6 カーネルと最小Referenceコード（概念仕様）

```python
def apply_mcx_reference(state, controls, values, target):
    """小規模なNumPy参照。stateは長さ2**nの配列で、in-place更新する。"""
    n = (len(state) - 1).bit_length()
    assert len(state) == (1 << n)
    assert len(controls) == len(values)
    assert len(set(controls)) == len(controls)
    assert target not in controls
    assert all(v in (0, 1) for v in values)
    assert all(0 <= c < n for c in controls)
    assert 0 <= target < n
    mask = sum(1 << c for c in controls)
    pattern = sum(v << c for c, v in zip(controls, values))
    tmask = 1 << target
    for i in range(1 << n):
        if (i & tmask) == 0 and (i & mask) == pattern:
            j = i | tmask
            state[i], state[j] = state[j], state[i]
```

このPythonループは**参照実装専用**。本番Kernelは、control maskとtarget bitから活動添字を算出するvectorized/native方式とする。C/C++ではコントロール位置列挙のオーバーヘッドを減らすため、`0`側ターゲットを含むfree bitへ単調に値を埋め込み、条件一致インデックスだけを列挙する方式も測る。Block間/MPIでは必要な連続runを集めてpackし、Block version / buffer pinning / recvの整合を検証する。

**優先テスト**：`k=0..6`、全1/0/混合制御、ターゲットと制御の任意位置、MCXを2回連続した恒等性、ランダムな複素初期状態、Frame不一致、上位だけのBlock置換、disk跨ぎ、MPI跨ぎ、SWAPあり/なしの等価性、checkpoint再開。Qiskit等の`ctrl_state`を入力するときは、ビット順の違いを正規化する単体テストを必須とする。

**小規模な検算結果（2026-10-12）**：`mcx_block_reference_test.py`により、`n=2..9`、種々のBlock幅・target位置・0〜6制御・ランダムな0/1制御値で、Block分割によるconditional local/partial exchangeと通常dense更新の**1,540件の数値同値性**を確認した。これは小規模のNumPy検算であり、**MPI実通信・SSD/HDD入出力・Clifford Frame混在・SWAP・障害復旧の動作を検証したものではない**。これらは後続フェーズで実装・テストする。

**CCZ/MCZのような多制御位相演算は、対角Gateとして別のNative候補にできる**。ただしMCXを常に`H–MCZ–H`へ分解する方針は採らない。ToffoliのAPIサポートはQulacsに存在し、汎用MCXと負制御の表現はQiskitの`MCXGate`にもあるため、相互運用試験の参照候補となる。実際の階層I/O性能は本設計で別途測定する。

### 3.5 Universal Gate基底はfallback／検証のために維持する

| モード／設定 | 用途 | 内容 |
|---|---|---|
| `--gate-execution auto`（既定） | 本番スケジューラ | 意味保存IRのまま、Frame・native・融合・joint・SWAP・分解をコスト比較 |
| `--gate-execution native-only` | 開発・能力検証 | ネイティブ非対応演算は明示エラー（性能比較用） |
| `--gate-execution force-decompose` | アブレーション | 全対応unitaryを指定基底にloweringしてから実行 |
| `--decomposition-basis clifford-t` | 制限基底の基準 | `H/S/T/CNOT`。一般の任意角回転は指定誤差で近似 |
| `--decomposition-basis clifford-t-rz` | パラメトリック基底 | 上記に`Rz(theta)`を追加。回転を近似T列にしない |

ゲートの意味の等価性を確保する例：

$$
S=T^2,\qquad Z=S^2,\qquad X=H Z H,
$$

$$
SWAP_{a,b}=CNOT_{a,b}\,CNOT_{b,a}\,CNOT_{a,b}.
$$

これらは**等価な代替表現**であり、`SWAP`を必ず3個のCNOT Kernelに展開する指示ではない。`CZ`・`SWAP`・`MCX`・任意角回転は可能ならnativeなまま実行できる。MCXの対応するClifford+T厳密分解は、ancilla・制御数・対応実装を確認した上で比較・代替用に維持する。厳密分解できるMCXに不要な任意角近似誤差を計上しない。

有限のClifford+T列では一般の任意角`Rz(theta)`を厳密表現できない。強制分解では各ゲートの許容誤差`epsilon_j`を記録し、unitary演算列について保守的には

$$
\left\|U_{\mathrm{ideal}}-U_{\mathrm{compiled}}\right\|
\le\sum_{j\in\mathrm{synthesized}}\epsilon_j
$$

を使う。**近似誤差と浮動小数点誤差を区別**し、精度条件に適合しない実行候補は選択しない。

### 3.6 Fusionは「分解しない」とも両立する

同じBlockがRAMにある間に複数の局所ゲートが続く場合、別々のKernel呼び出しではなく**小さい合成演算**へまとめられる。特に同じ対象bitの複数1Qゲートは1個の2×2行列へ合成できる。

$$
U_{\mathrm{fused}}=U_mU_{m-1}\cdots U_1.
$$

ただし合成対象bit数を増やすと、一般の密行列サイズは

$$
2^r\times2^r
$$

となるため、初期実装は`r<=2`を基本とし、`r=3`以上は実測してから許可する。**すべての局所ゲートを巨大なDenseMatrixへまとめない**。

### 3.7 Compiler / Journal / Frame / SWAPとの境界

- 入力の論理ゲートを1回だけJournal/DAGへ記録し、その意味・依存・原ゲートの属性を保存する。
- `Clifford`ならPer-Block Frame / Frame Groupへ吸収するか、後続の局所Kernelへ合成するかを選択する。`MCX`は`controlled_permutation`として分類する。0/1制御およびupper制御でフィルタした結果がlocal X/CNOTになる場合のみ、合法なFrame吸収を選択する。
- `T/Rz`などはFrameによる共役でPauli-string rotation化した後、**変換後の作用範囲とレイアウト**で再度native可否を判断する。
- `LOGICAL_SWAP`、`VIRTUAL_BLOCK_PERMUTATION`、`PHYSICAL_INDEX_BIT_SWAP`は引き続き区別する。レイアウト変更はゲートの分解とは独立に計画する。
- 遅延ログはゲートの高水準意味を保持し、**materialization直前のBlock residency、Frame Group、QubitLayout、Backendのnative対応能力**を確認して実行Kernelを選択する。
- `MEASURE/RESET/CLASSICAL_CONDITION/PREPARE_STATE`はunitary基底へ無理に分解しない。測定結果に依存するDAG辺の扱いを保証する。

### 3.8 任意ユニタリー行列：ユーザーに分解を要求しない（初期コア機能）

**決定事項：利用者は任意の固定ユニタリー行列を回路へ登録できる。** 入力前のClifford+T等への分解は要求しない。シミュレータはSemantic Gate IRとして保持し、**行列の形・構造・対象bitの配置・ハードウェアCost Model**からNative／構造化Kernel／Joint／SWAP／条件付き分解を選ぶ。

`k`量子ビットの密ユニタリーは`2**k × 2**k`で、`complex128`の行列そのものの容量は以下となる。

$$
M_U(k)=16\,4^k\ \mathrm{bytes}.
$$

密行列のサイズ上限は*ゲートを作用させる状態ベクトル容量とは別*に設定する。`k=10`で16 MiB、`k=12`で256 MiB。加えて、行列のApply対象となる`2**k`振幅の揃え方が必要であり、単に行列を保持できても低コストとは限らない。

#### 3.8.1 受付形式と入力の検証

- `DENSE_UNITARY(targets, matrix)`：少数量子ビットの任意複素行列。**1Q/2QのNative実行は必須**。3Q/4Qはnative／分解の実測比較を経て有効化する。それ以上は明示的な資源上限と対応Kernel条件のもとで受理する。
- `DIAGONAL_UNITARY(targets, diagonal)`、`PERMUTATION_UNITARY(targets, permutation)`：密行列へ無理に展開せず構造化データを保持する。既存のMCX・CZ・SWAP・位相規則と共通の実行経路へ接続する。
- `CONTROLLED_UNITARY(controls, control_values, targets, base_unitary)`：条件付きのNative局所適用やJoint Taskを比較する。制御集合と対象集合は重複禁止。入力からユーザーの独自ゲート名を保存してもよい。
- `PARAMETRIC_UNITARY(targets, family, parameter_exprs, descriptor)`：後述のシンボリック表現。実体行列を生成するか、Pauli generator／対角構造で直接Kernelを実行するかをbind時に判断する。

Dense行列の受付時は`shape=(2**k, 2**k)`、全要素finite、複素数dtype、**ターゲットbitの行列添字における順序**、メモリ制限、数値的ユニタリー性を検証する。

$$
\delta_U=\frac{\lVert U^\dagger U-I\rVert_F}{\sqrt{2^k}}\le\epsilon_{\mathrm{unitarity}}
$$

許容誤差はdtypeと規模に応じて設定し、失敗時は警告または登録拒否を選択する。**黙って極分解でunitaryへ投影して入力を変更しない**。ハッシュ・dtype・endianness・入力bit順と共にimmutableな行列アセットを保持し、Journalへ巨大行列を繰り返し埋め込まない。任意のユーザー提供Python式をシリアライズしたり、生の`eval`で実行したりしない。

#### 3.8.2 Nativeと分解の選択

| 性質・配置 | 実行候補 | 主な判定軸 |
|---|---|---|
| `k=1/2`、全対象がBlock-local | `NATIVE_DENSE_UNITARY` / 合成Kernel | 2×2/4×4の直接Apply、既存RAM Blockの再利用 |
| `k=3/4`、Block-local | Native小密行列／構造化分解／Fusion | 演算数・Kernel効率・分解長・RAM容量 |
| 対角／置換、上位・MPI-global含む | 構造化局所位相／Block参照置換／Joint | **非局所でも振幅混合が不要な場合**を保護 |
| 制御はupper、対象だけlocal | 一致Blockに限るNative Apply | upper predicateによるBlock選択 |
| 一般Uの対象がBlock/rankを跨ぐ | `NATIVE_JOINT_UNITARY`／物理bit SWAPによる局所化／分解 | 複数BlockのRAM予約、SSD/MPI通信と再配置の回収 |
| 多量子ビットの一般密行列 | 構造化情報があれば利用、なければ合法な分解 | `4**k`の行列容量、計算時間、合成誤差上限 |

Nativeとして保持する意味は「必ず単一巨大行列Kernelを実行すること」ではない。**入力時に行列情報を失わないこと**で、実際には必要時に小さいゲートへloweringしてよい。任意UのClifford判定・対角判定・置換判定は、確実に証明できた属性のみ最適化へ使う。数値的に近い属性を近似として採用する場合はゲート誤差予算を明示する。

### 3.9 パラメータ回路を第一級のIRにする（初期コア機能）

**決定事項：量子回路の登録時点で具体値が不明なパラメータを記録できる。** `theta`、`phi`、`2*theta+phi`、`-theta`等はシンボリック式DAGとして保持し、回路登録時に数値へ即時展開しない。これは1Q回転だけでなく、Pauli-string rotation、構造化任意ユニタリー、制御回転にも適用する。

```python
@dataclass(frozen=True)
class ParameterSymbol:
    parameter_id: str        # 同名でも識別子は衝突させない
    name: str
    bounds: tuple[float, float] | None = None

@dataclass(frozen=True)
class ParameterizedGate:
    op_id: int
    kind: str                 # RZ / RX / PAULI_ROT / PARAMETRIC_UNITARY ...
    targets: tuple[int, ...]
    parameter_expr_ids: tuple[str, ...]
    structural_class: str     # ALL_VALUESで成り立つ証明済み性質
    generator_ref: str | None
    matrix_asset_ref: str | None

@dataclass(frozen=True)
class ParameterBinding:
    circuit_hash: str
    binding_id: str           # immutable; Hash of canonical values
    values: tuple[tuple[str, float], ...]  # canonical sorted immutable pairs
```

`ParameterExpression`は型付AST（定数、記号、加減乗除、必要な三角関数）で表現し、実行時の任意コード評価を禁止する。`DAG`の各TaskとBlockバージョンへ、**影響を与え得るパラメータ集合の圧縮表現**を紐づける。パラメータの名前だけで同一性を判定せず安定IDを使い、束縛値・回路hash・レイアウト/Frame/依存frontierを一意に識別する。

#### 3.9.1 値が未定でもできる代数最適化

同じ対象量子ビットへの隣接回転は、パラメータが未確定でも融合できる。

$$
R_z(\alpha)R_z(\beta)=R_z(\alpha+\beta),\qquad
R_z(-\theta)R_z(\theta)=I.
$$

同じPauli文字列`P`の回転も合法な順序であれば融合できる。

$$
R_P(\theta)=e^{-i\theta P/2},\qquad
R_P(\alpha)R_P(\beta)=R_P(\alpha+\beta).
$$

パラメータが数値化されなくても**ゲート型、対象集合、対角／置換の保証、Cliffordに共役したときのPauli支持集合**から局所性やSWAP計画を作れる。非可換ゲートの順序を勝手に交換しない。また、一般の行列`U(theta)`には上記の回転等式を無条件に適用しない。

#### 3.9.2 値のBind後にのみ許可される特殊化

- `theta=0`の`Rz`/`Rx`/`Ry`は恒等演算として省略可能。
- `theta`が厳密に`pi/2`の整数倍である`Rz`等は、**対応するCliffordをFrameへ吸収できる可能性**がある。ただしFrameの作用空間／制御付き位相／Block間相対位相を確認する。
- 複数パラメータ値が打ち消し合うときだけ恒等になる式は、その**binding専用のguarded specialization**で削除する。
- 値に依存してGateがidentity/diagonal/Cliffordへ変わる場合でも、**汎用PlanTemplateは元の意味を保持したまま残す**。特殊化Planは`binding_id`や成立条件を伴い、別の束縛へ流用しない。

浮動小数点で「ほぼ`pi/2`」を厳密なCliffordとみなすと演算内容が変わり得るため、**厳密な記号式による一致**を基本とし、近似処理は別の誤差予算で明示管理する。`Rz(2*pi)`の`-I`のようなグローバル位相は、**制御ゲート下では条件付きの相対位相**になり得るので、演算単位で無条件に捨てない。

#### 3.9.3 任意の**学習可能**ユニタリーと、行列要素の直接最適化を区別

「固定の任意行列を入力する」ことと「任意行列の全要素をパラメータとして学習する」ことは区別する。各複素行列要素を独立のパラメータにすると、一般に**unitary制約を破る**ため、そのような最適化を暗黙に許可しない。

`k`量子ビットの、グローバル位相を除く一般的なspecial unitaryは`4**k-1`個の実パラメータを持つ。例えばHermitianなPauli文字列基底`P_j`を使い、次のような構造化パラメータ化をユーザーが選べる。

$$
U(\boldsymbol\theta)=\exp\left(-i\sum_{j=1}^{4^k-1}\theta_jP_j\right).
$$

一般にPauli文字列同士は非可換なので、上式を単純に`R_{P_1}(theta_1) ... R_{P_m}(theta_m)`へ置き換えてはならない。小規模なら行列指数を直接生成してbind時検証、大規模ならユーザーが指定したパラメトリックゲート列（Euler、Pauli rotations等）を**別の明示的な回路ファミリー**として扱う。U全体のglobal phaseは、**Controlled-Uでは相対位相になる**可能性があるので元の意味を保存する。

#### 3.9.4 上位量子ビットのシンボリック位相とBlockログ

`Rz(theta)`の対象bit`q`が**Block外部の上位bit**なら、Block内部の振幅に対して同じ位相が掛かる。`x_q(h)`をBlock番号`h`の対象bit値とすると、Blockごとに次の式を保存できる。

$$
\phi_h(\theta)=\frac{2x_q(h)-1}{2}\,\theta,
\qquad
|\psi_h\rangle\mapsto e^{i\phi_h(\theta)}|\psi_h\rangle.
$$

**パラメータが未確定でも振幅ファイルを更新せず、上位bit条件付きのPhase Ruleとして遅延できる**。同じビットへの連続回転はsymbolicに融合する。Block数が指数的に多い場合も、Blockごとの実体レコードではなく、`(bit-mask, parameter_expr_id, frontier)`の圧縮ルールで保持する。対象bitがBlock内部なら振幅毎に位相が異なるため、Block全体の単一スカラー位相に吸収してはならない。

### 3.10 反復実行：Parameter-aware Deferred Executionと状態再利用

パラメータ回路は、`register`時に**回路構造を一度だけ分析**し、複数のParameter Bindingを渡す**ジョブ群**として扱う。新しいBindingのために全回路を毎回再登録しない。Stage 2が生成する不変`PlanTemplate`は再利用し、Stage 3では束縛値・実効I/O性能・計画候補を使って合法な実行枝を選ぶ。

**安全なキャッシュ再利用規則**：Taskまたはmaterialized Blockの状態を変化させ得るパラメータ集合を`D(v)`とする。Binding `a/b`間でその計算結果を再利用できるのは、対象の全依存パラメータが同じ値で、演算frontier、入力状態、layout/frame等も整合する場合だけ。

$$
\operatorname{reuse}(v,a,b)\Longrightarrow
\left[\forall p\in D(v),\ a(p)=b(p)\right]
\land\ \operatorname{same\_frontier\_layout\_frame\_input}(v,a,b).
$$

**実装はこの十分条件を満たす場合のみ再利用し、未解析の依存は「変化の可能性あり」とみなす。** 入力状態全体の依存関係とBlock間共同ゲートの因果辺を追うこと。BlockごとのLocalOpだけを見て「他のBlockのパラメータに影響されない」と断定しない。

```text
Register circuit once
       |
Parameter dependency DAG + Parametric PlanTemplate
       |
       +-- Binding A: theta=0.1, phi=0.8 -> Job A
       +-- Binding B: theta=0.2, phi=0.8 -> Job B
       `-- Binding C: theta=0.3, phi=0.8 -> Job C
       |
Find provably common prefix / reusable materialized cuts
       |
Shared read-only base state + COW suffix states
       |
Schedule batch with RAM/SSD/MPI budgets, isolated binding versions
```

例えばパラメータと無関係な回路のprefixを一度具体化してSSDにCOW checkpointし、異なるBindingのsuffixへ共有できる。prefixが各Bindingで本当に一致すること、read-only baseへの書込み禁止、再生ログ・Frame・Layoutとcheckpoint世代の一致が必須。`K`回実行する場合の単純な性能モデル例は以下。

$$
T_{\mathrm{without\ reuse}}\approx
T_{\mathrm{compile}}+K(T_{\mathrm{prefix}}+T_{\mathrm{suffix}}),
$$

$$
T_{\mathrm{with\ reuse}}\approx
T_{\mathrm{compile}}+T_{\mathrm{prefix}}+T_{\mathrm{snapshot}}
+K(T_{\mathrm{restore}}+T_{\mathrm{suffix}}).
$$

上式はsuffix費用が各Bindingで等しい単純化であり、**checkpoint読込み・SSD容量・COW書戻し・遅延ログ再生・rank間同期費用**を必ず足す。多数Bindingの状態を同時保持するとdense状態容量も倍増するため、Batch数はRAM/SSDを考慮して制限する。共有prefixがない回路では、PlanTemplateとカーネルだけを再利用する。

### 3.11 勾配・VQE最適化との接続（初期はAPI、詳細実装は段階的）

期待値の最適化を対象に、**回路値評価と勾配評価を同じParametric DAGで管理**する。

- `Parameter-shift`：`R_P(theta)=exp(-i*theta*P/2)`のような**1つのPauli回転ゲートを対象にした場合**、期待値の勾配は次の2評価で計算できる。

$$
\frac{\partial f}{\partial\theta}
=\frac{f(\theta+\pi/2)-f(\theta-\pi/2)}{2}.
$$

  同じ記号パラメータが**複数ゲートに現れる場合は、それぞれのゲートに対する偏微分を求め、Chain Ruleで合計**する。任意のブラックボックス`U(theta)`へこの2点則を無条件に当てない。より一般のgenerator固有値構造には一般化shift rule、ない場合には導関数記述／有限差分等の代替が必要。
- `Adjoint differentiation`：dense state vectorの前進計算と逆向きの随伴計算を利用する。ただしout-of-coreでは**逆走査とcheckpoint/recomputeが大量I/O**となり得る。Parameter-shiftの複数Jobの並列実行／共通prefix再利用と、Adjointのストレージread/write・メモリ・通信を比較して選ぶ。
- `Derivative-aware custom unitary`：`U(theta)`を受け取る場合、`generator H`や`dU/dtheta`などの検証可能な導関数情報をオプションで受理する。**数値行列しかないブラックボックス**ならsymbolic最適化や解析勾配を約束せず、Bindingごとに行列生成・ユニタリー検証をする。
- `Observable batching`：一つのBindingについて複数Pauli期待値を求める場合、**同じ状態を使って複数Observableをstreaming/batching評価**する。Observable間で局所性と必要Block partnerを共有し、無駄な状態生成を避ける。

推定対象は`parameter_bind_count / bindings_per_plan / dependency_frontier / prefix_reuse_bytes / restored_bytes / gradient_method / gradient_evaluations / adjoint_checkpoint_IO / total_objective_time`とし、**1回の回路実行だけでなくVQEの複数反復・勾配評価全体のEnd-to-End時間**を比較する。

**参考実装**：Qulacsの`ParametricQuantumCircuit`はparametric RX/RY/RZ、multi-Pauli rotation、`set_parameter`、`backprop`を提供する。Qiskitの`ParameterExpression`と`assign_parameters`はsymbolic template/bindingの参考になり、PennyLaneは一般化Parameter-shiftと任意ユニタリーのパラメータ化を提供している（後述の参考リンク）。いずれも**今回のPer-Block/SSD/MPI状態再利用の性能保証ではない**。

### 3.12 分散期待値計算（Distributed Expectation Engine：初期コア機能）

**期待値計算を最終状態ベクトルの全体Gather後の処理としない。** `ObservableIR`を回路と独立した入力として登録し、Per-Block Log/Frame/Global DAG上の指定論理frontierに対する**分散Reduction Task**を生成する。対象はまず、実係数のPauli和として表せるHermitian observableと、対角observableをサポートする。任意の大きなHermitian行列は高水準IRを保持して、特殊構造が無い場合には限定サイズへ分解または明示的に非対応とする。

#### 3.12.1 Pauliをビットマスクで表現する

Pauli stringを、X/Yのビット反転集合`x`、Z/Yの位相集合`z`、Yの数`nY`で表現する。

$$
P=i^{n_Y}X^xZ^z,
\qquad P|i\rangle=i^{n_Y}(-1)^{\mathrm{popcount}(z\land i)}|i\oplus x\rangle.
$$

演算子の積の順序はこの式で**固定**する。論理bitから物理bitへの`QubitLayout`を確定してから`x/z`を生成する。ビット幅が64を超えても正しく扱えるよう、IRは可変長bitsetとし、カーネル用の複数word表現へloweringする。

Block内部`b`ビットとそれより上位のBlock IDに分ける。

$$
x_L=x\bmod 2^b,\quad x_H=\left\lfloor\frac{x}{2^b}\right\rfloor,\quad h'=h\oplus x_H.
$$

任意の2つの状態のBlock列`a`/`v`について、Pauli内積へのBlock `h` の寄与は、

$$
\mathcal I_h(a,P,v)=i^{n_Y}\sum_{\ell=0}^{2^b-1}
(-1)^{\mathrm{popcount}(z\land(h2^b+\ell))}
\overline{a_{h',\ell\oplus x_L}}\,v_{h,\ell}.
$$

$$
\langle a|P|v\rangle=\sum_h\mathcal I_h(a,P,v).
$$

- `x_H=0`：Blockは独立。各Blockの内積だけを計算する。MPIは最終Reductionのみ。
- `x_H!=0`：Block `h`と`h xor x_H`の**ペア**を使う。rankが異なる場合のみ必要な振幅をMPI送受信する。`z`の上位bitだけならBlock番号から位相が分かり、振幅交換は不要。
- Pauli項の数が多い場合、**まず`x_H`でグループ化**し、同じPartner Blockの取得（RAM Cache / SSD read / MPI fetch）を共有する。同じ`x_L`も合わせてグループ化すればカーネル内の再利用も可能。
- 一つのBlock pairを1回だけ評価し、Hermitian性を使って共役側の寄与を補う方法も選択可能。ただし`i^{nY}`と位相を含む正しいpairingを単体テストで証明する。最初は全`h`を一意に加算する参照実装を用いる。
- rankはPauli項ごとの**部分複素内積**を蓄積し、必要な全Blockの寄与を計算後に`MPI.Allreduce`する。期待値の最終出力は実数とし、虚部の大きさを数値誤差検査に使用する。

`H=Σ_j c_j P_j`を一括評価する際は、`ObservableTask`ごとに全状態を再度ロードするのではなく、`PauliPartnerGroup(x_H)`を軸にした`BlockPairTask`の連続実行を基本とする。複数observableの共通Pauli項を集約し、`ObservableID -> weighted terms`のSparseReductionで結果を復元する。

#### 3.12.2 Per-Block Clifford Frameで未具体化のまま測る

Block `h`の振幅表現が、

$$
|\psi_h\rangle=e^{i\theta_h}C_h|\phi_h\rangle
$$

の場合、Block対のobservable作用は、必要な上位bitの位相を含む局所作用`O_{hh'}`に対して、

$$
\langle\psi_h|O_{hh'}|\psi_{h'}\rangle
=e^{i(\theta_{h'}-\theta_h)}
\langle\phi_h|C_h^\dagger O_{hh'}C_{h'}|\phi_{h'}\rangle.
$$

- 同じBlock（または同じFrame）なら、Clifford共役で局所Pauliを別のPauliへ変換できる場合がある。
- **異なるFrame**では`C_h† O C_h'`が単一Pauliとは限らない。frame-aware native kernel、片側Frame整合、双方materializeの3候補を評価する。
- 相対位相`θ_h`を落とすと、非対角observableの結果が誤る。Blockごとに正規化しない。
- 観測のために全状態を強制的に最新にする必要はないが、対象の**一貫した論理frontier**に対応するBlock版・Log・Frameを正しく解決しなければならない。

#### 3.12.3 Observable IRと主なタスク

```python
@dataclass(frozen=True)
class PauliTerm:
    x_mask: int            # prototype: Python int; native backend uses multiword
    z_mask: int
    y_count: int
    coefficient: float

@dataclass(frozen=True)
class ObservableRequest:
    observable_ids: tuple[str, ...]
    logical_frontier: str
    state_version: str
    terms: tuple[PauliTerm, ...]

# Task types:
# GROUP_PAULI_PARTNERS / PREFETCH_PARTNER_BLOCK
# EVALUATE_LOCAL_PAULI / EVALUATE_PAIRED_PAULI
# REDUCE_PARTIAL_OBSERVABLE / VALIDATE_HERMITIAN_RESULT
```

`ObservableRequest`に必要なBlock版と原ゲート/パラメータBindingの整合を要求する。期待値だけ必要なジョブでは、全振幅の収集・出力を省略する。

### 3.13 分散Adjoint Differentiation（Backpropagation：初期コア設計）

期待値の随伴微分を**Forward StateとAdjoint Stateの2つの分散状態ストリーム**として扱う。途中測定を伴わないユニタリー回路に対して、まず逆ゲートによるForward状態の再構成方式を採用する。測定、非ユニタリー写像、確率的ノイズが存在する場合は無条件に逆再生せず、対応可能な別の微分方式を選ぶ。

#### 3.13.1 状態の定義と一般ゲートの勾配

$$
|\psi_j\rangle=U_j|\psi_{j-1}\rangle,\qquad
|\lambda_m\rangle=O|\psi_m\rangle.
$$

$$
|\lambda_{j-1}\rangle=U_j^\dagger|\lambda_j\rangle,\qquad
|\psi_{j-1}\rangle=U_j^\dagger|\psi_j\rangle.
$$

$$
\frac{\partial E}{\partial\theta_j}
=2\operatorname{Re}\left\langle\lambda_j\left|
\frac{\partial U_j}{\partial\theta_j}
\right|\psi_{j-1}\right\rangle.
$$

Pauli回転の場合は、

$$
U_j(\theta_j)=e^{-i\theta_jP_j/2},\qquad
\frac{\partial E}{\partial\theta_j}=
\operatorname{Im}\langle\lambda_j|P_j|\psi_j\rangle.
$$

**この勾配内積を、期待値と同じ`BlockPauliInner(bra, ket, mask)`で計算する。** `bra=lambda_j`、`ket=psi_j`が異なることだけが相違点。ゲートのgeneratorが全てBlock-localなら部分勾配はBlockごとに独立計算できるが、演算履歴中のglobal gateによる**前後の状態伝播**は別途Block間通信を必要とする。

パラメータを複数のゲートが共有する場合には、値のBindingを式DAGとして保持し、

$$
\frac{\partial E}{\partial\alpha_p}
=\sum_j\frac{\partial E}{\partial\theta_j}
\frac{\partial\theta_j}{\partial\alpha_p}
$$

を正しく適用する。計算したゲートごとの勾配をrank内で`parameter_id`へ加算し、全パラメータの局所勾配がそろった段階で**勾配ベクトルをまとめてAllreduce**できる。ただし逆伝播の非局所ゲート自体が必要とするMPI交換は省略できない。

#### 3.13.2 実行DAGとメモリコスト

```text
FORWARD distributed state / Per-Block Log + Frame + Layout
                |
      EVALUATE_OBSERVABLE (H=sum c_j P_j)
                +-- expectation reduction
                `-- BUILD_ADJOINT_STATE: lambda_m = H * psi_m
                |
REVERSE DAG (read last gate first; exact input/output version pair)
     |          |
     |          +-- psi_j -> U_j dagger -> psi_(j-1)
     |          `-- lambda_j -> U_j dagger -> lambda_(j-1)
     |
     +-- PARAM_GATE: BlockPauliInner(lambda_j, P_j, psi_j)
     +-- NONLOCAL_GATE: acquire paired block groups for BOTH state streams
     +-- FRAME/LAYOUT: align or apply inverse transformations consistently
                |
      LOCAL_GRADIENT_VECTOR -> MPI.Allreduce -> GRADIENT RESULT
```

`complex128`の状態容量を`S(n)=16*2^n` byteとすると、ユニタリーAdjointは原則として現在のForwardとAdjointの**2状態分**の保存が必要であり、

$$
S_{\rm adjoint,base}(n)\approx 2S(n)=32\,2^n\ \mathrm{bytes}.
$$

40 qubitでは基本状態が**32 TiB**、50 qubitでは**32 PiB**となる。これは`O|ψ>`生成の作業バッファ、Block cache、COW/snapshot、ログ、checkpoint、rank間通信バッファを含まない。ストレージで2本を管理する場合は、通常計算とは別の**SSD容量、持続読書き帯域、write amplification**を必ず評価する。

保存した全中間状態は必須ではない。ユニタリー逆再生は`U_j†`で現在のForward状態を復元するが、out-of-coreでは**逆方向の全状態走査**が高コストとなる。Checkpoint/recompute（Revolve類似）、逆再生、Parameter-shift並列ジョブを同一コストモデルで比較する。単純にcheckpointを増やせば速くなるわけではなく、SSD書き込み・容量増分を評価する。

原回路の**論理SWAP**と最適化器の**物理Index-bit SWAP／Layout変更**を区別し、逆DAGはそのPlanVersionの逆LayoutTransitionを保持する。ForwardとAdjointで`layout_id`、`frontier_id`、`frame_id`、Block版の整合が取れない状態で部分内積を計算しない。

#### 3.13.3 任意の微分可能ゲートと複数Observable

- `R_P(theta)`：Pauli Kernelの再利用を優先。
- パラメータ付きNative行列：明示的な`dU/dtheta`またはgeneratorを受理して、必要なBlock集合で`2Re<lambda|dU|psi>`を実行。
- 固定任意U：微分対象でなければ逆ユニタリーのみを使用。
- パラメータ付きブラックボックスUで導関数不明：一般の2点Parameter-shiftを勝手に適用せず、提供された代替分解、対応する一般化shift rule、または精度管理した有限差分へfallback。
- **複数の出力Observable**：線形結合の単一スカラー目的関数なら`lambda_m=H psi_m`を1本構築できる。独立した多数Observableの全Jacobianが必要なら、Observable別の随伴状態・逐次再利用／複数bra・再実行のメモリ/I/OトレードオフをPlannerが選ぶ。
- `Rz/Pauli`パラメータについて、観測量のHeisenberg逆伝播演算子とgeneratorが厳密に可換と証明できるときは、そのゲートの勾配を0として省略できる。**数値近似だけでゼロ扱いしない。**

### 3.14 Expectation + Gradientの共通Block Runtime

期待値と随伴勾配の両方を`BlockInnerProductKernel`、`PauliActionKernel`、`PartnerBlockFetcher`で実現する。

| 構成要素 | 期待値 | Adjoint勾配 |
|---|---|---|
| `BlockPauliInner(a,P,v)` | `a=v=psi` | `a=lambda_j, v=psi_j` |
| Block partner | `h xor x_H` | 同じ |
| Per-Block Frame | observable側の変換／align | 両状態の対応するFrameを確認・align |
| Reduction | 実数期待値配列 | パラメータ別の実数勾配配列 |
| 同期 | Block-pair frontier、最後にreduce | 各逆ゲートのjoint依存、最後にreduce |
| I/O | 多数Pauli項で同じBlock pair再利用 | 2状態の逆方向走査、generator pair再利用 |

**推奨スケジューリング**：局所性を保てる範囲で、`(partner mask, block group, state version, frontier)`をTaskの索引とする。Pauli項別のループを外側に置いて全状態を毎回読み直す方式は避ける。ただし必要Block groupがRAMに収まらなければチャンク化し、将来参照順序とディスク帯域から合法な処理順序を決める。

新しい最小API案：

```python
register_observable(observable_ir) -> ObservableID
estimate_expectation(circuit_id, observable_id, binding_id, hardware_profile)
evaluate_expectation(circuit_id, observable_id, binding_id, frontier) -> float
estimate_gradient(circuit_id, observable_id, binding_id, method='auto')
evaluate_gradient(circuit_id, observable_id, binding_id, method='auto') -> dict

# internal:
fetch_partner_blocks(frontier, x_high, layout, states=('forward',))
block_pauli_inner(bra_block, ket_block, pauli_mask, block_id)
reduce_partials(partials, reduction_id)
```

数値的安定性のため局所和にpairwise/Kahan等を選択可能とする。rank間ではReductionの順序による丸め差があるため、再現性重視モードでは固定木Reductionを検討する。float32/complex64は精度要件を別途校正する。

### 3.15 勾配方式のI/O-aware自動選択と正しさの境界

**比較する方式**：`
ADJOINT_REVERSIBLE` / `ADJOINT_CHECKPOINT_RECOMPUTE` / `PARAMETER_SHIFT_BATCH` / `GENERALIZED_SHIFT` / `FINITE_DIFFERENCE_FALLBACK`。

$$
T_{\rm adjoint}\approx
T_{\rm forward}+T_{H\psi}
+T_{\rm reverse\;two\;streams}+T_{\rm gradient\;pairs}+T_{\rm IO/sync},
$$

$$
T_{\rm shift}\approx
T_{\rm shared\;prefix}+
\sum_{j\in\text{shifted gate occurrences}}
\left(T_{j,+}+T_{j,-}\right)
+T_{\rm restore/sync}.
$$

これらは構成要素の**記号モデル**であり、単体帯域を単純に加算せず、Stage 1で採取した`2-stream forward+reverse / mixed SSD+MPI / Pauli pair fetch / gradient Allreduce`を用いて校正する。同じ論理パラメータが複数ゲートに現れるとき、通常の単一ゲート2点則だけでシフト評価回数を決めない。

- 大量パラメータ＋単一スカラーobjective＋十分な帯域ならAdjointが有力。
- 少数パラメータ、十分なrank/ノード余剰、共有prefixが長い場合はParameter-shiftの独立Jobが有利なことがある。
- I/O帯域不足で2状態の逆走査が負担になる場合はcheckpoint/recomputeや別方式を候補とする。
- 途中測定、ノイズ、適用できないgeneratorの場合は**正しい微分法が存在するか先に検証**し、Adjointを無理に適用しない。
- 方式の途中切替は、整合した状態snapshotと依存frontierがあり、重複計上・中間結果破壊が起こらない場合だけ許可する。

**参照PoC**：`distributed_expectation_adjoint_reference.py` はNumPy上の小規模状態について、PauliごとのBlockペア内積と、rank所有Block別の部分和（MPI Allreduceの**模擬**）、逆再生型Adjointの部分勾配を検証する。**MPI実通信・SSD I/O・Per-Block Frame実装を含む実行エンジンではない。** 実装者はこのファイルをリポジトリの`tests/reference/`へ取り込んでから本体実装を進める。

## 4. 三段階の最適化・推定ライフサイクル（初期から必須）

本シミュレータは**同じ最適化を毎ゲート繰り返さない**。最適化のタイミングを次の3段階に分離し、各段階で利用可能な情報・変更可能な対象・成果物を明確にする。

| 段階 | タイミング | 主な入力 | 決めること | 永続化する成果物 |
|---|---|---|---|---|
| **Stage 1: Hardware Discovery & Calibration** | システム初回起動時／構成変更後の再測定 | ノード、RAM、SSD/HDD、NIC、MPI、NUMA、実測値 | 実行資源と転送・計算コスト、使用可能な制約 | 版付き `HardwareProfile` |
| **Stage 2: Static Circuit Planning** | 量子回路登録時 | Circuit IR、Global DAG、Block/Frameモデル、`HardwareProfile` | Native/分解、Blockサイズ、初期Layout、候補SWAP、遅延バッチ、依存境界、配置計画の骨格 | 版付き `PlanTemplate`、`StaticEstimate` |
| **Stage 3: Adaptive Runtime Scheduling** | 実行時、タスク完了点・安全なfrontier | `PlanTemplate`、Block実在位置、実測帯域・遅延、キュー、進捗 | 想定性能の充足判定、prefetch・materialization・MPI/SSD取得元・Frame整合・SWAP・再配置の安全な切替 | `RuntimeProfile`、性能アラート、計画変更履歴、`ExecutionTrace`、更新済み残時間予測 |

```text
                        SYSTEM START / CONFIG CHANGE
                                   |
                      [1] Hardware Probe + Benchmark
                                   |
                       hardware-profile.json (version)
                                   |
CIRCUIT SUBMIT -> [2] Static Compiler / DAG / Layout / Swap / Cost Search
                                   |
                      plan-template.json + estimates
                                   |
                              JOB EXECUTION
                                   |
             [3] Runtime Telemetry <-> Adaptive Scheduler
                   |                   |              |
                Cache               MPI / I/O      Frame / Swap
                   +-------------------+--------------+
                                   |
                         Trace / Feedback / Checkpoint
                                   |
                          Profile recalibration next run
```

**「3回だけ最適化する」という意味ではない。** Stage 1は起動時の検出を基本とし、ハードウェア変更や性能ドリフト時は再測定する。Stage 2は回路登録・再登録時に計算量の大きい探索を許す。Stage 3は実行中に繰り返す**安価で制限された動的調整**である。

### 4.1 Stage 1：ハードウェア・ネットワーク構成の確認と校正

まず物理構成とランタイムで利用可能な資源を検出する。最低限、ノード数とrank配置、CPU/命令セット、NUMA配置、各rankの実利用可能RAM、ローカル/共有SSD/HDDとファイルシステム、Direct I/O可否・alignment、デバイスごとの空き容量、PCIe接続と共有経路、NIC種類・リンク速度・RDMA/InfiniBand/Ethernet、ネットワークトポロジ、MPI実装・バージョンを収集する。**物理RAM全量と、ジョブに割り当て可能なRAMは区別する。**

ハードウェア仕様だけでは予測できないため、実測の校正データも採取する。

- `RAM`：NUMA local/remote、copy/read/writeの持続帯域。
- `Storage`：Blockサイズ・I/O深度別のDirect I/O持続read/write、デバイス数別の集約性能、容量・TBW制約。
- `MPI`：Blockサイズ別のpair exchange、双方向通信、all-to-all、latency、通信と同期の実効値。
- `Compute`：native/fused/joint gate、Frame更新、ログ索引、物理SWAP/FusedSWAPの実測コスト。
- `Mixed`：I/O + MPI + CPUカーネルを同時稼働させた際の帯域低下、NUMA/PCIe共有による干渉。

計測は予め確保した安全なscratch領域・割当資源内で行い、破壊的なディスクテストを自動実行しない。ベンチマーク時のノード配置、ソフトウェア版、同時実行条件、測定誤差もprofileに残す。未測定値は推測値として識別し、`StaticEstimate`に不確実性を伝搬する。

**成果物**：`hardware-profile.json`（`profile_id`、機器構成hash、MPI/OS/Backend版、容量、実効帯域曲線、**Blockサイズ・I/O深度・read/write混合比・MPI併走条件ごとの基準分布**、Mixed Benchmark、測定日時、信頼区間）。通常ジョブ開始時にはフルベンチマークを毎回走らせず、profileの適合性を検査する。HW変更、性能劣化、管理者要求では段階的に再校正する。

### 4.2 Stage 2：量子回路登録時の静的最適化

この段階では入力回路の依存関係が既知なので、**CPU時間をかけたlookahead探索**ができる。システム構成はStage 1のprofileを参照する。演算ログは初期から論理状態の正本であり、以下の解析に使う。

1. 回路を意味保存`Gate IR`へ正規化し、Global DAGとPer-Block Log / Frame Group参照・Cross-Block Task候補を組み立てる。
2. 対角・置換・**条件付き置換（MCX：X/CX/CCX/CCCXを統合）**・Block-local・Clifford・非Clifford・Joint Taskを分類し、**Native-first / fusion / fallback lowering**の候補と近似誤差を明示する。MCXは`k_local/k_upper`、正負制御、target階層、Frame整合費用と活動振幅率から、local／仮想Block置換／partial exchange／full-block交換／SWAP局所化／分解候補を生成する。
3. **任意の小行列Uは形状・unitarity・bit順を検証**し、構造化性が証明される場合はその構造をIRへ保持する。**ParameterExpressionを値に展開せず依存DAGを作り、symbolic fusionや共通prefix候補を抽出する。** `Block size`、rankへの担当区間、RAM作業領域、初期Qubit Layout、SWAP/FusedSWAPによる局所化区間とlayout遷移候補を比較する。**物理SWAPを選べばSSD読書きやFrame整合も増える**ため、それらを含めて評価する。
4. Operation Logの遅延範囲、materialization候補、必要Block群、依存frontier、checkpoint境界を決定する。
5. RAM上限・SSD容量・NIC接続・許容誤差の制約を確認し、複数の合法な**実行プラン候補**を保持する。パラメータ非依存Planと、指定Bindingがある場合にのみ合法な特殊化候補を分離し、multi-runのPlan再利用利益とprefix checkpoint費用を見積もる。測定結果による条件分岐は結果を未確定のままplanへ記録する。
6. HardwareProfileの単体・Mixed帯域とExecutionPlanからI/Oバイト数・MPI量・再生コスト・推定時間の幅を算出する。

**成果物**：不変の`semantic-ir`、`plan-template.json`（`plan_id`, `circuit_hash`, `profile_id`, `layout_id`, `frontier`、タスク依存、**各Taskの想定実効帯域・レイテンシ・許容変動幅**、候補代替パス、再計画許可境界）、`static-estimate.json`。PlanTemplateは**固定の全I/O順序ではない**。最終的な取得先、prefetch時刻、eviction、非同期ジョブ順は実行時に選択する余地を残す。

静的最適化のコストが回路実行時間を上回らないように探索予算とタイムアウトを設け、短い回路は軽量heuristicへ切り替える。PlanTemplateはprofile更新時に**再評価または再コンパイル**できるよう版を保持する。

### 4.3 Stage 3：実行時の動的スケジューリング・再推定

実行時は実際のBlockの所在・残RAM・dirty状態・Frame Group所属・MPI待ち・NVMe/HDDキュー・帯域変動・NUMA競合・実行進捗を観測し、Stage 2の候補プランを**正しさを保つ範囲で**実行・調整する。

- `Prefetch / Evict / Writeback`：次回使用時点と実際の帯域・RAM利用に合わせて先読みや追い出しを調整。
- `Deferred materialization`：RAM常駐中のBlockへログをまとめ適用するか、空き時間に背景実行するか、依存境界まで待つか判断。
- `Data shipping`：版一致を確認してlocal SSD/HDD、remote RAM（MPI）、operation/frame差分再生から合法かつ速い経路を選択。
- `Frame alignment`：局所Frameを維持／共有／materializeする候補を、今後のアクセスと共同演算の費用から選択。
- `Native Gate / MCX`：実測I/O・MPI条件が変化した場合は、**未着手Taskに限り**MCXのpartial exchange、全Block交換、SWAP局所化、分解候補の費用を再評価する。
- `Parameter binding / reuse`：Bindingを**Job開始時にimmutable確定**し、U(theta)の行列生成・unitarity再検証、値依存のguarded specializationを選ぶ。異なるBinding間の共通prefix／状態再利用は入力・依存・Frame・Layoutの整合を確認した場合だけ認め、**進行中の状態をパラメータだけ書き換えて再利用しない**。途中で適用済みの振幅交換を別実装で二重適用しない。
- `Layout / SWAP`：静的候補の実行・延期をまず選ぶ。新しい再配置を計画する場合は、**振幅の物理転置費用、SSD/HDD全走査、MPI通信、Frame/ログ/bit順変更**を必ず再積算する。
- `Overlap / Scheduling`：CPU、NIC、SSD/HDD、PCIe/NUMAの競合を考慮し、依存がないTaskのみ並行実行。
- `Runtime Performance Guard`：**Stage 1/2が見積もった同等条件の実効帯域・遅延とStage 3の観測値を比較**。遅さが一時的か持続的か、装置・通信・干渉・キューのどこで起きたか分類する。
- `Safe Replanning`：性能低下時はPlanTemplateの代替候補を再評価し、必要なら先読み深度、同時I/O数、取得経路、背景materialization、SWAP実行時点、配置などを変更する。切替前後の正しさと費用を検証する。

**動的最適化で変更できないもの**：元回路の論理作用、既に確定したGateの一意性、演算依存DAGの合法な順序、Block/Replicaの前提バージョン、近似合成の誤差予算。大きなレイアウト変更や分散計画の切り替えは、必要なBlockが整合する`safe frontier`で合意し、Manifestとlayout版を更新してから公開する。全rank barrierを毎ゲートに設ける必要はない。

再計画にはそれ自体の計算・データ移動・checkpoint費用がある。従って、変更は予測節約時間がその費用と十分なマージンを超えるときだけ行う。

$$
\Delta T
= \widehat T_{\mathrm{continue}}
- (\widehat T_{\mathrm{new\ plan}} + T_{\mathrm{switch}}).
$$

`Delta T`が正でも誤差・競合の分だけ利点が消える場合があるので、閾値・cooldown・切替回数制限を設けてスケジュールの振動（thrashing）を防ぐ。

経過実時間・実行済みTask・残りDAGと、更新した実効帯域を利用して、残時間予測も継続更新する。

$$
\widehat T_{\mathrm{total}}(t)
=T_{\mathrm{elapsed}}(t)
+\widehat T_{\mathrm{remaining}}(\mathrm{remaining\ DAG},\ \mathrm{telemetry}(t)).
$$

**成果物**：`runtime-trace.jsonl`、**版付き`runtime-profile.json`、`health-events.jsonl`、`plan-change-events.jsonl`**、計測されたI/O/MPI量、materialization・SWAP/Frame整合の実測時間、逐次更新する残時間予測、合法な実行済みfrontierとcheckpoint manifest。実測結果は次回Stage 1/2のコスト校正へフィードバックする。

### 4.4 段階間の責任境界と不変条件

- **Stage 1は回路に依存しない。** `HardwareProfile`は実測事実と信頼性だけを表す。
- **Stage 2は論理意味を守る静的探索。** 許可された代替候補を含む`PlanTemplate`を生成し、実行時間・データ移動・容量を推定する。
- **Stage 3は動的な実行選択。** 許可済みTask間の優先順位・配置・先読みを調整し、より大きな変更は検証済みの再計画境界で行う。
- `profile_id / plan_id / layout_id / frame_group_version / block_frontier`をtraceとcheckpointへ保存し、異なる世代を混ぜない。
- 各段階に**plan-time、runtime optimization overhead、実行後の差分予測誤差**を記録する。
- 大規模回路ではBlock数が指数的に増えるため、Stage 2での**全Blockへのログ・配置・Frameの実体列挙は禁止**。range/mask/group/sparse overrideの圧縮表現を基本とする。
- **MCXに関しては**Stage 1が制御数と通信対象密度ごとの実測性能、Stage 2がsymbolic predicateベースの候補計画、Stage 3が実行時I/O帯域に基づく未着手partial/full/SWAP経路選択を担当する。
- **Parameterに関しては**Stage 1でNative U/回転/勾配KernelとI/O負荷を校正し、Stage 2でsymbolic依存解析とmulti-run Planを作り、Stage 3でBindingごとの特殊化・状態再利用・勾配方法を決定する。Bindingをジョブ実行中に無断で変更しない。

### 4.5 開発時に用意する最小API

```python
class HardwareProfiler:
    def discover(self) -> HardwareProfile: ...
    def calibrate(self, profile: HardwareProfile, scope: str) -> HardwareProfile: ...

class StaticCircuitOptimizer:
    def register(self, circuit, profile: HardwareProfile) -> PlanTemplate: ...
    def estimate(self, plan: PlanTemplate, profile: HardwareProfile) -> StaticEstimate: ...
    def bind(self, plan: PlanTemplate, values: dict) -> BoundPlanCandidate: ...
    def plan_parameter_batch(self, plan: PlanTemplate, bindings: list) -> BatchPlan: ...

class AdaptiveRuntimeScheduler:
    def start(self, plan: PlanTemplate, profile: HardwareProfile): ...
    def update(self, telemetry, completed_tasks): ...
    def schedule_ready_tasks(self): ...
    def replan_at_safe_frontier(self, observed_costs): ...
    def estimate_remaining(self) -> RuntimeEstimate: ...
    def evaluate_hardware_health(self) -> HealthReport: ...
    def apply_plan_version(self, candidate, safe_frontier): ...
```

最小実装から、`hardware-profile.json`→`plan-template.json`→`runtime-trace.jsonl`の関連付けが追えるようにする。実装段階が進んでも、この責務分離を維持する。

---

### 4.6 実行時性能監視の基本仕様（必須）

**要件**：実行中も「初期のHardwareProfileにある理論的なピーク値」ではなく、Stage 1で同種の負荷を与えたときの**実測基準値**とStage 2のタスクごとの予測に対して、現在の有効性能を継続評価する。性能が悪化したときは、単なる警告だけでなく、**実行計画の変更候補を生成し、実行可能な変更を実施できる**ようにする。

Stage 1の`HardwareProfile`は変更しない。Stage 3が検出した情報は**版付き`RuntimeProfile`**として重ね、計画・予測の更新に用いる。

最低限の観測対象：

| 対象 | 収集する実行時情報 | 目的 |
|---|---|---|
| SSD/HDDの各デバイス | 実行済みread/write byte、完了までの時間、実効帯域、平均/p95 latency、キュー深度、書戻し滞留、空き容量、I/O error | SSDの持続書込低下、デバイス単位の偏り、帯域飽和、容量不足の判別 |
| SSD/HDDを複数利用した系 | デバイス別のばらつき、合計実効R/W帯域、同時read/writeによる低下 | RAID/stripe/デバイス選択を見直す |
| MPI通信・rank別 | 実送信/受信byte、メッセージサイズ、peer別完了時間、待機時間、全rankの最大待ち、転送実効帯域 | NIC混雑・slow rank・不適切なMPI通信パターンの検出 |
| NIC・ネットワーク（権限があれば） | link状態・速度、packet/drop/error、RDMA/TCP統計、共有fabric混雑の兆候 | 突発的な回線・fabric問題の切り分け |
| CPU/RAM/NUMA/PCIe | kernel時間、RAM予約/pressure、ページフォールト、ローカル/遠隔NUMAアクセス、CPU負荷、任意の温度/スロットリング指標 | 実際はI/O装置以外が律速かを識別 |
| Scheduler / Journal / Frame | queue待ち時間、prefetch hit、dirty Block数、Block材料化時間、ログ再生時間、Frame整合時間、SWAP費用、ETA誤差 | 無用な再計画や最適化オーバーヘッドを抑える |

MPIやカーネルのcounterが取れないプラットフォームでは、**ランタイム自身が発行したTaskのbyte数と完了時刻だけでも監視可能**とする。OS側counterは補助診断であり、他プロセスのI/Oも含まれ得るので、アプリが処理したbyte数と混同しない。

**監視そのものがI/Oを増やしてはならない。** 原則は`MoveTask` / `MPITransferTask`の既存完了イベントからのpassive telemetryとし、大容量の`fio`やMPI全ノード帯域試験を実行中に自動起動しない。追加の検査は専用scratch領域・低負荷時に限り、ForegroundのSLAを優先する。

### 4.7 想定性能との比較：同一負荷条件・持続性

性能比較は、**同じデータサイズ・I/O方向・同時実行数・メッセージサイズ・NUMA・混合負荷**を満たすProfileから行う。例えば、`queue_depth=1`の単独SSD readと、`queue_depth=16`のMPI同時実行中のwriteを、そのまま比較しない。

1. Taskに`performance_class`を付与する：`backend/device/peer + read/write + block_bytes + queue_depth + concurrent_disk + concurrent_mpi + NUMA + layout/stage`。
2. Stage 1の単体・Mixed Benchmarkから最も近い基準を選択し、Stage 2の`PlanTemplate`予測時間を補正する。
3. Stage 3の実完了Taskから`RuntimeProfile`をローリング集計する。I/O待機時間と転送の実働時間は可能なら分ける。
4. サンプルが少ない、転送が発生していない、条件が大きく異なる場合は`INSUFFICIENT_DATA`または`PROFILE_MISMATCH`とし、性能低下を断定しない。
5. peer/rankごとの分布と**最も遅いrank（straggler）**も見る。平均値だけで全ジョブの律速を判断しない。

帯域の比較値とタスク遅延比の定義例：

$$
r_B(j,t)=\frac{B_{\mathrm{observed}}(j,t)}
                 {B_{\mathrm{expected}}(j\mid\mathrm{workload})}
$$

$$
r_T(j,t)=\frac{T_{\mathrm{observed}}(j,t)}
                 {T_{\mathrm{expected}}(j\mid\mathrm{workload})}
$$

`j`はデバイス・rank間経路・Task種別を特定する。この比率は**同じ転送量/同等負荷を正規化した比較**に使う。小さなTaskは帯域ではなくレイテンシ主導なので、別モデルへ分ける。

**暫定トリガー値（PoCで要校正）**：

| 判定 | 条件例 | 取る動作 |
|---|---|---|
| `HEALTHY` | 同一負荷で想定内 | 既存Planを継続 |
| `SUSPECT` | 実効帯域が想定の75%未満、またはp95遅延が想定の2倍超 | 原因分類・追加観測。直ちに大規模SWAPしない |
| `DEGRADED` | 上記が十分なsampleを伴い**3連続評価窓**で継続 | 代替プランのdry-runを生成 |
| `CRITICAL` | 実効帯域が想定の50%未満の持続、I/O error、容量枯渇、通信不能など | 安全なfallback、必要に応じcheckpoint/停止、運用者へ通知 |
| `RECOVERED` | 復旧閾値（例：85%以上）が複数窓継続 | 現在プランを継続するか戻すかも費用比較 |

評価窓は「例えば30〜60秒」の固定時間だけでなく、**完了Task数または転送byte数の下限**を満たすときに確定する。短時間の揺らぎに反応しすぎないよう、**閾値の上り下りにヒステリシス、クールダウン、最大切替回数**を設ける。`CRITICAL`なI/O errorなどは待機窓を短縮する。

想定からの低下を見つけた際は、実際に影響を与えている**残りのExecution DAGのcritical path**上にあるかを確認する。低速化したデバイスが現時点のボトルネックでなければ、不要なデータ移動を伴う再構成を避ける。

### 4.8 自動・手動の変更候補

**「ハードウェアを速くする」のではなく、「実効性能が変わった環境へ実行方法を合わせ直す」**。次の操作を、負荷に対する有効性と変更の危険度により分類する。

| 問題 | 低コストな即時調整（Task境界） | 高コストな調整（safe frontierで確定） |
|---|---|---|
| SSD read/write帯域低下 | `iodepth`、同時read/write数、prefetch深度、writeback時期、利用可能な最新Replicaの読出し元を変更 | デバイス間のBlock移行、striping再設計、必要ならストレージ上の物理再配置 |
| MPI帯域低下／混雑 | 有効なlocal SSD/remote RAM/Operation Shipping経路へ切替、通信と計算の重ね方・転送chunkを変更 | Layout・SWAP/FusedSWAPの実施計画変更、rank間担当Block集合の移行 |
| 特定rankのみ遅い | critical path外の局所Taskを優先、利用可能なReplicaの活用、遅いrankへの通信集中を抑制 | 版付きBlock再配置、対象rankのLayout遷移（互換条件が満たせる場合） |
| RAM圧迫・I/O競合 | cache予約率、pin上限、先読み量、eviction、background materialization rateを縮小 | 依存frontierを切って作業集合を再編成。Blockサイズ再設計はcheckpoint/reformatを伴う |
| NUMA/PCIe競合 | I/O worker・計算workerの配置、同時利用数を調整 | 物理配置/プロセス配置変更。rank再割当はランタイム能力に依存 |
| 空きSSD容量不足、デバイス障害 | 新規writeを停止／抑制、未書き戻しBlockを保護、正常なReplicaを選択 | 代替ディスクへの版付き移行、checkpoint後の再起動。安全な格納場所がなければ停止 |

`SWAP`によるglobal量子ビット局所化も再評価する。**ネットワーク性能が下がったからSWAPが常に得とは限らない**。物理index-bit swapで増えるSSD読書き、MPI通信、Frame alignment、layout変換、COW容量を見積もった上で、残りの回路で得られる利益を比較する。

切り替えは以下の3段階で分類する。

- `LIVE_TUNE`：in-flight bufferや依存frontierの意味を変えず、次Taskから変更できるI/O深度、先読み、取得先、実行優先順位、background concurrency。
- `FRONTIER_REPLAN`：Block配置、物理SWAP/FusedSWAP、Frame Group、Joint Task計画、layout変更など、必要なrank・Blockの整合した境界で合意／commitする操作。
- `RESTART_REQUIRED`：利用rank数の変更、MPI communicator再編成、利用不能なNICや物理デバイスの入れ替えなど、通常の固定rank MPIではジョブを再確保して再開する必要がある操作。

**MPIのrank数を、障害発生時に無条件で増減できるとは仮定しない。** 対応していないMPI環境では、checkpoint/restartやジョブスケジューラへの再投入を提案し、動的縮退を偽装しない。

### 4.9 再計画を実行する費用と条件

候補プランごとに、残りDAGのコストを**観測した`RuntimeProfile`を使って再計算**する。比較には、再計画のCPU時間だけでなく、データ移動、同期、checkpointやCOWの追加費用を含める。

$$
G_{\mathrm{replan}} =
\widehat T_{\mathrm{continue}}
-
\left(
\widehat T_{\mathrm{candidate}}
+T_{\mathrm{planning}}
+T_{\mathrm{migration}}
+T_{\mathrm{sync}}
+T_{\mathrm{checkpoint}}
\right)
$$

採用条件は、例えば次のすべて。

- `G_replan > 0`に加え、**推定誤差幅と最低改善率（暫定例：残時間の10%）を超える**こと。
- 新PlanがRAM・SSD容量・data version・Frame/位相・Global DAGの依存・合成誤差制約を満たすこと。
- 変更に必要なread/write/搬送用Bufferを先行確保できること。
- 既存I/O・MPIのin-flight Taskを安全に完了またはキャンセル可能な境界であること。
- 前回変更からのcooldownと、stageごとの変更回数上限を守ること。

局所調整では次Taskからポリシーを切り替えればよい。一方、物理SWAPやBlock再配置は、**`prepare → reserve → drain affected tasks → migrate/materialize → validate → commit version`**の順とし、globalに整合した対象frontierだけを公開する。

失敗時に**既に量子ゲートを適用・commitした状態を巻き戻したふりをしない**。確定前の移動は破棄して旧Planへ戻せるが、確定後は新Planから前進して再計画するか、永続化済みcheckpointから復旧する。版付きManifestと`plan_version`を保存する。

### 4.10 Runtime Performance Guardの状態機械

```text
HEALTHY
   |
   | expected/observed divergence
   v
SUSPECT -- transient or insufficient samples --> HEALTHY
   |
   | persistent degradation confirmed
   v
DEGRADED
   |
   | create candidates from PlanTemplate; estimate benefit
   v
REPLAN_PENDING -- no safe improvement --> DEGRADED (continue & alert)
   |
   | legal, cost-effective candidate; auto or operator approval
   v
SWITCHING -- prepare/validate fails --> old plan if uncommitted
   |
   | commit plan_version at safe frontier
   v
VERIFYING -- worse than expected --> alternative forward plan / checkpoint recovery
   |
   v
HEALTHY (new RuntimeProfile baseline kept separately)
```

監視は**ノード単位と集約値の両方**を持つ。Coordinatorが評価を集約して`plan_version`を提案し、対象rankだけが必要な合意・版確定を行う。**計測のために毎ゲート全rank barrierを設置しない**。Coordinatorが不調な場合も、監視不能と演算不正を混同せず、あらかじめ定義した動作（静的Plan継続・警告・checkpoint停止）へ移行する。

### 4.11 自動変更と運用者による明示変更

実行モードは以下を用意する。

- `observe`：性能差を監視し、警告と候補提示だけ。変更は行わない。
- `recommend`：候補Planと予測効果を提示し、承認後に適用する。
- `auto`：`LIVE_TUNE`と安全性を証明できる`FRONTIER_REPLAN`を自動適用。高リスク操作はpolicyで承認必須にする。

**暫定の設定ファイル例（実装仕様案、値は校正対象）**：

```yaml
runtime_monitor:
  enabled: true
  mode: auto                    # observe | recommend | auto
  sampling_period_seconds: 10
  minimum_completed_tasks: 8
  analysis_window_seconds: 60
  warning_bandwidth_ratio: 0.75
  critical_bandwidth_ratio: 0.50
  warning_p95_latency_ratio: 2.0
  sustained_windows: 3
  recovery_bandwidth_ratio: 0.85
  replan_cooldown_seconds: 120
  min_predicted_gain_ratio: 0.10
  max_replans_per_hour: 3
  max_monitor_cpu_fraction: 0.02
  allow_live_tuning: true
  allow_frontier_replan: true
  allow_rank_count_change: false
  destructive_benchmarks_during_run: false
```

`min_predicted_gain_ratio`を小さくすると、再計画が頻発して逆効果になり得るため、改善余地と不確実性を含める。性能監視が原因でジョブが遅くなる場合は監視頻度を自動調整する。

ジョブ実行中に変更可能なパラメータと、変更できないパラメータをスキーマとして宣言する。運用者が途中から`mode=observe`にしたり、指定SSDの利用を禁止したり、特定candidateを適用するインターフェースを用意する。**変更は操作ログに記録し、複数rankで版を同期する。** 保存済み状態の量子ビット順やDAGを黙って書き換える管理APIは禁止する。

### 4.12 ランタイムAPI・トレースと逐次リソース再推定

```python
class RuntimePerformanceMonitor:
    def record_task(self, completion_event): ...
    def observe_devices(self, optional_system_counters): ...
    def evaluate(self, hardware_profile, plan_template) -> "HealthReport": ...

class RuntimeCostModel:
    def update(self, performance_samples) -> "RuntimeProfile": ...
    def estimate_remaining(self, execution_dag, runtime_profile): ...

class SafeReplanner:
    def propose(self, health_report, execution_state) -> list["PlanCandidate"]: ...
    def dry_run(self, plan_candidate) -> "PlanImpact": ...
    def commit_at_safe_frontier(self, plan_candidate, affected_ranks): ...

class RuntimeControl:
    def set_mode(self, mode: str, operator: str): ...
    def set_policy(self, policy_delta, operator: str): ...
    def request_replan(self, scope: str, apply: bool = False): ...
```

`ExecutionTrace`には少なくとも、`task_id, rank, tier, device_id, peer_rank, bytes, queue_wait_ms, transfer_ms, latency_p95, performance_class, expected_bandwidth, observed_bandwidth, health_state, bottleneck, candidate_plan_id, plan_version, decision_reason, estimated_remaining_seconds, actual_switch_cost`を保持する。すべてを毎秒フルダンプせず、集約・sampling・必要イベントだけのログを併用してメタデータを抑える。

最新の残時間予測は次を区別して出す。

- **`baseline ETA`**：登録時のPlanTemplateとStage 1基準での予測。
- **`observed ETA`**：現在のRuntimeProfileのまま実行を続ける場合。
- **`adapted ETA`**：候補Planへ変更する場合の切替費用込みの予測。

**実行中の測定が初期HardwareProfileより優先されるのは残り時間・候補選択の見積もり上だけ**であり、元のprofileは履歴・再現性確保のため不変に保存する。

---

### 4.13 Distributed Observable / Adjoint専用の三段階最適化

| Stage | 観測・勾配に対する追加機能 | 例 |
|---|---|---|
| **Stage 1** | 実機の`BlockPauliInner`、`H|psi>`生成、2状態逆再生、Partner BlockのSSD/MPI取得、最後のベクトルAllreduce、混合同時負荷を測定 | `observable-gradient-hardware-profile.json` |
| **Stage 2** | Pauli項の`x_H`別グループ化、重複項集約、Frame変換/align候補、Parameter依存、可換性による厳密な勾配省略、Adjoint/Shift/Checkpoint候補Plan | `ObservablePlan`、`GradientPlanTemplate` |
| **Stage 3** | 実効SSD/MPI帯域からPartner取得順・Cache・逆再生/再計算・並列Job数・Gradient Methodを安全なfrontierで調整 | `GradientExecutionTrace`、計画変更理由 |

Stage 3はStage 1の**同じBlock/通信形状・同時負荷の校正曲線**と比較する。帯域劣化時にローカルBatch縮小、prefetch/Queue Depth変更、remote RAM/SSD取得経路切替等を試し、効果が移行費用を上回る場合のみ採用する。

## 5. アーキテクチャ（Per-Blockを基本単位とする）

```text
                Circuit / QASM / Rich Gate Input
                           |
             Semantic Gate IR / Normalizer
       (keep native U, symbolic Parameter expressions)
                           |
        Symbolic Parameter DAG / Matrix Asset Registry
                           |
             Global Operation Sequencer
           (op_id / dependency / layout / params)
                           |
                Global Dependency DAG
                  /                 \
       Local operation           Cross-Block
        dispatch/index           Task Journal
              |                       |
     +--------+--------+       Joint Task Edges
     |                 |              |
  Block A           Block B          ...
  - OpLog           - OpLog
  - FrameRef        - FrameRef
  - Phase           - Phase
  - Version         - Version
     \                 /
      +-- Frame Group Registry --+
                   |
    Parametric Binding / Safe Prefix Reuse Planner
                   |
       Deferred Materialization Planner
                   |
       Native / Fused / Fallback Gate Planner
                   |
       Qubit Layout / FusedSWAP Planner
                   |
                Resource-aware Scheduler <---------- Safe Replanner
                           |                              ^
                           |                        RuntimeProfile
                  Block / Move Tasks                      ^
                     /     |      \                       |
                RAM/Cache NVMe/HDD MPI Remote RAM         |
                     \     |      /                       |
                    Gate / Join Kernels ------------------+
                           |                    Task Completion Telemetry
                COW Block Versions + Manifest             |
                           |                      Performance Guard
                    Trace / Estimator --------------------+
                           ^
              Hardware Discovery / Calibration
                     (Hardware Profile)
```

**フィードバック方向**：Kernel/TransportのTask完了計測 → Performance Guard → `RuntimeProfile` → Safe Replanner → Resource-aware Scheduler。**データ経路**はScheduler → Block/Move Tasks → RAM/SSD/MPI → Kernel。監視はデータを直接改変しない。

### 5.1 各レイヤの責務

| コンポーネント | 主責務 |
|---|---|
| Gate Normalizer / Compiler | 入力ゲートの意味・構造を高水準IRに保存。任意行列Uを検証しimmutable assetへ登録、実行候補としてnative、fusion、選択的Clifford+T loweringと誤差管理を担当 |
| Parameter Engine / Matrix Registry | Symbol/Expression DAG、U(theta)のgeneratorや行列asset、Binding不変性、パラメータ依存frontier、合法なPlan特殊化を管理 |
| Parametric Batch / Gradient Planner | 複数Bindingの共通prefix・read-only checkpoint共有、Parameter-shift/Adjointの予想総I/Oとメモリを比較 |
| Global Sequencer | 正規化済みゲートを一度だけ受理し、全体の順序・一意ID・依存を確定 |
| Per-Block OpLog | Block-local操作、相対位相、ローカルMaterializationの未適用情報を保持 |
| Cross-Block Task Log + Global DAG | Block間/rank間の振幅混合、必要版、順序・因果関係を保持 |
| Frame Group Registry | 同じ`C_h`の共有、参照・コピーオンライト、条件付きFrame更新 |
| Deferred Planner | ログ融合、Frame整合、局所native・専用構造化Kernel・Joint Task・必要時の分解を選択 |
| Layout / SWAP Planner | 通信/SSD跨ぎbitを局所化する物理bit交換・FusedSWAPとレイアウトの履歴管理 |
| Placement Scheduler | 版付きBlock集合の取得先、RAM予約、先読み、追い出し、MPI・SSD転送 |
| Runtime Performance Guard | 各read/write/MPI/compute Taskの実効性能を同一負荷条件のStage 1/2期待値と比較、異常の持続性・原因・slow rankを分析 |
| RuntimeProfile / Safe Replanner | 実行中の実効コストモデルを保持。負荷低下時の代替候補・移行費用・安全frontierを検証し、計画版を切替 |
| Transport | `MemoryCopy` / `DirectIO` / `MPI` / 将来GPU |
| Kernel | 局所/複数Blockの振幅演算、Frame materialization |
| Checkpoint | 演算ログ・Frame/位相/置換・Block版の整合した永続化 |
| Estimator | 実行計画と実測資源性能から帯域・容量・時間を予測 |

## 6. Per-Block Operation Logの設計

### 6.1 論理的にはBlock独立、物理的には集約

**各Blockに独立した論理ログを持つ**。ただし全Blockに同じ下位ゲートを作用させる場合に、Block数分の同一レコードを物理的に追記する必要はない。

ログは3種類のレコードで構成する。

1. **LocalOp**：個別Blockに適用する演算・Frame/位相の更新。
2. **SharedOpRef**：Frame GroupまたはBlock集合に共通する演算テンプレートへの参照。
3. **JoinRef**：Global DAGにある複数Blockの共同タスクIDへの参照。

例：

```text
Global DAG:
  #100 H(q=1)             [shared local across all blocks]
  #101 CX(q=30, q=1)     [local in selected block group]
  #102 H(q=30)            [joint pairs; global node J102]

Block A OpLog:  SharedOpRef(100), LocalOp(101), JoinRef(J102)
Block B OpLog:  SharedOpRef(100),                  JoinRef(J102)

Cross-Block Task J102:
  inputs=(Block A version Va, Block B version Vb)
  output_frontier=F102
```

上の`Block A/B OpLog`は論理ビュー。**67百万Blockに1件ずつ物理レコードを書くという意味ではない**。共通イベント・パラメトリックな適用対象ルール・Shard上の索引で表現する。

### 6.2 Global DAGが必要な理由

Block AとBを混ぜる上位Hでは、A/Bのログを個別に実行するだけでは計算できない。Global DAGに**共同入力バージョンと共同出力バージョン**を持たせる。

- LocalOpはBlock単独のfrontierを進められる。
- JoinTaskは関連Blockすべての入力frontierを確認してから実行する。
- JoinTaskの後続LocalOpはその**出力版への依存**として記録する。古い入力版のFrameに後続操作を誤って反映しない。
- 先行するNon-Clifford LocalOpが未実行でも、後続Cliffordは左からFrameに合成できる。ただしLocalOpは元のFrame参照時点に対して正しく共役変換し、順序付きのbase-side Logとして保持する。
- ジョブ内で実行順序を変更する場合も非可換なゲート順を守る。
- 既に適用済みの演算をFrame更新とKernel実行で二重適用してはならない。

### 6.3 物理ストレージ形式

Blockごとにファイルを作る実装は避ける。数千万Blockではファイル数・inode・FDが問題となる。

```text
state/shard-00.bin       # 複数Blockの版とデータ
journal/local-00.log    # append-only、Block IDをキーとして集約
journal/shared-00.log   # 共有演算テンプレート/Group更新
journal/joint-00.log    # Cross-Block Task
index/block-00.bin      # Block別log範囲・frame ID・frontier
manifest/snapshot.json
```

初期PoCではJSONLでもよいが、本実装はappend-onlyセグメント＋疎索引＋バイナリ/列指向メタデータを検討する。Journalは小さな順次書き込みなので、振幅の`O_DIRECT`経路と同じI/O方式にする必要はない。

### 6.4 初期から満たすべき実行API

```python
journal.append_gate(gate)                 # sequencer経由。振幅の即時更新なし
block_log.view(block_id, frontier)        # local/shared/joinを合成した論理ビュー
frame_groups.apply_rule(group, gate)     # Group更新/COW分岐
planner.materialize(blocks, frontier)    # DAG依存を満たして一括具体化
runtime.commit_snapshot(frontier)        # 版・ログ・frameの整合cutを確定
```

## 7. Per-Block Clifford FrameとFrame Group

### 7.1 データ構造

`C_h`は内部`b`量子ビットだけに作用するCliffordをtableau等で表現する。各Blockが`frame_id`を参照し、実体は`FrameGroupRegistry`が共有する。

```python
@dataclass(frozen=True)
class BlockId:
    block_id: int
    layout_id: int

@dataclass(frozen=True)
class BlockVersion:
    block: BlockId
    version_id: str
    frontier_id: str
    checksum: str | None

@dataclass(frozen=True)
class BlockView:
    block: BlockId
    base_version: BlockVersion
    local_log_cursor: int
    global_frontier_id: str
    frame_id: str
    phase_id: str
    permutation_id: str

@dataclass
class FrameGroup:
    frame_id: str
    tableau: object  # Clifford on b lower qubits
    membership_rule: object  # explicit block range, mask, predicate, etc.
    generation: int
```

表は概念的な型。ログ・Frame・位相・置換の変化は版の意味を変えるので、`BlockView`を別の論理版として公開する。**物理振幅の`base_version`と、論理的に適用済みの`frontier`は別概念**である。

### 7.2 Frame共有とCopy-on-Write

同じFrameを持つBlockはtableau本体を共有する。下位Hのような全Block共通ゲートはFrame Groupを一度更新してよい。上位制御の条件付きCXで一部Blockだけ変わる場合は、Groupを論理分割して新Frameを共有する。

```text
Group G0 : Frame C          → Blocks {0..N-1}
  gate H(lower q)
Group G0': Frame H*C        → Blocks {0..N-1}
  controlled-X for h matching predicate
Group G1 : Frame X*(H*C)    → matching blocks
Group G2 : Frame H*C        → other blocks
```

どのBlockがどのFrame Groupに属するかを、巨大なBlock個数分のPythonオブジェクトで保持しない。区間、ビット条件、Sparse Override、Shard別索引等を使う。

### 7.3 位相の扱い

相対位相`θ_h`はBlock間混合結果を左右する。位相を捨てる、各Blockを独立正規化する、全Blockで勝手にglobal phaseを除去する実装は禁止。

- Clifford由来の位相を正確な離散位相（可能な範囲）で保持。
- 任意角対角ゲート由来の位相は浮動小数点等で別管理。
- 共通位相の正規化は**全体系で一つの基準**に対してのみ行う。
- 位相ルールもFrame Group同様に共有・条件付き更新できる。

### 7.4 Non-CliffordとFrameの共役

下位Pauli-string回転について、

$$
R_P(\vartheta)=e^{-i\vartheta P/2},
\qquad
R_P(\vartheta)C_h=C_h R_{C_h^\dagger PC_h}(\vartheta).
$$

Clifford共役後の演算は符号付きPauli-string回転なので、**Block内の非Clifford演算ログ**へ記録できる。Blockごとに`C_h`が異なれば、共役先のPauli stringも異なる。共役結果が同じFrame Groupでは変換結果も共有できる。

ただし、変換後のPauli supportが広がると振幅演算やゲート融合のコストが増す。Plannerは`KEEP_LOCAL_FRAME`を固定選択せず、以下を比較する。

```text
KEEP_LOCAL_FRAME   : 共役した演算をBlockのログへ記録
ALIGN_FRAME        : 次のJoint Taskへ向けBlock Frameを揃える
MATERIALIZE_FRAME  : C_hを振幅に適用しidentity化
JOINT_TASK         : Block間入力を必要とする演算をGlobal DAGに記録
```

### 7.5 必要なFrame表現の限界

`b=24`のBlockは256 MiB（`complex128`）。このときBlock数は次の通り。

| 量子ビット数 | Block数（b=24） |
|---:|---:|
| 40 | 65,536 |
| 50 | 67,108,864 |

50量子ビットで各Blockへ独立のtableau/Pythonオブジェクトを生成すると、Frameメタデータ自体が巨大になる。**Frame Group共有、疎な差分索引、on-disk shard化を最初から設計**する。Blockごとに独立したFrameを「持てる」ことと、すべてのFrameを物理的に複製することを混同しない。

## 8. 階層型Qubit LayoutとSWAP/FusedSWAP最適化（初期コア機能）

### 8.1 mpiQulacsとの対応と目的

mpiQulacsの`QuantumCircuitOptimizer`は、`SWAP/FusedSWAP`を挿入しglobal qubitへの直接通信を減らす手法を備える。公開仕様では`swap_level=0`（挿入なし）、`1`（元ゲート順序を変更せず挿入）、`2`（依存上合法な順序変更も利用）を区別している。

本開発ではこれを**MPIだけでなく、RAMキャッシュ・SSD/HDDを含む多段データ移動**へ拡張する。global-to-localの入れ替えにかかる*実際の通信・ディスク転置*と、将来のglobal gate実行で節約できるコストを比較する。

参考：
- [mpiQulacs README_MPI（公式）](https://github.com/qulacs/qulacs/blob/main/README_MPI.md)
- [NVIDIA cuStateVec：Distributed index bit swap](https://docs.nvidia.com/cuda/cuquantum/24.08.1/custatevec/distributed-index-bit-swap.html)

### 8.2 3つのbit領域を区別

`n`量子ビット、`P=2^g` MPI rank、rank内RAM作業Blockのビット数を`b`とした単純な配置では次のようになる。

```text
Physical index bits (low → high):
0 ... b-1      : Block-local / working set
b ... n-g-1    : Rank-local but Block/SSD/HDD-crossing
n-g ... n-1    : MPI-global (rank bits)
```

40量子ビット、MPI 4 rank、`b=24`なら、**24 local bits / 14 disk-crossing bits / 2 rank-global bits**となる。ここでの領域は現在の物理レイアウトとBlockサイズに依存し、論理qubitの固定属性ではない。

- **局所ゲート**：Block内だけで実行可能。
- **Rank内・Block跨ぎゲート**：MPI不要でもSSD/HDD読込み・異なるBlockの同時保持が必要なことがある。
- **MPI跨ぎゲート**：rank間振幅通信が必要になることがある。
- **対角ゲートや単純なBlock置換**：対象がglobal bitでも全振幅のMPI交換を要しない場合がある。`global bit`だけでコストを決めない。

### 8.3 論理SWAP・メタデータ変更・物理SWAPは別物

| 種別 | 目的 | データを動かすか |
|---|---|---|
| `LOGICAL_SWAP(a,b)` | 入力回路の論理的なSWAPゲート | 全後続演算を新しい論理wire対応へ解決するなら、マッピング更新で吸収できる場合がある |
| `VIRTUAL_BLOCK_PERMUTATION` | 完全なBlockの識別子を入れ替える | 物理コピーなしで済むのは、アクセス時の解釈・所有者・ログ参照を一貫して更新できる場合に限る |
| `PHYSICAL_INDEX_BIT_SWAP(p,q)` | global/ディスク跨ぎbitを局所位置へ再配置 | **通常は振幅データの再配置が必要**。MPI交換やストレージ転置の費用を計上 |
| `FUSED_INDEX_BIT_SWAP([(p,q),...])` | 複数の再配置を1つの実行計画にまとめる | 分割・複数交換より転送回数を減らせる可能性があるが実バイト数は計測 |

元の論理状態を`|Psi>`、論理→物理インデックスの置換を`L`としたとき、物理配列の振幅を`P_L |Psi>`とする。論理状態を保ったまま配置を`L'`へ変更する場合、**必ず配列側も対応させる**必要がある。

$$
|\phi_{\mathrm{phys}}\rangle=P_L|\Psi_{\mathrm{logical}}\rangle,
$$

$$
|\phi'_{\mathrm{phys}}\rangle=P_{L'}P_L^\dagger|\phi_{\mathrm{phys}}\rangle.
$$

`L`だけ更新して配列を変えない操作は、上式の**論理状態を保つ物理再配置**にはならない。例外として、**元の回路の論理SWAPをマッピング更新だけで実装する**場合は論理状態そのものがSWAPされるため合法。

### 8.4 Frame / Logとの整合性

Per-Block Frameは下位`b`ビットのCliffordなので、**Block内部とglobal bitを横断する物理SWAPは、単なるFrame IDの入れ替えでは済まない**場合がある。レイアウト変更の前後で、`BlockView`、相対位相、`FrameGroup`の所属、Journalの対象ビット、JoinTask入力集合を一貫して更新する。

基本規則：

1. `QubitLayout`に不変の`layout_id`を割り当て、`BlockVersion/BlockView/OpLog/FrameRef/ExecutionPlan/Checkpoint`へ伝播。
2. **同じレイアウトで解釈されたBlockを混在させない**。`PERMUTE_LAYOUT`はDAG上の特別なバリア／versioned taskとなる。ただし無関係な局所Taskは依存上可能なら先行・並列処理してよい。
3. SWAPがBlock内に閉じる場合は、Frameの共役更新やローカル振幅処理で表現できる。
4. Block境界を横断する場合は、局所Frameの作用先が跨Blockに変わる可能性がある。**安全なMVP方針は関係するFrameを実振幅へmaterializeしてから交換する**。必要時はJoint Taskに保持する。
5. 将来は、Frame変換がPer-Block形を保つと証明できる場合だけ、Frame Groupの索引/位相/置換を更新してmaterializationを省略できるようにする。
6. 入力回路のSWAPとOptimizerが挿入したSWAPは`origin=logical|layout`で区別し、元回路へ二重計上しない。

一般のFrameを`F`、物理index permutationを`P`とした場合、再配置後のFrameは形式的に

$$
F'=PFP^\dagger
$$

となるが、**これがPer-Block Cliffordの直和形に収まるとは限らない**。この場合はGlobal DAG内の遅延共同演算またはFrame materializationを使い、Frameを無理に各Blockへ分離しない。

### 8.5 SWAPを入れるかどうかの判定

一定の先読み窓`W`内の元ゲートをそのまま処理する費用と、SWAPによる局所化を比較する。

$$
C_{\mathrm{direct}}(W)
=\operatorname{EstimateCost}\left(\mathrm{execute}(W,L)\right),
$$

$$
C_{\mathrm{swap}}(W,L\to L')
=T_{\mathrm{repartition}}
+\operatorname{EstimateCost}\left(\mathrm{execute}(W,L')\right)
+T_{\mathrm{frame/log\ update}}
+T_{\mathrm{restore\ if\ required}}.
$$

`T_repartition`は**MPI転送、SSD/HDD読み書き、RAM再配置、同期待ち、COW容量**を含む。`T_restore_if_required`は出力レイアウトの復元が本当に必要な場合のみ発生し、レイアウトをそのまま次の窓へ引き継げるなら省略できる。各項を単純に足すだけではなく、Mixed Benchmarkと依存DAGでoverlap/競合を評価する。

$$
\mathrm{ChooseSwap}
\iff C_{\mathrm{swap}} + \Delta_{\mathrm{risk}} < C_{\mathrm{direct}}.
$$

`Delta_risk`は不確かな実効帯域、SSD書込耐久、チェックポイント容量、後続のFrame materialization負担などを表す安全側マージン。初期実装では貪欲探索または有限窓のbeam searchを採用し、グローバル最適解は要求しない。

**SWAPの通信一回の代価を、後続複数global gateの削減で回収できるときにのみ実施する。** SSDに全状態がある場合は、MPIだけを見る最適化とは異なる結果になり得る。

### 8.6 FusedSWAPと多段データ配置

- **Rank内bit同士**：RAM内で済むか、SSDからの非連続データ組立てが必要かで経路選択。
- **GlobalとBlock-local bit**：複数rank間で対応する振幅スライスを交換。RAMに全状態がない場合はSSD読込みと転置write-backまで必要になる。
- **GlobalとDisk-crossing bit**：MPI量は減る可能性があるが、rank内の再配置・SSDアクセスとのトレードオフがある。
- **複数交換**：一時バッファ・ストリーミング・連続I/Oを共有する`FUSED_INDEX_BIT_SWAP`を計画し、単独SWAP列より全状態走査数を減らせるか検証する。
- **容量制約**：一時swap先のCOW領域、double buffer、redo/recoveryに必要な容量も見積る。RAMへ全担当状態が収まることを前提にしない。

**FusedSWAPが常に安いわけではない**。デバイス配置やbit組合せによりtranspose用一時メモリとSSDランダムアクセスが増えるため、最初から`NO_SWAP`経路を残す。

### 8.7 実装上のPlannerタスク

```python
@dataclass(frozen=True)
class QubitLayout:
    layout_id: str
    logical_to_physical: tuple[int, ...]
    rank_bits: tuple[int, ...]
    block_local_bits: tuple[int, ...]

@dataclass(frozen=True)
class LayoutTransition:
    from_layout_id: str
    to_layout_id: str
    bit_swap_pairs: tuple[tuple[int, int], ...]
    affected_block_groups: tuple[str, ...]
    frame_policy: str  # materialize | provably_safe_rewrite

# Virtual logical SWAP and physical relocation are different tasks:
LOGICAL_SWAP = 'logical_swap'
PHYSICAL_INDEX_BIT_SWAP = 'physical_index_bit_swap'
FUSED_INDEX_BIT_SWAP = 'fused_index_bit_swap'
```

計画生成では、`layout_id`ごとのglobal/local判定、Frame整合、DAG依存、SSD・MPIのブロック移動、出力時の観測・bitstring順の解釈を一貫して更新する。

## 9. Materializationと演算・Frameの共同スケジューリング

### 9.1 Block-local Materialization

```text
Per-Block OpLog / FrameRef
           |
Resolve shared/local op sequence
           |
Reserve RAM buffer
           |
Read Block base version (if needed)
           |
Apply compatible pending ops / align local frame as required
           |
Keep RAM-resident OR COW write-back
           |
Advance logical frontier / version index
```

BlockをRAMへ読み込む際、依存関係の許す範囲で多くの局所ゲートをまとめる。ログ追記そのものがI/Oを減らすのではなく**データ再利用を増やす計画**が重要。後続の物理bit交換による配置変更を見越し、読み込んだBlockへ適用できるローカルゲート数を増やす。

### 9.2 Cross-Block Materialization

```text
Global DAG Joint Task (A,B)
        |
Check exact input versions / frame / phases
        |
Reserve working set for A+B or choose legal streaming kernel
        |
Fetch both versions (local SSD / MPI / remote cache)
        |
Frame alignment if required
        |
Apply joint kernel (e.g. upper-bit H)
        |
Publish output versions A' and B' atomically at task level
        |
Resume dependent local logs
```

**「上位Hが現れたら必ず即materialize」ではない**。Joint TaskそのものをDAG上に遅延でき、SWAPで対象を局所化するプランも比較する。ただし、その後のBlock-local操作は新しい論理出力版へ接続し、途中版への誤適用を防ぐ。

### 9.3 Triggerと背景処理

- 必要な出力/観測/測定が発生する。
- 次のJoint Taskの入力版が必要。
- BlockがRAMに存在していて、先に実行した方が再読み込みが減る。
- ログ再生時間/Frame整合費用/メタデータサイズが閾値を超える。
- SSD、NIC、CPUに空きがあり、Foregroundを阻害しない。
- checkpointに必要な一貫したfrontierを確定する。

ForegroundとBackgroundでSSD帯域・NIC帯域・PCIe・NUMAメモリを取り合う。**単なるCPU idle判定ではなく、混合負荷で有利なタスクだけ背景実行**する。

## 10. RAM・SSD/HDD・MPIの統一Placement

### 10.1 状態の所在地とMove

```python
@dataclass(frozen=True)
class Location:
    node: int
    tier: str  # RAM, NVME, HDD
    device: str | None
    offset: int | None

@dataclass
class Replica:
    block_version: BlockVersion
    location: Location
    state: str  # CLEAN, DIRTY, LOADING, WRITING, etc.
    pin_count: int
```

Transportは`MemoryCopy`、`DiskRead/Write`、`MPITransfer`、将来的にGPU転送へ分離する。Placementは単純な階層ではなく、移動コスト付きグラフとして表す。

### 10.2 RAM CacheとBlock Group予約

- キャッシュはアプリが明示管理。`resident/dirty/pin/version/next-use`等を保持する。
- `O_DIRECT`は巨大な振幅ファイルに優先検討し、OSページキャッシュとの二重保持を抑える。
- Journalは追記単位が小さいためbuffered I/Oを選べる。振幅とは耐久性ポリシーを分ける。
- `O_DIRECT`のアラインメント・永続性を別途処理。同期I/Oで正しさ確認後に`io_uring`等へ拡張。
- Joint Task実行前は必要なBlock集合のバッファをまとめて予約し、部分取得でのデッドロックを避ける。
- 読み書き中、MPI通信中のBufferは破棄・再利用しない。
- 将来参照、dirty writeback、Replica、再演算コストを見てevictする。

### 10.3 SSD/HDD階層とNUMA

複数NVMeなら持続読書き帯域・TBW・PCIe共有・NUMA配置を計測してBlockを分散配置する。HDDは大きい連続I/Oが中心。HPC共有ストレージはローカルHDDとみなさず、共有帯域・競合をHardware Profileへ入れる。

## 11. MPI：State Shipping・Operation Shipping・Frame Shipping

### 11.1 3種類の転送

| 方法 | 転送内容 | 成立条件 |
|---|---|---|
| State Shipping | 最新の振幅Block | 通信先で振幅が必要。相手版を持たなくても利用可能 |
| Operation Shipping | base版以降の局所演算ログ | 受信側が**同一base版**を保持し、必要な外部入力・依存が揃っている |
| Frame/Phase Shipping | `frame_id`、Frame差分・位相ルール | 受信側が同じ物理Block版と前提frontierを持ち、差分だけで論理表現を再現可能 |

State ShippingとOperation/Frame Shippingは全て版に紐づく。**H/CX等のCross-Blockゲートに必要な相手振幅を、数十Byteのログだけで常に代替できるわけではない**。

### 11.2 Log shippingの判定

```text
if exact_base_and_frontier_match
   and input_dependencies_available
   and replay_cost + log_network_cost < state_transfer_cost:
    ship_operation_or_frame_delta()
else:
    transfer_state_or_execute_joint_task()
```

送信元のFrameを`C_A`、受信側が古いFrameを`C_B`としていても、適合する`base_version`と演算historyがあるなら差分更新可能。ただし**frame_idだけを変更して振幅の意味を改変してはならない**。送るのは検証可能な操作列/差分と明示された順序。

### 11.3 MPI実装上の注意

- 既存の`mpi4py` Reference Simulatorを`MPITransport`へ取り込む。
- `Isend/Irecv`は可能だがMPIのprogressは環境依存。Mixed Benchmarkで確認する。
- BlockサイズがMPIメッセージ上限や実効帯域の最適値を超えるなら分割する。
- Global DAGの共同ゲートにはBlock集合ごとの版整合を要求し、毎ゲート全rank barrierは避ける。

## 12. Snapshot・Append-only Journal・障害復旧

### 12.1 保持する永続情報

```text
state/                   # base/COW物理振幅版
journal/local/           # Block局所演算のShardログ
journal/shared/          # 共有ゲート・Frame group変更
journal/joint/           # Cross-Block Task DAGログ
frame/registry/          # tableau / group / phase / permutation rules
index/block-view/        # Block別base版、参照ログ位置、frame_id、frontier
manifest/                # 一貫したcutとcommit世代
```

Logical Operation Logsは演算意味の正本の一部、Recovery WALはdurable commitのための記録として区別する。1件ごとの物理fsyncを強制せず、checkpoint境界で耐久性・再生可能性を保証する。

### 12.2 COWと分散Checkpoint

1. 正しい入力版、Frame/位相、Join依存を確認してmaterializeする。
2. 新Blockは別領域に書き、必要な書込完了・sync・checksumを確認する。
3. LocalOp、SharedOp、JoinTaskについて**一意なop_idと反映済みfrontier**を永続化する。
4. 全rankで整合するlogical cutのManifestを原子的に公開する。
5. 古いBlock/ログ・旧Frame Groupは、参照がなくなった後に回収する。

**非具体化ログを含むCheckpointも可能**だが、物理base、Frame Group、相対位相、置換、未実行DAGの全てが保存・再生可能であることが条件。`O_DIRECT`自体は同期・障害耐性を保証しない。

### 12.3 障害耐性テスト

- Joint Task途中で片方だけの版を公開していないか。
- manifest確定前後のクラッシュでログ欠落/二重適用しないか。
- FrameGroup分割中断でBlock所属を誤らないか。
- MPI受信側が別版のBlockへログを誤適用しないか。
- COW領域とjournalの書込み順で整合した再開が保証できるか。

## 13. Resource-aware SchedulerとCost Model

### 13.1 主要Task

```text
APPEND_LOCAL_OP / APPEND_SHARED_OP / REGISTER_JOIN_TASK
UPDATE_LOCAL_FRAME / SPLIT_FRAME_GROUP / UPDATE_RELATIVE_PHASE
RECORD_SEMANTIC_GATE / RECORD_SYNTHESIS_ERROR
VERIFY_UNITARY / REGISTER_MATRIX_ASSET / SYMBOLIC_GATE_FUSION
REGISTER_PARAMETER_SYMBOL / BIND_PARAMETER_SET / CHECK_PARAMETER_DEPENDENCIES
GUARDED_SPECIALIZE / REUSE_PREFIX_CHECKPOINT / COW_PARAMETER_SUFFIX
PLAN_GRADIENT / PARAMETER_SHIFT_JOB / ADJOINT_REVERSE_TASK
LOGICAL_SWAP / VIRTUAL_BLOCK_PERMUTATION / ALIGN_FRAME
NATIVE_MCX_LOCAL / MCX_CONDITIONAL_LOCAL / VIRTUAL_MCX_BLOCK_PERMUTATION
MCX_DISK_JOINT_EXCHANGE / MCX_MPI_PARTIAL_EXCHANGE / MCX_FULL_BLOCK_EXCHANGE
MCX_FALLBACK_DECOMPOSITION / MCX_COMPILE_PREDICATE
NATIVE_LOCAL_GATE / NATIVE_DIAGONAL / NATIVE_JOINT_GATE
FUSE_LOCAL_GATES / CHOOSE_GATE_IMPL / LOWER_FALLBACK_BASIS
PHYSICAL_INDEX_BIT_SWAP / FUSED_INDEX_BIT_SWAP / COMMIT_LAYOUT_VERSION
PREFETCH / FETCH_REMOTE / SHIP_OPLOG / SHIP_FRAME_DELTA
RESERVE_BLOCK_GROUP / APPLY_LOCAL_BATCH / APPLY_JOINT_KERNEL
MATERIALIZE_FRAME / WRITEBACK / EVICT
CHECKPOINT_COMMIT / RELEASE
OBSERVE_TASK_PERFORMANCE / UPDATE_RUNTIME_PROFILE / DETECT_DEGRADATION
PROPOSE_REPLAN / LIVE_TUNE / COMMIT_PLAN_VERSION / ALERT_OPERATOR
```

### 13.2 基本コスト

$$
T_{\mathrm{disk}}\approx
\frac{D_{\mathrm{read}}}{B_{\mathrm{read,eff}}}
+\frac{D_{\mathrm{write}}}{B_{\mathrm{write,eff}}},
$$

$$
T_{\mathrm{MPI}}\approx
\frac{D_{\mathrm{MPI}}}{B_{\mathrm{MPI,eff}}}
+N_{\mathrm{msg}}L_{\mathrm{MPI}}.
$$

ここへ**`T_native_kernel`、`T_fused_kernel`、`T_lowering_replay`**、`T_align_frame`、`T_group_split`、`T_log_replay`、`T_materialize`、`T_scheduler`、`T_checkpoint`、**`T_swap/transpose`、`T_layout_index`、`T_synthesis`、`T_unitary_validate/bind`、`T_symbolic_opt`、`T_parameter_restore`、`T_gradient`**を加える。実行候補は同一の必要精度に合致させ、同じlookahead窓でI/O・MPI・Frame整合・後続の再利用まで比較する。SSD/NIC/CPUが重なる場合は足し算だけでなく実測のMixed Benchmarkからcritical pathを推定する。

### 13.3 最適化対象

優先はend-to-end実行時間。補助指標としてSSD/HDD読み書き量、MPI量、Frame整合回数、Group分割数、Materialization回数、replayコスト、ピークRAM、ログ/索引メタデータ量、SSD TBWを計測する。**実行中の性能低下が検出された場合は、静的Plan継続の残時間と候補計画の切替・移行費用込み残時間を更新し、予測改善が誤差幅を超えるときだけ再計画する。**

SWAPによるMPI削減がストレージ再配置増加を上回るとは限らないため、**SWAPあり/なしの正味のI/O量・MPI量・critical path・COW/TBW**を併記する。Frameを過度に細分化すると索引が肥大化し、後続のCross-BlockゲートでFrame整合が重くなる。**Per-Block Frameがあるから常にFrameを分岐すればよいのではなく、共有状態を維持する費用も最適化対象**である。

### 13.4 Observable / Gradientの追加Taskと性能計上

```text
GROUP_PAULI_PARTNERS / EVALUATE_LOCAL_PAULI / EVALUATE_PAIRED_PAULI
BUILD_ADJOINT_STATE / REVERSE_GATE_FORWARD_STREAM / REVERSE_GATE_ADJOINT_STREAM
GRADIENT_LOCAL_CONTRIBUTION / REDUCE_GRADIENT_VECTOR
LOAD_FORWARD_CHECKPOINT / RECOMPUTE_FROM_PREFIX
VALIDATE_STATE_FRONTIER / ALIGN_TWO_STREAM_LAYOUT / REPLAN_GRADIENT_METHOD
```

`estimated_observable_block_fetch_bytes / remote_partner_pair_count /
pauli_group_count / gradient_two_stream_bytes / gradient_inverse_gate_passes /
checkpoint_recompute_bytes / gradient_allreduce_bytes / max_parallel_bindings /
expected_objective_plus_gradient_time`をEstimator・Traceに追加する。**勾配だけが速くてもobjectiveを含む反復総時間が遅い場合は採用しない。**

## 14. Hardware BenchmarkとResource Estimator

### 14.1 Hardware Profile

| 資源 | 測定項目 |
|---|---|
| RAM/NUMA | read/write/copy帯域、local/remoteアクセス |
| SSD/HDD | Direct I/O、持続読書き、Blockサイズ、SSD台数別、混合read/write、TBW |
| MPI/Network | pair exchange、双方向、all-to-all、レイテンシ、メッセージサイズ |
| Mixed | SSD I/O＋MPI＋Kernel＋ログ書込み＋Frame管理を同時実行 |
| Swap / FusedSWAP | rank-local、rank-global、disk-crossingの交換／融合、レイアウト復元、持続R/W量と交換時間 |
| Gate Kernels / Compiler | native 1Q/2Q/diagonal/permutation/Pauli/**MCX**、`k_local=0..6`、0/1制御、upper-controlのBlock選別、MPI partial/full exchange、packing/contiguous-run長、small-matrix fusion、分解の精度・ancilla/T-count/IR長 |
| **Arbitrary Unitary** | `U(2)`/`U(4)`と3Q/4Q小行列のNative・Fusion・Joint・分解、Matrix生成・unitarity検証、パラメータ値の束縛費用 |
| **Parametric Reuse / Gradient** | symbol依存索引、prefix checkpoint/COW再生、Binding数別のread/write、Parameter-shiftとAdjointのI/O・recompute費用 |
| Metadata | FrameGroup更新、条件付き分岐、ログ索引、Block数増加でのCPU/RAM |
| Recovery | checkpoint commit、再開、ログreplay、COW回収 |
| **Runtime Performance** | 同等負荷条件での初期想定／観測帯域・p95遅延・rank間のばらつき、SSD持続書込低下、NIC混雑、監視CPU負荷 |
| **Dynamic Replan** | 性能低下時の検出から変更までの時間、切替I/O・通信費用、ヒステリシス、改善実績、誤検出率 |

推奨Blockサイズの探索は4 MiB〜1 GiB程度から始める。帯域はSSDやNICの製品ピーク値でなく**継続実測値**を用いる。

この測定は**Stage 1の校正**としてバージョン管理し、Stage 2のPlanTemplate生成に入力する。Stage 3は計測済みProfileに対する差分を`runtime-trace`から推定し、実行中に破壊的な全ベンチマークを繰り返さない。

### 14.2 Dry-run推定項目

```text
n_qubits / complex64-or-complex128
block_size / number_of_blocks / per-node_block_count
physical_state_storage / checkpoint_capacity
per-block local log record counts (logical)
physical shared/joint log bytes
frame_group_count / sparse_override_count / phase_metadata_bytes
frame_align_calls / materialized_frame_operations
semantic_gate_counts / native_gate_counts / fused_gate_counts
arbitrary_u_count_by_width / native_dense_u_count / structured_u_count
unitary_matrix_asset_bytes / unitary_validation_time / unitary_decomposition_count
parameter_symbol_count / parameter_expr_node_count / parameter_binding_count
parameter_specialization_count / plan_reuse_count / bind_eval_time
parameter_dependency_metadata_bytes / prefix_reuse_bytes / prefix_checkpoint_bytes
parameter_suffix_read_bytes / parameter_suffix_write_bytes / gradient_method
gradient_shift_evaluations / adjoint_checkpoint_read_bytes / adjoint_recompute_time
fallback_decomposition_counts / basis_gate_counts / synthesis_error_budget / T_count
mcx_semantic_count_by_control_number / mcx_open_control_count
mcx_k_local_histogram / mcx_k_upper_histogram / mcx_tier_placement_histogram
mcx_native_local_count / mcx_conditional_local_count / mcx_virtual_block_permutation_count
mcx_partial_exchange_count / mcx_full_block_exchange_count / mcx_decomposed_count
mcx_logical_active_pairs / mcx_actual_touched_amplitudes / mcx_packed_mpi_payload_bytes
mcx_actual_mpi_bytes / mcx_disk_read_bytes / mcx_disk_write_bytes
mcx_pack_unpack_time / mcx_frame_alignment_cost / mcx_swap_localization_cost
physical_swap_count / fused_swap_count / layout_transitions
swap_mpi_bytes / swap_disk_read_bytes / swap_disk_write_bytes
SSD/HDD read_bytes / write_bytes / full-pass equivalents
MPI state_bytes / operation_log_bytes / frame_delta_bytes
peak_RAM / pinned_block_group_size
estimated_wall_time (lower / likely / upper)
expected_task_bandwidth_by_workload_class / expected_p95_latency
runtime_observed_bandwidth / runtime_p95_latency / slowest_rank
runtime_profile_version / health_state / replan_count / replan_overhead
baseline_ETA / observed_ETA / adapted_ETA / alert_reason
critical bottleneck / uncertainty
```

`full-pass equivalents`は移動したByteを状態全体のサイズで割った換算値であり、厳密な走査回数と必ずしも同じではない。見積りではGlobal DAGから生成したExecutionPlanの各Taskを積算する。

## 15. 正しさと実行時の不変条件

1. **Local LogはBlock内演算だけを独立に実行できる**。Joint TaskはGlobal DAGに一意に存在し、各Blockからは参照する。
2. **Frameは内部`b`量子ビットのClifford**。上位ビット位相・Block置換と別管理。
3. 各Blockの相対位相・ノルムを保持する。Blockを単独規格化しない。
4. `base_version`、`frame_id`、`phase_id`、`frontier_id`、`layout_id`の組が論理的意味を一意にする。
4a. 物理bit再配置は振幅配列と`QubitLayout`を共に更新する。**`layout_id`だけ書き換えることは、論理状態を維持する物理SWAPの代わりにならない**。
5. 全ての演算は一意な`op_id`を持ち、Frame更新・ログ再生・Kernelで**二重適用しない**。
6. 共同Taskには入力Block群の厳密な版を要求する。後続LocalOpは新しい出力版を対象にする。
7. Frame alignmentでは演算前の入力を破壊せず、出力Block全体を整合した版として公開する。
8. SharedOp/FrameGroup更新は巨大なBlock数へ無条件に物理fanoutしない。
9. 旧版ReplicaへのOperation Shippingは一致したbaseと依存がなければ禁止。
10. 最終振幅出力は未実行DAG・Frame・位相・置換をすべて解消する。
11. 途中測定は確率・状態更新・条件付き後続演算の依存を正しく扱う。
12. Frameやログのコスト削減が最終的な再生時間を悪化させる場合も必ず測る。
13. Clifford+T近似合成が必要なゲートのみ誤差を明示し、シミュレータの数値誤差と区別する。**native可能な局所・対角・置換ゲートを無条件に分解しない**。
14. FusedSWAPで複数bitを入れ替えても、元回路の論理演算順序・出力bitstring意味・観測量の作用は不変。
15. **RuntimeProfileはStage 1の不変なHardwareProfileを上書きしない。** 比較条件が一致しない・測定不足の場合は性能異常を断定しない。
16. `LIVE_TUNE`以外の変更はsafe frontierで入力Block版、Frame、Layout、Operation Logと対象rankの合意を検証してcommitする。
17. **実行時性能低下を理由に量子回路の意味、合成精度条件、測定結果を勝手に変更しない。**
18. 現在のMPIが対応しないrank数変更・障害ノード自動切離しは約束せず、checkpoint/restartの操作として扱う。
19. 性能監視・再計画による負荷と再配置コストを必ず測定し、効果がなければ静的Plan継続を選べる。
20. **MCXのClifford判定は制御数で分岐**。`k=0/1`はClifford、`k>=2`は非Clifford。upper制御でBlock選別後に残るlocal制御数でも同じ原則を適用し、Frameを無視したraw振幅交換を禁止する。
21. **upper-target MCXを無条件にBlock ID置換で代用しない**。全ての制御・標的がupperなら仮想Block置換が可能だが、local制御が一つでもあればJoint Taskで条件付きoffset交換が必要。
22. **MCXのOpen/Negative controlsを必ず保持**。control mask/valueはlayout版と結び、bit順・重複・target除外をチェックする。
23. **疎な演算対象は疎なI/O量を保証しない**。`2^{-k}`とSSD/MPIの実測転送量を区別し、packing/読書き粒度をコストに含める。
24. **ユーザー提供の固定Uはshape・finite・unitarity・bit順を登録時に検証**する。任意行列を勝手にClifford／対角として扱ったり、黙って修正したりしない。
25. **Parameter BindingはJob/Planのimmutableな版**として扱う。異なるBindingで最新Block/Frame/OpLogを混ぜない。
26. **共通prefix状態の再利用はパラメータ依存・frontier・Layout・Frame・入力状態が一致する場合だけ**。共有baseはread-onlyでCOW suffixに分岐する。
27. **パラメータ値に依存した最適化には適法条件/guardとbinding IDが必要**。別Bindingへ無検証で特殊化計画を流用しない。
28. **任意U(theta)の解析勾配は自動で存在しない**。generator/derivative記述を持つ場合のみ適切な解析法を選び、通常の任意行列には安易な2点Parameter-shiftを適用しない。

## 16. 検証用テストセット

### 16.1 小規模の数値照合

- 8〜16量子ビットで`b=2..8`を変え、dense NumPy参照と比較。
- **ランダムな非stabilizer初期状態**（Block間の複素相対位相あり）を必ず使う。
- 同じFrameを共有したまま下位H/S/CXを大量適用。
- 上位制御→下位標的CXでFrame Groupを条件分岐。
- 上位Z/S/Rz、上位X/CNOT/SWAPで位相・Block番号置換を検証。
- 異なる`C_A/C_B`を持つBlock対へ上位Hを実行し、上記のFrame alignment式と一致。
- 下位制御→上位標的CXの部分振幅移動を検証。
- `T/Rz`の共役がBlock別Pauli stringへ変わる例。
- `X/Z/CZ/SWAP/Rx/Ry/Rz/MCX`の**nativeと強制分解の両方式**で最終状態・観測量が一致すること（近似合成時は指定epsilon範囲内）。
- **global bitへ作用するCZ/RzがMPI振幅通信なしに位相更新できること**と、強制分解では通信・I/Oが増え得ることを確認する。
- **MCX配置・制御数の組合せテスト**：`k=0..6`、全1/全0/混合の制御、全local、upper制御＋local標的、全upper、local制御＋upper標的、disk-crossing・MPI-global・異なるFrame Group・遅延ログを組み合わせる。
- MCXの`Frame materialize→conditional swap`と、合法な局所Frame更新の双方がreferenceと一致することを確認する。raw BlockへFrameを無視したCCXを作用させた場合は、明示的に不一致を検出する。
- upper target・local controlsではpartial exchangeと全Block交換の結果を一致させ、MPIバイト数やbuffer packingオーバーヘッドの差を測定する。
- Block-localな任意1Q/2Q unitaryのnative・fusion・decompositionで結果を比較し、3Q以上の巨大DenseMatrixを無条件には選択しない。
- **任意U検証**：非unitary、NaN、shape違反、bit順違い、資源上限超過を拒否。対角/置換の正しい分類、制御U、Frame不一致でのJoint実行と参照結果を照合する。
- **Parameter検証**：`Rz(theta) Rz(-theta)`のsymbolic cancellation、同一Bindingでの再実行一致、`theta=0`やClifford角のguarded specialization、異なるBinding間の旧版混入を検出する。
- **Prefix/COW/勾配検証**：安全な共通prefixのみ再利用、Frame/Layout/Joint依存が異なれば共有不可、Parameter-shiftを有限差分・小規模参照と比較、Adjointのcheckpointコストと数値結果を別々に検証する。
- 論理SWAPをマッピングのみで表現する場合と、物理Index-bit SWAP+Mapping更新の場合を分けて検証。
- `local↔disk-crossing`・`local↔MPI-global`・`disk-crossing↔MPI-global`のデータ再配置、Frame/位相整合。
- FusedSWAPと単独SWAP列の一致、レイアウトを戻さず続行した場合の最終出力・観測の一致。
- Join Taskを遅延した後のLocalOpが**Joinの出力版**へ接続されることを検証。
- final vector、Pauli observable、途中測定/分岐、checkpoint restart。

比較は数値丸めを考慮した許容誤差で行い、最終状態の比較では必要に応じて全体系のglobal phaseのみ補正する。

### 16.2 多ノード・障害・メタデータ

- 1/2/4 rankで局所・共同Taskが一致。
- 古いReplicaのstate/log/frame混在を拒否。
- Join途中・FrameGroup分岐途中・manifest確定途中の中断。
- 65,536 / 67,108,864 Blockを**振幅本体なしのdry-run**で表現し、メタデータがBlock数比例で爆発しないかを検証。
- 非同期I/O中のBuffer pinning、複数Block予約、MPI通信progress。
- `no-swap / order-preserving / lookahead`全モードが同じ状態を返すこと。
- SWAP計画中のcheckpoint→restart後も物理データ・Frame・Log・Layout IDの対応が維持されること。

### 16.3 Runtime I/O性能低下と安全な再計画の試験（必須）

- **SSD writeが想定の75%→50%へ低下**：持続性能の低下検出、read/write混在を考慮した誤検出抑制、先読み深度・writeback並行度の変更を検証する。
- **1台だけSSDが遅い／容量が逼迫**：最新Replica/代替SSDがあればそちらへ切替。なければ無理な書込みをせず警告・安全停止する。
- **NICの実効MPI帯域が想定より低い**：local SSD・Operation Shipping・SWAP/FusedSWAP候補のコスト再計算を行い、必ず再配置費用込みで採否判定する。
- **1 rankだけ遅い**：平均帯域だけで判断しない。slow rankによるglobal stage延長を検出する。
- **一時的な輻輳（10〜30秒）**：同条件のサンプル不足・ヒステリシスにより不要な計画変更を抑制する。
- **低下後の回復**：すぐ元に戻すのではなく、cooldown・予測改善・計画切替費用で判定する。
- **Frame/Joint Taskまたはphysical SWAPの途中に低下**：未完了Taskを破壊せず、次の安全frontierまで待って切替える。
- **MPI障害／ノード脱落**：対応しないrank動的変更を行わず、checkpointからの復旧へ安全に移行する。
- **想定Performance Profileが未測定／不一致**：`PROFILE_MISMATCH`として警告し、無根拠な自動最適化をしない。
- **監視負荷**：監視OFF/ONでend-to-end実行時間を比較し、専用ベンチが本番を妨げないことを確認する。

性能劣化の注入は、**内部の帯域制限付きMockTransport／遅延注入I/O**を最初の手段にする。実際のネットワーク帯域制限やディスク負荷試験は隔離された評価環境・明示的な運用許可の下だけで実施し、共有HPC環境の装置を無断でスロットル・高負荷試験しない。

**合格条件**：低下前後の数値状態一致、`detect -> propose -> (optional) commit -> verify`のトレース、改善予測と実測の差分、誤検出件数、切替オーバーヘッドを一式として提出する。

### 16.4 分散期待値・Adjointの検証（初期から必要）

1. 2〜9量子ビットで`b`を変え、任意Pauli string（`I/X/Y/Z`）のBlock内積がNumPy全ベクトルと一致する。
2. `x_H=0`、`x_H!=0`、`z`のみ上位bitの三種で、必要なPartner Block集合とMPI要求が一致する。
3. 異なるFrame/相対位相を持つBlock対での観測計算を、実振幅へFrameを反映した参照結果と比較する。
4. Hamiltonianの複数Pauli項が単体期待値と一致する。グループ化有無でBlock読み込みByteが計画どおり変わる。
5. 逆伝播式を有限差分と照合。`Rz/Rx/Ry`、多体Pauli回転、Block外のbitに作用する回転を含める。
6. 同一パラメータを複数ゲートへ異なる係数で束縛し、Chain Ruleによる勾配の合計を確認する。
7. Forward/AdjointでレイアウトSWAP/Frame/frontierが一致しないケースを拒否する。
8. 途中測定や導関数を持たない任意Uへ未対応Adjointを黙って適用せず、正しいfallbackまたは明示エラーとする。
9. 1/2/4 MPI rankで局所部分和と最終Reductionを照合。各rankのGradientを一回にまとめてReductionした場合と逐次Reductionを照合する。
10. `O|psi>`生成、2状態Backward、SSD劣化・MPI混雑時の安全な再計画、Checkpointの復元と再開を検証する。

初期検証用`distributed_expectation_adjoint_reference.py`について、**875件のBlock Pauli内積、576件のrank別部分和（模擬分散）、45件のAdjoint勾配、計1,496件がNumPy参照・有限差分と一致**することを確認済み。既存のMCXテストとは独立。**実MPI通信、実SSD/HDD、Per-Block Clifford Frame混在はこの参照テストの検証範囲外**。実MPIとディスク混合環境での性能検証はその後に行う。

## 17. 性能比較と新規性の評価

単なるClifford tableau、out-of-core、遅延DAGやMPI分散だけでは新規性と断定しない。**Per-Block Frameと局所ログを基盤とし、Block間依存・階層I/O・MPIの共同最適化が有利となる条件**を実験で示す。

比較モード（**本来の実装の一部を無効化するアブレーション**）は以下とする。**実行ポリシー（native/auto/強制分解）とSWAP policyの比較は別軸**とし、同一回路・同一精度条件で直交比較する。

| モード | 比較仕様 | 主な検証点 |
|---|---|---|
| E0 eager | 逐次実振幅更新の参照 | 正しさ |
| E1 block-batching | Frame無し、局所Batch実行 | 単純なまとめ実行の効果 |
| E2 deferred-no-frame | Local/Shared/Joint Logで遅延。`C_h=I`固定 | ログとDAGの寄与 |
| E3 global-frame baseline | 可能なCliffordを単一全体Frameとして追跡 | 全体Frameとの比較（検証用） |
| **E4 per-block-frame** | **本設計の基本構成**。Local Frame＋位相＋Block置換 | Frame独立化とFrame整合の損益 |
| E5 frame-groups + MPI | E4＋Group共有＋State/Op/Frame shipping＋階層I/O | システム全体の利点 |

**SWAP policy比較**：`S0: no-swap`、`S1: order-preserving SWAP`、`S2: DAG-aware lookahead＋FusedSWAP`。すべてのpolicyについて、元回路への論理SWAPと物理レイアウト交換を区別し、交換に使ったMPI/SSD時間を含める。

**ゲート実行比較**：`auto-native`（既定）、`native-only`（未対応はエラー）、`force-decompose + clifford-t`（近似合成誤差/T-count報告）、`force-decompose + clifford-t-rz`を比較する。さらに局所1Q/2Qの融合有無、global対角CZの未分解効果、**MCXのNative conditional permutation／制御数とopen controlによる活動密度／部分交換／SWAP局所化／Clifford+T分解の差**も測る。**異なる合成誤差条件は同じ性能試験として比較しない**。

評価回路：Clifford-only、Clifford+T/Rz、対角回路、**Toffoli/CCCX/高制御MCXを多用する可逆論理・加算器・multi-controlled演算**、**任意U(2)/U(4)/3Q/4Q Native vs 分解、共有Parameterで繰り返すVQE/QAOA風のパラメータ回路、同じprefixと異なるsuffixのparameter sweep、ParamShift vs Adjoint**、局所Hamiltonian、QFT、random/all-to-all、非局所H/CX密度が高い回路。**全状態出力・期待値評価まで含むend-to-end時間**を評価する。

数値指標は実時間、SSD/HDD読書き量、MPI送信Byte、materialization量、Frame alignment time、Frame Group数、メタデータByte、Block再利用率、Estimator予測誤差など。

**実行時適応のアブレーション**として、同じ性能低下シナリオに対し、`Fixed Plan / Monitor-only / Automatic Live Tuning / Safe Frontier Replanning`を比較する。**全計測と再計画費用込みのend-to-end時間、劣化検出までの遅延、誤検出、切替回数、残時間予測誤差、元回路との一致**を報告する。

## 18. 実装ロードマップ（全機能を最初から設計する）

**最初のコミットからGlobal Sequencer、Per-Block Log、Per-Block Frame参照、Global DAGの境界を備える。** **同時に、HardwareProfiler / StaticCircuitOptimizer / AdaptiveRuntimeSchedulerを別コンポーネントとして切り、最初はスタブでもprofile→plan→traceを結合する。** 高度な最適化は段階的でも、Per-Block Frameを後付けするためのデータ構造変更を前提にしない。

### Phase 0：意味保存Gate IR・Native-first分岐・Layout参照テスト

- `uv`、pytest、NumPy eager reference。
- Stage 1: HardwareProfile schemaと最小CPU/RAM/disk/NIC discovery、Stage 2: 静的PlanTemplate schema、Stage 3: 版付きExecutionTraceのインターフェースを最初から用意する。
- **Runtime Performance Guardの型を最初から追加**：期待負荷条件付きベンチマーク値、`RuntimeProfile`、`HealthReport`、`PlanCandidate`、計画バージョン、監視モード、`LIVE_TUNE / FRONTIER_REPLAN / RESTART_REQUIRED`の区別。最初の実装では検出と候補提示だけでもよい。
- **`UnitaryValidator`、`MatrixAssetRegistry`、`ParameterSymbol/Expr DAG`、`ParameterBinding`、Binding別ブロックバージョンを最初から型定義**する。NumPy参照で固定U(2)/U(4)と`Rz(theta)`の象徴的な式合成・値束縛を検証する。
- `GateNormalizer`（ゲート意味・対角/置換/Clifford属性を保持）、`NativeGateSelector`（初期は1Q/2Q/diagonal/**MCX条件付き置換**）、`FallbackDecomposer`（`H/S/T/CNOT`＋任意`Rz`基底）、`GlobalSequencer`、`PerBlockLog`、`JointTaskDAG`、`BlockView`、`FrameGroupRegistry`、`QubitLayout`。
- **MCXは制御数に比例したDenseMatrixへ機械的にloweringせず、任意制御マスク・制御値0/1・標的bitからpredicate付き交換を導く**参照実装を作る。
- **論理SWAPと物理Index-bit SWAPの意味の違い**をNumPy参照で初期からテスト。compilerの近似誤差も保存。
- 下位H/S/CXと上位H/CXで異なるFrame・相対位相を検証。
- **Observable/Gradient基盤**：`PauliTerm`、`ObservableIR`、`BlockPauliInner`、Forward/Adjointの論理StateHandleと`GradientPlanTemplate`を初期から型定義。小規模NumPy参照と勾配の有限差分検証を追加。
- **完了条件**：8〜12量子ビットでsemantic gate→log→native/fused/fallback→frame更新→joint materializationの数値一致と、レイアウトを変更しても論理状態が不変であること。さらに期待値・AdjointのBlock内積が参照結果と一致する。

### Phase 1：単一ノード・RAM＋SSD/HDD

- Block Cache、COW、同期I/O、`O_DIRECT`可能なら採用。
- 単一ノードの`PHYSICAL_INDEX_BIT_SWAP`とBlock/SSD境界交換を実装し、出力LayoutとFrame/位相整合を確認。
- Local/Shared/Joint Log物理記録・再生、Group共有と条件付き分岐。**Block-local任意1Qと専用対角/CNOT/CZ/MCX（条件付き置換）カーネルを分解なしで実行**し、演算を融合してI/OとCPU時間を比較。
- RAM上限を強制してデータ移動・Frame整合のtraceを収集。
- Stage 1の単体I/O・RAM測定を始め、Stage 2で生成した静的Blockアクセス計画とStage 3の実測traceを比較する。
- **任意U(2)/U(4)をBlock内nativeでApply、3Q/4Qについてnativeと分解を比較**する。パラメータ非依存prefixのread-only snapshotとBinding別COW suffixを小規模に導入し、共有可否とread/write量をtrace化する。
- **SSD read/write Taskの完了情報（byte、時間、queue wait、block size、同時実行数）から、初期予測と現在の実効帯域を比較**。帯域低下はMockTransportで注入し、まず`observe`モードで検出の妥当性をテストする。
- `ObservablePlan`でPauli項をBlock partnerごとにグループ化。SSD上の同じBlock pairを読みながら多数observableをstreaming評価するPoCを作成。
- **完了条件**：eager / batching / deferred / per-block-frameの正しさとI/O比較に加え、native vs fallbackの同一ゲートの結果・時間を比較。**観測の全状態Gatherなしでの値一致とI/O traceも必須。**

### Phase 2：Clifford/Non-Cliffordの統合

- Block-local Clifford tableau、相対位相・Block置換。
- T/RzのFrame共役変換とPauli-string rotation kernel。**Symbolic ParameterがFrame吸収候補を変える場合は、bind時にguarded specializationだけを採用**し、汎用Planは温存する。
- `KEEP_LOCAL_FRAME` / `ALIGN_FRAME` / `MATERIALIZE_FRAME`のcost heuristic。
- **完了条件**：Clifford+T/Rzでeager referenceと一致。

### Phase 3：MPI＋ストレージ階層

- **実装設計**：[Phase 3 設計書](../docs/phase3_design.md) に、所有権・通信順序・分散 commit・2状態 Adjoint・容量/GC・検証条件を具体化する。
- **実装・検証**：[同期 MPI runtime](../docs/phase3_implementation.md)、[1/2/4 rank 実測](../docs/phase3_benchmark.md)。packed MCX、prefix/COW、S0/S1、期待値・随伴、checkpoint/restart を uv＋実 MPI で検証。
- **開発時の参照メモ**：[Mac Studio 単体の MPI 開発資料](../ref/mac_studio_mpi_quantum_simulator.md) を参照する。Open MPI＋mpi4py＋uv による同一 Mac 上の複数 rank、rank と Block の分離、タイル通信、macOS F_NOCACHE を検証する。同一ホストの実測通信性能を実ネットワークへそのまま流用しない。資料内の Phase 番号は独自のもので、本計画のロードマップを優先する。振幅ファイルは本計画の rank ごとの集約 segment を継続し、Block ごとにファイルを作らない。
- `mpi4py`をTransportへ統合、Joint Taskをrank間へ拡張。
- **MCXのupper-target / local-control時の条件付きpartial exchange**を実装し、全Block交換・分解とのMPIバイト数/実行時間を比較する。
- **異なるBindingのread-only prefixをMPI rank間で安全に共有できるか検証し、Frame/Layout/frontier一致を条件にcheckpointからsuffixを実行**する。状態転送の再利用分と全体再計算の費用を実測する。
- **MPI-global ↔ rank-local/disk-localのindex-bit SWAP**と、ゲート順序を変えないSWAP最適化(S1)を実装。まずNo-SWAP(S0)と比較。
- State / Operation / Frame shippingの合法性・版管理・再生。
- **MPI Transferごとの観測帯域・待ち時間をTask traceへ入れ、rank別/peer別にStage 1のネットワーク性能予測と照合**する。特定rankを低速化したケースも検証する。
- `DistributedExpectationEngine`：MPI rankを跨ぐPauli Partner交換・部分和・`Allreduce`を実装。`DistributedAdjointEngine`ではForward/Adjointの2状態をBlock単位で逆伝播し、勾配部分内積を共有Kernelで計算する。
- **完了条件**：2〜4 rankでMPI+SSDの数値一致、転送量traceが実測と一致。期待値・Adjointも参照状態/有限差分と一致する。

### Phase 4：非同期・階層スケジューラ

- **実装設計**：[Phase 4 設計書](../docs/phase4_design.md) に、非同期 request/buffer 寿命、epoch cut、Parameter batch、Runtime Guard、safe replan、S2/FusedSWAP、Gradient Scheduler と検証順序を具体化する。
- **複数nodeへの前提改修**：振幅・catalog・journalをnode-localに置き、prepare/commit/resume、prefix leaseとGCをMPI通信で成立させる。共有filesystemから全rankのprepareを直接読む前提を解消し、host/device単位のdisk admissionとcheckpointの保存容量を検証する。
- **開発・検証環境**：Mac上のMPIとruntime scheduler／local job runnerで1/2/4 rankを開発し、研究室クラスタのPBS割当内でLinux・実2 nodeの2/4 rank・local SSD・実通信経路を検証する。同一Macの結果をmulti-node性能と扱わない。site固有の構成・運用条件は内部資料で管理する。
- `io_uring`、nonblocking MPI、prefetch、background materialization。**Parameter batchスケジューラのbinding別メモリ割当とprefix checkpointのread/write競合を考慮**する。
- Stage 3のAdaptive Schedulerと**Runtime Performance Guard**を完成させる。profileに対応する実効帯域・遅延のローリング比較、持続性判定、ヒステリシス、alert、ETA再予測、`RuntimeProfile`の版付き更新を実装する。
- **性能低下を確認したとき、まずLIVE_TUNEを試し、改善が確実に見込める場合だけsafe frontierでFRONTIER_REPLANをcommit**する。切替費用、`plan_version`、失敗時fallbackを記録する。
- `observe/recommend/auto`モード、運用者からのポリシー変更・手動再計画・dry-run APIを実装する。
- future-aware cache、NUMA/PCIe競合、Frame整合とGroup分割のコスト。
- 先読み窓でDAGの合法なゲート順序変更を考慮するS2と、`FUSED_INDEX_BIT_SWAP`を実装。MPI削減とSSD増加の損益分岐を測定。
- **I/O-aware Gradient Scheduler**：Adjoint逆再生・Checkpoint/Recompute・Parameter-shift Job並列の候補費用を比較し、Block pair取得順、再計算・状態復元を安全に切り替える。
- **完了条件**：S0/S1/S2の正味速度差と、MPI削減量・swap増分ディスクI/O・Frame整合費用をtraceで説明できる。**SSD/MPI性能を意図的に落としたときに、継続観測→検出→低リスク変更／安全な再計画→新性能の確認まで再現可能である。**

### Phase 5：Resource Estimatorと実機ベンチマーク

- `svbench`→`hardware-profile.json`、`svsim estimate`→`execution-plan.json`。
- Stage 1で採取したprofileを再校正し、Stage 2の静的推定値とStage 3の逐次残時間推定の両方を評価する。
- **パラメータ反復実行について、素朴な全回路K回実行と、Plan再利用／prefix snapshot・COW／Parameter-shift／Adjointを同一精度・同一ハードウェアで比較**する。全iterationのwall-clock、I/O、Checkpoint容量、勾配誤差を報告する。
- **初期予測と実効値が離れた場合の再計画を、固定PlanとのA/B比較で検証**する。改善量は監視・再計画・移行の追加費用を含む実時間で報告し、誤検出率と再計画回数も評価する。
- 32〜36量子ビットで実測校正、40量子ビット級の容量・時間推定、50量子ビット級HPCのdry-run。
- `auto-native`と強制`clifford-t`/`clifford-t-rz`分解、`S0/S1/S2`別にtime/bytes/native/fused/fallback counts/T-count/合成誤差を出力。
- 期待値・勾配のBlock別通信/I/O量、2状態容量、observable数・parameter数に対するend-to-end時間、Gradient Methodの損益分岐を測定する。
- **完了条件**：推定誤差とBottleneck分析を報告できる。

## 19. 推奨リポジトリ構成とCLI

```text
svsim/
  pyproject.toml
  src/svsim/
    compiler/          # semantic gate normalization / conditional basis lowering
    unitary/           # validator / immutable dense U & structured matrix assets
    parameters/        # symbols / expression DAG / immutable binding & guard
    parametric/        # parameter batch / reuse frontier / prefix COW snapshots
    observables/       # Pauli mask IR / partner grouping / distributed expectation
    gradients/         # distributed adjoint / reversible replay / shift job cost planner
    ir/                # high-level semantic Gate IR / Global Sequencer / DAG
    journal/           # local / shared / joint append-only logs
    frame/             # per-block tableau / Frame Group / alignment
    phase/             # relative phase and virtual block permutation
    block/             # BlockView / version / replica / cache / layout
    layout/            # logical swap / index bit perm / layout transitions
    swap/              # no-swap / order-preserving / lookahead / fused
    planner/           # deferred batch / materialization / dependencies
    scheduler/         # placement / runtime adaptation / safe replan
    profiler/          # Stage 1 discovery / NUMA / NIC / storage / calibration
    static_optimizer/  # Stage 2 gate/layout/SWAP/Block cost search, PlanTemplate
    telemetry/         # Stage 3 runtime trace / bandwidth feedback / remaining-time
    runtime_monitor/   # performance classes / task metrics / expected vs observed
    health/            # health state machine / hysteresis / alerts / slow-rank detection
    runtime_control/   # observe/recommend/auto / live policy updates / operator approval
    replan/            # candidate scoring / safe-frontier commit / fallback
    transport/         # direct_io / memory / mpi
    kernels/           # native 1Q/2Q / generic MCX conditional-swap / diagonal / permutation / fusion / joint
    checkpoint/        # COW / manifest / recovery
    benchmark/         # disk / mpi / mixed / metadata
    estimator/         # HardwareProfile / execution plan cost
    cli.py
  tests/
    reference/
    per_block_log/
    compiler/
    mcx/                  # k=0..6, open controls, distributed partial exchange
    arbitrary_unitary/    # arbitrary matrices / property validation / native vs lowering
    parameter_binding/   # expression fusion / binding versions / guarded specialization
    parametric_reuse/    # common prefix / COW / gradient verification
    observable_blocks/  # block-local/paired Pauli, Hermitian reduction
    adjoint_blocks/     # forward/adjoint streams, finite-difference checks
    gradient_planner/   # adjoint vs shift / I/O aware checkpointing
    swap_layout/
    per_block_frame/
    frame_alignment/
    joint_tasks/
    mpi/
    failure_recovery/
    performance_guard/
    degraded_io_replan/
  docs/adr/
```

CLI（仕様案・未実装）：

```bash
uv sync
uv run pytest

# 標準実行は常にPer-Block Log + Frame + Global DAGを経由
uv run svsim run circuit.qasm \
  --block-size 256MiB --ram-budget 4GiB \
  --gate-execution auto --decomposition-basis clifford-t-rz   --swap-policy order-preserving \
  --storage /scratch/svsim --trace trace.json

uv run svbench all --output hardware-profile.json
uv run svsim estimate circuit.qasm \
  --hardware hardware-profile.json \
  --gate-execution auto --decomposition-basis clifford-t-rz   --swap-policy lookahead \
  --output plan.json

mpirun -np 4 uv run svsim run circuit.qasm \
  --swap-policy lookahead --swap-fusion auto \
  --storage /scratch/svsim --trace trace-mpi.json
```

### 3段階のCLIフロー（設計予定、未実装）

```bash
# Stage 1: 設置時と構成変更時のHardwareProfile作成・校正
uv run svsim hardware probe --output hardware-profile.json
uv run svbench all --profile hardware-profile.json --update

# Stage 2: 回路登録時の静的最適化と推定
uv run svsim register circuit.qasm --hardware hardware-profile.json \
  --output plan-template.json --estimate static-estimate.json

# Optional: 同一PlanTemplateでパラメータを多数回評価（未実装のCLI仕様）
uv run svsim run --plan plan-template.json --params params-a.json \
  --parameter-cache /scratch/svsim/param-cache --trace params-a-trace.jsonl
uv run svsim batch --plan plan-template.json --params-file parameter-sets.jsonl \
  --reuse-common-prefix --output parameter-results.jsonl
uv run svsim gradient --plan plan-template.json --params params-a.json \
  --observable pauli-observables.json --method auto --output gradient.json

# Stage 3: 実行時の実測適応（検証済みPlanTemplateを利用）
mpirun -np 4 uv run svsim run --plan plan-template.json \
  --hardware hardware-profile.json --adaptive \
  --trace runtime-trace.jsonl

# 残時間の推定（ジョブのtraceと現在のplanから）
uv run svsim estimate-remaining --plan plan-template.json \
  --trace runtime-trace.jsonl

# 実行中の性能充足率・遅いrank/SSD・再計画理由を確認
uv run svsim runtime status --job <JOB_ID> --detail io,mpi,eta

# 監視だけ / 推奨 / 自動変更の動作モードを切り替える
uv run svsim runtime policy --job <JOB_ID> \
  --mode auto --config runtime-policy.yaml

# 現在の実効性能でPlanを再計算し、適用前に差分を確認
uv run svsim runtime replan --job <JOB_ID> --dry-run

# safe frontierで検証済みのplan candidateを明示的に適用
uv run svsim runtime apply --job <JOB_ID> --candidate <CANDIDATE_ID>
```

これらはインターフェース案で、実装済みのコマンドではない。運用中のprofile不適合時は警告または再登録を要求し、静的プランの前提を黙って書き換えない。

`--deferred`は標準動作の有効化オプションにしない。遅延ログ・Per-Block Frameは常時基本構成。eager/no-frameなどはデバッグ・実験のための明示的比較モード。

## 20. ADR：決定事項と未決事項

### 決定事項

- **高水準Semantic Gate IRを標準**とし、任意のU(2**k)行列を検証して受付け、ユーザーによる事前分解を原則不要とする。パラメータ式はsymbolic DAGとして保持し、Binding別にPlan特殊化、合法な共通prefix再利用とGradient Plannerを提供する。実行はnative/構造化Kernel・small fusion・joint・SWAPを優先候補としてコスト比較する。**MCX（X/CNOT/Toffoli/CCCXを統合）は任意制御数の条件付き振幅置換として初期サポート**し、`k>=2`をClifford tableauへ吸収したり制御数に比例するDenseMatrixへ変換したりしない。Clifford+T（またはClifford+T+Rz）は**fallbackおよび比較用の分解基底**とする。
- **Qubit LayoutとSWAP/FusedSWAP Plannerは基本構造**。no-swap比較モードを維持し、MPI＋SSDを横断する振幅転置をCost Modelに含める。
- **Per-Block Operation LogとPer-Block Clifford Frameが基本構造**。
- Block内Cliffordのみを`C_h`へ追跡し、上位位相・Block置換と分離する。
- Frame Groupの物理共有/COW・共有演算テンプレート参照を使い、Block数に比例した物理メタデータfanoutを抑える。
- Cross-BlockゲートはGlobal DAGの一意なJoint Taskで表し、Local Logは参照する。
- Blockの相対位相・未正規化振幅を維持し、Frame整合コストを計上する。
- `mpi4py`を通信Backend、Linux Direct I/Oを振幅用候補とする。
- 見積り用traceは初期実装から収集する。
- **Stage 1の版付きHardwareProfile、Stage 2の版付き静的PlanTemplate、Stage 3の実測に基づく動的調整**を独立コンポーネントとして実装する。Stage 3は合法な代替候補とsafe frontierの制約を守る。
- **Runtime Performance Guardを必須**とする。Stage 1の計測条件付き基準性能に対し、Stage 3のSSD/HDD I/O・MPI・rank別遅延の充足度を検査する。劣化時は`RuntimeProfile`を別版で作り、安全で得な計画だけ切り替える。`observe/recommend/auto`を提供する。
- 固定rankのMPI実行中に、NIC物理能力やrank数を任意に変更できるとは仮定しない。必要ならcheckpoint/restartと再登録で対応する。

### PoCで比較・決定

- Blockサイズ、Frame Group membership_rule表現（mask/range/sparse override）。
- 共有演算・Frame更新の物理ログ形式と索引構造。
- Frame間変換をどの順で行い、同一Frameへのmergeをいつ行うか。
- 位相の数値形式と誤差管理、checkpointの一貫したcut。
- Joint Taskの最大working-setと合法なstreaming kernel設計。
- Frame/Operation shippingを許す演算集合と再生コスト閾値。
- `clifford-t`合成アルゴリズムと許容epsilon、CNOT/T countと精度のトレードオフ。native Kernelを選ぶ演算種別・融合幅・cost推定の閾値をベンチマークで決める。
- MCXの`k_local`ごとのpartial exchangeのscatter/gather・contiguous-run/packing方式、Frame alignment先行／SWAP局所化／分解の損益分岐、open controls、巨大Block数に対するmask/range型の仮想置換索引方式。
- 任意密行列UのNative許容幅（3Q/4Q）、最大行列容量、unitarity閾値、属性検出の誤差予算、Controlled-Uと分解のコスト。
- ParameterExpressionで許可する演算集合、parameter-dependent frameのguard、cache fingerprint構造、prefix checkpointの採用条件とBatchサイズ、VQE用Gradient PlannerのMethod自動選択。
- SWAP先読み窓、候補ローカルbit、multi-bit FusedSWAPの最大幅、交換途中のcheckpoint / COW方式。
- レイアウト越境時のFrame Group materializationをどこまで省略できるかの安全条件。
- Frame Group数の増殖を防ぐcompact/merge方針。
- 性能低下判定の窓・サンプル数・閾値・ヒステリシス、誤警報と見逃しのトレードオフ。
- I/O同時実行数／背景処理／SWAP計画の動的変更が、どの回路・ハードウェア負荷で改善するか。
- ランタイム性能観測とプラン再計画のCPU・メモリ・ログ肥大化に対する上限。

## 21. 開発担当者が最初に提出する成果物

1. **ADRと不変条件**：Native-firstを既定とした分解選択・分解精度、Per-Block Frameの作用空間、相対位相、Cross-Block Joint Task、論理frontierと物理base、**論理SWAPと物理Layout遷移**の違い。任意U検証、symbolicパラメータのimmutable Binding、値依存特殊化のguard、parameter-sensitive BlockVersionについて明記。
2. **最小実装**：Semantic Gate IR→Global Sequencer→Block Log/FrameGroup→Native/Fused/Fallback Planner→Joint DAG→materialize。8〜12量子ビットで参照と一致。固定U(2)/U(4)、Rz(theta)のsymbolic fusionとbindにも対応する。
3. **数値テスト**：異なるFrameのBlock間H、Block置換、相対位相、非Clifford共役、測定とCheckpoint。
4. **メタデータ負荷テスト**：40/50量子ビット相当のBlock数を状態なしで作り、物理ログ/Frame Groupが爆発しないことを測る。
5. **ベンチマーク報告**：native vs force-decomposeとBlock batching、deferred-no-frame、per-block-frame、frame-groups/MPIのI/O・CPU・wall-clock比較。MCXについては制御数`0..6`、全local / 全upper / upper-target partial exchange、open control、MPI packingコストを最低限含める。
6. **SWAPポリシー評価**：S0/S1/S2別にレイアウト更新の正しさ、FusedSWAP効果、MPI削減vs SSD増分・TBWを比較。
7. **Runtime Performance GuardのPoC**：SSD/MPI帯域低下を擬似注入し、初期想定との偏差、アラート、自動候補、safe frontierでの切替と結果一致、固定Planとの総実行時間差を報告。
8. **運用制御**：`observe / recommend / auto`の変更、手動dry-runとcandidate適用、`RuntimeProfile`・`plan_version`・判断根拠の監査ログ。

## 22. 参考実装・先行研究と研究仮説

- [SnuQS (ICS 2022)](https://doi.org/10.1145/3524059.3532375) — ストレージ利用型の状態ベクトルシミュレーション。
- [QDAO](https://github.com/Zhaoyilunnn/qdao) — メモリ階層を考慮した回路分割とデータ移動。
- [HiSVSIM](https://github.com/pnnl/hisvsim) — 階層的な局所性最適化。
- [Qiskit Aer](https://qiskit.github.io/qiskit-aer/howtos/running_gpu.html) — 分散シミュレーションとchunking。
- [mpiQulacs official MPI README](https://github.com/qulacs/qulacs/blob/main/README_MPI.md) — automatic `SWAP/FusedSWAP` insertion / `swap_level` 0,1,2。
- [Qulacs Advanced Guide](https://docs.qulacs.org/en/latest/guide/2.0_python_advanced.html) — 特殊ゲートKernelと一般行列ゲートの特性。
- [Qiskit MCXGate](https://qiskit.qotlabs.org/docs/api/qiskit/qiskit.circuit.library.MCXGate) — 任意制御数とopen/negative controlの表現、他シミュレータとの意味の照合に利用。
- [Qulacs Gate API: TOFFOLI](https://docs.qulacs.org/en/v0.6.10/pyRef/qulacs/gate/index.html) — `TOFFOLI(control1, control2, target)`を独立したゲートとして提供。APIの存在は本設計の各ハードウェア階層での性能保証ではない。
- [Qulacs Python Tutorial: DenseMatrix / merge](https://docs.qulacs.org/en/latest/intro/4.1_python_tutorial.html) — 任意小行列ゲートとゲート合成。
- [PennyLane Lightning-GPU MPI adjoint](https://docs.pennylane.ai/projects/lightning/en/stable/lightning_gpu/device.html) — MPI分散Adjointとobservable数に応じたメモリ/再計算トレードオフの参考。今回のSSD out-of-coreでの高速化を保証するものではない。
- [NVIDIA cuStateVec Pauli expectation API](https://docs.nvidia.com/cuda/cuquantum/23.03.0/custatevec/api/functions.html) — Pauli observableのバッチ評価の参考。
- [Qulacs ParametricQuantumCircuit API](https://docs.qulacs.org/en/latest/api/classParametricQuantumCircuit.html) — パラメータ設定、multi-Pauli rotation、backprop。
- [Qiskit ParameterExpression](https://docs.quantum.ibm.com/api/qiskit/qiskit.circuit.ParameterExpression) — シンボリックなParameterの演算・評価。
- [Qiskit QuantumCircuit.assign_parameters](https://docs.quantum.ibm.com/api/qiskit/dev/qiskit.circuit.QuantumCircuit) — 回路の束縛・再利用の参考。
- [PennyLane ArbitraryUnitary](https://docs.pennylane.ai/en/stable/code/api/pennylane.ArbitraryUnitary.html) — 任意ユニタリーのPauli回転パラメータ化。
- [PennyLane parameter-shift rule](https://docs.pennylane.ai/en/stable/code/api/pennylane.gradients.generate_shift_rule.html) — generatorスペクトルに応じた一般化shift rule。
- [cuStateVec distributed index bit swap](https://docs.nvidia.com/cuda/cuquantum/24.08.1/custatevec/distributed-index-bit-swap.html) — global/local index-bit permutation and rank pair scheduling。
- [Stim](https://quantum-journal.org/papers/q-2021-07-06-497/) — stabilizer/Clifford向け高速処理の比較参照。
- [Linux open(2)](https://man7.org/linux/man-pages/man2/open.2.html) — `O_DIRECT`の制限。

本研究で検証する差分は、**意味保存Gate IRを用いたNative-first／条件付き分解選択（任意UとMCXを含む）、シンボリックParameter依存DAG・Block-level共通prefix再利用とGradient MethodのI/O-aware選択、Per-Block遅延ログとClifford Frame共有、SWAP/FusedSWAPによるMPI/SSD/HDD階層またぎの量子ビット配置最適化、Cross-Block依存DAG・Frame整合・ハードウェア推定を共同で決定できるか**である。ログ、Clifford追跡、out-of-core、MPIは個別に先行技術が存在するため、これだけで新規性を断定しない。

---

## 最終原則

> **入力ゲートの意味を保持したまま遅延ログへ記録し、ユーザーが固定任意Uを事前分解せず登録できるようにする。Block-localな小密行列、対角・置換ゲート、MCX（Toffoli・CCCXなど任意制御数）の条件付き置換は原則nativeに処理する。パラメータをシンボリックなまま保存し、代数最適化、束縛別のguarded specialization、合法な共通prefix再利用とGradient Plannerを持つ。Frame更新・小規模ゲート融合・SWAPによる局所化・限定基底へのfallback分解を同一Cost Modelで比較し、Global DAGの整合性とMPI/SSD/HDDの移動最適化を両立する。**期待値とAdjoint勾配はBlockPauliInnerを共通基盤とし、Pauli partner取得・Frame整合・Forward/Adjointの2状態逆再生・MPI Reductionを同一Plan/Runtime Cost Modelで管理する。** さらに実行中のI/O・MPI実効性能を初期想定と検証し、持続的な性能低下時には安全なplan変更を実施する。**

この統一モデルを、単一ノードRAM、SSD/HDD out-of-core、MPI分散、それらの混合構成で共通利用する。
