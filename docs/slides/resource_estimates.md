# スライド18〜30：構成例と容量・処理時間・最適化の検討

2026-10-13。4構成は容量計画の例で、実機の性能測定、製品選定、実行時間や大規模実行の保証ではない。数値は complex128 の全状態を圧縮せず保持する前提で計算する。

## 共通条件

- 状態1個：`S(n) = 16 * 2**n byte`。GiB/TiB/PiB は2進単位。
- node数 N、均等配置した担当量 `s = S/N`。分散例は 1 rank/node、N=8。
- Block幅 b=24、payload B=256 MiB。最大4Q native Dense の g<=16 Block Jointを一つずつ計算し、prefetch／MPI creditを制限する。
- 追加prefix、他のBinding job、独立exportファイル、他datasetのcheckpointコピーは含まない。
- 初期状態はBlock生成器／streaming importから供給する。全状態の`initial_state`配列やeager検算用配列を別にRAMへ作る容量は含めず、通常の期待値・勾配では全振幅Gatherを行わない。
- 余裕25%は仮定で、無制限の断片化・orphan・MPI内部メモリを保証する係数ではない。実際のpeakと空き容量を各nodeで検査する。
- RAMの予約量はmanaged budgetにOS／MPI内部等の余裕を加えた構成例。既存実装のprocess RSSを測定した値ではない。

| qubit | 状態1個 |
|---:|---:|
| 30 | 16 GiB |
| 32 | 64 GiB |
| 36 | 1 TiB |
| 40 | 16 TiB |
| 44 | 256 TiB |
| 50 | 16 PiB |

遅延Log／Frame／virtual permutationはwriteや通信を省略するが、一般の状態をこの表より小さく保存できると仮定しない。

## RAMのみ

32Q、S=64 GiB、追加workspace/metadata W=16 GiB。

| 用途 | 容量式 | 計算値 | node RAM予約例 |
|---|---|---:|---:|
| Forward | `1.25*(2S+W)` | 180 GiB | 256 GiB |
| streaming Adjoint | `1.25*(4S+W)` | 340 GiB | 512 GiB |

Forwardの2Sは旧／新COW state、Adjointの4Sは保護final・forward・adjoint・一方ずつ更新する次出力。全中間stateを保持するRAM oracleの必要量は別で、回路長に依存する。

振幅用SSDは0。ただしdurable checkpoint／exportの保存先を含まない。現行MemoryBlockStoreでもcatalog/journal用の小容量filesystemが必要で、完全に無ディスクのruntimeを実装済みとは扱わない。

## SSDを使う3構成

各nodeのRAM予約例は64 GiB：cache 32、work 16、metadata 1、OS／MPI内部等の余裕15 GiB。managed部分は49 GiB。b=24の一般g=16 Jointの保守的admission `(2g+3)B = 8.75 GiB`、限定prefetch／tileをwork16 GiBへ収める。一Jointずつの前提であり、複数Joint／Bindingを並列化する場合は再計算する。

現行Phase 3のfile storeはappend-onlyで、GCは保守的なancestor closureとwhole-segment回収である。現在のstate数個分をディスク容量の上限として使わない。

初期1状態分を書き、Forwardで8回、Adjointで追加16回の「全状態write相当量」を仮定する。この回数はsemantic gate数ではなく、新しいextentを書いたpayload合計をSで割った量。metadata-only operationは0、partial／local更新はその割合で数える。

```text
Forward SSD/node = 1.25 * (1 + 8) * s     = 11.25s
＋Adjoint SSD/node = 1.25 * (1 + 8 + 16) * s = 31.25s
```

| 構成 | N / n | s/node | RAM/node | Forward SSD計算→予約 | ＋Adjoint SSD計算→予約 |
|---|---|---:|---:|---:|---:|
| 単一node＋SSD | 1 / 36 | 1 TiB | 64 GiB | 11.25→16 TiB | 31.25→32 TiB |
| Ethernet＋SSD | 8 / 40 | 2 TiB | 64 GiB | 22.5→32 TiB | 62.5→64 TiB |
| InfiniBand＋SSD | 8 / 40 | 2 TiB | 64 GiB | 22.5→32 TiB | 62.5→64 TiB |

8-node全体ではRAM512 GiB、SSD予約はForward256 TiB／＋Adjoint512 TiB。空き領域の予約例であり、製品公称容量ではない。例えば16 TiBは約17.59 TB decimalで、filesystemや既存データを除いた空き領域が必要。

checkpoint manifestが既存extentを参照するだけなら同じpayloadを二重加算しない。別領域へexport／複製する場合はそのbytesを追加する。checkpoint／prefixの生存参照、dirty writeback、segment tail、orphan、alignment paddingと断片化が上の仮定を超える場合は追加容量が必要。途中のphysical SWAPも実際の新規extent writeに含める。

より一般的には `D_required >= initial_extents + sum(new_extent_storage_bytes) + extra_copies + reserved_pending_outputs + margin` をrank storeごとに検査する。長い回路のappend累積を25%だけで吸収できるとはしない。

## 生存版GCを保証できる将来モデル

生存版の回収／compactionが成立し、保持snapshot数とCOW同時出力を制限できる場合の比較用モデル：

- Forward：checkpoint＋current＋nextの3s、余裕込み3.75s。
- Adjoint：checkpoint＋2 stream＋両streamのCOW出力まで5s、余裕込み6.25s。

単一36Qでは3.75／6.25 TiB、予約例4／8 TiB。8-node40Qでは7.5／12.5 TiB/node、予約例8／16 TiB/node。これは現行append-onlyの16／32、32／64 TiBの予約例と前提が違い、現行容量保証として使わない。

## networkの仮定と制約

Ethernetは100 GbE・通常TCP、InfiniBandは200 Gb/s・MPI/UCXを接続例として仮定する。NIC購入の推奨や実効帯域の測定ではない。同じn/N/保持版で必要なSとSSD量は変わらない。帯域・latency・CPU負荷・SSDとの混合負荷・NUMA・progressを実測して実行計画と並列度を選ぶ。

InfiniBand/RDMAでは通信bufferの登録・pin、memlock上限、MPI/UCXの利用、HCA配置を確認する。既存bufferをpinすることと、新しいpayload copyの確保を区別し、追加stagingがあれば実測して計上する。[Open MPIのInfiniBand/RoCEとmemory registrationの説明](https://docs.open-mpi.org/en/main/tuning-apps/networking/ib-and-roce.html)

node-local storeはrestart時にも取得可能という前提。replication、rank/node脱落後の継続、障害時の追加コピー用容量は含まない。本repoのPhase 3実測は同一Macの1/2/4 rankであり、ここで挙げたmulti-node構成を検証済みとはしない。

参照：[Phase 3実装と容量/GC](../phase3_implementation.md)、[Phase 3実測](../phase3_benchmark.md)、[Phase 4 resource ledger設計](../phase4_design.md)。

## 量子化学VQE：各構成に収まる処理時間パターン

スライド24〜26に追加。容量例の32/36/40Qは短い更新回路の仮定であり、そのまま深い量子化学ansatzが収まるという意味ではない。ここでは状態サイズを小さくして、各構成のRAM/SSD予約量に収まるselected-excitation VQEを計画する。

活性空間の表記は `(活性電子数, 空間軌道数)`。Jordan–Wigner、spin-orbitalごとに1 qubit、taperなしとして `n=2*空間軌道数`。凍結軌道の寄与はinactive energy／有効積分へ含める。[ActiveSpaceTransformer](https://qiskit-community.github.io/qiskit-nature/stubs/qiskit_nature.second_q.transformers.ActiveSpaceTransformer.html)、[JordanWignerMapper](https://qiskit-community.github.io/qiskit-nature/stubs/qiskit_nature.second_q.mappers.JordanWignerMapper.html)

H2O/N2の閉殻・cc-pVTZを想定し、geometry／具体的な活性軌道／積分screeningは未確定。積分は生成していない。下表のHamiltonian Pauli項数Mは容量・時間モデルの入力仮定で、分子の実測項数や、正確なHamiltonianの上限ではない。実際のFCIDUMP／mapped operatorを用意したらMとmask分布を置換する。時間モデルは積分生成／SCF／軌道選択を除くVQE部分。

| 構成 | 量子化学の想定 | qubit | 全状態S | Hamiltonian M（仮定） | node |
|---|---|---:|---:|---:|---:|
| A RAM | H2O型 CAS(8e,12o) | 24 | 256 MiB | 5,000 | 1 |
| B SSD | N2型 CAS(10e,14o) | 28 | 4 GiB | 20,000 | 1 |
| C Ethernet | N2型 CAS(10e,16o) | 32 | 64 GiB | 50,000 | 8 |
| D InfiniBand | Cと同じ問題 | 32 | 64 GiB | 50,000 | 8 |

ansatzはHartree–Fockから始め、spin／particle numberを保つ16個の**選択した実double excitation**を一度ずつ因子化して適用する。各excitationを8個のnative Pauli rotationへ変換し、G=128、独立parameter数16とする。これはfull UCCSDでも、化学精度の保証でもない。[double excitationの8 exponentiation](https://docs.pennylane.ai/en/stable/code/api/pennylane.FermionicDoubleExcitation.html)

100反復、各反復で1 energy＋analytic adjoint gradientを評価する。line searchの追加評価、geometry scan、shot sampling、parameter-shift job、収束判定の追加反復は含めない。反復100は実験設計の仮定で、100回で収束する主張ではない。

### 深い回路の容量確認

1 Bindingのscratchには保守的に `初期1 + forward G + inverse forward G + inverse adjoint G + Opsi生成1 = 386` 状態write相当を見込む。25%の仮定marginを加えて482.5s。毎反復終了時にscalar結果だけ保存し、旧Binding scratchを閉じてleaseを解除・領域を撤去してから次を実行する。scratch自動撤去は現行runtimeの保証ではなく、batch/workflowに必要な条件。

| 構成 | RAM必要量/node | RAM予約/node | SSD peak/node | SSD予約/node | 条件付き収容 |
|---|---:|---:|---:|---:|---|
| A RAM | `1.25*(4*0.25+16)=21.25 GiB` | 512 GiB | 0 | 0 | 可、volatile |
| B SSD | 作業/cache/余裕込み64 GiB | 64 GiB | 1.885 TiB | 32 TiB | 可 |
| C Ethernet | 同64 GiB | 64 GiB | 3.770 TiB | 64 TiB | 可 |
| D InfiniBand | 同64 GiB | 64 GiB | 3.770 TiB | 64 TiB | 可 |

C/Dはs=8 GiB/node。100反復すべてのscratchを残すとBは約188.5 TiB、C/Dは約377.0 TiB/nodeになり、予約量に収まらない。独立prefix／export／orphan／断片化が仮定を超えた場合も再見積もりする。

### 時間モデル：Phase 4の条件付き計画値

**現行Phase 3の実行時間ではない。** 全nodeの均衡したkernel並列、Frame/logを解消したlogical Blockの再利用、energyとOpsi双方でupper-X maskごとのpartner reuseを仮定する。現行adjointのOpsiは項ごとのfetchなので、この再利用は追加の最適化が必要。

| 入力条件 | 仮定 |
|---|---:|
| Pauli内積・actionのcompute throughput ρ | 100,000,000 amplitude-term/s/node |
| RAM実効帯域 βR | 32 GiB/s/node |
| SSD read＋write共有の実効帯域 βD | 2 GiB/s/node |
| Ethernet送信側実効帯域 βN | 8 GiB/s/node |
| InfiniBand送信側実効帯域 βN | 16 GiB/s/node |
| control／optimizer／同期の時間margin | 5%（仮定、上限保証ではない） |

ρと帯域は未測定の目標・仮定で、NIC公称値ではない。ρはcompute-onlyとしてRAM rooflineとのmaxを使う。実測kernel rateを代入する場合はメモリを内包する時間を重複加算しない。同時read/writeやNIC共有で帯域が下がる場合はこの値を下げる。

one-/two-body JW Hamiltonianの各Pauli stringはX/Yを最大4個持つ。g=n-b、r=log2Nとしてupper-X mask種類の保守的な個数を次で置く。

```text
K  = sum(comb(g,j), j=0..min(4,g))
Kr = K - sum(comb(g-r,j), j=0..min(4,g-r))
```

b=min(24,n)、AのK/Kr=1/0、B=16/0、C/D=163/132。KはHamiltonian項数Mではなくpartner取得のgroup数。実際に存在しないmaskも数えた上限モデル。term数を減らしてもgroupが同じならIO/MPIは同じとし、frame/layout/placementが異なる再取得は別途加算する。

```text
s: node担当状態のGiB、Aamp = 2**n/N
Tcompute = max(M*Aamp/ρ, 2*M*s/βR)
Tenergy  = Tcompute + 2*K*s/βD + Kr*s/βN
Tfwd     = G*(2*s/βR + 2*s/βD + 2*s/βN)
Tgen     = max(G*Aamp/ρ, 2*G*s/βR) + 2*G*s/βD + 2*G*s/βN
Titer    = 1.05*(3*Tfwd + 2*Tenergy + Tgen + 2*s/βD)
Ttotal   = 100*Titer
```

該当しないdisk/network項は0。energy単独はTfwd＋Tenergy。2*TenergyはenergyとOpsi生成、3*Tfwdはforwardと両inverse stream、Tgenは128 source-occurrenceのgenerator inner。最後の2s/βDは初期保存・Checkpointに相当する1回のread/write余裕であり、既存extentを参照するmanifestが必ず2s書く意味ではない。

all Gでfull read/write、分散時は2sの通信も仮定し、metadata-only/localで省略できるgateは保守的に数える。RAM内のpartner readは2M*s/βRに含む。SSDの2K readは両partnerの取得、MPIはremote partnerの送信量をnodeごとに一回数え、recvを二重計数しない。異なるmaskのpeer projectionの再利用は仮定しない。**I/O・通信・計算のoverlapによる加速は加点せず加算する。** MPI全rankのscalar reductionなどは5%margin内の仮定で、実測で超えるなら別項へ置換する。

| 構成 | 1 energy（forward込） | 1 energy＋gradient | 100反復合計 |
|---|---:|---:|---:|
| A RAM、24Q | 約14分 | 約30分 | 約2.1日 |
| B SSD、28Q | 約15.1時間 | 約32.1時間 | 約134日 |
| C Ethernet、32Q/8node | 約75.3時間 | 約159.2時間 | 約663日 |
| D InfiniBand、32Q/8node | 約75.3時間 | 約159.0時間 | 約662日 |

この条件ではPauli項走査のCPU費用が支配的で、InfiniBandだけを速くしても全体時間はほぼ変わらない。入力ρを0.5〜2倍に変えた感度も確認し、単一の数字を精度保証として扱わない。native PAULI_ROTでloweringした仮定なので、CNOT ladderへ分解する別runtimeのgate数・時間へそのまま使わない。

| 構成 | ρを5千万〜2億に変えた100反復の感度範囲 |
|---|---:|
| A RAM | 約1.0〜4.1日 |
| B SSD | 約68〜265日 |
| C Ethernet | 約337〜1,317日 |
| D InfiniBand | 約336〜1,316日 |

この範囲はkernel仮定だけを変更した感度で、統計的な信頼区間ではない。実際のM、追加評価、混合負荷、geometryごとの収束が変われば範囲外にもなる。

### 現行kernelとの差と再現ファイル

同じMacで現行Python `pauli_inner`をb=16、X/Z mask=3/5、warmup後3回測り、約1.6百万 amplitude-term/sだった（[生データ](vqe_estimates.json)）。これはI/O／MPI／Frameを含まないlocal microbenchmarkで、量子化学実行の測定ではない。目標ρ=1億は約60倍であり、上の表を現在のPython実装の時間として読まない。現行MPIはkernelをnode数分すべて並列に進める保証もない。

[計算script](estimate_vqe.py)と[JSON](vqe_estimates.json)に仮定・算式・容量判定・時間内訳を保存した。geometry／活性積分／実際のM・K・G、matched mixed profileと実kernel throughputが得られたら、目標値を置換して再校正する。

```sh
uv run --locked --extra mpi python docs/slides/estimate_vqe.py \
  --output docs/slides/vqe_estimates.json --measure-python-kernel
```

## 量子化学QPE：1 mHa・2次対称トロッター・逐次位相推定

VQEのスライド24〜26と見積もりは保持し、スライド27〜29に**QPE**、30に最適化検討を追加した。期待値とトロッター実行の最適化は[別の検討資料](../quantum_chemistry_optimization.md)に分離する。

活性空間は `(活性電子数, 空間軌道数)`、Jordan–Wigner、taperなし。system qubitは空間軌道数の2倍。H2O/N2型・閉殻・cc-pVTZを想定するが、geometry、活性積分、screening、Pauli項数Mは未生成・未測定。[ActiveSpaceTransformer](https://qiskit-community.github.io/qiskit-nature/stubs/qiskit_nature.second_q.transformers.ActiveSpaceTransformer.html)、[JordanWignerMapper](https://qiskit-community.github.io/qiskit-nature/stubs/qiskit_nature.second_q.mappers.JordanWignerMapper.html)

### 問題と収容容量

| 構成 | 活性空間（仮定） | system qubit | IPEA込みqubit | full state | M（仮定） |
|---|---|---:|---:|---:|---:|
| A RAM | H2O型 CAS(8e,12o) | 24 | 25 | 512 MiB | 5,000 |
| B SSD | N2型 CAS(10e,14o) | 28 | 29 | 8 GiB | 20,000 |
| C Ethernet | N2型 CAS(10e,16o) | 32 | 33 | 128 GiB、16 GiB/node | 50,000 |
| D InfiniBand | Cと同じ、8 node | 32 | 33 | Cと同じ | 50,000 |

逐次位相推定（IPEA）はancillaを1個再利用し、20回のmeasurement/reset/feedforwardを1 trajectoryで実行する。通常QPEの20 ancillaを一度に追加すれば状態はsystem-onlyの2^20倍となり、この予約量では収まらない。全分岐木は保存せず、1 trajectoryの条件付き純粋状態を保持する。[iterative phase estimationの論文](https://arxiv.org/abs/quant-ph/0610214)

各nodeで2個のworking stateをRAMに置く条件付きモデル。work16 GiB＋metadata1 GiB＋25%余裕を含め、Aは22.5 GiB、Bは41.25 GiB、C/Dは61.25 GiB/node。以前のRAM予約512/64/64 GiBに収まる。ancillaはBlock-localに配置し、rank bitをsystem側に割り当てる。

SSDは初期／最新bit checkpoint／次出力／reclaim中の予備bankの4状態に25%余裕を加える**bounded working store**を前提にする。Bは40 GiB=0.0391 TiB、C/Dは80 GiB=0.0781 TiB/nodeで、SSD予約32/64 TiB内。Trotterの各primitiveでdiskへ保存せず、各phase bit境界をcheckpointとする。

**このreclaim/bank方式とIPEA measurement/resetは未実装。現行Phase 3のappend-onlyでB/C/Dへの収容を保証できない。** 新規outputを全primitiveで書く保守的モデルでは、1 trajectoryあたりB約1.64×10^9 TiB、C/D約8.19×10^9 TiB/nodeとなり予約量を超える。このモデルのphase3_append_only_disk_fits=falseをJSONに残す。RAMのworking version retirementと圧縮repeat planも必要で、billionsのGateOp/root/journalを展開しない。資源が収まるという表記は、これらの機能を実装・検証した後の条件付き計画である。

### 精度と同一unitaryのpowers

対象active-space Hamiltonian Hに対して総誤差目標1 mHa=0.001 Ha。位相分解能に0.5 mHa、Trotter biasに0.5 mHaを割り当てる。basis／活性空間近似を含む物理分子のchemical accuracy保証ではない。

H−E_refの全spectrumが±Λ、Λ=100 Haに収まると仮定する。実際の積分からspectral boundを検証し、E_refを復元する。`U=exp(-i(H-E_ref)tau)`、`tau=pi/(2Λ)=0.01570796 Ha^-1`。signed phaseは±1/4内に置き、`E=E_ref-2pi*phase_signed/tau`でdecodeする。[QPEのcontrolled powers](https://quantum.cloud.ibm.com/learning/en/courses/utility-scale-quantum-computing/quantum-phase-estimation)

```text
m = ceil(log2(4Λ / 0.0005)) = 20 phase bits
energy grid = 4Λ / 2^m = 0.38147 mHa < 0.5 mHa
delta_max = 0.01 Ha^-1              # 仮定、精度未認証
r0 = ceil(tau/delta_max) = 2
delta_actual = tau/r0 = 0.00785398 Ha^-1
S2(delta) = product(l=1..M) exp(-i h_l P_l delta/2)
          * product(l=M..1) exp(-i h_l P_l delta/2)
U0 = S2(tau/r0)^r0
```

U0^(2^j)は**同じU0の繰返し**で実装する。非可換Hで各rotation angleだけを2^j倍にする置換は同じunitaryのpowerではない。IPEAが測るのはこの近似unitaryのphaseであり、mを増やすだけではTrotter biasは消えない。

delta_maxだけで0.5 mHaを保証できない。係数・項順序・交換子／nested commutator、small-system exact oracle、step-size sweepでbiasを認証する。必要ならdeltaを縮小し、時間を再計算する。[Trotter誤差とcommutatorの解析](https://arxiv.org/abs/1912.08854)

実際のgrid0.38147＋Trotter目標0.5=0.88147 mHa。残り0.11853 mHaを数値誤差等の余裕とするが、長い回路での浮動小数点誤差も別途検証する。QPEのphase distributionにはtailがあるため、gridは全shotの誤差保証ではない。

### native gate countと試行回数

現在のIRはcontrolled PAULI_ROTを拒否する。計画のbaselineは厳密な2 native rotationへのloweringを使う。

```text
R_P(theta) = exp(-i theta P/2)
C_a[R_P(theta)] = R_P(theta/2) * R_(Z_a P)(-theta/2)
Trotter steps/run = r0*(2^m-1) = 2,097,150
controlled Pauli factors/run <= 2M * steps
native rotations/run <= 4M * steps
```

Mにはscalar/identity項も含めた保守的な全項passを想定する。identity／E_refによるphaseはcontrol ancillaのrelative phaseなので捨てない。回転角は直接floatで扱い、Clifford+T合成／T-countを時間へ使わない。隣接half-stepのmerge、commuting group、native controlled kernel、logical-layout最適化の利益はbaselineに加点していない。

| 構成 | native rotations / 1 trajectory |
|---|---:|
| A | 4.1943×10^10 |
| B | 1.67772×10^11 |
| C/D | 4.1943×10^11 |

独立した32 trajectoryを初期状態から再準備する。近似unitaryのgroundへのoverlap0.25、条件付きgood-bin確率0.4を**仮定**すると、少なくとも1回のground＋good phaseが出る確率は `1-(1-0.25*0.4)^32=96.57%`。これはgroundを識別できる保証ではなく、未知overlap／gap／Trotterによるeigenvector変化は別検証が必要。最小の観測phaseを無条件にgroundと採用しない。

### 条件付き時間：全Pauliを別passで処理するbaseline

目標compute rateρ=1億 amplitude update/s/node、RAM32 GiB/s/node、SSD2 GiB/s/node、Ethernet8／InfiniBand16 GiB/s/nodeを**未測定の仮定**として残す。前版のPauli内積rateを回転kernelの実測値とみなさない。現行Python速度を予測した表ではない。

```text
s = full-QPE state GiB / N、n_full=system+1
Tnative = max(2^n_full/(Nρ), 2s/βRAM)
Tnetwork = 2s/βnetwork   # multi-node全factorがremoteという保守的モデル
Tcheckpoint = (m+1)*2s/βSSD
Ttrajectory = 1.05 * (native_count*(Tnative+Tnetwork)+Tcheckpoint)
Ttotal = 32*Ttrajectory
```

非該当disk/networkは0。RAM-resident working statesなのでSSD read/writeはbit checkpointで課金する。2s/βSSDはread/write共有bandwidthの保守的な費用で、すべてのsnapshotで必ずfull copyする意味ではない。量子状態のsimulation wall timeと、tauで表した物理的time evolutionを区別する。CPU/ネットワークを加算しoverlapの速度利益は含めない。remote比率、native fusion、device contention、measurement/reduction、state prepとcontrol overheadは実測で置換する。

| 構成 | 1 trajectory | 32 trajectory |
|---|---:|---:|
| A RAM、25Q | 約468年 | 約1.50万年 |
| B SSD、29Q | 約3.00万年 | 約95.9万年 |
| C Ethernet、33Q/8node | 約20.6万年 | 約658万年 |
| D InfiniBand、33Q/8node | 約17.8万年 | 約569万年 |

この非常に大きな値は、1 mHa QPEのlong controlled powersと、各Pauli factorで全状態を別々に更新するreferenceの費用を表す。実用時間の保証ではない。係数／Trotter biasが未検証のためcertified estimateでもない。項grouping／diagonal fusion／layout／working-bank reuse／controlled branch処理の検討が必要で、その利益を後付けで仮定しない。

[計算script](estimate_chemistry.py)と[JSON](chemistry_estimates.json)をQPE model_version=2へ更新。delta_max=0.02/0.005、energy目標10 mHaの感度もJSONへ保存する。errorを再認証せず大きなdeltaで高速化を主張しない。

```sh
uv run --locked --extra mpi python docs/slides/estimate_chemistry.py \
  --output docs/slides/chemistry_estimates.json --energy-mha 1 --delta-max 0.01
```
