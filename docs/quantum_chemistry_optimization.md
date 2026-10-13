# 量子化学シミュレーション：QPEと別に検討する最適化

2026-10-13。ユーザー指定は1 mHa・2次対称トロッター・逐次位相推定。現行runtimeの実装追加ではなく、[容量・時間推定](slides/resource_estimates.md)に続く検討設計である。

## QPEのbaselineと実装境界

Hをactive-spaceの実電子Hamiltonian、U0を同じ2次対称product formulaで固定する。controlled U0^(2^j)は同一U0の反復で作る。native回転を用い、Clifford+T合成・T-countを費用にしない。current IRにはcontrolled PAULI_ROTがないため、baselineは2 native rotationへ厳密loweringする。IPEAの途中measurement/reset、圧縮repeat plan、bounded working-version retirementは新規機能であり、現行Phase 3でQPEを実行済みとは扱わない。

system+ancillaの全状態サイズ、control branchのrelative phase、signed phaseとE_refの復元、Born確率の全rank reductionを保つ。古典制御は全rankで同じbit結果を配送する。1 trajectoryのsamplingと全measurement分岐のexact treeを区別する。

総目標1 mHaのphase budget 0.5 mHaとTrotter budget 0.5 mHaを分ける。Λ=100 Ha、step上限0.01 Ha^-1は仮定で、積分からspectral bound／commutatorと数値誤差を検証するまでは精度認証しない。QPEが測る近似unitaryのground overlapを、元HのHF overlapと無条件に同一視しない。

## 1. 状態を直接見る期待値計算

対象は `E=<psi|H|psi>`。シミュレータは振幅を持つため、Pauli測定のshot allocation／measurement groupingのために何千回も回路をsampleする必要はない。floating-pointとHamiltonian近似の誤差は残る。「直接」は全状態をrank 0へGatherする意味ではなく、Block-local／partner内積と複素Allreduceで計算する。

Phase 3は期待値のupper-X maskをgroup化してpartnerを取得するが、各Pauli項のkernelはPython loopであり、Opsi生成では項ごとにfetchしている。最初に両経路のpartner reuse、Frame/log解消のversion付きcache、vectorized/native kernelを共通化し、I/O・CPU費用を別々に測る。

共有full X maskの項を `D_x(i)=sum_z h_(x,z) i^popcount(x&z) (-1)^popcount(i&z)` とまとめれば、`H|psi>`の寄与は `out[i xor x]+=D_x(i)*psi[i]`。Block/tileごとにDを生成し、global 2^n tableをRAMへ展開しない。upper Xだけ一致する場合は、同じpartner Blockを読んでlower offset kernelをまとめる。Yのphase、Hermitian conjugacy、global hのZ parityを検査する。

別候補としてfermion occupation/parityからone-/two-body作用を直接計算し、Pauli展開の定数倍を省く。1/2-RDMを作れば同じ状態の異なる積分セットへcontractを再利用できるが、RDM構築そのものの費用とmetadata量を計上する。state、Hamiltonian、Binding、layout、Frameが変わればcacheを失効する。

varianceは `Var(H)=||H psi||^2-E^2` で計算できる。H^2のM^2項への展開より、Hpsi workspaceのstreaming actionとnormを比較する。追加1状態とCOW/SSDを予約する。QPE後の条件付きstateのdirect E／varianceは診断にもなるが、一般のsuperpositionのEは複数eigenvalueの平均であり、QPEの固有値・ground識別の代替ではない。

## 2. 最後まで進めて逆ゲートを適用する候補

unitary segmentを `psi_j=U_j...U_1 psi_0` とする。final psi_Lから `psi_(j-1)=U_j^dagger psi_j` と逆再生すれば、中間stateの再構築と直接期待値計算が可能。全中間状態を保存する代わりにfinal、working state、必要checkpointを保持する。局所bit／rank bitのphysical layout inverseとactual execution tapeも逆順に戻す。

**psiだけ戻して固定Oの期待値を測れば、中間stateの期待値になる。finalの期待値が保たれるとは限らない。** suffix `V_j=U_L...U_(j+1)` について `O_j=V_j^dagger O V_j` を使えば `E_L=<psi_j|O_j|psi_j>`。ただし一般の非Clifford共役でPauli項が増えるため、Heisenberg operator propagationの展開量をcost/admissionへ入れる。

二stream候補は `lambda_L=O psi_L` を作り、psiとlambdaを同じinverseで戻す。`<psi_j|lambda_j>=E_L` は不変で、source generatorを挿入した内積はadjoint gradientにも使える。このoverlapは「全中間stateで固定OのE_jを得た」意味ではない。固定HのE_jが必要ならH作用を各対象cutで再評価するか、operator/observable共役を正しく追跡する。

比較対象は (a) forward途中に直接Eを測る、(b) finalから逆再生し各対象cutで直接E、(c) final bra/ket二stream、(d) checkpoint/recompute、(e)合法なoperator propagation。gate再演算、H action、Frame解消、partner traffic、checkpoint、2state peak、layout inverse込みで比較する。少数の中間cutが必要ならcheckpoint、unitary全区間のgradientならadjoint、operatorがClifford下で小さいなら共役候補、という条件を実測で判定する。

QPEのancilla measurementは非unitary projectionなので、このinverse法をtrajectory全体へ適用しない。measurement outcome／確率／normalizationを記録し、直前checkpointか同じ条件付きbranchのforward replayで復元する。測定後stateから逆ゲートだけで測定前stateを復元した扱いはしない。

## 3. トロッター実行と配置の最適化

| 候補 | 合法性・必要な検証 | 期待する費用削減 |
|---|---|---|
| controlled Pauli専用kernel | 既存2rotation loweringとcomplex amplitude一致、inactive branch不変 | inactive branchと相殺する中間passを省く |
| 隣接half-step／同一Pauli merge | 隣接、同じsource/Binding/control、phaseを含めて厳密一致 | Strang境界のpass数削減 |
| diagonal／commuting group fusion | 交換を証明。非可換termを角度合算しない | 一度の振幅走査に複数項をまとめる |
| common support group kernel | bounded g／tile／Dense assets、Frame解消の再利用 | 小規模group内の複数rotationをまとめる |
| upper-X partner／Frame cache | extent/view/version/layout／Binding一致、pinとCOW | 同じBlockのSSD/MPI再取得を省く |
| S1/S2/FusedSWAP | 元Trotter unitaryを保存、physical配列とlayoutを同時変更 | rank跨ぎXを減らす。swapの追加IOも計上 |
| bounded epoch working banks | 公開前COW、exclusive lease、request drain、checkpoint回復 | primitiveごとのSSD appendを止める |
| low-rank／orbital basis representation | 同じHか近似誤差を予算化。basis rotationの費用を含む | term数／state pass／rank通信を減らす |

one-/two-body電子構造Hamiltonianのlow-rank表現は候補として研究するが、factor数を入力なしに仮定しない。[low-rank electronic structure simulation](https://arxiv.org/abs/1808.02625)

ancillaをBlock-localに置く案、Hamiltonianのfrequent supportをlocal bitへ配置する案、rank-local upper bitへ寄せる案を比較する。Pauli Z主体のglobal作用はmetadataで処理可能な場合があり、単にglobal bitを含むだけでMPIと数えない。今回の時間表は全factorがfull-pass／remoteという保守的referenceで、利益は未反映。

非可換なfactorの順序を変えたりdeltaを変えたりすると、近似unitary U0自体が変わる。IPEAの途中で異なるU0を混ぜず、errorを再認証して初期状態から新trajectoryを開始する。単なる合法commuting reorder／厳密kernel fusion／physical layoutの変更とは区別する。

別方式として各時刻t_jのexact `exp(-iHt_j)` を個別のproduct formulaで近似するQPEも比較対象にできる。その場合は共通U0のphaseという解釈を外し、全controlled evolutionの近似誤差とQPE分布の誤差をまとめて予算化する。時刻ごとのstep数を減らしただけで同じ精度を満たすとはしない。今回の時間baselineは共通U0の繰返しに固定する。

同じPauliが非可換factorを挟んで再登場しても、角度だけ足してよいとはしない。一般の `S2(delta)^r` を `S2(r delta)` に置換しない。constant／identity／E_refはcontrolled phaseとして残す。

## 4. シミュレータ固有の別方式

Hadamard testのancilla確率はcomplex overlap `C(t)=<psi|exp(-iHt)|psi>` から直接計算できる。data-onlyのUpsiと元psiの二streamでancillaの明示vectorを省く候補を比較する。ただしIPEAでは測定後にdataも条件付きで変わる。

outcome bとfeedback phase alphaに対するKraus作用 `K_b=(I+(-1)^b exp(i alpha) U^power)/2` をdataへ適用し、`p_b=||K_b psi||^2` とnormalizationを追跡すれば、full ancilla simulationとのbranch一致を検証できる。確率だけsampleしdataをそのまま残す方法は同じIPEAではない。この最適化はbaselineへ算入せず、source/referenceとの完全一致が得られてから見積もりを更新する。

## 5. 検証と次の成果物

small active-space Hamiltonianを実際に生成し、geometry/basis/frozen orbital/係数cutoff/Pauli termとhashを保存する。exact diagonalizationはsmall oracleに限る。spectral bound、step sweep、QPE histogram、phase/energyのsigned decode、ground overlap／gap、direct E／varianceを照合する。[Trotter error theory](https://arxiv.org/abs/1912.08854)、[iterative phase estimation](https://arxiv.org/abs/quant-ph/0610214)

controlled identity／Y／open-control、delta符号、inverse、Strang repetition、共通Frameと異Frame、rank/bit layout、measurement/reset、branch normalization、cut rollback、bank retirement／GCを1/2/4 rankで検証する。Gの巨大なrepeatをGateOp一覧へ展開せず、template＋counterで正しさとmetadata boundを確認する。

baseline→direct expectation kernel→controlled branch/fusion→group/layout→working storeの順で、state誤差・phase/Trotter bias・RAM/SSD peak・CPU／I/O／MPI・end-to-endを計測する。精度1 mHaとcoherent U0を保つ条件が成立した候補だけを採用する。unitary逆再生の費用は期待値／gradientの別reportとし、QPE時間へ混在させない。

今回の成果物は設計と推定の更新。QPE runtime、measurement、fusion、working store、実Hamiltonianの生成・大規模実測は別途実装する。
