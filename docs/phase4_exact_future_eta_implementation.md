# P4-002：残りepochの正確な実行形状と実測費用によるETA

更新日：2026-10-15。P4-002の残件「細粒度の残work」「in-flight残量」「抽出・評価のoverhead」のうち、S0／S2（および1 rankのS1）で残り全体を実測費用で価格付けする部分を実装した。P4-002はIN_PROGRESSのまま維持する。

## 前提の確認

- **source SWAPはlayoutを変えない。** SWAPは上位bitならpermutation rule、それ以外はkernelとして実行され、`logical_to_physical`は不変である（S0／S2で確認）。layoutを動かすのはS1の判断（multi-rank）と明示的なphysical swap／replanだけで、後者2つはsnapshotを無効化する。従来の`POST_SWAP_LAYOUT_UNKNOWN`は誤った保守判定だったので廃止した。
- **epochの区切りは決定的である。** async schedulerはpublished frontierの次から`lookahead_gates`ずつepochを切り、各epochで同じ`plan_window`の順序とfusionを使う（`lookahead_gates`は32以下で、capture windowの上限と同じ）。したがって、policy・layout・Binding・endpointが変わらない限り、残りの全executor呼出しとpublication数は一意に決まる。

## 構成

| 部品 | 内容 |
|---|---|
| `remaining_work.executor_groups` | 1 epochのexecutor呼出し列。schedulerと同じ規則で、`replan_cost_producer.remaining_calls`もこれを使う |
| `RemainingWork.future` | window以後のepochを、executor条件（op_idを除く）ごとの件数、epochごとの`epoch-publication`、現在のepochを含む`epoch-boundary`として持つ。`future_complete`、`future_reasons`付き |
| `RuntimeETA.evaluate` | futureが完全で理由が無ければ、semantic tailの代わりにwindow＋futureを評価する。そうでなければ従来どおりtailで、unknownになる |
| stage `epoch-publication`／`epoch-boundary` | epochのdrain〜GlobalCut commit、epoch後の`RuntimeControl.frontier()`（raw書出し、rank summary、ETA更新、適応判断）を、それぞれmeasured stageとして記録する |
| `observed_task_cost` | executor classと同じ規則（burn-in 3、8件以上、held-out coverage 0.8以上、drift判定）でepoch stageにも答える |

**unknownになる場合。** multi-rankのS1（`FUTURE_S1_LAYOUT_UNKNOWN`）、class数がmax_groupsを超える（`FUTURE_CLASS_LIMIT`）、byte上限を超える（`FUTURE_METADATA_LIMIT`）、未bindのparameter、windowの切詰め。byte上限を超えたときはfutureだけを落とし、windowとtailは残す。

**走査の予算。** futureはepoch単位で走査し、1回のcaptureで`scan_items`（既定256 source）と`scan_seconds`（20 ms）までに制限する。frontierがepoch単位で進んだ場合、windowへ移った分をtailとfutureから引くだけで走査を続ける（以前はepochごとに最初から走査し直しており、残りが256 sourceを超えると完了しなかった）。epochに揃わない前進や、frontier以外のcontext変化では走査し直す。

**byte上限。** ETAのserialized上限はmetadata reserveの1/32である。従来の8 KiBの上限を64 KiBに広げた。既定の128 KiB reserveでは4 KiBのままなので、RAMの予約は変わらない。その場合、futureは`FUTURE_METADATA_LIMIT`で落ちることがある。futureを使うには、metadata reserveを約2 MiB以上にする。

## private epoch内の残り時間

epoch実行中（private）のcaptureは、最後のpublished snapshotから、STAGEDのsourceを除いた残りを使う。
- **実行中の呼出し。** schedulerは実行中のexecutor呼出し（またはpublication）と開始時刻を`_executor_in_flight`に置く。captureはそのtaskを`call-residual`（classと経過時間）に置き換える。
- **残量の価格付け。** 同classの認証済み実測値のうち、経過時間より長いものの残量（d−elapsed）を使う。中央値をexpected、min／maxに相対10％の幅を足してlower／upperとする。長いものが5件未満なら`RESIDUAL_OVERRUN`でunknownのままにする。
- **未完了request。** 呼出しとpublicationの実測時間の中にあるので、個別には足さない（`in_flight_covered`）。呼出しの合間（直前の呼出しがSTAGED）では、残りのwritebackはpublicationのdrainに含まれる。
- **unknownのまま残る場合。** 実行中の呼出しがwindowに無い、またはunknown理由を持つときは、`PRIVATE_EPOCH_RESIDUAL_UNCALIBRATED`を残す。

## 監視費用の修正

completion collectorの`cost_observations()`は呼出しのたびに全行をdeepcopyしていた。ETA更新はclassごとにこれを呼ぶので、epoch境界の費用がrunの長さとcollectorの大きさに比例して増えた。小回路（6 qubit、160 gate、2 MiB collector）では、1 epochあたり4.6 msから14 msへ増え、run全体が2.4秒から5.7秒になった。
- **修正。** collectorにrevision（submitごとに増える）を持たせ、読取り専用の`cost_rows()`をrevisionごとにcacheした。rolling costはclassの索引も同じrevisionでcacheする。外部向けの`cost_observations()`は従来どおりcopyを返す。
- **結果。** epoch境界は約4 msで一定になり、runは2.4秒に戻った。

## 角度を除いたclassへのfallback

rolling costのclassは角度まで一致させるので、角度だけが違うgateの回路では同じclassが繰り返されず、observed viewは常にunknownだった。そこで、exactなclassの件数が足りないときだけ、角度を除いたclass（Clifford／diagonal／permutationのflag、target、layout、policy、engineは一致させる）を使う。このclassにもexactと同じburn-in、drift、held-out coverageの認証を掛ける。そのため、実測の所要時間が角度に依存しない場合にだけ採用される。sourceのlabelは`observed-pooled-angle`である。

## 検証

`tests/test_remaining_work.py`に次を追加した（角度poolingの認証と、異なるtargetへは広げないことを含む）。
- **futureの正確さ。** futureの呼出し数がschedulerの`remaining_calls`と一致すること（S0／S2）。publicationとboundaryの数、op_idを含まないclassの集約。
- **走査の予算と前進。** epoch単位の走査が予算内で進むこと、class上限でfutureだけが落ちること。epoch単位の前進後に、1回のcaptureで最初からの走査と同じ形状になること、揃わない前進で走査し直すこと。
- **実runtime。** 実measuredのepoch stageからobserved viewが残り全体を価格付けすること。
- **private epoch。** 呼出しの合間ではresidual理由が無いこと、実行中の呼出しがresidual taskになること、価格付けできない呼出しではunknownのままであること。
- **その他。** residualの値と`RESIDUAL_OVERRUN`、collectorのcacheがrevisionに従うこと。

MPIのoracle（1／2／4 rank）は、現在のepochのboundary taskを数えるよう更新した。Macの高速版（`-m "not slow"`）は873 passed／14 skipped。

**予備確認（Mac、1 rank、file store、6 qubit、160 gate）。**
- **50％地点。** S2では、epoch境界での予測2.11秒に対して実残り2.43秒（−13％）だった。
- **private epoch。** 実行中にexecutor呼出しの合間でETAを更新すると、12点すべてが答えた。誤差は−5〜−8％で、多くの点が区間に入った。
- **held-out評価。** 複数のworkload、Linuxを含む評価は次節に記録する。

## held-out評価

`examples/p4_002_observed_eta.py`（Geminiが下書きし、Sonnetが修正・実行）で、epoch境界のcheck（frontier 40／60／80％）からのobserved viewの予測を、そこから完了までの実時間（rank MAX）と比べた。workloadは、同じlayerを繰り返す4種と、layerごとに角度を変える2種で、30〜37 epochある。証跡は[内部記録](internal/p4_002_observed_eta/README.md)にある。

**初回評価（future classの集約とoverflow修正の前）。**

| 環境 | check | answered | median | p95 | worst | coverage |
|---|---|---|---|---|---|---|
| Mac 1 rank（6 workload、S0/S2、file/RAM） | 216 | 36% | 5.8% | 21.1% | 82.7% | 82.1% |
| Mac 2 rank | 216 | 29% | 7.9% | 56.3% | 83.8% | 87.3% |
| Linux buffered 1 rank RAM | 54 | 30% | 3.7% | 24.7% | 46.6% | 93.8% |
| Linux buffered 2 rank RAM | 54 | 15% | 8.1% | 14.2% | 14.4% | 100% |
| Linux O_DIRECT 1 rank file（1 workload） | 9 | 89% | 3.9% | 39.8% | 56.4% | 87.5% |
| Mac private epoch（呼出しの合間） | 32 | 69% | 4.6% | 9.8% | 13.2% | 100% |

- **未回答の原因。** 答えたcheckでは、全環境でmedianが30％目標内だった。未回答の原因は2つあった。
  - 角度が変わる回路では、future classが上限を超えた（`FUTURE_CLASS_LIMIT`）。
  - semantic tailがclass上限で溢れると、epochごとに走査し直していた。そのためepoch boundaryの費用が残量に比例して変わり（12→5 ms）、認証されなかった。
- **修正。** futureを角度を除いたgate形状で集約した。角度だけが違うclassは、角度poolingの認証済みcostで価格付けする。溢れたtailも前進分を正確に引くようにした。boundaryは4〜5.4 msで一定になった。

**修正後（Mac 1 rank、4 workload×S0/S2×file/RAM×3地点×2 trial）。**
- **全体。** 96 checkのうち50％が答えた。median 6.6％、p95 19.7％、worst 22.3％、coverage 93.8％。
- **角度を変える回路。** `rx-ry-var`は0／36から9／24に増えた。
- **残る未回答。** 30〜40 epochの短いrunで、classがburn-in後8件・held-out coverage 0.8の認証に届かないものである。認証規則は緩めていない。

**worstの原因。** 80％台のworstは、check後に実行が他trialの数倍に停滞した個別trialである（過小予測、原因は未計測）。

**監視overhead。** ETAのon／off比は、Mac 1 rankで+2〜4％だった。2 rankとLinuxでは、pairのばらつきが差より大きく、判定できなかった。

## end-to-end endpoint（2026-10-15追加）

`RuntimeControl.set_eta_endpoint(expectation=…, gradient={'observable':…, 'parameters':…, 'method':…}, checkpoint=True)`で、ETAの終点をgateの後まで延ばせる。引数なしで呼ぶとgates-onlyに戻る。
- **endpoint task。** snapshotの`endpoint_tasks`に1件ずつ足す。resourceはmainで、gateと直列になる。`endpoint`のlabelは`gates+expectation+checkpoint`のようになる。
- **計測。** 価格の元は、同じ条件の実測stageである。
  - async simulatorは、`expectation`（Pauli maskの組。係数は含めない）、`gradient`（method、observable、parameter集合、circuit hash）、`checkpoint-commit`を、それぞれmeasured stageとして記録する。
  - 直前のmaterializeはgateの仕事なので、これらのstageには含めない。
- **価格付け。** executor classと同じ認証（burn-in 3、8件、drift、held-out coverage）を掛ける。observed viewは次の順に探す。
  1. 不変profile
  2. runのrolling実測
  3. 事前校正のexecutor class profile（endpoint stageも書き出す）
- **unknown。** 認証されないstageがあれば、合計はunknownになる。

**確認（Mac、file store、Trotter型の反復：1 layerを足す→実行→期待値→checkpointを14回）。**
- **expectation。** 約2 msで安定し、認証された。
- **checkpoint。** 11 msから35 msへ、checkpointの回数に比例して増え、`LOW_HELD_OUT_COVERAGE`で認証されなかった。そのため、checkpointを含む合計はunknownである（正しい動作）。
- **当時の観測。** checkpoint stageは11 msから35 msへ増え、`LOW_HELD_OUT_COVERAGE`となった。当時のhistory全体を書き直すという説明はpayload format 2導入前の形式に基づく古い説明で、現行形式の増加理由は下記「checkpoint payload v2と成長費用の再確認」を参照する。

## multi-rankのS1（2026-10-15追加）

実行時のS1判断（`choose_s1`）は、そのepoch内の残りgateだけを見て、各gateの直前に決まる。network profileとstorage帯域が無いとき（または1 rankのとき）は、一度もlayoutを動かさない。
- **判断の再現。** `epoch_calls`は、このS1判断をgateごとに再現し、判断後のlayoutでexecutor classを数える。判断の費用は1 gateあたりO(lookahead)である。
- **swapの扱い。** swap自体はgateの計測時間の中で行われるので、別のtaskとして足さない（二重計上になるため）。
- **走査の前進。** layoutが変わるので、S1が動くrunでは、走査のkeyからlayoutとplacement epochを除く。代わりに、走査した各epochの開始layoutを最大4096件記録する。前進時には、新しいwindowの開始layoutが記録と一致する場合だけ増分で進め、一致しなければ走査し直す。
- **時間model view。** semantic tailは現在のlayoutを仮定するので、S1が動くrunでは`FUTURE_S1_LAYOUT_UNKNOWN`でunknownにする。observed viewは正確なfutureを使う。
- **試験。** `tests/mpi_s1_eta_worker.py`（2 rank、合成の通信profile）で確認した。rank bitのqubitへの回転が続く回路で、S1が実際にlayoutを動かすとき、予測したclass×回数（判断後のlayoutを含む）が、実際に記録された呼出しと完全に一致する。1 epoch進めた後の増分captureも、最初からの走査と一致する。

## 残件

- **答える割合。** 短いrunでは答える割合が低い。burn-inの後に8件が必要なほか、件数を満たしてもheld-out coverage／driftの条件で未回答になる（下の再評価）。長いrunとLinux file store・multi-rankでの再評価も残る。
- **overhead。** Linuxとmulti-rankの監視overheadは、ばらつきが大きく判定できていない。

- **S1。** multi-rankのS1は、判断を再現して正確に数える（上記）。swapを含む呼出しと含まない呼出しは同じclassになるので、swapが起きるclassでは認証されにくい。
- **endpoint。** observable／gradient／checkpointのendpointは実装した。gradientとcheckpointの認証済み価格は、反復回数とcheckpoint形式（history全体の書き直し）に依存する。
- **coverage。** payloadとcritical chainのcoverageは未対応である。
- **時間model view。** fused groupのpass数にfutureを使っていない。
- **gate形状の一致。** 角度以外のgate形状（target、control、fusion group）が繰り返されない回路では、classを認証できず、observed viewはunknownになる。

## 短いrunの認証再確認（2026-10-15追加）

**時系列の修正。** collectorの`cost_rows()`はcondition別ringを順に返していた。角度を繰り返すclassや、同じclassへまとめられる複数conditionでは、pooling後の並びが実行順とは限らず、burn-inと古い半分／新しい半分の分離が不正確だった。accepted rowのsubmit revisionを記録し、cost viewを必ず実submit順にする。event IDの字句順は使わない。sequence管理には1 rowあたり320 byteをcollectorの`max_bytes`に予約し、ring eviction時に解放する。rawの形式は変えない。

**安全条件。** burn-in 3件、認証にはその後8件、held-out coverage 0.8、drift limit 0.5、policy／plan freshnessを維持した。回帰試験では、2つの角度を交互に使う呼出しが途中で3倍遅くなった場合、従来のring順では両半分へ遅い値が混ざって誤認証できた。修正後は`DRIFTING`となりunknownを維持する。ringの置換、byte予算、cache invalidationも試験した。

**診断。** 評価scriptの`class_diag.missing_classes`に、exactと角度pooledそれぞれのburn-in後件数、held-out件数、coverage、判定理由を出す。exactだけが`INSUFFICIENT_SAMPLES`でも、fallbackの本当の失敗理由が`LOW_HELD_OUT_COVERAGE`である場合を区別できる。collectorの条件数／byte上限によるdrop数も記録する。

**実測（Mac、1 rank、buffered、cooperative、30 epoch）。** `rx-ry-low,rx-ry-var` × S0/S2 × file/RAM × 40/60/80％地点 × 2 trialの48 checkで、30件（62.5％）が回答した。回答分のmedian絶対相対誤差8.6％、p95 18.5％、worst 67.5％、区間coverage 83.3％だった。未認証taskは39件（同classのwindow／future重複を含む）で、fallback側を含む判定は`LOW_HELD_OUT_COVERAGE` 37件、`DRIFTING` 2件だった。全てburn-in後9／15／21件があり、単なる8件不足はなかった。exactだけの診断では14件が件数不足に見えていた。

sequence管理のbyte課金を加えた後の60％地点の8 check smokeは、3件（37.5％）回答、median誤差8.7％、coverage 100％、collectorのdropは全件0だった。48 check評価はbyte課金追加前、smokeは追加後で、どちらも時系列修正後である。証跡は[48 check](internal/p4_002_observed_eta/chronological-holdout-20261015/mac-48-checks.json)と[byte課金後smoke](internal/p4_002_observed_eta/chronological-holdout-20261015/mac-budget-smoke.json)に保存した。再現は`PYTHONPATH=src python examples/p4_002_observed_eta.py --workloads rx-ry-low,rx-ry-var --policies S0,S2 --stores file,ram --trials 2 --skip-overhead --output /tmp/p4-eta.json --scratch /tmp/p4-eta`（smokeは`--trials 2`を`--quick`へ変更）。

この変更は認証の正しさと診断を修正したもので、短いrunの回答率改善を実証したものではない。従来の約50％とは異なるworkload集合・試行で、同時に別作業も実行していたため、差の比較はできない。短いrunのcoverage不良／driftの原因調査、長いrun、Linux／multi-rankの再評価は引き続き残件である。対象試験`tests/test_runtime_measurement.py tests/test_runtime_cost_registry.py tests/test_remaining_work.py`は51 passed。

## endpoint認証の理由をruntime ETAへ返す（2026-10-16）

短いrunの未回答は、従来`MISSING_COST:<task>`だけで示されていた。collector側にはexact／pooled-angleの認証理由があったが、診断scriptの外では見えなかった。runtime observed viewにも`COST_UNCERTIFIED:<reason>:<task>`を加え、exactと角度pooledの失敗を区別する。`MISSING_COST`も残し、未認証sampleをETAへ価格として渡さない。

判定条件は変更していない。burn-in後8件、held-out coverage 0.8、drift limit 0.5、policy／plan freshnessを維持する。実測と認証を分離する回帰では、角度pooledのheld-out driftがETA理由へ現れ、期待値はunknownのままだった。安定したcheckpoint sampleのlower／expected／upperが順序付きで保持されることも確認した。runtimeのrolling intervalはtraining／held-out実測のenvelopeであり、confidence intervalやend-to-end保証ではない。これにより低回答率の原因をruntime API上で調査できるが、回答率を改善した実証ではない。endpointを含む短run、長run、Linux／multi-rank overheadの受入は残る。

## checkpoint payload v2と成長費用の再確認（2026-10-16）

旧記録の「毎回history全体を書き直すためrun全体がO(N²)」という説明は、現行のpayload format 2には当てはまらない。gates／historyは512件のcontent-addressed chunkで保存し、満杯の既存chunkは再利用する。小さなpayload試算では、8／16／32／64／128／256／512件時に更新対象chunkは1個で、更新chunk bytesは722／1,462／2,948／5,921／11,953／24,227／48,774だった。520件で既存の512件chunkを再利用すると更新対象はtailと新chunkの2個、769 bytesだった（合成データ、payload encodingだけの試算であり、checkpoint end-to-end時間ではない）。512件未満のtailは毎回再シリアライズされ、全gateの`gate_data`変換とrank間hash合意も残る。以前の11→35 ms観測だけから現行版の成長則を決めない。

`checkpoint-commit`のconditionは、実測／予測の両方でformat 2のgate/history総数、既存chunkの再利用数、更新tail件数、chunk参照数を持つ。記録時はcommit直前のshapeを使う。format 2のpayload cacheは、source list identityと長さが同じ場合に限ってpartial tailも再利用する。appendで長さが変わればtailを再構成する。これはunchanged same-size commitの再計算を減らし、gate/historyの全件変換とrank間hash合意は毎回行う。

成長を含む小さなend-to-end smoke（Mac、1 rank、file store、14回、各4 RX gate→expectation→checkpoint）ではcheckpoint stageが14.556〜20.787 msへ増えた。次のendpointはsource sizeが異なるため`COST_UNCERTIFIED:INSUFFICIENT_SAMPLES:endpoint:checkpoint`でunknownとなった。boundaryも`LOW_HELD_OUT_COVERAGE`でunknownだった。同サイズoracle（1 gateを実行後、sourceを増やさず14回checkpoint）では、初回dirty-tailと再利用済tailの2 shapeに分かれ、再利用済tail shapeは14 sampleになった。実際の全gate変換と全data hashを含むcheckpoint ETAはunknown理由なしで回答し、lower／expected／upperは8.668／8.916／9.564 msだった。rank間condition一致は2-rank MPIでも確認した。単一trialのinterval coverageではない。サイズ別checkpoint classのheld-out評価、複数trialでの予測区間、短run全体の回答率比較は残る。

## 実endpoint heldout baseline（2026-10-16）

`examples/p4_002_endpoint_heldout.py`で、同size／同payload shapeの実校正後にquery→次実completionを行うprequential評価を追加した。Mac COMM_SELF、4 qubit、4／32／128 gate、各endpoint3独立trial、校正16回＋heldout5回。手動costを入れず、全unknown/error、condition、query前sample数、実completion、数値oracleを[raw JSONとprotocol](internal/p4_002_observed_eta/endpoint-heldout-20261016/README.md)へ保存した。校正432・heldout135・growth9、約376秒。前半はroot TeX buildと重複した診断観測であり、性能改善の実証ではない。

- checkpoint：38/45回答、相対誤差median 8.01%、p95 11.27%、worst 14.97%、API wall区間coverage 13.2%。未測定のsource/history増加shapeは9/9 unknownで、直後の実checkpointとresume oracleは成功。
- expectation：45/45回答、median 25.15%、p95 45.44%、worst 47.77%、API coverage 44.4%に対し内部stage coverage 95.6%。128 gateではmedian 32.40%、API/stage比1.446で、timer外のmaterialize前処理をendpoint modelが落としていたことを確認した。
- adjoint gradient：45/45回答、median 0.69%、p95 5.06%、worst 10.11%、coverage 97.8%。gradientもcontext/schedule/vote/traceがstage timer外である。

この結果をendpointの受入完了とはしない。explicit expectation/gradientのfuture APIに必要なmaterialize前処理（実gate/epochを除く）を別stageとして補完し、gradient timerにも必須準備を含めた後に再測定する。checkpoint低coverage、Linux／multi-rank、非quiescent snapshot、monitor overheadは引き続き評価対象である。
