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

## 残件

- **答える割合。** 短いrunでは答える割合が約50％に留まる。classの認証には、burn-inの後に8件が必要だからである。長いrunとLinux file store・multi-rankでの再評価も残る。
- **overhead。** Linuxとmulti-rankの監視overheadは、ばらつきが大きく判定できていない。

- **S1。** multi-rankのS1のfuture（各gateのS1判断とphysical swap費用）は未対応である。
- **endpoint。** observable／gradient／checkpointを含むendpointは未対応である。
- **coverage。** payloadとcritical chainのcoverageは未対応である。
- **時間model view。** fused groupのpass数にfutureを使っていない。
- **gate形状の一致。** 角度以外のgate形状（target、control、fusion group）が繰り返されない回路では、classを認証できず、observed viewはunknownになる。
