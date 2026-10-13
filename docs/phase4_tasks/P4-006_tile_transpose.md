# P4-006：予算内のaligned destination tile転置

状態：設計／OPEN。依存：009。費用採用は001/002/004。共通契約は[全体設計](../phase4_remaining_design.md)。full output blockを置けない条件に対し、ordered physical swapの検証済みtile routeを追加する。

## 現状・変更

[fused_swap.py](../../src/svsim/fused_swap.py)は最大2 pairの合成permutationとfull destination blockを作る。`source_labels()`と`sim.fetch()`はsource blockの取得/Frame解消を前提にする。destinationだけ小さくしてsource fetchのfull-block workspaceを無視する方式は採用しない。

新規予定：`tile_transpose.py`、`tile_extent_writer.py`、`tile_resolver.py`。storage/async_store_io、MPI progress、fused_swap、history/checkpointを拡張する。既存full-block routeと単独swapをreferenceとして残す。

## Mappingとroute certificate

ordered pair listを順に適用した`pi`、逆順pairの`pi_inverse`を使う。destination `(h, offset)`からsource index=`pi_inverse((h<<b)|offset)`を求め、source block/offsetとactual extent ownerへ解決する。pair overlapや階層を含め、destination-first方式で各出力を一意に担当する。

`TileRouteCertificate`はpair order、n/b/P、dtype、tile/alignment、Frame/log class、resolver、local/remote owner、group/tile working set、COW disk、coverage方式、inverse witnessを持つ。tile bytesはalignmentとcomplex128の倍数、末尾paddingを定義する。設計初期値256 KiBでもbudgetに応じて合法候補を選び、n/b/Pは変えない。

## 実行アルゴリズム

1. old root/layout/Frameをfreezeし、009で出力extent・coverage metadata・drainを予約する。new rootはprivate。
2. ownerごとにdestination block/offset rangeをlazyに走査し、tile一つのsource fragmentを合成indexからまとめる。全Block descriptorや全fragment表をRAMへ作らない。
3. source slotとalignmentに合わせてread rangeを広げ、valid payloadだけを選ぶ。source ownerがtile-resolvableなFrame/log/phaseを旧layoutで適用し、typed MPIでfragmentを送る。data/control順序とslot generationを全rankで合意する。
4. bounded destination tileへscatterする。covered rangeの穴・重複・offset・count・generationを検査し、paddingを規定値で埋める。
5. `TileExtentWriter`で未公開extentの非重複rangeへwriteする。成功completionまでbufferをpinし、allocator/sql/checksum publicationはmain thread。
6. 全payload rangeが揃った後、logical byte順のblock checksumを確定し、不変versionへsealする。fragment arrival順のhashをblock checksumに使わない。paddingはlogical hashから除外する。
7. new rootのcoverage・owner・checksumをvoteし、root/layout/placement/inverse/historyを一つのtransitionで公開する。durable replanへ使う場合は004のdecision protocolを通す。

checksumのordered計算にはtile順writeまたはbounded再readを使い、その追加passをcostに入れる。coverageはowner-local SQL/range recordへ保存し、blockごとにRAMで保持するtile bitmapも上限を設ける。旧extentを書き換えず、partial outputを既存読込みAPIへ公開しない。

## Frame・資源・拒否

006Aはidentity Frame＋解消可能なscalar phase/認証済み単純logに限定する。006Bでpaired-tile butterflyやpermutation等、証明したFrame resolverを追加する。arbitrary Clifford/logがfull blockを必要とし予算に入らない場合は`UNSUPPORTED_FRAME_TILE_ROUTE`で拒否する。Frameの種類を落として間違った振幅を返さない。

peakはsource aligned tile/group＋destination tile＋I/O/MPI/packing pins＋coverage/catalog＋rollback/COW＋resolver workspace。同じleaseのpinはcopyとして二重計上しない。foregroundとterminal/drain creditを予約し、inputだけ確保してoutput待ちする循環を防ぐ。source/destination両側のadmissionをrouteへ含める。

file backendを初期対象とする。MemoryBlockStoreのresident payloadを部分extentへ変更することは別capabilityで、file spillの費用・容量なしにRAM tile経路へ流用しない。aligned short I/Oの残rangeが再発行不能なら失敗とし、direct→bufferedへ暗黙に変更しない。

## Faultと受入

| 対象 | 必須検証 |
|---|---|
| mapping | lower/lower、lower/local-upper、lower/rank-bit、upper/upper、overlap pair、identity pair、inverseと単独swap列 |
| Frame | identity、認証済みtile resolver、unsupported/full-block必要条件の事前拒否、未消化log |
| tile writer | 境界/padding、同range重複、穴、順序逆転、checksum、short write/read、direct alignment |
| capacity | full-block不可能だがtile可能なcase、source resolver不可能、RAM/disk/minfree/drain不足 |
| lifecycle | fragment/SQL/fsync/decision/ack fault、partial extent未公開、old/new durable cutとGC保持 |
| numerical/performance | state/expectation、actual tapeを用いるadjoint/gradient、sequential/fused/tileのpasses/bytes/peak |

Mac fake completion→P=1/2/4 buffered/F_NOCACHE→許可後Linux/O_DIRECT・別owner通信。candidateがtileをadvertiseする前にroute certificateとfault証跡を揃える。合法性を保って拒否したFrameはsupport境界へ明記し、一般Frameすべてをtile対応と呼ばない。
