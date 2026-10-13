# P4-007：Frame・Bindingを含むfuture-aware cache費用

状態：IN_PROGRESS（2026-10-14）。依存：001、002、009。004へ採用費用を供給する。共通契約は[全体設計](../phase4_remaining_design.md)。

raw／resolved identity descriptor、bounded future hint、validated costとunknown／staleのLRU fallbackを先行実装した。[実装範囲と検証](../phase4_remaining_implementation.md#cache-identityと費用fallbackp4-007)。[追加実装](../phase4_safety_monitoring_implementation.md)でhint／trace／counterの共有RAM課金、stale／close時精算、既知future sequenceのbounded summaryを追加した。resolved buffer共有は未有効。全future計画の自動抽出、actual profile供給、prefix reader lifetime／公平性とpaired性能評価は残る。

## 現状・責務

[async_cache.py](../../src/svsim/async_cache.py)は新規`cache_cost.py`／`cache_identity.py`のfinite hintを受け付け、validated／同unit／freshnessが揃う場合だけ費用を比較する。不足時はLRU。旧next-use distanceだけではcost evictionを行わない。prefetchはraw immutable extent、Frame解消はcaller。ParameterBatchの公平admission接続は残る。

raw cacheとresolved cacheを別kindにする。raw keyはdataset/store/version、storage ownerとformat/dtype。resolved keyはさらにsource coordinate、Frame/log/phase、layout、parameter dependencyの値とexact witnessを含む。same global Binding IDだけで一致/不一致を判定しない。依存しないparameter差だけなら同一性を証明し、log/phaseへ影響する差は共有しない。

## 入出力・費用

`FutureUseSummary`はbounded window32/最大64 hint、version/resolved key、next-use range、reuse/reader数、Binding/job、必須/投機、取得経路、confidenceと版を持つ。002のsnapshotから構築し、observed futureを後から過去へ渡さない。

`EvictionDecision`はeligible集合、選択key、writeback/再取得/再解消費用、credit、fallback理由、snapshot/profile IDを返す。評価する費用はdirty writeback、同条件のlocal/remote read、Frame/logの再materialization、再利用の合法性、prefix寿命、共有device争用。すべて同じtime単位へ揃え、mixed whole-fetch cellとそのread/Frame componentを重ねない。

## アルゴリズム

1. pending read/write、foreground/job pin、reader、rollback/prefix leaseを参照し、evict不能なentryを先に除く。
2. eligibleなkeyの次use/reuseを有限summaryで照合。未知cost/古いhint/未知依存ではそのentryの費用比較をしない。
3. known entryは保持による回避費用とdirty/writeback費用を評価し、失う利益が最小のentryから選ぶ。同点はclean/LRUの決定的順序。
4. fallbackが必要なら従来LRUを使い、dirtyはbounded writeback→completion後evict。読込み/書込み失敗では新しい成功costを作らない。
5. prefetchは共有budget、job quota、foreground/drain headroom内で、予測したnext-useまでに完了可能なimmutable rawを優先する。不要なspeculationは発行前cancel、発行後は終端までpinしてdiscard。

initial implementationはraw cacheへFrame再解消費用のhintを付ける。resolved bufferのBinding間共有はidentity証明とreader lifetimeを実装した次段階だけで有効にする。cacheのversion hintだけでlogical stateを共有しない。

## Fairness・version変更

jobごとに最低foreground working setを予約し、prefetchは残余creditで行う。weighted round robin＋aging、reader/writer別creditを設け、prefix共有で一jobが全cacheを占めないようにする。job増減はaggregate host/rank quota内でのみ許可する。

source cut、Binding/witness、Frame/log、layout、placement、plan、lease closureが変わればhintを無効化する。policy/profile変更でも再評価する。GCとprefix releaseはpin/reader/foreign owner leaseを全rankで照合し、stale hintがあることをlive rootの証明に使わない。

## 計測・受入

hit/miss/prefetch-hit/unused、extra bytes、writeback queue、Frame解消回数、reload、job待ち時間、quota share、managed/pin/retained version peak、end-to-endを記録する。Phase 5で確認したclean old-version保持も予算に含める。hit率だけで性能を合格としない。

007A identity/fallback、007B actual cost＋bounded future、007C Binding/prefix共有＋公平性に分ける。必須caseはsame/raw異Binding、依存値/ExactPi witness差、stale layout、pending writer、LRU対照、host device競合、foreign prefix lease、quota境界、GC/pin、適応後のstate/expectation/gradient一致。

同source/精度/budgetでLRUとcost policyをpaired比較し、利益と悪化・余分なI/Oをすべてreportする。unknown時のsafe fallbackと実測比較が揃ってDONE。lookaheadヒント数を増やすだけでは完了としない。
