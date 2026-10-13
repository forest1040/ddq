# Phase 4：回収・cache課金・勾配資源検証・監視費用

更新日：2026-10-14。前回の[P4-003／005／008先行実装](phase4_validation_implementation.md)後、残件の依存元を補強した。対象はP4-001／007／008／009。Lunaがdisk回収、cache課金、勾配資源検証を担当し、Codexが共有budget、runtime収集の対照とactual MPI検証へ接続した。各IDの受入条件を維持し、全タスクのDONEを宣言しない。

## P4-009：明示回収を一度だけ適用する

[DiskBudget.reconcile](../src/svsim/disk_budget.py)は同じSQLite transaction内でreservationを読んで検査し、RESERVED／WRITINGからだけterminalへ条件付き更新する。二重回収、terminal rowの再回収、記録済みmaterialized bytesを減らす操作を拒否する。process crash後の未物理化予約を期限やPIDだけで解放しない。

実子processが100 byte予約し、WRITING／materialized=40と40 byteのpartial fileをfsyncした後、`os._exit`で終了する試験を追加した。再open後も60 byteのpendingと40 byteのactual usageを維持し、明示reconcile後も残存fileがquotaを消費する。1 byteの過剰予約と二重reconcileを拒否する。

ledger自身はowner停止、hot journal復旧、inventory、authoritative cut／decisionを検査しない。callerがこれらの確認結果を明示evidenceとして渡す契約である。[writer監査](phase4_writer_audit.md)に事前予約／VFS／精算／回収の境界をまとめた。全writerのfault／recoveryと独立datasetのhost調停は残り、strict_capacity_guaranteeはfalse。

## P4-007：hint／traceを共有RAMへ課金する

[ManagedMetadata](../src/svsim/cache.py)が名前付き予約をMemoryBudgetへ課金する。summary／context、legacy next-use hint、policy traceと理由counterをbounded poolとして扱う。JSON byteから定数と倍率で保守的に予約し、buffer予算から無償のmetadataを使わない。この予約はmanaged scopeであり、Python／MPI／OS全体のRSS保証ではない。

[AsyncBlockCache](../src/svsim/async_cache.py)はhint置換前にdeltaを予約し、容量不足なら旧hintとbudgetを維持してfalseを返す。未校正costはLRU。contextがstaleになればsummaryを削除・精算してLRUへ戻る。bounded traceが満杯またはRAM不足ならdrop countを残す。cache shutdown時はhint／traceを一度だけ精算する。

batchとparameter-shiftの新規childへbudgetを移す際、metadataの予約先もparentへ移す。childのmetadataとruntime poolsはそれぞれのcloseで精算し、共有metadataを二重releaseしない。

[FutureUseSummary.from_pending_versions](../src/svsim/cache_cost.py)は既知の順序付きversion列から最初の32 operation以内のfirst-use／reuse countをまとめる。costは生成しない。actual schedulerの全future planを自動抽出する接続、Binding共有、quota公平性、LRUとの最終paired性能比較は残る。

## P4-008：明示したresource upperをrawに照合する

calibration rawに`resources_measured`を追加した。明示resource_probeがRAMとdiskの両peakを整数・非負として返した場合だけtrueにする。scratch scanによる補助値でunknownな測定を認証しない。summaryもunknownを0で埋めない。

certifierの明示RAM／disk upperがheld-out各rawの測定peakをすべて覆う場合だけenvelopeを作る。adapterもproducer／hash／context／coverageに加え、同じresource検査を繰り返す。reportを再hashして1 byte小さなupperへ変えても採用しない。observed maxからguaranteeを自動生成しない。

認証済みactual profileでのauto性能、host／残容量／2-streamの包括admissionとgenerator／Frame／replan／別hostの受入は残る。旧reportに測定flagが無い場合はproduction envelopeを採用しない。

## P4-001：completion collector費用の対照とbackend取得

`sim.runtime.configure_measurements(..., enabled=False)`でcompletion収集だけを無効にできる。既定はtrue。collectorはvalidator／event／group／cost evidenceを作らず、snapshotにdisabledを明記する。無効側も同じcollector poolをRAMへ予約し、cache容量差による速度差を混ぜない。state検証、durable checkpoint、trace、I/O／MPI progress、既存Guardは両側で維持する。

[actual campaign](../examples/phase4_remaining_campaign.py)の`--action monitor`は同一source／Binding／S2／resourceでcollector-off/onを別axisとしてpaired比較する。primaryはconstruct→forward→state検証→checkpoint→collector snapshot→close→cleanup、execute wallは補助metric。収集以外の監視全体を消す比較ではない。通常の`--action all`の対象は従来の3 suiteを維持する。

async handleがstoreのhandle表と別に所有されるため、前回rawのresolved backend情報が空だった。新campaignは実I/Oがemitしたtraceからidentityを取得し、全rankの値を集約する。前回rawを遡って書き換えず、新しい資料に観測を保存する。

collectorのon/off、actual backend、rank MAX、state／expectation／gradientと予算をMacだけで検証する。full registry、Frame／packing／metadata独立stage、自動cost接続、全monitorのoverhead、Linux／cross-hostは残る。具体的な測定条件と結果は内部資料へ保存する。

## 検証資料

最終関連回帰132件とcollector対照6構成・108 trial（warmup24／本測定84）の条件、raw照合と結果を[内部資料](internal/phase4_safety_monitoring_validation/README.md)に保存した。Mac direct要求では全本測定rankのtraceからmacos-f-nocacheを確認した。collectorに限定したon/off差であり、全monitor費用や未来回路への予測とは扱わない。
