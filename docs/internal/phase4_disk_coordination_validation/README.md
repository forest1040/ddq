# Phase 4：共有disk coordinatorとforeground RAMのMac検証

2026-10-14。対応は[実装資料](../../phase4_disk_coordination_implementation.md)。クラスタ接続・SSH・PBS投入は行っていない。uv環境の実MPIをMac内で使う。[manifest](manifest.json)にsource／worker／lock hash、構成と再現commandを固定する。

## 最終全件回帰

最終treeの全件回帰は **650 passed／2 skipped、594.15秒**：[log](pytest-full.txt)。skipは当該Mac環境で対象外の2件。manifestのsource／worker hashが最終treeと一致することと、logのSHA-256を再照合した。MPI communicatorはMac内だけで使用する。

## 関連回帰

共有disk、catalog、lifecycle、checkpoint、勾配／batch、cacheと実MPIの関連回帰は **107 passed、272.82秒**：[log](pytest-related.txt)。run中にcacheの診断drop countと固定budgetの単体試験、workerの同host確認とroot count assertionを補強したため、このlogを最終treeの全件検証とは呼ばない。最終単体でcache／ledgerの30件を確認し、上記全件回帰で最終treeを確認した。

初回の[失敗log](pytest-initial-failures.txt)には、fixtureのgradient keyword指定不足6件と、任意cache診断情報が最低workspaceを消費した既存大状態3件が含まれる。fixtureでは公開APIのkeywordに修正した。[origin契約の失敗log](pytest-origin-contract-failures.txt)も保存した。caller forkのparameter-shiftには全回路originが必要な既存契約に合わせ、caller forkではadjoint、captured originを持つtemplateではparameter-shiftを検証する。tmp prefix参照を手書きで偽装しない。性能統計やproduction profileへ初回failureを混ぜない。保存したfailure logは行末空白だけ正規化した。

## 構成と検証内容

同host P=1/2/4、buffered／Mac direct=F_NOCACHE、n=4、b=2、complex128。cooperative IO worker1、OMP／OPENBLAS／VECLIB=1。各独立runtime managed RAM2 MiB／rank、共有coordinator quota64 MiB／host-volume、入力のrootとledgerは同じlocal filesystemとする。

dataset A／B、Aのprefix fork、templateから作るparameter-shift forkが同じledgerを使う。stateをeager referenceと1e-10、期待値cos(0.37)を1e-10、勾配-sin(0.37)を1e-9で照合する。別clientに発行した8 MiBのpending tokenを全clientが観測し、合算残容量を1 byte超える予約を拒否し、一度releaseした後は全clientで消える。

同host／volumeでrankのledger pathが異なる場合は、実ledgerを開く前にcollective拒否する。policy mismatchは既存tokenを保護し、shared flagをfalseにしてもbound済みpolicyを迂回できない。legacy active tokenの初回bind、別rootの既存dataを加えたquota超過、global root count境界も検査する。

親配下の300 child registrationはpersistent rootを1件に保ち、実file bytesをすべて数える。Root.AとRoot.A2を混同せず、後から親へまとめてもbytes／pendingを減らさない。非重複rootの上限は256件。

大状態の既存MPI testはn=14+log2(P)で、state／rankがmanaged RAMを超えるままにする。期待値とmanaged peak、durable checkpointを検証する。diagnostic metadataはforeground headroomから借りず、必要時に精算してLRUへ戻る。宣言RAM予算や大状態条件は緩めない。

## 受入の境界

同じledgerの参加dataset／suffixに対する合算調停である。異なるledger、外部job、全volumeの強制書込み上限を保証しない。strict_capacity_guarantee=false。temporary branchのcloseではdurable directoryを自動削除せず、そのbytesを継続課金する。安全なGC、tokenとauthoritative cutの結合、全writer crash／recovery、別volume／Linux／cross-hostは未達。P4-007／009はIN_PROGRESSのままである。
