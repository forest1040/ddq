# P4-011：v3共有treeからowner-local v4へのoffline移行

状態：設計／OPEN。依存：009、既存v4 node-local checkpoint。共通契約は[全体設計](../phase4_remaining_design.md)。

## 現状・対応範囲

[distributed_checkpoint.py](../../src/svsim/distributed_checkpoint.py)は同一host/既存owner volumeのv3 upgradeとP=1 stored adapterを持つ。共有treeの別volumeコピー・配布adapterは未実装。新規予定：`offline_migration.py`、`migration_inventory.py`、明示migration command。

初期supportは固定n/b/P・complex128、quiescentなsource、識別可能なv3 snapshot/prepare、全owner shardとancestor/prefix closureが読み取れるcase。offline copyではshared sourceを読み得るが、移行後の計算/再開はshared pathを参照しない。live migration、rank増減、失われたshard復旧、循環/不明prefix closureは拒否する。

初期binary copyはpayload/slot/alignment/segment形式が一致するtargetを対象とする。異なるslot/alignmentへ移す場合はextent再配置adapterと新offset/checksum/capacity検証が必要なので、初期capabilityでは拒否する。backend変更だけの場合もtarget transportのalignment・durability条件を検査する。

## 入出力とinventory

`MigrationSpec`はsource snapshot/format/root、targetのowner別local volume、placement map、snapshot/prefix選択、copy/chunk/metadata、RAM/disk/wall上限、environment、checksum、再実行方針を持つ。targetが空または同migration IDの未完了stageであることを要求する。既存datasetを上書きしない。

`MigrationInventory`はcatalog/journal/asset/segment、必要root closure、file size/hash、source immutability証拠、owner、依存prefix/storeをdisk-backed/range形式で持つ。全extent表をRAMへ展開しない。ファイルpathは認証したsource/target namespace内へ正規化し、symlink/path traversalや他owner rootへの直接openを拒否する。

## 手順

1. read-only `plan_migration()`でsnapshotのschemaとclosureを検査する。sourceは書込み停止/lease固定、catalogを単なるlive file copyにしない。hot journalやsource変化を検出したら停止する。
2. 各target hostがlocal root/volumeと実空き・JobSpec割当を確認し、009でstaged copy＋catalog rebuild＋checksum＋prepare/decision/drainを予約する。environment容量も含める。
3. `migration-id/owner-stage`へbounded chunkでcopyする。sourceを読めるowner/指定loaderから必要ownerへMPIで配送し、通常計算でsource loaderを必須にしない。非参加hostのpathをcompute rankから直接openしない。
4. content/size/checksum、journal cursor、asset ID、root closure、prefix参照を検証し、target-local catalog/placementへ再構築する。foreign storeを同時に移すか認証済みlocal closureへ組み直す。shared absolute pathを残すcaseは拒否する。
5. 全ownerで新v4 prepareを作り、既存authoritative decision protocolでcommit/配送/ackする。v3 prepareだけではtargetを再開対象にしない。
6. fixed rank数のcollective resumeで状態・期待値・必要なら勾配、prefix fork/lease/GCを検証してtarget公開。sourceを自動削除せず、利用者が切替を確認できるreceiptを保存する。

source→targetのstore/root/snapshot/placement参照mapをreceiptに持ち、gradient origin、prefix lease、actual historyの参照もclosure内で置換・検証する。host/volume/libraryが変わった旧profile・candidateは失効させ、再校正までauto切替を無効にする。移行でsource semanticsやexact witnessを変えない。

## 再実行・fault

chunk進捗はmigration ID、source fingerprint、owner、range、hashで記録する。再実行はsource不変と既存chunk hashを確認し、未完了rangeだけを再copyする。partial chunkを成功扱いにしない。stage cleanupは自migrationの未公開fileだけ、commit済みtargetをrollback/deleteしない。

copy/SQL/checksum/prepare/decision/ack各faultで、source不変とtargetの非公開またはauthoritative new cutを保持する。source変化・容量不足・missing owner/asset/prefix・destination generation違いはcollective拒否する。crashed copy reservationをlease expiryだけで戻さず009のreconcileを使う。

## 受入・完了

| 段階 | 証跡 |
|---|---|
| 011A inventory/plan | v3/P=1、ancestor/prefix closure、size/count上限、unsupported sourceの理由、copyなしdry-run |
| 011B Mac別root | source fingerprint、途中copyの再開、quota/fault、v4 hash、全旧source accessを禁止したresume |
| 011C Linux別volume/host | 許可後のowner配布、O_DIRECT支持条件、same rank collective resume、fork/lease/GC |

global phaseを合わせないstate/expectation、必要gradientとactual layout/history一致を確認する。targetを再開後、source shared treeを読むと失敗するaccess guardを必須にする。copied receiptだけでnode-local計算を証明しない。

source形式を絞る場合はsupport/reject一覧とPhase 4本設計/backlogを一緒に改訂する。制限の文章だけで別volume移行をDONEへ変えず、対応範囲のcopy/checksum/collective v4 commit証跡を揃える。
