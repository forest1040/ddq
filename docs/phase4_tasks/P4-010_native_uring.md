# P4-010：Linux native io_uringの実機検証

状態：IN_PROGRESS。2026-10-14、利用者が許可したLinuxサーバのDockerへuv／MPI／liburing開発環境を構築し、実kernelのnative I/O検証に着手した。従来のクラスタにはliburing header/libraryを新規導入していない。optional項目。共通契約は[全体設計](../phase4_remaining_design.md)。

[環境構築・追加検証](../linux_development.md)と[内部証跡](../internal/ent_linux_validation/README.md)を参照する。明示opt-inの実kernelテストと1／2／4 rank MPIテストを追加し、requested nativeの実体とTHREAD_SINGLEを確認する。全fault／durability／性能比較matrixは残るためDONEとはしない。

## 現状・対象

[uring_backend.py](../../src/svsim/uring_backend.py)、[native/uring_backend.c](../../src/svsim/native/uring_backend.c)、[test_uring_backend.py](../../tests/test_uring_backend.py)にadapterとfake CQE試験がある。native build、実kernel、CQE completion/cancel/durabilityの証跡を追加する。Macのfake成功をnative measured sampleへ変換しない。

## Capabilityとartifact

Linux OS、kernel/library/header/compiler/build hash、container/seccomp等の環境情報をread-onlyで調べる。ring setupとallowed operation、alignment、queue capacity、registered resource対応は明示したbounded capability probeで確認する。unavailableは理由とscopeを保存し、勝手にsudo/install/kernel設定を行わない。

buildはoptional artifactとしてuv環境から呼び出し、通常importとworker fallbackを維持する。requested nativeが使えない場合は明示拒否、autoのworker fallbackはrequested/resolved engineとreasonを出す。native queue metadataをmanaged budget、ring/library/process RSSを別allocationへ計上する。

## 受入試験

| 項目 | 検証 |
|---|---|
| 正常 | workerと同file/input/slotでread/write/fsync一致、複数slot・任意completion order |
| direct | Linux O_DIRECT address/offset/length、EOF/truncated、aligned short再発行、unaligned残range拒否 |
| CQE | negative/error、short、unknown/stale user_data generation、duplicate completion、queue full |
| cancel | cancel CQEとoriginal終端を別管理、cancel勝敗双方、元I/O terminalまでpin保持 |
| lifetime | context close前後、FD/lease/reader共有、dirty version publication、drain/cancel/shutdown |
| durability | write completionとflush成功の区別、checkpoint prepare/decision、fsync失敗でold/new cut |
| budget | ring capacity増加restart、RAM/disk/backpressure、foreground starvationとterminal余裕 |
| scheduler/MPI | MPI main-thread、THREAD_SINGLE cooperative、P=1/2/4、owner-local data |

fault/errorはnativeが実際に返したCQEとfake注入を区別する。native scopeのrawにsubmission/CQE/result/generation/pin transitionを保存し、error sampleをbandwidthへ混ぜない。完了protocolが回復不能なMPI failureはjob停止とし、communicator recoveryを約束しない。

## 実行順と完了

010A Mac fake regressionを維持しLinux compile/capabilityを準備、010B利用可能なLinux単一nodeでworker対照、010C許可後のreal MPI/lifecycle統合と比較。今回のDocker実行は各コンテナ4 CPU・8 GiB RAM、probe 60秒、native suite 600秒、通常回帰の一括上限7200秒。native MPI各ケースは180秒上限を持つ。専用scratchとコンテナだけを使い、他jobへ負荷を注入しない。性能比較の同条件trialとnative大状態、全障害matrixは後続作業とする。

利用可能な環境で必須matrixと数値/cleanupが揃えばDONE。利用不能ならBLOCKED理由を保持し、Phase 4のoptional unavailableとして最終reportへ記載する。Linux worker/O_DIRECTの必須検証は別に完了させる。新規環境導入や既存サイトの設定変更は具体案と許可確認の後に行う。
