# ent上のLinux Docker開発・検証

2026-10-14。利用者が`ssh ent`とDockerによるLinux開発を明示的に許可した範囲で実施した。研究室の`lg`／PBSは使用していない。接続先・絶対パス・ホスト情報を含むため、外部共有時の非公開化／匿名化対象とする。`docs/internal/`という配置自体にはアクセス制限がない。

## 調査と隔離

`ent`はLinux Mint 22.1、kernel 6.8.0-138-generic x86_64、20 logical CPU、RAM 33,369,694,208 bytes。Docker 29.7.2、overlayfs storage driver、Docker rootは`/var/lib/docker`。ホストにはOpenMPI 4.1.6とliburing runtimeがあり、uvとliburing開発ヘッダーはない。`io_uring_disabled=0`。既存の2コンテナは変更していない。

`/tmp`と`/home`は`/dev/nvme0n1p2`上のローカルNVMe ext4で、調査時は約795 GiB空きだった。作業rootは`/tmp/ddq-linux-dev-20261014-bnuv6j`。ソースは必要な`src/tests/examples/docker`、project設定、lock等だけをrsyncし、`.git`、内部資料、参考資料、計画、ローカルvenvは転送していない。

イメージtagは`ddq-linux:dev-20261014`。ホストと同じUID/GID 1000でnon-root実行し、4 CPU・8 GiB RAM・512 PID・256 MiB shared memory・network noneを指定した。ホストへのpackage導入、sudo、daemon／kernel／sysctl変更は行っていない。scratch／outputは専用directoryに保存し、自分の検証コンテナだけを終了時に削除する。

## 再実行手順

Macのリポジトリ直下から必要なソースだけを同期する。既存の作業rootを再利用する場合は、実行中の検証がないことを確認してから更新する。

```sh
REMOTE_ROOT=/tmp/ddq-linux-dev-20261014-bnuv6j
rsync -a --exclude '__pycache__' --exclude '*.pyc' \
  src tests examples docker pyproject.toml uv.lock README.md .dockerignore \
  "ent:$REMOTE_ROOT/context/"
ssh ent
```

`ent`側で以下を実行する。新しい検証directoryはscriptが作成し、runごとの証跡を上書きしない。

```sh
cd /tmp/ddq-linux-dev-20261014-bnuv6j/context
docker build --platform linux/amd64 -f docker/linux/Dockerfile \
  --build-arg DEV_UID="$(id -u)" --build-arg DEV_GID="$(id -g)" \
  -t ddq-linux:dev-20261014 .
DDQ_LINUX_IMAGE=ddq-linux:dev-20261014 \
  bash docker/linux/run_validation.sh /tmp/ddq-linux-dev-20261014-bnuv6j
```

作業rootは`/tmp`にあり、再起動や一時fileの清掃で失われる可能性がある。ソースと環境定義はGit、必要な検証証跡はこのdirectoryへ戻して保存する。開発に使用するLinux実行先は当面`ent`のDockerとし、Macの回帰も維持する。

## Toolchainと証跡

コンテナはCPython 3.12.15、uv 0.12.23、NumPy 2.5.4、pytest 9.1.1、mpi4py 4.1.2、OpenMPI 4.1.4、liburing 2.3、GCC 12.2.0、SQLite 3.40.1。mpi4pyはコンテナのOpenMPIへリンクし、ホストOpenMPIを使わない。base image digestとPython lockは固定しているが、apt package取得先はsnapshot固定していないため、toolchain logとimage IDを保存する。

### Native I/O／MPI

[native log](initial/native.txt)、[JUnit](initial/native.xml)は **12 passed、459.15秒**。6件の実kernel I/O試験と、buffered／O_DIRECT × P=1/2/4の6件のnative MPI試験で、skip／fallbackはない。MPIは各rankの実engineとTHREAD_SINGLEを照合し、状態・期待値・adjoint勾配・prefix／fork／lease／GC・checkpoint／resumeを確認した。

既定seccompの[probe](initial/probe-default.txt)はexit 2、`Operation not permitted (-1)`を記録した。限定profileの[probe](initial/probe-native.txt)はexit 0、engine=`io_uring`。source／compiled binary hash、queue capacityとmetadata予約byteを含む。通常回帰とnative成功を同じseccomp条件と扱わない。

[初回source hash](initial/source-hashes.txt)、[toolchain](initial/toolchain.txt)、[image](initial/image.json)、[status](initial/status.tsv)を保存した。NativeUringEngineを含む`src/`と`examples/`の全hashが現在のtreeと一致することを照合した。後続修正はテストfixture、test-only engine assertionとcollector検査、runnerのhash／stage選択であり、native実装・C sourceは変更していない。

### Mac関連回帰

最終の[Mac関連回帰](mac-related.txt)は **47 passed、2.21秒**。uv環境でcache、開発runner（実2 rankを含む）、native fake CQEとcollectorの接続、runtime計測、worker I/Oを確認した。

### Linux回帰で見つかったfixtureの前提

初回の[回帰log](initial/regression.txt)、[JUnit](initial/regression.xml)は **8 failed／136 passed、222.77秒で中断**。失敗箇所を特定した時点で自分の検証コンテナへSIGINTを送り、修正後の再検証へ切り替えた。全件回帰が完了したlogではない。

- cacheのstale／missing-getter試験2件は、Macの16 KiBとLinuxの4 KiBのmmap page差で、hint解放後に追加bufferが収まり、evictionが不要になっていた。解放後も1 blockのevictionが必要となるfixtureへ修正し、既知costのケースや本番RAM予算は変更していない。
- runnerの6件は、default scratch=`.`をroot-ownedの`/workspace`へ作ろうとして`PermissionError`となった。テストだけを専用のwritable cwdへ移し、default APIを維持した。reportとscratchのdirectoryも分離する。
- 修正後の4 rank followupでは数値／resource検証は成功したが、短縮した90秒のcampaign上限に達し、auto診断が正しくunmeasuredとなった。correctness smokeをCLI既定の300秒／subprocess 360秒へ揃えた。試行数・RAM・SSD予算、validatorと本番のlimit処理は変えていない。

fixture修正後の[全件起動log](regression-first/regression.txt)、[JUnit](regression-first/regression.xml)は **4 failed／337 passed／12 skipped、1489.78秒で中断**。失敗は4 rank followupの90秒上限1件と、Phase 3 MPIの120秒上限3件。Phase 3のscopeはpacked／full、4-qubit dense、prefix、RAM backend、payload縮小、勾配等をまとめた8-qubit回路で、短いdeadlineに達していた。数値チェックを削らず、Dockerで`DDQ_MPI_TEST_TIMEOUT=600`を渡せるようにした。Macの既定は120秒を維持する。

修正後の[followup campaign](campaign/campaign.txt)、[JUnit](campaign/campaign.xml)は **5 passed、575.78秒**。変更したmoduleの全ケースを独立した作業コピーで実行し、全件起動のsourceを実行中に書き換えていない。後半の未完了ケースとPhase 3の再検証は、完了済みの349件を[ID一覧](remaining-deselections.txt)で除いて実行する。followup module全体は別の5件で検証する。

後半の[log](remaining/remaining.txt)、[JUnit](remaining/remaining.xml)は **6 failed／298 passed／2 skipped、2442.36秒**。Phase 3の3構成は600秒以内で成功した。失敗6件は、S2 extendedとRAMを超えるstateの4 rankで120秒に達した2件と、tileのbackendをmacOS固定で照合していた4件。Phase 4 MPIもDockerの600秒上限を選べるようにし、Macの既定120秒を維持した。tileはLinux O_DIRECTのbackend名と4096-byte alignment／tileを使い、Mac／bufferedの64-byte設定は維持する。

最終の[再検証log](retry/retry.txt)、[JUnit](retry/retry.xml)は **18 passed、228.41秒**。失敗したMPI2件と変更したtile moduleの全16件を確認した。最終の[Mac関連log](mac-final.txt)も **63 passed、8.00秒**。

### 分割結果の全ケース照合

[最終Linux collection](retry/collection.txt)の**656件すべて**と[集計JSON](merged-regression.json)のnode ID集合が一致し、最新の再検証を反映した結果は **642 passed／14 skipped／0 failed／0 errors**。14 skipは通常実行でopt-inしていないnative12件とMac専用F_NOCACHE2件で、native12件の成功は別のseccomp scopeとして上記に保存している。これは単一invocationの成功logではなく、全件起動・remaining・campaign・retryのJUnitをnode IDで照合した分割回帰である。中断時にpytestが出すIDのないsynthetic testcaseは集計に含めていない。元のfailure logを削除・上書きしていない。

[最終source hash](retry/source-hashes.txt)を現在のtreeと照合した。source／examplesはnative検証から不変。以降に変更したテストのcoverageは、followup全5件、Phase 3 MPI全3件、tile全16件とPhase 4 MPIの再検証2件で補う。残りのPhase 4 MPIケースは同じvalidatorでより短い120秒以内に成功した。

[manifest](manifest.json)にimage ID、resource条件、各結果とartifact SHA-256を保存する。runnerの`.log`はリポジトリのignore規則に合わせ、保存時に`.txt`へ改名している。ログ／JUnitの行末空白のみを除去したfileは、正規化前後のSHA-256をmanifestへ保存する。再検証を並行した際は各コンテナ4 CPU・8 GiB、最大2コンテナであり、このtest所要時間を単独性能校正に使わない。成功・skip各ケースの合計は3873.15秒で、一括runnerの全件上限は7200秒へ設定した。分割時の3600秒等の上限は元のrunに対応する。

## 受入の境界

実kernel／実MPIのsingle-host correctness検証であり、性能校正profileやcross-host性能の証跡ではない。native errorとfake注入を混ぜない。cancelテストは競合のいずれかの合法結果と元CQEまでのpin保持を検査し、両結果を観測したという意味ではない。NativeUringEngineはsubmissionの致命的なエラーでkernel ownershipが不確かな場合にringを停止し、process終了までpinを保守的に保持する。MPI communicator回復を保証しない。

P4-010はIN_PROGRESSで管理し、全fault／durability matrixを残す。P4-001〜009のすべてのLinux受入条件やPhase 4全体が完了したとは宣言しない。
