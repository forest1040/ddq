# Linux / macOS におけるファイルキャッシュを抑制した I/O

## 概要

Linux の `O_DIRECT` は、可能な範囲でページキャッシュをバイパスするための `open()` フラグです。macOS には同じ `O_DIRECT` フラグはありませんが、`fcntl(fd, F_NOCACHE, 1)` でファイルデータのキャッシュを抑制できます。

**注意:** `F_NOCACHE` は `O_DIRECT` と完全に等価ではありません。とくに既存キャッシュの利用、I/O 制約、実効性能はファイルシステムや OS によって異なります。どちらもデバイスへの書き込み永続化を保証する API ではありません。

## 比較

| 項目 | Linux | macOS |
|---|---|---|
| 主な API | `open(path, O_RDONLY \| O_DIRECT)` | `open()` 後に `fcntl(fd, F_NOCACHE, 1)` |
| 目的 | ページキャッシュを可能な限りバイパス | ファイルデータのキャッシュを抑制 |
| I/O アラインメント | ファイルシステム等に応じて制約あり | Linux `O_DIRECT` と同じ制約を前提にしない |
| 既存のページキャッシュ | 通常のバッファ I/O とは異なる経路 | 既存キャッシュから読み込まれる場合がある |
| 先読み制御 | `posix_fadvise()` など | `fcntl(fd, F_RDAHEAD, 0)` |
| 永続化 | `fsync()` 等は別途必要 | `fsync()`、必要なら `F_FULLFSYNC` を検討 |

## macOS の C 実装例

```c
#include <fcntl.h>
#include <unistd.h>
#include <stdio.h>

int main(void) {
    int fd = open("data.bin", O_RDONLY);
    if (fd < 0) {
        perror("open");
        return 1;
    }

    if (fcntl(fd, F_NOCACHE, 1) == -1) {
        perror("fcntl(F_NOCACHE)");
        close(fd);
        return 1;
    }

    /* ランダムアクセス主体で先読みが不要な場合のみ検討 */
    /* fcntl(fd, F_RDAHEAD, 0); */

    char buffer[4096];
    ssize_t n = read(fd, buffer, sizeof(buffer));
    if (n < 0) perror("read");

    close(fd);
    return n < 0 ? 1 : 0;
}
```

実行例:

```bash
cc -O2 -Wall -Wextra direct_io_macos.c -o direct_io_macos
./direct_io_macos
```

`F_RDAHEAD` の無効化は必須ではありません。連続アクセスなら先読みが有効なことがあります。

## Linux の C 実装例

```c
#define _GNU_SOURCE
#include <fcntl.h>
#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>

int main(void) {
    int fd = open("data.bin", O_RDONLY | O_DIRECT);
    if (fd < 0) {
        perror("open");
        return 1;
    }

    void *buf = NULL;
    /* 4096 は例。実際の要件は対象ファイルシステム等で確認する。 */
    if (posix_memalign(&buf, 4096, 4096) != 0) {
        close(fd);
        return 1;
    }
    ssize_t n = read(fd, buf, 4096);
    if (n < 0) perror("read");

    free(buf);
    close(fd);
    return n < 0 ? 1 : 0;
}
```

Linux ではバッファのアドレス・読み書きサイズ・ファイルオフセットのアラインメント要件が重要です。利用可能なら `statx(..., STATX_DIOALIGN, ...)` で確認します（対応状況はファイルシステム等によります）。

## 大規模量子回路シミュレータ向けの設計

メモリと SSD/NVMe を併用するブロック型状態ベクトルシミュレータでは、OS のページキャッシュとは別に、アプリケーションでブロック再利用を管理する設計が有力です。

```text
Quantum Circuit Executor
          |
    I/O Scheduler
          |
Application Block Cache (RAM)
          |
   Platform I/O Backend
       /         \
Linux: O_DIRECT  macOS: F_NOCACHE
       \         /
          SSD/NVMe
```

### 推奨方針

1. **再利用頻度が高いブロック**: アプリケーション側 RAM キャッシュに保持する。
2. **再利用頻度が低い大容量ストリーミング I/O**: Linux の `O_DIRECT`、macOS の `F_NOCACHE` を候補にする。
3. **先読み・非同期処理**: スケジューラのアクセス予測に基づき、I/O と計算をオーバーラップさせる。
4. **ブロック配置**: ゲート操作が局所的になるよう、ブロック交換や SSD 読み出しを最小化する。
5. **選択式バックエンド**: `BufferedIO` / `DirectOrNoCacheIO` / `MmapIO` を切り替え、プラットフォームごとにベンチマークする。
6. **注意点**: `mmap` は通常ページキャッシュと関係するため、直接 I/O と同じ性質ではない。`F_NOCACHE` もキャッシュの完全排除は保証しない。

### ベンチマーク指標

- ブロックサイズ別の逐次・ランダム読み書きスループット
- IOPS、p50/p95/p99 レイテンシ
- RSS、OS のファイルキャッシュ使用量、メモリ圧迫
- アプリケーション側キャッシュヒット率
- 計算と I/O の重なり率、総実行時間
- コールドキャッシュとウォームキャッシュの差

## 結論

macOS では Linux の `O_DIRECT` をそのまま使用できませんが、`F_NOCACHE` によるファイルキャッシュ抑制が可能です。マルチノード・メモリ・SSD を統合した量子回路シミュレータでは、OS 別の I/O バックエンドを分離し、ブロック管理は共通のスケジューラで行う構造を推奨します。
