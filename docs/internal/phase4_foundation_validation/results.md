# 最終Mac matrixの診断結果

全24構成・336 trial（うちmeasured 280）でstate／norm検証を通過し、collector LOSSはゼロ。P=1のMPI 2 suiteはnot-applicable。全caseのbuild source hashは一致する。全trialの最大norm誤差は 2.22e-16。

n=4、b=2。時間はexecute区間のrank MAXの中央値（ms）、peakは各caseで測った最大managed RAM（MiB）。小状態ではmetadata・予約・監視費用が支配的なので、大状態／量子化学へ外挿しない。raw extremaは予測区間ではない。

| P | engine | FILE backend | queue上限 | kernel＋SSD ms | RAM＋MPI ms | SSD＋MPI ms | managed peak MiB |
|---|---|---|---|---|---|---|---|
| 1 | cooperative | buffered | 1 | 262.75 | N/A | N/A | 1.548 |
| 1 | cooperative | buffered | 2 | 262.65 | N/A | N/A | 1.548 |
| 1 | cooperative | F_NOCACHE | 1 | 260.14 | N/A | N/A | 1.548 |
| 1 | cooperative | F_NOCACHE | 2 | 266.76 | N/A | N/A | 1.548 |
| 1 | worker | buffered | 1 | 257.64 | N/A | N/A | 1.548 |
| 1 | worker | buffered | 2 | 257.44 | N/A | N/A | 1.548 |
| 1 | worker | F_NOCACHE | 1 | 254.37 | N/A | N/A | 1.548 |
| 1 | worker | F_NOCACHE | 2 | 258.71 | N/A | N/A | 1.548 |
| 2 | cooperative | buffered | 1 | 492.48 | 552.13 | 662.83 | 1.455 |
| 2 | cooperative | buffered | 2 | 477.82 | 567.50 | 666.23 | 1.456 |
| 2 | cooperative | F_NOCACHE | 1 | 540.60 | 555.74 | 748.43 | 1.455 |
| 2 | cooperative | F_NOCACHE | 2 | 527.05 | 550.02 | 736.84 | 1.456 |
| 2 | worker | buffered | 1 | 479.51 | 557.31 | 680.78 | 1.455 |
| 2 | worker | buffered | 2 | 493.69 | 560.71 | 669.33 | 1.456 |
| 2 | worker | F_NOCACHE | 1 | 470.33 | 571.22 | 651.82 | 1.455 |
| 2 | worker | F_NOCACHE | 2 | 482.26 | 556.93 | 687.03 | 1.456 |
| 4 | cooperative | buffered | 1 | 1672.93 | 2176.17 | 2303.45 | 1.346 |
| 4 | cooperative | buffered | 2 | 1675.90 | 2195.56 | 2241.07 | 1.346 |
| 4 | cooperative | F_NOCACHE | 1 | 1716.42 | 2198.79 | 2369.63 | 1.346 |
| 4 | cooperative | F_NOCACHE | 2 | 1693.40 | 2218.56 | 2331.19 | 1.346 |
| 4 | worker | buffered | 1 | 1684.89 | 2184.39 | 2278.13 | 1.346 |
| 4 | worker | buffered | 2 | 1693.13 | 2205.52 | 2232.14 | 1.346 |
| 4 | worker | F_NOCACHE | 1 | 1674.44 | 2212.31 | 2288.17 | 1.346 |
| 4 | worker | F_NOCACHE | 2 | 1690.20 | 2187.10 | 2297.11 | 1.346 |

再現commandと資源は [README](README.md)。rawは [matrix manifest](mac-matrix-final-20261014/matrix-run.json) と各case JSONを参照する。数値検証後に追加したdrain flag／journal cursorの拒否は回帰テストで別途確認する。記録したbuildを現在のtreeや別allocationへ自動一致させない。
