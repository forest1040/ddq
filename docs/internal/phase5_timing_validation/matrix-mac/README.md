# Mac ETA matrix

`run_matrix.sh`で実行した。async executor、b=4、各条件で校正suite×3回、held-out 4 workload×5回×2点。4 rank／RAMは同一Mac上の4 process×（main＋io worker 2）でMPI progressのbusy pollが競合した。校正が40分で4 roundの2 round目にとどまり、wall上限1200秒を超えたため手動で停止した。部分runからはmodelを作らない設計なので、この条件は未実施である。
