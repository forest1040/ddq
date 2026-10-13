# Phase 5 基盤のMac計測とstate-free検証

測定日：2026-10-13。Apple M3 Ultra、RAM 512 GiB、32 core（performance 24／efficiency 8）。thread割当ラベル1、OMP／OpenBLAS／VECLIB上限1、core bindingなし。温度・clock・PSSは未取得。Mac単独の小規模検証であり、クラスタは使用していない。

## 有限校正cell

block_bits=12、complex128 payload=64 KiB、warmup1、measured5。RAM管理予算128 MiB、scratch disk8 MiB、cooperative wall20秒。bufferedとdirectを順に独立campaignで実行し、各10 cellの全trialが成功、scratchを回収した。

| operation | buffered campaign median | direct campaign median |
|---|---:|---:|
| RAM copy 64 KiB | 1.875 µs | 1.791 µs |
| local H target0 | 2.552 ms | 2.588 ms |
| local RX target0、angle0.37 | 2.573 ms | 2.605 ms |
| local Pauli rotation Y/X/Z、angle0.37 | 1.268 ms | 1.286 ms |
| compensated Pauli inner、x=3,z=5 | 2.403 ms | 2.425 ms |
| compensated Pauli inner、x=5,z=5 | 2.396 ms | 2.452 ms |
| single-slot read 64 KiB | 3.417 µs | 52.792 µs |
| single-slot write 64 KiB | 4.125 µs | 54.792 µs |
| dirty-write後のos.fsync | 32.584 µs | 1.791 µs |
| 同main thread H（6Q）＋write＋read | 55.375 µs | 148.375 µs |

directのresolved backendは`macos-f-nocache`。kernel/RAMは両campaignとも同じRAM経路で、storage列だけbackendが異なる。read/writeは同じ64 KiB slotを再利用する有限probeであり、SSD全体の持続帯域や大規模cold-cache性能へ適用しない。少数sampleのp95は診断値。mixedは直列のjoint wallで、非同期overlapの利益を示す数値ではない。

raw trialと集計は[buffered profile](internal/phase5_mac_calibration/buffered.json)、[direct profile](internal/phase5_mac_calibration/direct.json)、対応する[buffered run manifest](internal/phase5_mac_calibration/buffered.json.run.json)／[direct run manifest](internal/phase5_mac_calibration/direct.json.run.json)に保存した。revision／dirty source／uv.lockと出力SHA-256により計測時のコード・ファイルを識別する。詳細pathや構成を含むため内部資料として管理する。

## 静的plan

16Q RAM reference（H、RX、Pauli rotation各1回、b=12）の状態は1 MiB、managed RAM referenceは8,511,488 byte。host RAMはlibrary/OS余裕込み612,491,264 byte。指定rank128 MiB／host1 GiB／disk128 MiBに対してcapacity=true、execution support=true。kernel component referenceは約0.1023秒だが、総時間はmetadata等の未校正によりnullである。[plan](internal/phase5_mac_calibration/local.plan.json)で内訳・assumption・coverageを確認できる。

| dry-run | 状態payload | semantic gates | 解析Python peak | 容量（今回の予算） | 対応route | 総時間 |
|---|---:|---:|---:|---|---|---|
| 40Q | 16 TiB | 2 | 16,782,700 byte | false | true | unknown |
| 50Q | 16 PiB | 2兆 | 16,782,652 byte | false | false：巨大repeat executor未実装 | unknown |

peakはimports後からtracemallocで観測したPython allocationで、process RSSではない。入力のbounded read bufferを含む。どちらも振幅確保・Block列挙・repeat展開なしで終了した。40Qのroute対応はGateOp経路の存在であり、与えたdisk8 GiB等で実行できるという判定ではない。

結果は[40Q plan](internal/phase5_mac_calibration/40q.plan.json)、[50Q plan](internal/phase5_mac_calibration/50q.plan.json)、[dry-run metrics](internal/phase5_mac_calibration/dry_run_metrics.json)を参照。50Q workloadを[例](examples/phase5_50q_workload.json)として公開した。これらは一般回路の解析検証であり、QPE／VQEの量子化学実測ではない。既存スライドのQulacs／化学アルゴリズム見積もりをこのkernel probeで置換していない。
