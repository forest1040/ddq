# P4-001 Reproducible Evidence Bundle Report

- **Bundle SHA256**: `17086977b7c62803c138dffc8ac639075a0f79bee14bfc7137e1afa1dbec57c6`
- **Git Revision**: `400d4fd017b8bad137a4fe978aa124362c789932` (clean)
- **Generated At**: `2026-10-15T00:43:43.542176+00:00`
- **Total Files**: 1330
- **Unparsed JSON**: 0

---

## 1. Benchmark Execution Groups (ベンチマーク実行グループ)

Variation: (max−min)/median of per-run medians across the group's workloads (workload size spread, not noise). Trial variation: median over runs of each run's own (max−min)/median trial spread (measurement noise).

| Evidence Dir | Mode | Storage | Backend | Ranks | Nodes | Runs | Completed | Validated | Median (s) | P95 (s) | Worst (s) | Variation | Trial variation |
|---|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `phase5_timing_validation/matrix-linux` | distributed-async | file | buffered | 1 | 1 | 56 | 56 | 56 | 1.041343 | 9.401946 | 47.821117 | 45.7804 | 1.7151 |
| `phase5_timing_validation/matrix-mac` | distributed-async | file | buffered | 1 | 1 | 56 | 56 | 56 | 0.122321 | 0.719307 | 2.349981 | 18.9356 | 0.0480 |
| `phase5_timing_validation/matrix-linux` | distributed-async | file | buffered | 2 | 1 | 52 | 52 | 52 | 1.751038 | 17.386801 | 43.632309 | 24.7993 | 1.1006 |
| `phase5_timing_validation/matrix-linux-batch` | distributed-async | file | buffered | 2 | 1 | 52 | 52 | 52 | 1.031674 | 4.595832 | 9.474519 | 8.9793 | 2.1002 |
| `phase5_timing_validation/matrix-mac` | distributed-async | file | buffered | 2 | 1 | 52 | 52 | 52 | 0.189519 | 0.843350 | 2.766654 | 14.3459 | 0.0158 |
| `phase5_timing_validation/odirect-linux` | distributed-async | file | direct | 1 | 1 | 31 | 31 | 31 | 0.407937 | 6.138474 | 14.369416 | 34.9023 | 0.1021 |
| `phase5_timing_validation/odirect-linux` | distributed-async | file | direct | 2 | 1 | 37 | 37 | 37 | 0.506690 | 1.908575 | 7.257877 | 13.9144 | 0.0647 |
| `phase5_crosshost/run2` | distributed-async | file | direct | 2 | 2 | 22 | 0 | 0 | n/a | n/a | n/a | n/a | n/a |
| `phase5_crosshost/run3` | distributed-async | file | direct | 2 | 2 | 26 | 26 | 26 | 0.188959 | 1.854257 | 4.761051 | 24.9057 | 0.0787 |
| `phase5_timing_validation/eta-linux` | distributed-async | ram | buffered | 1 | 1 | 56 | 56 | 56 | 1.197562 | 6.398711 | 24.901753 | 20.6613 | 1.4279 |
| `phase5_timing_validation/eta-linux-v1` | distributed-async | ram | buffered | 1 | 1 | 56 | 56 | 56 | 0.710411 | 6.875490 | 11.029450 | 15.3125 | 2.8520 |
| `phase5_timing_validation/eta-mac` | distributed-async | ram | buffered | 1 | 1 | 56 | 56 | 56 | 0.091076 | 0.395731 | 1.271942 | 13.5553 | 0.0295 |
| `phase5_timing_validation/matrix-linux-batch` | distributed-async | ram | buffered | 1 | 1 | 56 | 56 | 56 | 1.061945 | 2.410853 | 2.585656 | 2.2817 | 0.7986 |
| `phase5_timing_validation/matrix-linux` | distributed-async | ram | buffered | 2 | 1 | 52 | 52 | 52 | 0.996728 | 6.558172 | 10.512660 | 10.3050 | 2.6942 |
| `phase5_timing_validation/matrix-linux-batch` | distributed-async | ram | buffered | 2 | 1 | 52 | 52 | 52 | 0.446590 | 3.252094 | 7.994267 | 17.3716 | 3.3886 |
| `phase5_timing_validation/matrix-mac` | distributed-async | ram | buffered | 2 | 1 | 52 | 52 | 52 | 0.128405 | 0.442585 | 1.318330 | 9.8561 | 0.0281 |
| `phase5_timing_validation/matrix-linux` | distributed-async | ram | buffered | 4 | 1 | 52 | 52 | 52 | 2.054104 | 7.840070 | 9.718132 | 4.5192 | 2.0143 |
| `phase5_timing_validation/linux` | distributed-sync | ram | buffered | 1 | 1 | 131 | 131 | 131 | 0.933034 | 9.790705 | 39.447327 | 42.1282 | 0.1101 |
| `phase5_timing_validation/mac` | distributed-sync | ram | buffered | 1 | 1 | 56 | 56 | 56 | 0.231870 | 1.386498 | 1.945243 | 8.1528 | 0.0183 |

## 2. Runtime ETA Accuracy (ETA 予測精度)

| Path | Checks | Predicted | Median Abs Rel Err | P95 Abs Rel Err | Worst Abs Rel Err | Interval Coverage |
|---|---:|---:|---:|---:|---:|---:|
| `phase5_crosshost/run3/eta-x2/eta-report.json` | 24 | 24 | 7.8046% | 9.8019% | 10.2799% | 0.96 |
| `phase5_timing_validation/eta-linux-v1/eta-report.json` | 40 | 40 | 19.0422% | 64.9222% | 133.5237% | 0.47 |
| `phase5_timing_validation/eta-linux/eta-report.json` | 40 | 40 | 3.2182% | 37.4860% | 80.1134% | 0.88 |
| `phase5_timing_validation/eta-mac/eta-report.json` | 40 | 40 | 5.7085% | 11.0161% | 28.8296% | 1.00 |
| `phase5_timing_validation/matrix-linux-batch/eta-r1-ram/eta-report.json` | 40 | 40 | 6.7577% | 17.8660% | 21.5614% | 1.00 |
| `phase5_timing_validation/matrix-linux-batch/eta-r2-file/eta-report.json` | 40 | 40 | 22.5432% | 63.8738% | 77.6300% | 1.00 |
| `phase5_timing_validation/matrix-linux-batch/eta-r2-ram/eta-report.json` | 40 | 40 | 18.4948% | 47.8723% | 48.4340% | 1.00 |
| `phase5_timing_validation/matrix-linux/eta-r1-file/eta-report.json` | 40 | 40 | 5.3309% | 56.2232% | 66.5480% | 0.90 |
| `phase5_timing_validation/matrix-linux/eta-r2-file/eta-report.json` | 40 | 40 | 32.7890% | 57.0098% | 234.7070% | 0.53 |
| `phase5_timing_validation/matrix-linux/eta-r2-ram/eta-report.json` | 40 | 40 | 31.3147% | 56.6729% | 185.3528% | 1.00 |
| `phase5_timing_validation/matrix-linux/eta-r4-ram/eta-report.json` | 40 | 40 | 11.1027% | 70.8140% | 157.6564% | 1.00 |
| `phase5_timing_validation/matrix-mac/eta-r1-file/eta-report.json` | 40 | 40 | 1.2413% | 7.9201% | 10.8457% | 1.00 |
| `phase5_timing_validation/matrix-mac/eta-r2-file/eta-report.json` | 40 | 40 | 4.3493% | 8.3382% | 14.2477% | 1.00 |
| `phase5_timing_validation/matrix-mac/eta-r2-ram/eta-report.json` | 40 | 40 | 4.4701% | 19.1720% | 32.2922% | 0.97 |
| `phase5_timing_validation/odirect-linux/eta-r1/eta-report.json` | 40 | 40 | 10.5951% | 47.0528% | 124.4452% | 0.53 |
| `phase5_timing_validation/odirect-linux/eta-r2/eta-report.json` | 40 | 40 | 9.4027% | 54.1674% | 59.3398% | 0.95 |

## 3. Time Models (タイムモデル)

| Path | Model ID | Backend | Storage | Mode | Ranks | Calib Runs | Excluded Unstable | Median Sync Latency (s) |
|---|---|---|---|---|---:|---:|---:|---:|
| `phase5_crosshost/run3/eta-x2/model.json` | `d6f38f39f1b1...` | direct | file | distributed-async | 2 | 22 | 0 | 1.796873e-04 |
| `phase5_timing_validation/eta-linux-v1/model.json` | `7ee8e413588f...` | buffered | ram | distributed-async | 1 | 52 | 0 | 8.406802e-04 |
| `phase5_timing_validation/eta-linux/model.json` | `1a1cd297405b...` | buffered | ram | distributed-async | 1 | 52 | 7 | 1.019869e-03 |
| `phase5_timing_validation/eta-mac/model.json` | `a7d5d9611b0e...` | buffered | ram | distributed-async | 1 | 52 | 0 | 2.797900e-05 |
| `phase5_timing_validation/linux/reduced/model.json` | `15a991c50ec2...` | buffered | ram | distributed-sync | 1 | 27 | 0 | n/a |
| `phase5_timing_validation/linux/v2/model.json` | `5d237b136906...` | buffered | ram | distributed-sync | 1 | 36 | 0 | 8.246114e-04 |
| `phase5_timing_validation/linux/v3-sync-after-lease/model.json` | `1601991c4976...` | buffered | ram | distributed-sync | 1 | 52 | 0 | 8.109823e-04 |
| `phase5_timing_validation/mac/model.json` | `494ab3fd8f80...` | buffered | ram | distributed-sync | 1 | 52 | 0 | 2.411450e-05 |
| `phase5_timing_validation/matrix-linux-batch/eta-r1-ram/model.json` | `9dcd3ae1bdcc...` | buffered | ram | distributed-async | 1 | 52 | 4 | 4.846375e-03 |
| `phase5_timing_validation/matrix-linux-batch/eta-r2-file/model.json` | `f8af1725aa06...` | buffered | file | distributed-async | 2 | 48 | 6 | 1.427278e-03 |
| `phase5_timing_validation/matrix-linux-batch/eta-r2-ram/model.json` | `2cee99ea8937...` | buffered | ram | distributed-async | 2 | 48 | 7 | 1.425151e-03 |
| `phase5_timing_validation/matrix-linux/eta-r1-file/model.json` | `31ea949e466d...` | buffered | file | distributed-async | 1 | 52 | 6 | 8.529170e-04 |
| `phase5_timing_validation/matrix-linux/eta-r2-file/model.json` | `02cfa9e4de1f...` | buffered | file | distributed-async | 2 | 48 | 11 | 1.430616e-03 |
| `phase5_timing_validation/matrix-linux/eta-r2-ram/model.json` | `165705086e1e...` | buffered | ram | distributed-async | 2 | 48 | 13 | 1.429361e-03 |
| `phase5_timing_validation/matrix-linux/eta-r4-ram/model.json` | `d8dd42b4972d...` | buffered | ram | distributed-async | 4 | 48 | 22 | 1.462269e-03 |
| `phase5_timing_validation/matrix-mac/eta-r1-file/model.json` | `1a2186468e14...` | buffered | file | distributed-async | 1 | 52 | 0 | 2.479152e-05 |
| `phase5_timing_validation/matrix-mac/eta-r2-file/model.json` | `52827db6c577...` | buffered | file | distributed-async | 2 | 48 | 0 | 3.506272e-05 |
| `phase5_timing_validation/matrix-mac/eta-r2-ram/model.json` | `229ea0e2fbb3...` | buffered | ram | distributed-async | 2 | 48 | 0 | 3.713550e-05 |
| `phase5_timing_validation/odirect-linux/eta-r1/model.json` | `62b7812d1636...` | direct | file | distributed-async | 1 | 27 | 0 | 7.954969e-04 |
| `phase5_timing_validation/odirect-linux/eta-r2/model.json` | `beaeceb5b7cf...` | direct | file | distributed-async | 2 | 33 | 1 | 8.907436e-04 |

## 4. Comparison Reports & Monitor Overhead (比較レポート / モニターオーバーヘッド)

| Path | Case ID | Variant Rows (Variant: Median Wall s) | Comparisons (Variant: Ratio Median, Diff CI95) |
|---|---|---|---|
| `phase4_adaptive_validation/linux-20ms/p1-missing-baseline.json` | `adaptive-missing-baseline-p1` | fixed: 3.2661s<br>auto: 3.3480s | auto: ratio 1.0411, CI95 [-0.0428, 4.7333] |
| `phase4_adaptive_validation/linux-20ms/p1-recovery.json` | `adaptive-recovery-p1` | fixed: 3.2671s<br>auto: 2.7234s | auto: ratio 1.0095, CI95 [-0.5437, 0.1102] |
| `phase4_adaptive_validation/linux-20ms/p1-sustained.json` | `adaptive-sustained-p1` | fixed: 3.1215s<br>auto: 3.2367s | auto: ratio 1.0227, CI95 [0.0402, 0.2361] |
| `phase4_adaptive_validation/linux-20ms/p1-transient.json` | `adaptive-transient-p1` | fixed: 2.7205s<br>auto: 4.3214s | auto: ratio 0.9884, CI95 [-0.1217, 0.2003] |
| `phase4_adaptive_validation/linux-20ms/p2-sustained.json` | `adaptive-sustained-p2` | fixed: 8.0071s<br>auto: 4.2945s | auto: ratio 0.9875, CI95 [-1.3567, -0.0220] |
| `phase4_adaptive_validation/linux-resource/p1-storage.json` | `adaptive-sustained-storage-p1` | fixed: 6.1464s<br>auto: 2.6592s | auto: ratio 0.4369, CI95 [-3.5509, -3.2705] |
| `phase4_adaptive_validation/linux-resource/p2-mpi.json` | `adaptive-sustained-mpi-p2` | fixed: 8.6386s<br>auto: 8.6470s | auto: ratio 1.0013, CI95 [0.0007, 0.0221] |
| `phase4_adaptive_validation/linux-resource/p2-storage.json` | `adaptive-sustained-storage-p2` | fixed: 8.8249s<br>auto: 8.5687s | auto: ratio 0.5236, CI95 [-8.2252, -0.5421] |
| `phase4_adaptive_validation/linux/p1-missing-baseline.json` | `adaptive-missing-baseline-p1` | fixed: 6.1850s<br>auto: 6.2381s | auto: ratio 1.0049, CI95 [-0.0338, 0.0903] |
| `phase4_adaptive_validation/linux/p1-recovery.json` | `adaptive-recovery-p1` | fixed: 10.0027s<br>auto: 2.1164s | auto: ratio 0.5793, CI95 [-4.1110, 0.1690] |
| `phase4_adaptive_validation/linux/p1-sustained.json` | `adaptive-sustained-p1` | fixed: 10.9157s<br>auto: 2.6546s | auto: ratio 0.3749, CI95 [-7.8998, -4.2513] |
| `phase4_adaptive_validation/linux/p1-transient.json` | `adaptive-transient-p1` | fixed: 10.4947s<br>auto: 10.3932s | auto: ratio 0.9816, CI95 [-5.1702, 0.2059] |
| `phase4_adaptive_validation/linux/p2-sustained.json` | `adaptive-sustained-p2` | fixed: 5.7293s<br>auto: 2.7170s | auto: ratio 0.4691, CI95 [-3.0897, -2.9960] |
| `phase4_adaptive_validation/mac-resource/p1-storage.json` | `adaptive-sustained-storage-p1` | fixed: 3.7660s<br>auto: 1.4484s | auto: ratio 0.3844, CI95 [-2.3307, -2.3046] |
| `phase4_adaptive_validation/mac-resource/p2-mpi.json` | `adaptive-sustained-mpi-p2` | fixed: 6.3714s<br>auto: 6.3764s | auto: ratio 1.0009, CI95 [-0.0104, 0.0106] |
| `phase4_adaptive_validation/mac-resource/p2-storage.json` | `adaptive-sustained-storage-p2` | fixed: 6.2197s<br>auto: 2.3726s | auto: ratio 0.3811, CI95 [-3.8530, -3.8409] |
| `phase4_adaptive_validation/mac/p1-missing-baseline.json` | `adaptive-missing-baseline-p1` | fixed: 1.0478s<br>auto: 1.0437s | auto: ratio 1.0001, CI95 [-0.0060, 0.0329] |
| `phase4_adaptive_validation/mac/p1-recovery.json` | `adaptive-recovery-p1` | fixed: 0.5780s<br>auto: 0.3502s | auto: ratio 0.6058, CI95 [-0.2534, -0.2231] |
| `phase4_adaptive_validation/mac/p1-sustained.json` | `adaptive-sustained-p1` | fixed: 0.9687s<br>auto: 0.4501s | auto: ratio 0.4642, CI95 [-0.5205, -0.5157] |
| `phase4_adaptive_validation/mac/p1-transient.json` | `adaptive-transient-p1` | fixed: 0.4532s<br>auto: 0.4505s | auto: ratio 0.9941, CI95 [-0.0101, 0.0035] |
| `phase4_adaptive_validation/mac/p2-sustained.json` | `adaptive-sustained-p2` | fixed: 1.1033s<br>auto: 0.5488s | auto: ratio 0.4974, CI95 [-0.5607, -0.5458] |
| `phase4_adaptive_validation/mac/p4-sustained.json` | `adaptive-sustained-p4` | fixed: 1.6199s<br>auto: 1.0540s | auto: ratio 0.6498, CI95 [-0.5783, -0.5623] |
| `phase4_adaptive_validation/mac/p4b.json` | `adaptive-sustained-p4` | fixed: 1.6190s<br>auto: 1.0503s | auto: ratio 0.6483, CI95 [-0.6029, -0.5618] |
| `phase4_monitor_overhead/linux/p1.json` | `completion-monitor-p1` | all-off: 0.5158s<br>collector: 0.4953s<br>collector-summary: 0.5244s<br>all-on: 0.4904s | collector: ratio 1.0073, CI95 [-0.0310, 0.0342]<br>collector-summary: ratio 1.0656, CI95 [0.0105, 0.0534]<br>all-on: ratio 1.0170, CI95 [-0.0369, 0.0362] |
| `phase4_monitor_overhead/linux/p2.json` | `completion-monitor-p2` | all-off: 0.6088s<br>collector: 0.6234s<br>collector-summary: 0.6395s<br>all-on: 0.6520s | collector: ratio 1.0080, CI95 [0.0005, 0.0172]<br>collector-summary: ratio 1.0418, CI95 [0.0206, 0.0366]<br>all-on: ratio 1.0400, CI95 [0.0143, 0.0443] |
| `phase4_monitor_overhead/mac/p1.json` | `completion-monitor-p1` | all-off: 0.0936s<br>collector: 0.0978s<br>collector-summary: 0.1006s<br>all-on: 0.1024s | collector: ratio 1.0423, CI95 [0.0033, 0.0048]<br>collector-summary: ratio 1.0746, CI95 [0.0067, 0.0121]<br>all-on: ratio 1.0945, CI95 [0.0076, 0.0096] |
| `phase4_monitor_overhead/mac/p2.json` | `completion-monitor-p2` | all-off: 0.1287s<br>collector: 0.1355s<br>collector-summary: 0.1394s<br>all-on: 0.1432s | collector: ratio 1.0531, CI95 [0.0031, 0.0085]<br>collector-summary: ratio 1.0824, CI95 [0.0102, 0.0108]<br>all-on: ratio 1.1046, CI95 [0.0121, 0.0595] |
| `phase5_timing_validation/linux/reduced/report.json` | `None` | n/a | n/a |
| `phase5_timing_validation/linux/v2/report.json` | `None` | n/a | n/a |
| `phase5_timing_validation/linux/v3-sync-after-lease/report.json` | `None` | n/a | n/a |
| `phase5_timing_validation/mac/report.json` | `None` | n/a | n/a |

## 5. Hardware Profiles (ハードウェアプロファイル)

| Path | Profile ID | Configuration Fingerprint |
|---|---|---|
| `phase5_crosshost/results/eta-x2/probe.json` | `de2f65b46488bc3894d71ec82823b808c432811013ea4813f25a452d3c70151c` | `53fc89a385f70abce9c8db1b2994b9b054c5493b0e1f25ff8556134d2971c756` |
| `phase5_crosshost/run2/eta-x2/probe.json` | `766ff912c4aabb35deb0b2b67771766461e389b416ec1740f95a351427925173` | `8f88696877788e1341008e4c4ca282736302163f737c51521e36e8b85067b8a8` |
| `phase5_crosshost/run3/eta-x2/probe.json` | `fafb656866219a7e0fe9873d20fca79faa949f51a339167d00cb529d12cc9562` | `dfb5c38a862a7a2686459553c90fda3b3897b098d97abad30dd3f5672399a88d` |
| `phase5_mac_calibration/buffered.json` | `3b2f6c1c13180e6a64829e8ec4c857695f9231d94c7390ec093a3b1c5f95681f` | `e458d38526950d54d035cbc8717b23326f24b2916c0137108544fac3430bab8e` |
| `phase5_mac_calibration/direct.json` | `8e612fc001ce355dc778ccce6c1535aa361d29a8f5c0074a55e56194fe6b2c79` | `e458d38526950d54d035cbc8717b23326f24b2916c0137108544fac3430bab8e` |
| `phase5_mac_calibration/probe.json` | `353d098b0f43f76e747d67b881462794370c430584bd94e7f46f4d3c0f95059c` | `e458d38526950d54d035cbc8717b23326f24b2916c0137108544fac3430bab8e` |
| `phase5_timing_validation/eta-linux-v1/probe.json` | `d05a1682ba960c3a1f248f51c1027646bfc3640a502c3bdd0c1ef6231f9a6250` | `500bc671956f8d98f9d2081d2b281cadf30823de1d09174d66e70f533ff0a3a7` |
| `phase5_timing_validation/eta-linux/probe.json` | `8857548799a32d1208200b54181232d38a406c0fe2c137083716c5a4e0ee9180` | `8f565704bacad5a4285129ac05efaf3ac46089c3d32ca308034ecf7d207bdc7e` |
| `phase5_timing_validation/eta-mac/probe.json` | `eeb2f2d7dd244bf4aee772983ebf35aab6b739a787f11f74130e61506e2faf5f` | `4201753410d777af218ab26dc713e0cb5a3c2647c7273b334c720e1e9e406f96` |
| `phase5_timing_validation/linux/growth/probe.json` | `81049aa0a563064f2acbfeb6cc11ba344e56185cdab225536b0ce417cf6c96c6` | `73dd4546c6444b181eaeb80c4f4aa632ec514ada677a921f29696be1fe28e0d7` |
| `phase5_timing_validation/linux/reduced/probe.json` | `71c59ed243e8f037a8716445cef90fe15a808bb334b64c7775bb128ad2dd4777` | `f1c850e7d79a05c1cb398c3cab50f1173b13cdf2766962b7ac00c6ea203c2120` |
| `phase5_timing_validation/linux/sync-probe/probe.json` | `dea36c080af29673cbaefb953f578efce4f16d18352b1ceefa62421550d5726d` | `7e93a6d400603c985e5f92dd6c1d8f2942999f0d10731a730625bfe6300e382d` |
| `phase5_timing_validation/linux/v2/probe.json` | `3befd702bf3dccf2e1bd8264335ee010687975cce9beab888feff4ec745305d1` | `9d45279873e5927a9043a1801d599b0322aba61f70a0b3848c6a2ac1fc72b234` |
| `phase5_timing_validation/linux/v3-sync-after-lease/probe.json` | `e979e15041f95309a413b0e9f54ca9b2de87c4aed4e1cabed186b67517f2d1c7` | `7c4ad3b771f22361fbd85cb1123a2d40d37ecd047bca2259b6319dafc8f06b8c` |
| `phase5_timing_validation/mac/probe.json` | `d06fdc7aeb4c90169b9b306cd84397246b3ea88a52fe217f651d02f72eb704a5` | `f45aa704774a8d3476e32dc832904a81da9999612049263895e12d6368ab2f54` |
| `phase5_timing_validation/matrix-linux-batch/eta-r1-ram/probe.json` | `c05630e085cf7525f60e2b7f99d1382a360233cdde66eaf9a40ef262719d973f` | `a1c5585920bd05130575a0976239cf621753a6800974c1b58fb4fc87315abe3d` |
| `phase5_timing_validation/matrix-linux-batch/eta-r2-file/probe.json` | `2ab7af9c3308d6710a6c458702afb07e272747218928147cc66166a50cf9b5f2` | `c781287b016a5147eb1bc2a191d2c6e499c9186ccb5474d060bb5fc26a3a9991` |
| `phase5_timing_validation/matrix-linux-batch/eta-r2-ram/probe.json` | `94a281b19c7c8c09882720f7b637bcf88d2da3cf2dacff8ca7986b6887b5b7ed` | `548a7173fdf893a40bee574a7e73ecef61ff703b61cf0220eab45e9aadad2f30` |
| `phase5_timing_validation/matrix-linux/eta-r1-file/probe.json` | `33eb9801f05e41614a3a81f7c500db858bc553abc5eaab07b5b4cb73e928b8af` | `7801402d8a4f8a111cdd9de5268b0cc0b940f8c5e0408bba6316e44e0127d4ac` |
| `phase5_timing_validation/matrix-linux/eta-r2-file/probe.json` | `6b9812831040c576c1af38c2927992137dccb284e6b9fcd57517996450937c09` | `31b56a92c19c322ad0ce3d6b743a1e7a47f2a2be56c1418ec59bdd887b6c403b` |
| `phase5_timing_validation/matrix-linux/eta-r2-ram/probe.json` | `0ea54a04111b4801ca033f8d998d0c07516b87b8b6e38c8d57e0e8a671877dae` | `d95b9511d362f06e6f65f4bf5d6bc721a77fa0ff5c1c4316156eb32344688b21` |
| `phase5_timing_validation/matrix-linux/eta-r4-ram/probe.json` | `abc1a53b7e4d2bf85d19cd342a967307f28c90a0575a760ac5c19c2e702f890e` | `66a33862efaeaddb1f86630e25d42aa2e1f6a9332aa87806cf5290316e65c7c4` |
| `phase5_timing_validation/matrix-mac/eta-r1-file/probe.json` | `fa58f04008f5d8e50d7d6d8aaf2598a5dfd201d71f24dbae7ace6dbf50d5a1a6` | `829a3a6a2d631666b8bc37a315d06ec53da41729444c9727c16ccb448aa571df` |
| `phase5_timing_validation/matrix-mac/eta-r2-file/probe.json` | `aa0bbd73e9ce8ee121613a87212febbd30cd6db7b4e0e9ec4fcb8e8ba74ef40c` | `a7557de450be252b2019bc0690ef3ef2bdf242d0d3bb85b4e5783ec5e7b5fccc` |
| `phase5_timing_validation/matrix-mac/eta-r2-ram/probe.json` | `3ea5dfec7999b1679d096b94de663280b02d2dc91387cf02d21c951a9613304c` | `2ccd4d25f1b1813abbbb3a26a1011677e71e564ee7dc7d43a064b65aaad6fd5c` |
| `phase5_timing_validation/odirect-linux/eta-r1/probe.json` | `ba1614bc1e71a33e624869d0c3ac8ba7a876f110c9add906653ee4719b6f5cd6` | `3a8c3645b10c452ae174b43cbd097a92197dda61e10dd40c3803788dc37ca35e` |
| `phase5_timing_validation/odirect-linux/eta-r2/probe.json` | `019d282380c3812650b913ea7366208c75daa217f8d3714f2b815b226f9cda75` | `03a926af1488d5d56c10e028aa3a5a1f84508868b45bf335a4b9639cbd348ccb` |

## 6. Runtime Calibrations (ランタイムキャリブレーション)

| Path | Status | Suite Medians (s) | Quality | Envelope decisions |
|---|---|---|---|---|
| `phase4_foundation_validation/mac-matrix-20261014/p1-cooperative-buffered-q1.json` | complete | kernel-storage: 0.8461s, memory-mpi: n/a, storage-mpi: n/a | finite-diagnostics-only | n/a |
| `phase4_foundation_validation/mac-matrix-20261014/p1-cooperative-buffered-q2.json` | complete | kernel-storage: 0.8311s, memory-mpi: n/a, storage-mpi: n/a | finite-diagnostics-only | n/a |
| `phase4_foundation_validation/mac-matrix-20261014/p1-cooperative-direct-q1.json` | complete | kernel-storage: 0.8844s, memory-mpi: n/a, storage-mpi: n/a | finite-diagnostics-only | n/a |
| `phase4_foundation_validation/mac-matrix-20261014/p1-cooperative-direct-q2.json` | complete | kernel-storage: 0.8790s, memory-mpi: n/a, storage-mpi: n/a | finite-diagnostics-only | n/a |
| `phase4_foundation_validation/mac-matrix-20261014/p1-worker-buffered-q1.json` | complete | kernel-storage: 0.8826s, memory-mpi: n/a, storage-mpi: n/a | finite-diagnostics-only | n/a |
| `phase4_foundation_validation/mac-matrix-20261014/p1-worker-buffered-q2.json` | complete | kernel-storage: 0.8586s, memory-mpi: n/a, storage-mpi: n/a | finite-diagnostics-only | n/a |
| `phase4_foundation_validation/mac-matrix-20261014/p1-worker-direct-q1.json` | complete | kernel-storage: 0.8531s, memory-mpi: n/a, storage-mpi: n/a | finite-diagnostics-only | n/a |
| `phase4_foundation_validation/mac-matrix-20261014/p1-worker-direct-q2.json` | complete | kernel-storage: 0.8637s, memory-mpi: n/a, storage-mpi: n/a | finite-diagnostics-only | n/a |
| `phase4_foundation_validation/mac-matrix-20261014/p2-cooperative-buffered-q1.json` | complete | kernel-storage: 1.3135s, memory-mpi: 1.4538s, storage-mpi: 1.8698s | finite-diagnostics-only | n/a |
| `phase4_foundation_validation/mac-matrix-20261014/p2-cooperative-buffered-q2.json` | complete | kernel-storage: 1.3344s, memory-mpi: 1.4653s, storage-mpi: 1.9056s | finite-diagnostics-only | n/a |
| `phase4_foundation_validation/mac-matrix-20261014/p2-cooperative-direct-q1.json` | complete | kernel-storage: 1.5227s, memory-mpi: 1.4628s, storage-mpi: 2.1742s | finite-diagnostics-only | n/a |
| `phase4_foundation_validation/mac-matrix-20261014/p2-cooperative-direct-q2.json` | complete | kernel-storage: 1.5179s, memory-mpi: 1.4810s, storage-mpi: 2.1840s | finite-diagnostics-only | n/a |
| `phase4_foundation_validation/mac-matrix-20261014/p2-worker-buffered-q1.json` | complete | kernel-storage: 1.3141s, memory-mpi: 1.4590s, storage-mpi: 1.9163s | finite-diagnostics-only | n/a |
| `phase4_foundation_validation/mac-matrix-20261014/p2-worker-buffered-q2.json` | complete | kernel-storage: 1.3427s, memory-mpi: 1.4594s, storage-mpi: 1.8967s | finite-diagnostics-only | n/a |
| `phase4_foundation_validation/mac-matrix-20261014/p2-worker-direct-q1.json` | complete | kernel-storage: 1.2800s, memory-mpi: 1.4688s, storage-mpi: 1.8575s | finite-diagnostics-only | n/a |
| `phase4_foundation_validation/mac-matrix-20261014/p2-worker-direct-q2.json` | complete | kernel-storage: 1.3153s, memory-mpi: 1.4640s, storage-mpi: 1.9315s | finite-diagnostics-only | n/a |
| `phase4_foundation_validation/mac-matrix-20261014/p4-worker-buffered-q1.json` | complete | kernel-storage: 4.3631s, memory-mpi: 5.4195s, storage-mpi: 5.9794s | finite-diagnostics-only | n/a |
| `phase4_foundation_validation/mac-matrix-final-20261014/p1-cooperative-buffered-q1.json` | complete | kernel-storage: 0.2627s, memory-mpi: n/a, storage-mpi: n/a | finite-diagnostics-only | n/a |
| `phase4_foundation_validation/mac-matrix-final-20261014/p1-cooperative-buffered-q2.json` | complete | kernel-storage: 0.2627s, memory-mpi: n/a, storage-mpi: n/a | finite-diagnostics-only | n/a |
| `phase4_foundation_validation/mac-matrix-final-20261014/p1-cooperative-direct-q1.json` | complete | kernel-storage: 0.2601s, memory-mpi: n/a, storage-mpi: n/a | finite-diagnostics-only | n/a |
| `phase4_foundation_validation/mac-matrix-final-20261014/p1-cooperative-direct-q2.json` | complete | kernel-storage: 0.2668s, memory-mpi: n/a, storage-mpi: n/a | finite-diagnostics-only | n/a |
| `phase4_foundation_validation/mac-matrix-final-20261014/p1-worker-buffered-q1.json` | complete | kernel-storage: 0.2576s, memory-mpi: n/a, storage-mpi: n/a | finite-diagnostics-only | n/a |
| `phase4_foundation_validation/mac-matrix-final-20261014/p1-worker-buffered-q2.json` | complete | kernel-storage: 0.2574s, memory-mpi: n/a, storage-mpi: n/a | finite-diagnostics-only | n/a |
| `phase4_foundation_validation/mac-matrix-final-20261014/p1-worker-direct-q1.json` | complete | kernel-storage: 0.2544s, memory-mpi: n/a, storage-mpi: n/a | finite-diagnostics-only | n/a |
| `phase4_foundation_validation/mac-matrix-final-20261014/p1-worker-direct-q2.json` | complete | kernel-storage: 0.2587s, memory-mpi: n/a, storage-mpi: n/a | finite-diagnostics-only | n/a |
| `phase4_foundation_validation/mac-matrix-final-20261014/p2-cooperative-buffered-q1.json` | complete | kernel-storage: 0.4925s, memory-mpi: 0.5521s, storage-mpi: 0.6628s | finite-diagnostics-only | n/a |
| `phase4_foundation_validation/mac-matrix-final-20261014/p2-cooperative-buffered-q2.json` | complete | kernel-storage: 0.4778s, memory-mpi: 0.5675s, storage-mpi: 0.6662s | finite-diagnostics-only | n/a |
| `phase4_foundation_validation/mac-matrix-final-20261014/p2-cooperative-direct-q1.json` | complete | kernel-storage: 0.5406s, memory-mpi: 0.5557s, storage-mpi: 0.7484s | finite-diagnostics-only | n/a |
| `phase4_foundation_validation/mac-matrix-final-20261014/p2-cooperative-direct-q2.json` | complete | kernel-storage: 0.5271s, memory-mpi: 0.5500s, storage-mpi: 0.7368s | finite-diagnostics-only | n/a |
| `phase4_foundation_validation/mac-matrix-final-20261014/p2-worker-buffered-q1.json` | complete | kernel-storage: 0.4795s, memory-mpi: 0.5573s, storage-mpi: 0.6808s | finite-diagnostics-only | n/a |
| `phase4_foundation_validation/mac-matrix-final-20261014/p2-worker-buffered-q2.json` | complete | kernel-storage: 0.4937s, memory-mpi: 0.5607s, storage-mpi: 0.6693s | finite-diagnostics-only | n/a |
| `phase4_foundation_validation/mac-matrix-final-20261014/p2-worker-direct-q1.json` | complete | kernel-storage: 0.4703s, memory-mpi: 0.5712s, storage-mpi: 0.6518s | finite-diagnostics-only | n/a |
| `phase4_foundation_validation/mac-matrix-final-20261014/p2-worker-direct-q2.json` | complete | kernel-storage: 0.4823s, memory-mpi: 0.5569s, storage-mpi: 0.6870s | finite-diagnostics-only | n/a |
| `phase4_foundation_validation/mac-matrix-final-20261014/p4-cooperative-buffered-q1.json` | complete | kernel-storage: 1.6729s, memory-mpi: 2.1762s, storage-mpi: 2.3035s | finite-diagnostics-only | n/a |
| `phase4_foundation_validation/mac-matrix-final-20261014/p4-cooperative-buffered-q2.json` | complete | kernel-storage: 1.6759s, memory-mpi: 2.1956s, storage-mpi: 2.2411s | finite-diagnostics-only | n/a |
| `phase4_foundation_validation/mac-matrix-final-20261014/p4-cooperative-direct-q1.json` | complete | kernel-storage: 1.7164s, memory-mpi: 2.1988s, storage-mpi: 2.3696s | finite-diagnostics-only | n/a |
| `phase4_foundation_validation/mac-matrix-final-20261014/p4-cooperative-direct-q2.json` | complete | kernel-storage: 1.6934s, memory-mpi: 2.2186s, storage-mpi: 2.3312s | finite-diagnostics-only | n/a |
| `phase4_foundation_validation/mac-matrix-final-20261014/p4-worker-buffered-q1.json` | complete | kernel-storage: 1.6849s, memory-mpi: 2.1844s, storage-mpi: 2.2781s | finite-diagnostics-only | n/a |
| `phase4_foundation_validation/mac-matrix-final-20261014/p4-worker-buffered-q2.json` | complete | kernel-storage: 1.6931s, memory-mpi: 2.2055s, storage-mpi: 2.2321s | finite-diagnostics-only | n/a |
| `phase4_foundation_validation/mac-matrix-final-20261014/p4-worker-direct-q1.json` | complete | kernel-storage: 1.6744s, memory-mpi: 2.2123s, storage-mpi: 2.2882s | finite-diagnostics-only | n/a |
| `phase4_foundation_validation/mac-matrix-final-20261014/p4-worker-direct-q2.json` | complete | kernel-storage: 1.6902s, memory-mpi: 2.1871s, storage-mpi: 2.2971s | finite-diagnostics-only | n/a |
| `phase4_foundation_validation/p1-cooperative-buffered.json` | complete | kernel-storage: 0.2641s | finite-diagnostics-only | n/a |
| `phase4_foundation_validation/p1-cooperative-direct.json` | complete | kernel-storage: 0.2654s | finite-diagnostics-only | n/a |
| `phase4_foundation_validation/p1-worker-buffered.json` | complete | kernel-storage: 0.2633s | finite-diagnostics-only | n/a |
| `phase4_foundation_validation/p1-worker-direct.json` | complete | kernel-storage: 0.2627s | finite-diagnostics-only | n/a |
| `phase5_crosshost/run2/runtime-x2.json` | failed | kernel-storage: n/a, memory-mpi: n/a, storage-mpi: n/a | finite-diagnostics-only | n/a |
| `phase5_crosshost/run3/runtime-x2.json` | partial | kernel-storage: 1.1091s, memory-mpi: 0.4988s, storage-mpi: 1.2988s | finite-diagnostics-only | n/a |
| `phase5_timing_validation/odirect-linux/runtime-cooperative-p1.json` | partial | kernel-storage: 1.7185s, memory-mpi: n/a, storage-mpi: n/a | finite-diagnostics-only | n/a |
| `phase5_timing_validation/odirect-linux/runtime-cooperative-p2.json` | partial | kernel-storage: 1.6562s, memory-mpi: 0.6122s, storage-mpi: 2.0005s | finite-diagnostics-only | n/a |
| `phase5_timing_validation/odirect-linux/runtime-worker-p1.json` | partial | kernel-storage: 7.0836s, memory-mpi: n/a, storage-mpi: n/a | finite-diagnostics-only | n/a |
| `phase5_timing_validation/odirect-linux/runtime-worker-p2.json` | partial | kernel-storage: 6.5252s, memory-mpi: 0.6224s, storage-mpi: 5.1642s | finite-diagnostics-only | n/a |
| `phase5_timing_validation/runtime-envelope/runtime-cooperative-p1.json` | complete | kernel-storage: 1.7470s, memory-mpi: n/a, storage-mpi: n/a | held-out-envelope | kernel-storage: CERTIFIED, memory-mpi: NOT_APPLICABLE_SINGLE_RANK, storage-mpi: NOT_APPLICABLE_SINGLE_RANK |
| `phase5_timing_validation/runtime-envelope/runtime-cooperative-p2.json` | complete | kernel-storage: 6.6185s, memory-mpi: 2.1533s, storage-mpi: 1.9435s | held-out-envelope | kernel-storage: CERTIFIED, memory-mpi: LOW_HELD_OUT_COVERAGE:0.20, storage-mpi: CERTIFIED |
| `phase5_timing_validation/runtime-envelope/runtime-worker-p1.json` | complete | kernel-storage: 1.7751s, memory-mpi: n/a, storage-mpi: n/a | held-out-envelope | kernel-storage: CERTIFIED, memory-mpi: NOT_APPLICABLE_SINGLE_RANK, storage-mpi: NOT_APPLICABLE_SINGLE_RANK |
| `phase5_timing_validation/runtime-envelope/runtime-worker-p2.json` | complete | kernel-storage: 1.6204s, memory-mpi: 0.6097s, storage-mpi: 1.9545s | held-out-envelope | kernel-storage: CERTIFIED, memory-mpi: CERTIFIED, storage-mpi: CERTIFIED |
