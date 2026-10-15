# P4-004：layout migration候補と事前校正profile（設計）

作成日：2026-10-15。利用者の判断（案A）：「layout移行の費用は、held-out検証済みの事前校正profileから取る。Phase 5時間modelは診断のままにする」。P4-004の残件「rank bitを局所化するlayout migration候補と、その移行費用の実測」を対象にする。

## 問題

MPI劣化の下では、S2などpolicy候補は呼出し数を変えないので利益が無い（`INSUFFICIENT_GAIN`）。利益が出るのは、多く使われる論理qubitをrank bitからlocal bitへ移すlayout移行である。移行後のexecutor classは、そのrunではまだ一度も実行されていないので、rolling実測では価格付けできない。さらに、注入中のrowはinjectedとして記録され、rolling costには入らない。

## 構成

| 部品 | 内容 |
|---|---|
| executor class profile | 事前の校正runで、各layoutの回路を実行し、rolling認証（burn-in 3、8件以上、held-out coverage 0.8以上、drift判定）を通ったexecutor classだけを書き出す。物理swapの`physical-bit-swap` stageも同じ規則で認証する。行はclass key（op_id除去）、物理context（n、block_bits、backend、engine、ranks、threads）、lower／expected／upper、held-out件数とcoverageを持つ。文書はconfiguration fingerprintとsha256のIDを持つ |
| `RuntimeControl.attach_executor_profile` | fingerprintを現在のhost／buildのprobeと照合し、kind `executor-class-seconds`をprovenance measuredで登録する。Binding、policy epoch、plan versionは照合しない（物理費用を決めないため）。layout、policy、engine、角度はclass keyで一致させる |
| 候補 `layout_swap` | 2 rank以上のとき、残り（endpointまで）のgateが最も多く使う論理qubitのうちrank bitにあるものと、最も使われないlocal bitの組を最大2つ作る。分類はFRONTIER_REPLANである |
| 価格付け | continue下限＝既存の式（gate数×service下限＋現在の呼出し数×超過下限）。候補上限＝候補layoutでの残りexecutor class（正確なepoch形状）×profileのupper＋超過上限×（候補で超過がかかる呼出し数）＋移行（profileの`physical-bit-swap` upper）＋checkpoint＋planning＋drain。publicationと境界の回数は両者で等しいので両側から除く |
| 超過の帰属 | 劣化evidenceが`resource='mpi'`と帰属できる場合だけ、超過をrank bitに触れる呼出しに限る。帰属できなければ全呼出しに掛ける（保守的） |
| 適用 | `apply_candidate`がlayout_swapを受けると、drain→全rank合意→`physical_index_bit_swap`→`plan_version`を進める→durable snapshot。失敗時は既存の段階別faultと同じく旧cut |
| VERIFY | 候補classのprofile upperの最大を呼出しあたりのservice上限にし、超過は上の帰属規則で付ける |

**unknownになる場合。** 次のいずれかがあれば、候補はunknown（`UNKNOWN_COST_COMPONENT`）で採用しない。
- profileに無いclass、またはswap。
- 劣化evidenceが無い。
- 残りの走査が上限（4096 source）を超える。
- fingerprintが一致しない。

## 受入

- **単体。** 候補の生成、classの数え方、profileの照合（fingerprint違いは拒否）、unknownの各理由、gainの計算。
- **実MPI（2 rank）。** MPI注入下で、事前profileがあればlayout_swapを自動適用してVERIFYに成功し、paired差のCI95上限が0未満になること。profileが無い、または残りが短いときは切り替えないこと。適用後の状態はoracleと一致すること。
- **Mac・ent。** paired campaignで確認する。

時間modelは使わない。profileに無いclassを推定で埋めることもしない。
