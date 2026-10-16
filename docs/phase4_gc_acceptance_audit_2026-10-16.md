# P4-009 GC受入監査と実装補修

2026-10-16。writer／recovery matrixの監査中に、offline branch GCがcommit済みの物理extentを削除し得る実装穴を確認した。bootstrap、state export、cluster受入は別担当であり、この補修をP4-009全体のDONEとは扱わない。

## 実装穴と補修

- segment番号を`name[8:14]`で切っていた。通常のfilename format `:06d`は最小幅であり、番号1,000,000以上も合法である。旧実装はlive `segment-1000000.bin`を100000と誤認し、removableへ入れてapplyでも削除していた。番号全体をparseし、正規formatと一致する場合だけ扱うようにした。
- manifestをraw textのhex正規表現だけで検査し、JSON escapeされたroot UUIDを見逃していた。bounded regular fileをchecked readし、JSONの文字列値／keyへdecodeした後にroot参照を保護する。読めない／壊れたmanifestは推測で除外しない。
- global manifestsがinspection limitを超えると先頭8192件だけを使ってlivenessを判断していた。上限に達したら全体を拒否し、未知の残りをdeadと扱わない。alias、別volume、非排他的fileも拒否する。
- branch GCの削除候補はsizeだけで再確認していた。planにdevice／inode／birth identityを保存し、catalog変更前とunlink直前に照合する。birth identityを取得できない場合や、同じsizeの別fileへ置換された場合は削除しない。

## 回帰とoracle

`tests/test_uncommitted_gc.py`は実`FileBlockStore`のallocatorを1,000,000から開始して3個だけの実extentを作る。live 1個とdead 2個を明確に分け、GC後もlive segmentのhashとchecksum付きread／numpy値が一致することを検証する。補修前はこのlive segmentがremovableに入り、回帰が失敗することを実測した。

JSON escapeされたmanifest参照、global manifest上限超過、alias／壊れたJSON／非正規segment名での無変更拒否、catalog open直後の同内容file置換を追加した。既存のdry run／plan digest／lease／quota満杯temporary GC／二重適用拒否を含め、GC 17件が成功した。`SVSIM_TEST_BACKEND=direct`でも同じ実file oracleを使える。
