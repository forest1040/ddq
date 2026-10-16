"""The cached partial payload tail is reusable only for an unchanged source length."""

from svsim.checkpoint_payload import compact_payload


def test_compact_payload_reuses_unchanged_tail_and_rebuilds_after_append():
    gates = [{'gate': i} for i in range(3)]
    history = [{'event': i} for i in range(2)]
    sources = {'gates': id(gates), 'history': id(history)}
    cache = {}

    first, first_chunks = compact_payload({'gates': gates, 'history': history}, cache, sources)
    assert len(first_chunks) == 2
    second, unchanged_chunks = compact_payload({'gates': gates, 'history': history}, cache, sources)
    assert second['gates_chunks'] == first['gates_chunks']
    assert second['history_chunks'] == first['history_chunks']
    assert unchanged_chunks[first['gates_chunks'][0]['sha256']] is first_chunks[
        first['gates_chunks'][0]['sha256']]
    assert unchanged_chunks[first['history_chunks'][0]['sha256']] is first_chunks[
        first['history_chunks'][0]['sha256']]

    gates.append({'gate': 3})
    history.append({'event': 2})
    appended, changed_chunks = compact_payload({'gates': gates, 'history': history}, cache, sources)
    assert len(changed_chunks) == 2
    assert appended['gates_chunks'][0] != first['gates_chunks'][0]
    assert appended['history_chunks'][0] != first['history_chunks'][0]
