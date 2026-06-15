"""Advisory LLM-judge tests — the only network module. No network here:
we test the pure parsing/degradation paths and the no-endpoint short-circuit."""

from agents.pge_eval import judge


def test_parse_axis_valid_json():
    out = judge._parse_axis('{"score": 0.8, "flags": ["redundant class"]}')
    assert out["score"] == 0.8
    assert out["flags"] == ["redundant class"]


def test_parse_axis_embedded_in_prose():
    out = judge._parse_axis('Here is my verdict: {"score": 1.0, "flags": []} done')
    assert out["score"] == 1.0
    assert out["flags"] == []


def test_parse_axis_malformed_degrades():
    out = judge._parse_axis("not json at all")
    assert out["score"] is None
    assert out["flags"]  # carries a parse-failure flag


def test_parse_axis_null_score():
    out = judge._parse_axis('{"score": null, "flags": ["x"]}')
    assert out["score"] is None
    assert out["flags"] == ["x"]


def test_empty_axis():
    a = judge._empty_axis("no endpoint")
    assert a["score"] is None and a["flags"] == ["no endpoint"]


def test_run_judge_no_endpoint_is_offline(monkeypatch):
    # No endpoint -> short-circuit, never touches the network.
    import agents.engine_base as eb

    monkeypatch.setattr(
        eb, "call_serving_endpoint",
        lambda *a, **k: (_ for _ in ()).throw(AssertionError("network call")),
    )
    out = judge.run_judge(
        host="", token="", endpoint_name="", ontology={"classes": []}, artifact={}
    )
    assert out["ontology"]["score"] is None
    assert out["mapping"]["score"] is None


def test_run_judge_failopen_when_endpoint_errors(monkeypatch):
    # An endpoint that errors must degrade to empty axes, never raise.
    import agents.engine_base as eb

    def _boom(*a, **k):
        raise RuntimeError("503 model overloaded")

    monkeypatch.setattr(eb, "call_serving_endpoint", _boom)
    out = judge.run_judge(
        host="h", token="t", endpoint_name="ep",
        ontology={"classes": [{"name": "A", "dataProperties": []}]},
        artifact={"mapping_run_log": []},
    )
    assert out["ontology"]["score"] is None
    assert out["mapping"]["score"] is None
