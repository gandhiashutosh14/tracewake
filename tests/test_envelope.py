import pytest

from tracewake.envelope import TraceEnvelope, digest_envelopes, validate

EVENT = {"seq": 1, "ts": "2026-09-19T00:00:00+00:00", "run_id": "r1", "type": "run_started", "data": {"objective": "x"}}


def test_from_event_and_json_round_trip():
    env = TraceEnvelope.from_event(EVENT, policy_id="abc123")
    assert env.id == "r1:1"
    assert env.key() == b"r1"
    back = TraceEnvelope.from_json(env.to_json())
    assert back == env
    assert back.policy_id == "abc123"


def test_from_event_accepts_objects_with_attributes():
    class E:
        seq, ts, run_id, type, data = 2, "t", "r", "note", {"a": 1}

    env = TraceEnvelope.from_event(E())
    assert (env.seq, env.type, env.data) == (2, "note", {"a": 1})


def test_validate_reports_every_problem():
    problems = validate({"run_id": "", "seq": 0, "ts": "", "type": "nope", "data": []})
    text = " ".join(problems)
    for needle in ("run_id", "seq", "ts", "data", "unknown orchestrator event type"):
        assert needle in text


def test_unknown_type_is_only_an_error_for_the_orchestrator_producer():
    base = {"run_id": "r", "seq": 1, "ts": "t", "type": "custom", "data": {}}
    assert validate({**base, "producer": "other-agent-runtime"}) == []
    assert validate(base)  # default producer is the orchestrator, whose vocabulary is closed


def test_unsupported_envelope_version_is_rejected():
    env = TraceEnvelope.from_event(EVENT)
    d = env.to_dict()
    d["envelope_version"] = "2"
    with pytest.raises(ValueError):
        TraceEnvelope.from_json(__import__("json").dumps(d).encode())


def test_invalid_event_raises():
    with pytest.raises(ValueError):
        TraceEnvelope.from_event({**EVENT, "seq": 0})


def test_digest_depends_on_content_not_key_order():
    a = TraceEnvelope.from_event({**EVENT, "data": {"x": 1, "y": 2}})
    b = TraceEnvelope.from_event({**EVENT, "data": {"y": 2, "x": 1}})
    c = TraceEnvelope.from_event({**EVENT, "data": {"x": 1, "y": 3}})
    assert a.digest() == b.digest()
    assert a.digest() != c.digest()


def test_run_digest_is_order_independent_by_seq():
    envs = [TraceEnvelope.from_event({**EVENT, "seq": i, "type": "note"}) for i in range(1, 6)]
    assert digest_envelopes(envs) == digest_envelopes(list(reversed(envs)))
    assert digest_envelopes(envs) != digest_envelopes(envs[:-1])
