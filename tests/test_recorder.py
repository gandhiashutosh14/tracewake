import json

import pytest

from tracewake.bus import MemoryBus
from tracewake.envelope import TraceEnvelope
from tracewake.recorder import Recorder, load_trace_dir, read_trace_file


class FakeTrace:
    """The shape of the orchestrator's DecisionTrace: subscribe(fn) and emit(type, **data)."""

    def __init__(self, run_id):
        self.run_id, self.seq, self.listeners = run_id, 0, []

    def subscribe(self, fn):
        self.listeners.append(fn)

    def emit(self, type, **data):
        self.seq += 1
        ev = {"seq": self.seq, "ts": "2026-09-19T00:00:00+00:00", "run_id": self.run_id, "type": type, "data": data}
        for fn in self.listeners:
            fn(ev)
        return ev


def test_recorder_publishes_every_emitted_event():
    bus = MemoryBus()
    rec = Recorder(bus, "agent.decisions", policy_id="p1")
    trace = FakeTrace("run-x")
    trace.subscribe(rec)
    trace.emit("run_started", objective="o")
    trace.emit("run_finished", completed=[])
    assert rec.published == 2
    envs = [TraceEnvelope.from_json(m.value) for m in bus.consume("agent.decisions")]
    assert [e.seq for e in envs] == [1, 2]
    assert {e.policy_id for e in envs} == {"p1"}
    assert {m.key for m in bus.consume("agent.decisions")} == {b"run-x"}


def test_recorder_works_with_the_real_decision_trace():
    trace_mod = pytest.importorskip("orchestrator.trace")
    bus = MemoryBus()
    rec = Recorder(bus, "t")
    trace = trace_mod.DecisionTrace("run-real")
    trace.subscribe(rec)
    trace.emit("run_started", objective="o")
    trace.emit("note", text="hello")
    assert rec.published == 2
    assert TraceEnvelope.from_json(bus.consume("t")[-1].value).type == "note"


def test_recorder_rejects_an_invalid_event():
    rec = Recorder(MemoryBus(), "t")
    with pytest.raises(ValueError):
        rec({"seq": 0, "ts": "t", "run_id": "r", "type": "note", "data": {}})


def test_publish_file_and_load_trace_dir(tmp_path):
    lines = [{"seq": 2, "ts": "t", "run_id": "r9", "type": "run_finished", "data": {}},
             {"seq": 1, "ts": "t", "run_id": "r9", "type": "run_started", "data": {"objective": "o"}}]
    path = tmp_path / "r9.jsonl"
    path.write_text("".join(json.dumps(l) + "\n" for l in lines) + "\n", encoding="utf-8")
    assert len(read_trace_file(str(path))) == 2
    runs = load_trace_dir(str(tmp_path))
    assert list(runs) == ["r9"]
    assert [e["seq"] for e in runs["r9"]] == [1, 2]
    bus = MemoryBus()
    assert Recorder(bus, "t").publish_file(str(path)) == 2
