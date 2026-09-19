"""Recorder: turns a run's DecisionTrace events into TraceEnvelopes on the log.

Live use is one line in the orchestrator::

    trace.subscribe(Recorder(bus, "agent.decisions", policy_id=policy.policy_id))

because ``DecisionTrace.subscribe`` calls its listeners synchronously on every ``emit``. The same
Recorder also publishes recorded JSONL trace files, which is how the committed fixtures reach the
log in the demo and in CI.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from .bus import Bus
from .envelope import PRODUCER_ORCHESTRATOR, TraceEnvelope


class Recorder:
    def __init__(self, bus: Bus, topic: str, *, policy_id: Optional[str] = None,
                 producer: str = PRODUCER_ORCHESTRATOR, partitions: Optional[int] = None):
        self.bus = bus
        self.topic = topic
        self.policy_id = policy_id
        self.producer = producer
        self.published = 0
        self.bus.ensure_topic(topic, partitions)

    def __call__(self, event: Any) -> TraceEnvelope:
        env = TraceEnvelope.from_event(event, policy_id=self.policy_id, producer=self.producer)
        self.bus.publish(self.topic, env.key(), env.to_json())
        self.published += 1
        return env

    def publish_events(self, events: Iterable[Any]) -> int:
        n = 0
        for e in events:
            self(e)
            n += 1
        self.bus.flush()
        return n

    def publish_file(self, path: str) -> int:
        return self.publish_events(read_trace_file(path))


def read_trace_file(path: str) -> List[Dict[str, Any]]:
    """One DecisionTrace JSONL file (one event per line) as dicts, in file order."""
    events: List[Dict[str, Any]] = []
    for line in Path(path).read_text(encoding="utf-8").splitlines():
        if line.strip():
            events.append(json.loads(line))
    return events


def load_trace_dir(directory: str) -> Dict[str, List[Dict[str, Any]]]:
    """Every ``*.jsonl`` file in a directory, keyed by the run id inside the events."""
    runs: Dict[str, List[Dict[str, Any]]] = {}
    for path in sorted(Path(directory).glob("*.jsonl")):
        events = read_trace_file(str(path))
        if not events:
            continue
        run_id = str(events[0].get("run_id"))
        runs.setdefault(run_id, []).extend(events)
    for run_id in runs:
        runs[run_id].sort(key=lambda e: int(e.get("seq", 0)))
    return runs
