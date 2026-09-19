"""TraceEnvelope: the versioned event that leaves an agent's run and enters the log.

The governed-agent-orchestrator journals one event per decision: a sequence number, a UTC
timestamp, the run id, a closed event type and a data payload. TraceEnvelope wraps that event
with what the log needs and the run does not carry: which policy was in force (``policy_id``),
which producer emitted it, and which envelope version to parse it with. The run id is the
partition key, so every event of a run is stored in order on one partition.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass, field
from typing import Any, Dict, Iterable, List, Optional

ENVELOPE_VERSION = "1"
PRODUCER_ORCHESTRATOR = "governed-agent-orchestrator"

# The orchestrator's closed event vocabulary (orchestrator/trace.py, EVENT_TYPES).
ORCHESTRATOR_EVENT_TYPES = frozenset({
    "run_started", "plan_proposed", "plan_rejected", "plan_accepted", "planner_fallback",
    "wave_started", "step_started", "step_finished", "step_failed", "step_fallback",
    "approval_required", "approval_granted", "approval_denied",
    "tool_call_allowed", "tool_call_denied", "budget_exhausted",
    "run_finished", "run_failed", "note",
})


def canonical(obj: Any) -> bytes:
    """Deterministic JSON bytes: sorted keys, no whitespace, non-JSON values as strings."""
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), default=str, ensure_ascii=False).encode("utf-8")


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


@dataclass(frozen=True)
class TraceEnvelope:
    run_id: str
    seq: int
    ts: str
    type: str
    data: Dict[str, Any] = field(default_factory=dict)
    policy_id: Optional[str] = None
    producer: str = PRODUCER_ORCHESTRATOR
    envelope_version: str = ENVELOPE_VERSION

    # ------------------------------------------------------------------
    @property
    def id(self) -> str:
        return f"{self.run_id}:{self.seq}"

    def key(self) -> bytes:
        return self.run_id.encode("utf-8")

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d["id"] = self.id
        return d

    def to_json(self) -> bytes:
        return canonical(self.to_dict())

    def digest(self) -> str:
        """Content hash of the envelope, used for ledger equality checks."""
        return sha256_hex(self.to_json())

    # ------------------------------------------------------------------
    @classmethod
    def from_event(cls, event: Any, *, policy_id: Optional[str] = None,
                   producer: str = PRODUCER_ORCHESTRATOR) -> "TraceEnvelope":
        """Wrap a DecisionTrace event (an object with seq/ts/run_id/type/data, or that dict)."""
        d = event if isinstance(event, dict) else {
            "seq": getattr(event, "seq"), "ts": getattr(event, "ts"), "run_id": getattr(event, "run_id"),
            "type": getattr(event, "type"), "data": getattr(event, "data"),
        }
        env = cls(run_id=str(d.get("run_id", "")), seq=d.get("seq"), ts=str(d.get("ts", "")),
                  type=str(d.get("type", "")), data=dict(d.get("data") or {}),
                  policy_id=policy_id, producer=producer)
        problems = validate(env.to_dict())
        if problems:
            raise ValueError("Invalid trace event: " + "; ".join(problems))
        return env

    @classmethod
    def from_json(cls, raw: bytes) -> "TraceEnvelope":
        d = json.loads(raw.decode("utf-8"))
        problems = validate(d)
        if problems:
            raise ValueError("Invalid envelope: " + "; ".join(problems))
        return cls(run_id=d["run_id"], seq=int(d["seq"]), ts=d["ts"], type=d["type"], data=d.get("data") or {},
                   policy_id=d.get("policy_id"), producer=d.get("producer", PRODUCER_ORCHESTRATOR),
                   envelope_version=str(d.get("envelope_version", ENVELOPE_VERSION)))


def validate(d: Dict[str, Any]) -> List[str]:
    """Problems with an envelope dict, as sentences. Empty means valid."""
    problems: List[str] = []
    if not isinstance(d.get("run_id"), str) or not d.get("run_id"):
        problems.append("run_id must be a non-empty string")
    seq = d.get("seq")
    if isinstance(seq, bool) or not isinstance(seq, int) or seq < 1:
        problems.append("seq must be an integer >= 1")
    if not isinstance(d.get("ts"), str) or not d.get("ts"):
        problems.append("ts must be a non-empty string")
    if not isinstance(d.get("type"), str) or not d.get("type"):
        problems.append("type must be a non-empty string")
    if not isinstance(d.get("data", {}), dict):
        problems.append("data must be an object")
    version = str(d.get("envelope_version", ENVELOPE_VERSION))
    if version != ENVELOPE_VERSION:
        problems.append(f"envelope_version {version!r} is not supported (expected {ENVELOPE_VERSION!r})")
    producer = d.get("producer", PRODUCER_ORCHESTRATOR)
    if producer == PRODUCER_ORCHESTRATOR and isinstance(d.get("type"), str) and d["type"] not in ORCHESTRATOR_EVENT_TYPES:
        problems.append(f"unknown orchestrator event type {d['type']!r}")
    return problems


def envelopes_for(events: Iterable[Any], *, policy_id: Optional[str] = None,
                  producer: str = PRODUCER_ORCHESTRATOR) -> List[TraceEnvelope]:
    return [TraceEnvelope.from_event(e, policy_id=policy_id, producer=producer) for e in events]


def digest_envelopes(envelopes: Iterable[TraceEnvelope]) -> str:
    """One hash for a run: the envelopes in sequence order, each by its own digest."""
    ordered = sorted(envelopes, key=lambda e: e.seq)
    return sha256_hex(canonical([e.digest() for e in ordered]))
