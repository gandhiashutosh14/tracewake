"""WakeLedger: a run's decision ledger, rebuilt from the log alone.

The ledger is a SQLite table keyed by ``(run_id, seq)``. A consumer may be destroyed and started
again from offset zero; because every insert is idempotent on that key, re-reading the log, or
receiving a message twice, changes nothing. ``digest()`` hashes a run's envelopes in sequence
order, which is what ReplayProof compares against the events that were published.
"""
from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional

from .bus import Bus, Message
from .envelope import TraceEnvelope, digest_envelopes, validate

SCHEMA = """
CREATE TABLE IF NOT EXISTS events (
    run_id TEXT NOT NULL,
    seq INTEGER NOT NULL,
    ts TEXT NOT NULL,
    type TEXT NOT NULL,
    data TEXT NOT NULL,
    policy_id TEXT,
    producer TEXT NOT NULL,
    envelope_version TEXT NOT NULL,
    partition INTEGER,
    log_offset INTEGER,
    PRIMARY KEY (run_id, seq)
);
CREATE TABLE IF NOT EXISTS rejected (
    partition INTEGER,
    log_offset INTEGER,
    reason TEXT NOT NULL
);
"""


@dataclass
class IngestStats:
    consumed: int = 0
    inserted: int = 0
    duplicates: int = 0
    invalid: int = 0

    def to_dict(self) -> Dict[str, int]:
        return {"consumed": self.consumed, "inserted": self.inserted, "duplicates": self.duplicates, "invalid": self.invalid}


class WakeLedger:
    def __init__(self, path: str = ":memory:"):
        self.path = path
        self._db = sqlite3.connect(path)
        self._db.executescript(SCHEMA)

    # ------------------------------------------------------------------ writing
    def ingest(self, bus: Bus, topic: str) -> IngestStats:
        """Read the whole topic from offset zero and insert what is not already present."""
        return self.ingest_messages(bus.consume(topic))

    def ingest_messages(self, messages: Iterable[Message]) -> IngestStats:
        stats = IngestStats()
        cur = self._db.cursor()
        for m in messages:
            stats.consumed += 1
            try:
                env = TraceEnvelope.from_json(m.value)
            except (ValueError, UnicodeDecodeError, json.JSONDecodeError) as e:
                stats.invalid += 1
                cur.execute("INSERT INTO rejected VALUES (?, ?, ?)", (m.partition, m.offset, str(e)[:500]))
                continue
            cur.execute(
                "INSERT OR IGNORE INTO events VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (env.run_id, env.seq, env.ts, env.type, json.dumps(env.data, sort_keys=True, default=str),
                 env.policy_id, env.producer, env.envelope_version, m.partition, m.offset))
            if cur.rowcount == 1:
                stats.inserted += 1
            else:
                stats.duplicates += 1
        self._db.commit()
        return stats

    def reset(self) -> None:
        self._db.executescript("DELETE FROM events; DELETE FROM rejected;")
        self._db.commit()

    # ------------------------------------------------------------------ reading
    def runs(self) -> List[str]:
        return [r[0] for r in self._db.execute("SELECT DISTINCT run_id FROM events ORDER BY run_id")]

    def envelopes(self, run_id: str) -> List[TraceEnvelope]:
        rows = self._db.execute(
            "SELECT run_id, seq, ts, type, data, policy_id, producer, envelope_version FROM events "
            "WHERE run_id = ? ORDER BY seq", (run_id,)).fetchall()
        return [TraceEnvelope(run_id=r[0], seq=r[1], ts=r[2], type=r[3], data=json.loads(r[4]), policy_id=r[5],
                              producer=r[6], envelope_version=r[7]) for r in rows]

    def events(self, run_id: str) -> List[Dict[str, Any]]:
        """The run's events as the orchestrator wrote them (seq, ts, run_id, type, data)."""
        return [{"seq": e.seq, "ts": e.ts, "run_id": e.run_id, "type": e.type, "data": e.data} for e in self.envelopes(run_id)]

    def all_events(self) -> Dict[str, List[Dict[str, Any]]]:
        return {run_id: self.events(run_id) for run_id in self.runs()}

    def count(self) -> int:
        return self._db.execute("SELECT COUNT(*) FROM events").fetchone()[0]

    def digest(self, run_id: str) -> str:
        return digest_envelopes(self.envelopes(run_id))

    def digests(self) -> Dict[str, str]:
        return {run_id: self.digest(run_id) for run_id in self.runs()}

    def gaps(self, run_id: str) -> List[int]:
        """Sequence numbers missing between 1 and the highest one stored."""
        seqs = [r[0] for r in self._db.execute("SELECT seq FROM events WHERE run_id = ? ORDER BY seq", (run_id,))]
        if not seqs:
            return []
        present = set(seqs)
        return [s for s in range(1, seqs[-1] + 1) if s not in present]

    def policy_ids(self, run_id: str) -> List[str]:
        return [r[0] for r in self._db.execute(
            "SELECT DISTINCT policy_id FROM events WHERE run_id = ? AND policy_id IS NOT NULL", (run_id,))]

    # ------------------------------------------------------------------ questions a reviewer asks
    def summary(self, run_id: str) -> Dict[str, Any]:
        evs = self.events(run_id)
        by_type: Dict[str, int] = {}
        for e in evs:
            by_type[e["type"]] = by_type.get(e["type"], 0) + 1
        final = next((e for e in reversed(evs) if e["type"] in ("run_finished", "run_failed")), None)
        objective = next((e["data"].get("objective") for e in evs if e["type"] == "run_started"), None)
        return {
            "run_id": run_id,
            "objective": objective,
            "events": len(evs),
            "status": final["type"] if final else "incomplete",
            "completed_steps": (final or {}).get("data", {}).get("completed", []),
            "denied_steps": (final or {}).get("data", {}).get("denied", {}),
            "policy_ids": self.policy_ids(run_id),
            "gaps": self.gaps(run_id),
            "by_type": by_type,
        }

    def calls(self, *, effect: Optional[str] = None, outcome: Optional[str] = None) -> List[Dict[str, Any]]:
        """Tool-call decisions across every run, optionally filtered by effect class or outcome."""
        out: List[Dict[str, Any]] = []
        for run_id in self.runs():
            for e in self.events(run_id):
                if e["type"] not in ("tool_call_allowed", "tool_call_denied"):
                    continue
                verdict = "ALLOWED" if e["type"] == "tool_call_allowed" else "DENIED"
                if outcome and verdict != outcome:
                    continue
                if effect and e["data"].get("effect") != effect:
                    continue
                out.append({"run_id": run_id, "seq": e["seq"], "step": e["data"].get("step"),
                            "capability": e["data"].get("capability"), "outcome": verdict,
                            "effect": e["data"].get("effect"), "reason": e["data"].get("reason"),
                            "violations": e["data"].get("violations", [])})
        return out

    def approvals(self) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        for run_id in self.runs():
            for e in self.events(run_id):
                if e["type"] in ("approval_granted", "approval_denied"):
                    out.append({"run_id": run_id, "seq": e["seq"], "decision": e["type"],
                                "step": e["data"].get("step"), "by": e["data"].get("by")})
        return out

    def close(self) -> None:
        self._db.close()


def is_valid_message(m: Message) -> bool:
    try:
        return not validate(json.loads(m.value.decode("utf-8")))
    except (ValueError, UnicodeDecodeError):
        return False
