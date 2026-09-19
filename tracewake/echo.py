"""PolicyEcho: re-decide the recorded tool calls of a run under a changed policy.

Nothing is re-executed. For every tool decision the log holds, PolicyEcho recovers the arguments
the guard saw (from ``step_started`` or ``approval_required`` for calls that ran, from the
accepted plan's literal arguments for calls that were refused, and from the proposed plan for
plans the validator rejected), recomputes the decision under the old policy to prove the replay is
faithful to the record, then recomputes it under the new policy and classifies the result:

* ``unchanged``: the new policy decides the same way;
* ``flipped``: ``ALLOWED -> DENIED`` or ``DENIED -> ALLOWED``;
* ``needs-evidence``: the new policy constrains an argument whose value the log does not contain.
"""
from __future__ import annotations

import re
from collections import Counter
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from .policy import Policy, check_constraints
from .policy import diff as policy_diff

ALLOWED, DENIED = "ALLOWED", "DENIED"
UNCHANGED, FLIPPED, NEEDS_EVIDENCE = "unchanged", "flipped", "needs-evidence"
_STEP_ERROR = re.compile(r"^Step (\S+) \(([^)]+)\): (Input '.+)$")


@dataclass
class Decision:
    run_id: str
    seq: int
    step: str
    capability: str
    level: str                      # "call": the runtime guard; "plan": the validator, before anything ran
    recorded: str                   # ALLOWED or DENIED
    inputs: Dict[str, Any]
    evidence: str                   # resolved | literal | partial | missing
    missing: List[str] = field(default_factory=list)
    via_fallback: bool = False
    recorded_violations: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


def _is_ref(v: Any) -> bool:
    return isinstance(v, str) and v.startswith("$")


def extract_decisions(events: List[Dict[str, Any]]) -> Tuple[List[Decision], Dict[str, int]]:
    """Every recorded tool decision of one run, with the arguments the log holds for it."""
    ordered = sorted(events, key=lambda e: int(e["seq"]))
    run_id = str(ordered[0]["run_id"]) if ordered else ""
    skipped = {"budget_denials": 0}

    # Pass 1: what the log knows about each step's arguments.
    accepted: Dict[str, Dict[str, Any]] = {}
    resolved: Dict[str, Dict[str, Any]] = {}
    for e in ordered:
        d = e.get("data") or {}
        if e["type"] == "plan_accepted":
            for s in (d.get("plan") or {}).get("steps", []):
                accepted[s["id"]] = {"capability": s["capability"], "inputs": dict(s.get("inputs") or {})}
        elif e["type"] == "step_started" and d.get("step"):
            resolved[d["step"]] = dict(d.get("inputs") or {})
        elif e["type"] == "approval_required" and d.get("step"):
            resolved.setdefault(d["step"], dict(d.get("inputs") or {}))

    # Pass 2: the decisions.
    decisions: List[Decision] = []
    for e in ordered:
        t, d, seq = e["type"], e.get("data") or {}, int(e["seq"])
        if t == "plan_proposed" and d.get("errors"):
            per_step: Dict[str, List[str]] = {}
            caps: Dict[str, str] = {}
            for err in d["errors"]:
                m = _STEP_ERROR.match(str(err))
                if m:
                    per_step.setdefault(m.group(1), []).append(m.group(3))
                    caps[m.group(1)] = m.group(2)
            steps = {s["id"]: s for s in (d.get("plan") or {}).get("steps", [])}
            for sid, violations in per_step.items():
                literal = dict((steps.get(sid) or {}).get("inputs") or {})
                decisions.append(Decision(run_id, seq, sid, caps[sid], "plan", DENIED, literal, "literal",
                                          recorded_violations=violations))
        elif t == "tool_call_allowed":
            step, cap = str(d.get("step")), str(d.get("capability"))
            if step in resolved:
                decisions.append(Decision(run_id, seq, step, cap, "call", ALLOWED, resolved[step], "resolved",
                                          via_fallback=bool(d.get("via_fallback"))))
            else:
                inputs, evidence, missing = _literal_evidence(accepted.get(step))
                decisions.append(Decision(run_id, seq, step, cap, "call", ALLOWED, inputs, evidence, missing,
                                          via_fallback=bool(d.get("via_fallback"))))
        elif t == "tool_call_denied":
            if d.get("reason") != "constraint":
                skipped["budget_denials"] += 1
                continue
            step, cap = str(d.get("step")), str(d.get("capability"))
            if step in resolved:
                inputs, evidence, missing = resolved[step], "resolved", []
            else:
                inputs, evidence, missing = _literal_evidence(accepted.get(step))
            decisions.append(Decision(run_id, seq, step, cap, "call", DENIED, inputs, evidence, missing,
                                      via_fallback=bool(d.get("via_fallback")),
                                      recorded_violations=list(d.get("violations") or [])))
    return decisions, skipped


def _literal_evidence(planned: Optional[Dict[str, Any]]) -> Tuple[Dict[str, Any], str, List[str]]:
    if planned is None:
        return {}, "missing", []
    inputs = dict(planned.get("inputs") or {})
    missing = [k for k, v in inputs.items() if _is_ref(v)]
    return inputs, ("partial" if missing else "literal"), missing


@dataclass
class EchoRow:
    decision: Decision
    recomputed_old: Optional[str]
    old_violations: List[str]
    new: Optional[str]
    new_violations: List[str]
    classification: str
    faithful: Optional[bool]
    notes: List[str] = field(default_factory=list)

    @property
    def transition(self) -> str:
        if self.classification == NEEDS_EVIDENCE:
            return "NEEDS EVIDENCE"
        return f"{self.decision.recorded} -> {self.new}"

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d["transition"] = self.transition
        return d


def _inputs_for(cap, inputs: Dict[str, Any], via_fallback: bool) -> Dict[str, Any]:
    # The runtime passes a fallback capability only the inputs it declares; mirror that.
    if via_fallback and cap is not None and cap.inputs:
        return {k: v for k, v in inputs.items() if k in cap.inputs}
    return inputs


def replay(decisions: List[Decision], old: Policy, new: Policy) -> List[EchoRow]:
    rows: List[EchoRow] = []
    for d in decisions:
        cap_old, cap_new = old.get(d.capability), new.get(d.capability)
        notes: List[str] = []

        if cap_old is None:
            old_viol = [f"capability '{d.capability}' is not in the old policy"]
        else:
            old_viol = check_constraints(cap_old.constraints, _inputs_for(cap_old, d.inputs, d.via_fallback))
        recomputed_old = DENIED if old_viol else ALLOWED
        faithful = (recomputed_old == d.recorded) if d.evidence in ("resolved", "literal") else None
        if faithful is False:
            notes.append("recomputation under the old policy disagrees with the record")

        if cap_new is None:
            outcome, new_viol = DENIED, [f"capability '{d.capability}' is not in the new policy"]
            classification = UNCHANGED if outcome == d.recorded else FLIPPED
        else:
            needed = [m for m in d.missing if m in cap_new.constraints]
            if d.evidence == "missing" or needed:
                outcome, new_viol, classification = None, [], NEEDS_EVIDENCE
                what = ", ".join(needed) if needed else "the call's arguments"
                notes.append(f"the log does not contain the value of {what}, which the new policy constrains")
            else:
                new_viol = check_constraints(cap_new.constraints, _inputs_for(cap_new, d.inputs, d.via_fallback))
                outcome = DENIED if new_viol else ALLOWED
                classification = UNCHANGED if outcome == d.recorded else FLIPPED
            if cap_old is not None:
                if cap_new.requires_approval != cap_old.requires_approval:
                    notes.append("now requires approval" if cap_new.requires_approval else "no longer requires approval")
                if cap_new.effect != cap_old.effect:
                    notes.append(f"effect class {cap_old.effect} -> {cap_new.effect}")
        rows.append(EchoRow(d, recomputed_old, old_viol, outcome, new_viol, classification, faithful, notes))
    return rows


@dataclass
class EchoReport:
    old_policy: Dict[str, str]
    new_policy: Dict[str, str]
    diff: List[str]
    rows: List[EchoRow]
    skipped: Dict[str, int]
    generated_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat(timespec="seconds"))

    @property
    def counts(self) -> Dict[str, int]:
        c = Counter(r.classification for r in self.rows)
        return {UNCHANGED: c[UNCHANGED], FLIPPED: c[FLIPPED], NEEDS_EVIDENCE: c[NEEDS_EVIDENCE]}

    @property
    def transitions(self) -> Dict[str, int]:
        return dict(Counter(r.transition for r in self.rows))

    @property
    def faithfulness(self) -> Dict[str, int]:
        checked = [r for r in self.rows if r.faithful is not None]
        return {"checked": len(checked), "mismatches": sum(1 for r in checked if not r.faithful)}

    def to_dict(self) -> Dict[str, Any]:
        return {"old_policy": self.old_policy, "new_policy": self.new_policy, "diff": self.diff,
                "counts": self.counts, "transitions": self.transitions, "faithfulness": self.faithfulness,
                "skipped": self.skipped, "generated_at": self.generated_at,
                "rows": [r.to_dict() for r in self.rows]}

    def render_markdown(self, *, title: str = "PolicyEcho: replay under a changed policy") -> str:
        c, f = self.counts, self.faithfulness
        lines = [f"# {title}", "",
                 f"Old policy `{self.old_policy['policy_id']}` ({self.old_policy['source']}) -> "
                 f"new policy `{self.new_policy['policy_id']}` ({self.new_policy['source']}). Generated {self.generated_at}.", "",
                 "## What changed in the policy", ""]
        lines += [f"- {line}" for line in self.diff] or ["- nothing"]
        lines += ["", "## Result", "",
                  f"{len(self.rows)} recorded decisions: **{c[UNCHANGED]} unchanged, {c[FLIPPED]} flipped, "
                  f"{c[NEEDS_EVIDENCE]} need evidence**. Faithfulness check: {f['checked']} decisions recomputed under the "
                  f"old policy, {f['mismatches']} disagreed with the record. Budget denials are runtime state, not policy, "
                  f"and were not re-decided ({self.skipped.get('budget_denials', 0)}).", "",
                  "| run | seq | level | step | capability | old -> new | class | why |",
                  "|---|---|---|---|---|---|---|---|"]
        for r in self.rows:
            if r.classification == FLIPPED and r.new == ALLOWED:
                why = "no longer applies: " + "; ".join(r.decision.recorded_violations or r.old_violations)
            elif r.classification != UNCHANGED:
                why = "; ".join(r.new_violations or r.notes)
            else:
                why = "; ".join(r.notes)
            lines.append(f"| {r.decision.run_id} | {r.decision.seq} | {r.decision.level} | {r.decision.step} | "
                         f"{r.decision.capability} | {r.transition} | {r.classification} | "
                         f"{why.replace('|', '/')} |")
        return "\n".join(lines) + "\n"


def echo(runs: Dict[str, List[Dict[str, Any]]], old: Policy, new: Policy) -> EchoReport:
    rows: List[EchoRow] = []
    skipped: Counter = Counter()
    for run_id in sorted(runs):
        decisions, sk = extract_decisions(runs[run_id])
        skipped.update(sk)
        rows.extend(replay(decisions, old, new))
    return EchoReport(old_policy={"source": Path(old.source).name, "policy_id": old.policy_id},
                      new_policy={"source": Path(new.source).name, "policy_id": new.policy_id},
                      diff=policy_diff(old, new), rows=rows, skipped=dict(skipped))
