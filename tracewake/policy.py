"""Policy: the part of a capability catalog that decides whether a tool call is allowed.

The governed-agent-orchestrator loads a capability catalog (``capabilities.json``) and its guard
checks every tool call's arguments against the constraints declared there. TRACEWAKE reads the
same file, keeps only what governs a decision (constraints, effect class, approval requirement),
identifies the policy by a hash of that content, and re-implements ``check_constraints`` with the
same semantics so a replayed decision matches the original guard. The unit tests cross-check this
implementation against the orchestrator's own when it is installed.
"""
from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

from .envelope import canonical, sha256_hex

CONSTRAINT_KEYS = {"min", "max", "allowed", "allowed_domains", "pattern", "max_length"}


@dataclass(frozen=True)
class CapabilityPolicy:
    name: str
    inputs: Dict[str, str] = field(default_factory=dict)
    constraints: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    effect: str = "reversible"
    requires_approval: bool = False

    def governing(self) -> Dict[str, Any]:
        return {"constraints": self.constraints, "effect": self.effect, "requires_approval": self.requires_approval}


class Policy:
    def __init__(self, capabilities: List[CapabilityPolicy], source: str = "<memory>"):
        self._caps = {c.name: c for c in capabilities}
        self.source = source
        errors: List[str] = []
        for c in self._caps.values():
            for name, spec in c.constraints.items():
                if not isinstance(spec, dict):
                    errors.append(f"{c.name}: constraint for '{name}' must be an object")
                    continue
                unknown = set(spec) - CONSTRAINT_KEYS
                if unknown:
                    errors.append(f"{c.name}: unknown constraint keys for '{name}': {', '.join(sorted(unknown))}")
                if "pattern" in spec:
                    try:
                        re.compile(str(spec["pattern"]))
                    except re.error as e:
                        errors.append(f"{c.name}: pattern for '{name}' does not compile ({e})")
        if errors:
            raise ValueError("Invalid policy: " + "; ".join(errors))

    @classmethod
    def load(cls, path: str) -> "Policy":
        data = json.loads(Path(path).read_text(encoding="utf-8"))
        caps = [CapabilityPolicy(name=d["name"], inputs=dict(d.get("inputs") or {}),
                                 constraints=dict(d.get("constraints") or {}), effect=d.get("effect", "reversible"),
                                 requires_approval=bool(d.get("requires_approval", False))) for d in data]
        return cls(caps, source=str(path))

    @property
    def policy_id(self) -> str:
        return sha256_hex(canonical({name: c.governing() for name, c in sorted(self._caps.items())}))[:12]

    def get(self, name: str) -> Optional[CapabilityPolicy]:
        return self._caps.get(name)

    def names(self) -> List[str]:
        return list(self._caps)

    def __len__(self) -> int:
        return len(self._caps)


def check_constraints(constraints: Dict[str, Dict[str, Any]], inputs: Dict[str, Any]) -> List[str]:
    """Every violated constraint for these inputs; an empty list means the call may proceed.

    Same rules as the orchestrator's guard: absent inputs are not checked, unresolved references
    (``$step.field``) are skipped, numeric bounds need a number, allowed-domain matching is
    case-insensitive on the part after ``@``, patterns must match the whole value.
    """
    violations: List[str] = []
    for name, spec in constraints.items():
        if name not in inputs:
            continue
        value = inputs[name]
        if isinstance(value, str) and value.startswith("$"):
            continue
        if "min" in spec or "max" in spec:
            try:
                number = float(value)
            except (TypeError, ValueError):
                violations.append(f"Input '{name}' must be a number, got {value!r}.")
                continue
            if "min" in spec and number < float(spec["min"]):
                violations.append(f"Input '{name}' is {value!r}; the minimum is {spec['min']}.")
            if "max" in spec and number > float(spec["max"]):
                violations.append(f"Input '{name}' is {value!r}; the maximum is {spec['max']}.")
        if "allowed" in spec and value not in spec["allowed"]:
            violations.append(f"Input '{name}' is {value!r}; allowed values: {', '.join(map(str, spec['allowed']))}.")
        if "allowed_domains" in spec:
            text = str(value).strip().lower()
            domain = text.rpartition("@")[2] if "@" in text else ""
            if not domain or domain not in [d.lower() for d in spec["allowed_domains"]]:
                violations.append(f"Input '{name}' is {value!r}, which is not an address in an allowed domain "
                                  f"({', '.join(spec['allowed_domains'])}).")
        if "pattern" in spec and not re.fullmatch(str(spec["pattern"]), str(value)):
            violations.append(f"Input '{name}' is {value!r}; it must match /{spec['pattern']}/.")
        if "max_length" in spec and len(str(value)) > int(spec["max_length"]):
            violations.append(f"Input '{name}' is {len(str(value))} characters long; the maximum is {spec['max_length']}.")
    return violations


def diff(old: Policy, new: Policy) -> List[str]:
    """What changed between two policies, as sentences a reviewer can read."""
    lines: List[str] = []
    for name in sorted(set(old.names()) | set(new.names())):
        a, b = old.get(name), new.get(name)
        if a is None:
            lines.append(f"{name}: added")
            continue
        if b is None:
            lines.append(f"{name}: removed")
            continue
        for inp in sorted(set(a.constraints) | set(b.constraints)):
            ca, cb = a.constraints.get(inp), b.constraints.get(inp)
            if ca != cb:
                lines.append(f"{name}.{inp}: {json.dumps(ca, sort_keys=True)} -> {json.dumps(cb, sort_keys=True)}")
        if a.effect != b.effect:
            lines.append(f"{name}: effect {a.effect} -> {b.effect}")
        if a.requires_approval != b.requires_approval:
            lines.append(f"{name}: requires_approval {a.requires_approval} -> {b.requires_approval}")
    return lines
