"""ReplayProof: the checks that make the rest of TRACEWAKE a claim rather than a hope.

Given a log, recorded runs and two policies, the proof publishes the runs, rebuilds the ledger
from the log alone, destroys it and rebuilds it again, delivers a run twice, replays under the new
policy, and checks that every property TRACEWAKE promises actually held. The report records the
environment, the numbers and the command, so a reader can tell a real run from a description.
"""
from __future__ import annotations

import os
import platform
import subprocess
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from . import __version__
from .bus import Bus
from .echo import EchoReport, echo
from .envelope import digest_envelopes, envelopes_for
from .ledger import WakeLedger
from .policy import Policy
from .recorder import Recorder

ROOT = Path(__file__).resolve().parent.parent


@dataclass
class Check:
    name: str
    passed: bool
    detail: str
    numbers: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class ProofReport:
    checks: List[Check]
    echo: EchoReport
    environment: Dict[str, Any]
    numbers: Dict[str, Any]
    command: str

    @property
    def passed(self) -> bool:
        return all(c.passed for c in self.checks)

    def to_dict(self) -> Dict[str, Any]:
        return {"passed": self.passed, "checks": [c.to_dict() for c in self.checks], "numbers": self.numbers,
                "environment": self.environment, "command": self.command, "echo": self.echo.to_dict()}

    def render_markdown(self) -> str:
        env, n = self.environment, self.numbers
        verdict = "PASSED" if self.passed else "FAILED"
        lines = ["# ReplayProof", "",
                 f"**{verdict}**: {sum(c.passed for c in self.checks)} of {len(self.checks)} checks. "
                 f"Generated {env['generated_at']} at revision `{env['revision']}` with `{self.command}`.", "",
                 f"Log: {env['bus']}. Python {env['python']} on {env['platform']}; tracewake {env['tracewake']}, "
                 f"kafka-python {env['kafka_python']}.", "",
                 "## Numbers", "",
                 "| Measure | Value |", "|---|---|",
                 f"| Runs published | {n['runs']} |",
                 f"| Events published | {n['events']} |",
                 f"| Publish time | {n['publish_s']:.3f} s ({n['publish_events_per_s']:.0f} events/s) |",
                 f"| Ledger rebuild time (first read from offset 0) | {n['ingest_s']:.3f} s |",
                 f"| Duplicate deliveries injected | {n['duplicates_injected']} |",
                 f"| Decisions replayed | {n['decisions']} |",
                 "", "## Checks", "", "| Check | Result | Detail |", "|---|---|---|"]
        for c in self.checks:
            lines.append(f"| {c.name} | {'pass' if c.passed else 'FAIL'} | {c.detail.replace('|', '/')} |")
        lines += ["", self.echo.render_markdown(title="PolicyEcho result")]
        return "\n".join(lines)


def _revision() -> str:
    sha = os.environ.get("GITHUB_SHA")
    if sha:
        return sha[:7]
    try:
        return subprocess.check_output(["git", "rev-parse", "--short", "HEAD"], cwd=ROOT, text=True,
                                       stderr=subprocess.DEVNULL).strip()
    except Exception:  # noqa: BLE001
        return "unknown"


def _kafka_python_version() -> str:
    try:
        from importlib.metadata import version
        return version("kafka-python")
    except Exception:  # noqa: BLE001
        return "unknown"


def run_proof(bus: Bus, topic: str, runs: Dict[str, List[Dict[str, Any]]], old: Policy, new: Policy, *,
              ledger_path: str = ":memory:", command: str = "tracewake proof") -> ProofReport:
    checks: List[Check] = []
    numbers: Dict[str, Any] = {}

    # 1. Publish every run, keyed by run id, stamped with the policy that was in force.
    recorder = Recorder(bus, topic, policy_id=old.policy_id)
    t0 = time.perf_counter()
    published = sum(recorder.publish_events(events) for _, events in sorted(runs.items()))
    publish_s = time.perf_counter() - t0
    source_digests = {run_id: digest_envelopes(envelopes_for(events, policy_id=old.policy_id))
                      for run_id, events in runs.items()}

    # 2. Rebuild the ledger from the log alone.
    ledger = WakeLedger(ledger_path)
    ledger.reset()
    t1 = time.perf_counter()
    stats = ledger.ingest(bus, topic)
    ingest_s = time.perf_counter() - t1
    checks.append(Check("every published event reached the ledger", stats.inserted == published and stats.invalid == 0,
                        f"published {published}, consumed {stats.consumed}, inserted {stats.inserted}, invalid {stats.invalid}",
                        stats.to_dict()))
    gaps = {r: ledger.gaps(r) for r in ledger.runs()}
    checks.append(Check("no run has a gap in its sequence numbers", all(not g for g in gaps.values()),
                        f"{len(gaps)} runs, gaps: {sum(len(g) for g in gaps.values())}"))
    first = ledger.digests()
    checks.append(Check("ledger content equals the published events, run by run", first == source_digests,
                        f"{sum(1 for r in source_digests if first.get(r) == source_digests[r])} of {len(source_digests)} run digests match"))

    # 3. Destroy the ledger and rebuild it from offset zero.
    ledger.reset()
    again = ledger.ingest(bus, topic)
    second = ledger.digests()
    checks.append(Check("ledger is identical after being destroyed and rebuilt from offset 0", second == first and again.inserted == published,
                        f"rebuilt {again.inserted} events; digests equal: {second == first}"))

    # 4. Deliver one run twice; nothing may change.
    dup_run = sorted(runs)[0]
    duplicates = recorder.publish_events(runs[dup_run])
    third_stats = ledger.ingest(bus, topic)
    third = ledger.digests()
    checks.append(Check("a run delivered twice changes nothing", third == first and third_stats.inserted == 0
                        and third_stats.duplicates >= duplicates,
                        f"re-published {duplicates} events; inserted {third_stats.inserted}, duplicates seen {third_stats.duplicates}"))

    # 5. Replay under the new policy, from the ledger and from the source, and compare.
    report_ledger = echo(ledger.all_events(), old, new)
    report_source = echo(runs, old, new)
    faith = report_ledger.faithfulness
    checks.append(Check("replay reproduces every recorded decision under the old policy", faith["mismatches"] == 0 and faith["checked"] > 0,
                        f"{faith['checked']} decisions recomputed, {faith['mismatches']} mismatches"))
    same = [r.to_dict() for r in report_ledger.rows] == [r.to_dict() for r in report_source.rows]
    checks.append(Check("replay from the ledger equals replay from the source events", same,
                        f"{len(report_ledger.rows)} rows compared"))
    checks.append(Check("the policy change is visible in the replay", report_ledger.counts["flipped"] > 0 or not report_ledger.diff,
                        f"{report_ledger.counts['flipped']} flipped, {report_ledger.counts['needs-evidence']} need evidence"))

    numbers.update({"runs": len(runs), "events": published, "publish_s": publish_s,
                    "publish_events_per_s": (published / publish_s) if publish_s > 0 else 0.0,
                    "ingest_s": ingest_s, "duplicates_injected": duplicates, "decisions": len(report_ledger.rows),
                    "ledger_events": ledger.count()})
    environment = {"generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"), "revision": _revision(),
                   "bus": bus.describe(), "python": platform.python_version(), "platform": platform.platform(),
                   "tracewake": __version__, "kafka_python": _kafka_python_version(), "topic": topic}
    ledger.close()
    return ProofReport(checks, report_ledger, environment, numbers, command)
