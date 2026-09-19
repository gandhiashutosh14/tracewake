"""Record the governed-agent-orchestrator's demo scenarios as TRACEWAKE fixtures.

Run with an environment where the orchestrator is installed (its own virtual environment works):

    python scripts/make_fixtures.py --orchestrator ../governed-agent-orchestrator --out fixtures/orchestrator

Writes one JSONL file per run, exactly as DecisionTrace journals it, plus PROVENANCE.json with the
orchestrator revision, the objectives, the catalog hash and the command. No language model is
involved: the orchestrator's deterministic heuristic planner produces every plan.
"""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import platform
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

EXTRA_OBJECTIVES = {
    # Recorded as DENIED at plan time under policy v1 (example.org is not an allowed domain there).
    "partner": "Forecast next year's revenue and send it to partner@example.org.",
}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--orchestrator", required=True, help="path to a governed-agent-orchestrator checkout")
    ap.add_argument("--out", default="fixtures/orchestrator")
    args = ap.parse_args()

    root = Path(args.orchestrator).resolve()
    sys.path.insert(0, str(root))
    from orchestrator.catalog import Catalog  # noqa: E402
    from orchestrator.demo import _run_objective, scenario_allowed, scenario_denied, scenario_exhausted  # noqa: E402

    catalog_path = root / "capabilities.json"
    catalog = Catalog.load(str(catalog_path))

    async def record():
        scenarios = [("allowed", await scenario_allowed(catalog)), ("denied", await scenario_denied(catalog)),
                     ("exhausted", await scenario_exhausted(catalog))]
        for name, objective in EXTRA_OBJECTIVES.items():
            s = await _run_objective(catalog, objective, approve=True)
            s.name = name
            scenarios.append((name, s))
        return scenarios

    scenarios = asyncio.run(record())
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    for old in out.glob("*.jsonl"):
        old.unlink()
    written = []
    for name, s in scenarios:
        events = [e.to_dict() for e in s.events]
        path = out / f"{name}.jsonl"
        path.write_text("".join(json.dumps(e, default=str) + "\n" for e in events), encoding="utf-8")
        written.append({"file": path.name, "run_id": events[0]["run_id"], "objective": s.description,
                        "status": s.status, "events": len(events)})

    try:
        rev = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, text=True).strip()
    except Exception:  # noqa: BLE001
        rev = "unknown"
    provenance = {
        "producer": "governed-agent-orchestrator",
        "orchestrator_revision": rev,
        "orchestrator_repo": "https://github.com/gandhiashutosh14/governed-agent-orchestrator",
        "catalog_sha256": hashlib.sha256(catalog_path.read_bytes()).hexdigest(),
        "planner": "deterministic heuristic planner; no language model",
        "recorded_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "python": platform.python_version(),
        "command": "python scripts/make_fixtures.py --orchestrator <checkout> --out " + args.out,
        "runs": written,
    }
    (out / "PROVENANCE.json").write_text(json.dumps(provenance, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(provenance, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
