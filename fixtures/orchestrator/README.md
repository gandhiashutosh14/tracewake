# Recorded runs

Four DecisionTrace journals written by the governed-agent-orchestrator, unchanged, one event per
line. `PROVENANCE.json` names the orchestrator revision, the catalog hash, the objectives and the
command that produced them. The planner is the orchestrator's deterministic heuristic planner; no
language model was involved.

| File | Objective | Outcome |
|---|---|---|
| `allowed.jsonl` | Forecast next year's revenue and send it to finance@example.com. | completed: three read-only calls, an approval, one irreversible send |
| `denied.jsonl` | The same, addressed to finance@evil-example.org. | rejected at plan time: recipient not in an allowed domain |
| `partner.jsonl` | The same, addressed to partner@example.org. | rejected at plan time under policy v1; allowed under v2 |
| `exhausted.jsonl` | Two independent lookups and a summary under a one-call budget. | one call allowed, the rest denied for budget |

Re-record them with `python scripts/make_fixtures.py --orchestrator <checkout> --out fixtures/orchestrator`.
