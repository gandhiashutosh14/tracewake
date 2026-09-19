import json

from tracewake.bus import MemoryBus
from tracewake.policy import Policy
from tracewake.proof import run_proof
from tracewake.recorder import load_trace_dir


def test_proof_passes_on_the_in_memory_log(fixtures_dir, policies_dir):
    runs = load_trace_dir(str(fixtures_dir))
    v1, v2 = Policy.load(str(policies_dir / "v1.json")), Policy.load(str(policies_dir / "v2.json"))
    report = run_proof(MemoryBus(), "t", runs, v1, v2, command="pytest")
    assert report.passed, [c for c in report.checks if not c.passed]
    assert len(report.checks) == 8
    assert report.numbers["events"] == sum(len(v) for v in runs.values())
    assert report.numbers["runs"] == len(runs)
    assert report.numbers["ledger_events"] == report.numbers["events"]
    text = report.render_markdown()
    assert "**PASSED**" in text and "PolicyEcho result" in text
    json.loads(json.dumps(report.to_dict(), default=str))


def test_proof_with_an_unchanged_policy_is_still_consistent(fixtures_dir, policies_dir):
    runs = load_trace_dir(str(fixtures_dir))
    v1 = Policy.load(str(policies_dir / "v1.json"))
    report = run_proof(MemoryBus(), "t", runs, v1, v1)
    assert report.passed
    assert report.echo.counts["flipped"] == 0
