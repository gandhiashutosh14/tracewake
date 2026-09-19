import json

from tracewake.echo import ALLOWED, DENIED, FLIPPED, NEEDS_EVIDENCE, UNCHANGED, echo, extract_decisions, replay
from tracewake.policy import CapabilityPolicy, Policy
from tracewake.recorder import load_trace_dir


def load(policies_dir):
    return Policy.load(str(policies_dir / "v1.json")), Policy.load(str(policies_dir / "v2.json"))


def run_named(fixtures_dir, name):
    runs = load_trace_dir(str(fixtures_dir))
    prov = json.loads((fixtures_dir / "PROVENANCE.json").read_text(encoding="utf-8"))
    run_id = next(r["run_id"] for r in prov["runs"] if r["file"] == f"{name}.jsonl")
    return runs[run_id]


def test_allowed_run_yields_a_resolved_send_report_decision(fixtures_dir):
    decisions, skipped = extract_decisions(run_named(fixtures_dir, "allowed"))
    send = [d for d in decisions if d.capability == "send_report"]
    assert len(send) == 1
    assert send[0].recorded == ALLOWED and send[0].evidence == "resolved" and send[0].level == "call"
    assert send[0].inputs["recipient"] == "finance@example.com"
    assert skipped["budget_denials"] == 0


def test_plan_level_denial_is_extracted(fixtures_dir):
    decisions, _ = extract_decisions(run_named(fixtures_dir, "denied"))
    plan = [d for d in decisions if d.level == "plan"]
    assert len(plan) == 1
    assert plan[0].recorded == DENIED and plan[0].capability == "send_report" and plan[0].evidence == "literal"
    assert any("allowed domain" in v for v in plan[0].recorded_violations)


def test_budget_denials_are_skipped_not_replayed(fixtures_dir):
    decisions, skipped = extract_decisions(run_named(fixtures_dir, "exhausted"))
    assert skipped["budget_denials"] >= 1
    assert all(d.recorded == ALLOWED for d in decisions)


def test_replay_v1_to_v2_reports_the_flips(fixtures_dir, policies_dir):
    v1, v2 = load(policies_dir)
    report = echo(load_trace_dir(str(fixtures_dir)), v1, v2)
    assert report.faithfulness["mismatches"] == 0 and report.faithfulness["checked"] > 0
    by = {(r.decision.capability, r.decision.inputs.get("recipient")): r for r in report.rows if r.decision.capability == "send_report"}
    finance = by[("send_report", "finance@example.com")]
    partner = by[("send_report", "partner@example.org")]
    assert finance.transition == "ALLOWED -> DENIED" and finance.classification == FLIPPED
    assert partner.transition == "DENIED -> ALLOWED" and partner.classification == FLIPPED
    assert report.counts[FLIPPED] >= 2
    assert any(line.startswith("send_report.recipient:") for line in report.diff)


def test_replay_under_the_same_policy_changes_nothing(fixtures_dir, policies_dir):
    v1, _ = load(policies_dir)
    report = echo(load_trace_dir(str(fixtures_dir)), v1, v1)
    assert report.counts[FLIPPED] == 0 and report.diff == []
    assert all(r.classification == UNCHANGED for r in report.rows)


def test_needs_evidence_when_the_log_lacks_a_value():
    events = [
        {"seq": 1, "ts": "t", "run_id": "r", "type": "plan_accepted", "data": {"plan": {"steps": [
            {"id": "s1", "capability": "draft_summary", "inputs": {"objective": "o", "facts": "$s0.summary"}}]}}},
        {"seq": 2, "ts": "t", "run_id": "r", "type": "tool_call_denied",
         "data": {"step": "s1", "capability": "draft_summary", "reason": "constraint",
                  "violations": ["Input 'facts' is 900 characters long; the maximum is 400."]}},
    ]
    old = Policy([CapabilityPolicy("draft_summary", constraints={"facts": {"max_length": 400}})])
    new = Policy([CapabilityPolicy("draft_summary", constraints={"facts": {"max_length": 1000}})])
    decisions, _ = extract_decisions(events)
    assert decisions[0].evidence == "partial" and decisions[0].missing == ["facts"]
    rows = replay(decisions, old, new)
    assert rows[0].classification == NEEDS_EVIDENCE and rows[0].faithful is None
    assert "does not contain the value of facts" in rows[0].notes[0]


def test_fallback_calls_only_see_the_inputs_the_fallback_declares():
    events = [
        {"seq": 1, "ts": "t", "run_id": "r", "type": "step_started",
         "data": {"step": "s1", "capability": "primary", "inputs": {"query": "x", "extra": "yyyy"}}},
        {"seq": 2, "ts": "t", "run_id": "r", "type": "tool_call_allowed",
         "data": {"step": "s1", "capability": "backup", "effect": "reversible", "via_fallback": True}},
    ]
    old = Policy([CapabilityPolicy("backup", inputs={"query": "string"})])
    new = Policy([CapabilityPolicy("backup", inputs={"query": "string"}, constraints={"extra": {"max_length": 1}})])
    decisions, _ = extract_decisions(events)
    rows = replay(decisions, old, new)
    assert rows[0].classification == UNCHANGED and rows[0].faithful is True


def test_capability_removed_from_policy_counts_as_denied():
    events = [
        {"seq": 1, "ts": "t", "run_id": "r", "type": "step_started", "data": {"step": "s1", "capability": "x", "inputs": {}}},
        {"seq": 2, "ts": "t", "run_id": "r", "type": "tool_call_allowed", "data": {"step": "s1", "capability": "x"}},
    ]
    decisions, _ = extract_decisions(events)
    rows = replay(decisions, Policy([CapabilityPolicy("x")]), Policy([]))
    assert rows[0].transition == "ALLOWED -> DENIED" and rows[0].classification == FLIPPED


def test_report_renders_markdown_and_json(fixtures_dir, policies_dir):
    v1, v2 = load(policies_dir)
    report = echo(load_trace_dir(str(fixtures_dir)), v1, v2)
    text = report.render_markdown()
    assert "flipped" in text and "ALLOWED -> DENIED" not in text.split("## Result")[0]
    payload = json.loads(json.dumps(report.to_dict()))
    assert payload["counts"]["flipped"] == report.counts[FLIPPED]
    assert payload["rows"][0]["transition"]
