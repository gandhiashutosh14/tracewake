import itertools

import pytest

from tracewake.policy import CapabilityPolicy, Policy, check_constraints, diff


def test_load_and_policy_id(policies_dir):
    v1, v2 = Policy.load(str(policies_dir / "v1.json")), Policy.load(str(policies_dir / "v2.json"))
    assert len(v1) == 8
    assert v1.policy_id == Policy.load(str(policies_dir / "v1.json")).policy_id
    assert v1.policy_id != v2.policy_id
    assert len(v1.policy_id) == 12
    assert v1.get("send_report").requires_approval and v1.get("send_report").effect == "irreversible"


def test_policy_id_covers_only_what_governs_a_decision():
    a = Policy([CapabilityPolicy("x", inputs={"q": "string"}, constraints={"q": {"max_length": 5}})])
    b = Policy([CapabilityPolicy("x", inputs={"q": "string", "extra": "string"}, constraints={"q": {"max_length": 5}})])
    c = Policy([CapabilityPolicy("x", inputs={"q": "string"}, constraints={"q": {"max_length": 6}})])
    assert a.policy_id == b.policy_id
    assert a.policy_id != c.policy_id


@pytest.mark.parametrize("constraints,inputs,expect_violation", [
    ({"n": {"min": 1, "max": 50}}, {"n": 3}, False),
    ({"n": {"min": 1, "max": 50}}, {"n": 51}, True),
    ({"n": {"min": 1}}, {"n": 0}, True),
    ({"n": {"min": 1}}, {"n": "abc"}, True),
    ({"n": {"min": 1}}, {}, False),                                  # absent inputs are not checked
    ({"n": {"min": 1}}, {"n": "$s1.value"}, False),                  # unresolved references are skipped
    ({"k": {"allowed": ["a", "b"]}}, {"k": "b"}, False),
    ({"k": {"allowed": ["a", "b"]}}, {"k": "c"}, True),
    ({"r": {"allowed_domains": ["Example.com"]}}, {"r": "Finance@EXAMPLE.com"}, False),
    ({"r": {"allowed_domains": ["example.com"]}}, {"r": "x@example.org"}, True),
    ({"r": {"allowed_domains": ["example.com"]}}, {"r": "no-at-sign"}, True),
    ({"c": {"pattern": r"[a-z]+"}}, {"c": "abc"}, False),
    ({"c": {"pattern": r"[a-z]+"}}, {"c": "abc1"}, True),            # whole value must match
    ({"t": {"max_length": 3}}, {"t": "abcd"}, True),
    ({"t": {"max_length": 3}}, {"t": "abc"}, False),
])
def test_check_constraints(constraints, inputs, expect_violation):
    assert bool(check_constraints(constraints, inputs)) is expect_violation


def test_constraint_semantics_match_the_orchestrator_guard():
    guard = pytest.importorskip("orchestrator.guard")
    specs = [{"min": 1, "max": 50}, {"allowed": ["a", 3]}, {"allowed_domains": ["example.com", "Corp.org"]},
             {"pattern": r"[a-z]+\d?"}, {"max_length": 4}, {"min": 0, "max_length": 2}]
    values = [3, 51, 0, "abc", "abc1", "a", "b@example.com", "b@corp.org", "b@evil.org", "nobody", "$s1.x", None, 2.5]
    for spec, value in itertools.product(specs, values):
        ours = check_constraints({"v": spec}, {"v": value})
        theirs = guard.check_constraints({"v": spec}, {"v": value})
        assert ours == theirs, (spec, value, ours, theirs)


def test_diff_lists_every_change(policies_dir):
    v1, v2 = Policy.load(str(policies_dir / "v1.json")), Policy.load(str(policies_dir / "v2.json"))
    lines = diff(v1, v2)
    assert any(l.startswith("send_report.recipient:") for l in lines)
    assert any(l.startswith("top_genres_by_tracks_sold.top_n:") for l in lines)
    assert diff(v1, v1) == []
    assert diff(Policy([]), v1) == [f"{n}: added" for n in sorted(v1.names())]


def test_invalid_policies_are_rejected():
    with pytest.raises(ValueError):
        Policy([CapabilityPolicy("x", constraints={"q": {"bogus": 1}})])
    with pytest.raises(ValueError):
        Policy([CapabilityPolicy("x", constraints={"q": {"pattern": "("}})])
