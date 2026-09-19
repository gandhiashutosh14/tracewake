import json

from tracewake.cli import main


def test_demo_writes_both_reports(tmp_path, capsys):
    out, js = tmp_path / "demo.md", tmp_path / "demo.json"
    assert main(["demo", "--out", str(out), "--json", str(js)]) == 0
    assert "**PASSED**" in out.read_text(encoding="utf-8")
    payload = json.loads(js.read_text(encoding="utf-8"))
    assert payload["passed"] is True and payload["echo"]["counts"]["flipped"] >= 2
    assert "ReplayProof" in capsys.readouterr().out


def test_echo_from_fixtures(tmp_path):
    out = tmp_path / "echo.md"
    assert main(["echo", "--out", str(out)]) == 0
    assert "ALLOWED -> DENIED" in out.read_text(encoding="utf-8")


def test_demo_without_fixtures_fails_clearly(tmp_path):
    assert main(["demo", "--fixtures", str(tmp_path / "missing")]) == 2
