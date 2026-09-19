"""Test configuration.

If a sibling checkout of governed-agent-orchestrator exists and the package is not installed, put it
on the path so the cross-check tests (TRACEWAKE's constraint semantics against the original guard,
the Recorder against the real DecisionTrace) can run locally as they do in CI.
"""
from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
FIXTURES = ROOT / "fixtures" / "orchestrator"
POLICIES = ROOT / "policies"

if importlib.util.find_spec("orchestrator") is None:
    sibling = ROOT.parent / "governed-agent-orchestrator"
    if (sibling / "orchestrator" / "guard.py").exists():
        sys.path.insert(0, str(sibling))


@pytest.fixture(scope="session")
def fixtures_dir() -> Path:
    return FIXTURES


@pytest.fixture(scope="session")
def policies_dir() -> Path:
    return POLICIES
