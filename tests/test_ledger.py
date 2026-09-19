from tracewake.bus import MemoryBus
from tracewake.envelope import digest_envelopes, envelopes_for
from tracewake.ledger import WakeLedger
from tracewake.recorder import Recorder, load_trace_dir

POLICY = "p1"


def publish_fixtures(bus, topic, fixtures_dir):
    runs = load_trace_dir(str(fixtures_dir))
    rec = Recorder(bus, topic, policy_id=POLICY)
    for run_id in sorted(runs):
        rec.publish_events(runs[run_id])
    return runs


def test_ingest_counts_and_digests_match_the_source(fixtures_dir):
    bus = MemoryBus()
    runs = publish_fixtures(bus, "t", fixtures_dir)
    ledger = WakeLedger()
    stats = ledger.ingest(bus, "t")
    total = sum(len(v) for v in runs.values())
    assert (stats.consumed, stats.inserted, stats.duplicates, stats.invalid) == (total, total, 0, 0)
    assert ledger.count() == total
    assert ledger.digests() == {r: digest_envelopes(envelopes_for(ev, policy_id=POLICY)) for r, ev in runs.items()}
    assert set(ledger.runs()) == set(runs)


def test_duplicate_deliveries_change_nothing(fixtures_dir):
    bus = MemoryBus()
    runs = publish_fixtures(bus, "t", fixtures_dir)
    ledger = WakeLedger()
    ledger.ingest(bus, "t")
    before = ledger.digests()
    first = sorted(runs)[0]
    Recorder(bus, "t", policy_id=POLICY).publish_events(runs[first])
    stats = ledger.ingest(bus, "t")
    assert stats.inserted == 0
    assert stats.duplicates == sum(len(v) for v in runs.values()) + len(runs[first])
    assert ledger.digests() == before


def test_gaps_are_reported(fixtures_dir):
    bus = MemoryBus()
    runs = load_trace_dir(str(fixtures_dir))
    run_id = max(runs, key=lambda r: len(runs[r]))  # the longest run, so seq 3 is not its last event
    Recorder(bus, "t").publish_events([e for e in runs[run_id] if e["seq"] != 3])
    ledger = WakeLedger()
    ledger.ingest(bus, "t")
    assert ledger.gaps(run_id) == [3]
    assert ledger.summary(run_id)["gaps"] == [3]


def test_invalid_messages_are_counted_not_stored():
    bus = MemoryBus()
    bus.publish("t", b"k", b"not json at all")
    bus.publish("t", b"k", b'{"run_id": "r", "seq": 0, "ts": "t", "type": "note", "data": {}}')
    ledger = WakeLedger()
    stats = ledger.ingest(bus, "t")
    assert (stats.consumed, stats.inserted, stats.invalid) == (2, 0, 2)
    assert ledger.count() == 0


def test_summary_and_reviewer_queries(fixtures_dir):
    bus = MemoryBus()
    publish_fixtures(bus, "t", fixtures_dir)
    ledger = WakeLedger()
    ledger.ingest(bus, "t")
    statuses = {ledger.summary(r)["status"] for r in ledger.runs()}
    assert "run_finished" in statuses
    irreversible = ledger.calls(effect="irreversible", outcome="ALLOWED")
    assert irreversible and all(c["capability"] == "send_report" for c in irreversible)
    assert any(a["decision"] == "approval_granted" for a in ledger.approvals())
    assert all(ledger.summary(r)["policy_ids"] == [POLICY] for r in ledger.runs())


def test_reset_then_rebuild_from_offset_zero(fixtures_dir):
    bus = MemoryBus()
    publish_fixtures(bus, "t", fixtures_dir)
    ledger = WakeLedger()
    ledger.ingest(bus, "t")
    digests = ledger.digests()
    ledger.reset()
    assert ledger.count() == 0
    ledger.ingest(bus, "t")
    assert ledger.digests() == digests


def test_file_backed_ledger_persists(tmp_path, fixtures_dir):
    bus = MemoryBus()
    publish_fixtures(bus, "t", fixtures_dir)
    path = str(tmp_path / "ledger.db")
    ledger = WakeLedger(path)
    ledger.ingest(bus, "t")
    n, digests = ledger.count(), ledger.digests()
    ledger.close()
    reopened = WakeLedger(path)
    assert reopened.count() == n and reopened.digests() == digests
    reopened.close()


def test_events_round_trip_the_orchestrator_shape(fixtures_dir):
    bus = MemoryBus()
    runs = publish_fixtures(bus, "t", fixtures_dir)
    ledger = WakeLedger()
    ledger.ingest(bus, "t")
    for run_id, events in runs.items():
        assert ledger.events(run_id) == events
