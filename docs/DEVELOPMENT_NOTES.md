# Development notes

How TRACEWAKE was built. It was written from scratch on 2026-09-19; there is no earlier history.

## Why this project exists

The author's other repositories are about agents that act under limits: a runtime that checks
every tool call ([governed-agent-orchestrator](https://github.com/gandhiashutosh14/governed-agent-orchestrator))
and a replay of recorded decisions under changed rules ([regimeforge](https://github.com/gandhiashutosh14/regimeforge)).
A note introducing AutoMQ, an open-source Kafka on object storage, raised the question of what a
Kafka log looks like when the events are agent decisions. AutoMQ's own writing argues for exactly
that, but neither of its repositories contains an agent example. TRACEWAKE is the smallest honest
answer: publish the orchestrator's journal to the log, rebuild it, replay it, and prove it.

## Design decisions

- **The envelope carries the policy id.** The orchestrator does not record which catalog was in
  force. The Recorder stamps a hash of the catalog's governing content (constraints, effect classes,
  approval requirements) on every envelope, so the ledger can answer "under which rules?".
- **Replay reuses the guard's exact semantics.** `tracewake/policy.py` re-implements the
  orchestrator's `check_constraints` line for line rather than translating constraints into another
  rule language. A test compares the two implementations over a grid of specs and values when the
  orchestrator is importable, which CI ensures.
- **Reproduce the past before predicting the change.** Every decision is first recomputed under the
  old policy and compared with the record. A mismatch would mean the replay is not faithful, and the
  proof fails on it.
- **`needs-evidence` is a verdict, not an error.** When the log lacks a value the new policy would
  need, the row says so. The orchestrator's refusal event does not carry the resolved arguments,
  so such calls end there; the fix belongs upstream and is on the roadmap.
- **Plan-level refusals count.** The orchestrator's validator rejects a plan whose literal arguments
  violate a constraint before anything runs, and journals the reasons in `plan_proposed.errors`.
  Those are recorded decisions too, and one of them is the `DENIED -> ALLOWED` flip in the demo.
- **Budget denials are skipped.** A call budget is runtime state, not policy; the report counts them
  and says why they were not re-decided.
- **Two logs, one interface.** The in-memory log keeps every unit test broker-free; the Kafka log is
  the same interface on kafka-python, exercised in CI against AutoMQ.
- **Fixtures are recorded, not written.** `scripts/make_fixtures.py` runs the orchestrator's demo
  scenarios plus one extra objective and writes the journals unchanged, with a provenance file
  naming the orchestrator revision and catalog hash.

## What the tests caught

- The replay loop reused the name of its policy argument for the recomputed outcome, so the second
  decision in every run was replayed against a string instead of a policy. Nine tests failed at
  once; the outcome variable was renamed.
- The gap test removed sequence number 3 from the first run in sorted order, which had only three
  events, so nothing was missing. It now uses the longest run.
- The first replay table printed the old and new outcomes in separate columns; a test that looked
  for the transition text failed, and the table gained an `old -> new` column, which reads better.
- Reports embedded absolute local paths of the policy files; they now print the file names.
- The first CI run against AutoMQ failed before the broker started: the `minio/minio` and
  `minio/mc` image tags in AutoMQ's 1.7.4 compose file were no longer on Docker Hub. The compose
  file now pulls the official MinIO images from quay.io, pinned, and says why.
- **The proof then passed once and failed on the next push with 12 of 40 events in the ledger.**
  The broker log explained it: AutoMQ loads a new partition's object-storage log about half a
  second after the topic is created, the producer's first batches for one partition hit that
  window, the retries ran out, and the broker rejected the batch with an
  `OutOfOrderSequenceException`. kafka-python reports a refused send only through the future that
  `send()` returns, and the publisher never looked at it, so the loss was silent and the earlier
  pass was luck. Two changes: `ensure_topic` now waits until every partition has a leader that
  serves offsets before anything is published, and `flush()` waits on every send's future and
  raises on the first refusal, with a test that a refused send cannot pass quietly. The proof's
  first check ("every published event reached the ledger") is what caught it.

## Verification

| Check | Result |
|---|---|
| `pytest -q` | 68 passed |
| `tracewake demo` (in-memory log) | 8 of 8 checks; 4 runs, 40 events, 7 decisions, 2 flipped, 0 mismatches ([`reports/demo-memory.md`](../reports/demo-memory.md)) |
| `tracewake proof --bootstrap localhost:9092` against AutoMQ 1.7.4 + MinIO (CI) | 8 of 8 checks, same counts; publish 1.53 s, ledger rebuild 0.15 s ([`reports/replayproof-automq-2026-09-19.md`](../reports/replayproof-automq-2026-09-19.md)) |

## What is and is not claimed

The numbers describe four recorded runs of a deterministic demo on a public sample database. They
show that the ledger and the replay behave as stated on that data and on a real Kafka-protocol log.
They say nothing about AutoMQ's performance or cost, about language-model planners, or about
policies shaped differently from the orchestrator's catalog.
