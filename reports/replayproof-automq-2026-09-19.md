# ReplayProof against AutoMQ 1.7.4

> Produced by the `ReplayProof against AutoMQ` job of GitHub Actions run [35420305967](https://github.com/gandhiashutosh14/tracewake/actions/runs/35420305967) on an `ubuntu-latest` runner: AutoMQ `automqinc/automq:1.7.4` (single node, KRaft) with MinIO as object storage from `docker/compose.yaml`, reached through kafka-python, which reported protocol version 3.9 in the handshake. The broker log of the run contains no rejected batch. The report file is the job's artifact, unedited apart from this note.

**PASSED**: 8 of 8 checks. Generated 2026-09-19T04:05:29+00:00 at revision `317c279` with `tracewake proof --bootstrap localhost:9092 --topic tracewake.proof.1789790726`.

Log: KafkaBus on localhost:9092 (kafka-python). Python 3.12.14 on Linux-6.17.0-1022-azure-x86_64-with-glibc2.39; tracewake 0.1.0a1, kafka-python 3.0.11.

## Numbers

| Measure | Value |
|---|---|
| Runs published | 4 |
| Events published | 40 |
| Publish time | 0.991 s (40 events/s) |
| Ledger rebuild time (first read from offset 0) | 0.136 s |
| Duplicate deliveries injected | 3 |
| Decisions replayed | 7 |

## Checks

| Check | Result | Detail |
|---|---|---|
| every published event reached the ledger | pass | published 40, consumed 40, inserted 40, invalid 0 |
| no run has a gap in its sequence numbers | pass | 4 runs, gaps: 0 |
| ledger content equals the published events, run by run | pass | 4 of 4 run digests match |
| ledger is identical after being destroyed and rebuilt from offset 0 | pass | rebuilt 40 events; digests equal: True |
| a run delivered twice changes nothing | pass | re-published 3 events; inserted 0, duplicates seen 43 |
| replay reproduces every recorded decision under the old policy | pass | 7 decisions recomputed, 0 mismatches |
| replay from the ledger equals replay from the source events | pass | 7 rows compared |
| the policy change is visible in the replay | pass | 2 flipped, 0 need evidence |

# PolicyEcho result

Old policy `cc9add63cb2c` (v1.json) -> new policy `2dc3e7fa29a9` (v2.json). Generated 2026-09-19T04:05:29+00:00.

## What changed in the policy

- send_report.recipient: {"allowed_domains": ["example.com"]} -> {"allowed_domains": ["example.org"]}
- top_genres_by_tracks_sold.top_n: {"max": 50, "min": 1} -> {"max": 2, "min": 1}

## Result

7 recorded decisions: **5 unchanged, 2 flipped, 0 need evidence**. Faithfulness check: 7 decisions recomputed under the old policy, 0 disagreed with the record. Budget denials are runtime state, not policy, and were not re-decided (2).

| run | seq | level | step | capability | old -> new | class | why |
|---|---|---|---|---|---|---|---|
| 1a93d14799 | 2 | plan | s4 | send_report | DENIED -> DENIED | unchanged |  |
| 5cdb3161bc | 2 | plan | s4 | send_report | DENIED -> ALLOWED | flipped | no longer applies: Input 'recipient' is 'partner@example.org', which is not an address in an allowed domain (example.com). |
| 805eb5d59e | 5 | call | s1 | revenue_by_year | ALLOWED -> ALLOWED | unchanged |  |
| 805eb5d59e | 9 | call | s2 | forecast_next_year | ALLOWED -> ALLOWED | unchanged |  |
| 805eb5d59e | 13 | call | s3 | draft_summary | ALLOWED -> ALLOWED | unchanged |  |
| 805eb5d59e | 22 | call | s4 | send_report | ALLOWED -> DENIED | flipped | Input 'recipient' is 'finance@example.com', which is not an address in an allowed domain (example.org). |
| demo-exhausted | 2 | call | s1 | revenue_by_year | ALLOWED -> ALLOWED | unchanged |  |
