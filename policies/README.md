# Policies

Both files use the governed-agent-orchestrator's `capabilities.json` format. TRACEWAKE reads only
the parts that govern a decision: each capability's `constraints`, `effect` and
`requires_approval`. The policy id printed in reports is a hash of exactly those parts.

- `v1.json`: the orchestrator's catalog as recorded in the fixtures (reports may go to
  `example.com`; ranking queries may return up to 50 rows).
- `v2.json`: the changed policy for the demo. Reports may go to `example.org` instead, and
  `top_genres_by_tracks_sold` is capped at 2 rows.

Any two files in this format can be replayed against each other with
`tracewake echo --from A.json --to B.json`.
