# Third-party software and attribution

TRACEWAKE's own code is MIT-licensed (see `LICENSE`). It runs against, and is tested with, the
following projects, none of which is bundled in this repository:

- **AutoMQ** (https://github.com/AutoMQ/automq), Apache License 2.0. The Kafka-compatible event
  log used in the CI verification. `docker/compose.yaml` is adapted from AutoMQ's
  `docker/docker-compose.yaml` at tag `1.7.4` (Apache License 2.0); the changes are noted in the
  file header.
- **Apache Kafka** (https://kafka.apache.org), Apache License 2.0. AutoMQ implements the Kafka
  protocol; TRACEWAKE speaks to it through an ordinary Kafka client.
- **kafka-python** (https://github.com/dpkp/kafka-python), Apache License 2.0. The Python client.
- **MinIO** (https://min.io), GNU AGPL v3. The S3-compatible object store used only inside the CI
  compose stack as AutoMQ's storage; nothing from MinIO is redistributed here.
- **governed-agent-orchestrator** (https://github.com/gandhiashutosh14/governed-agent-orchestrator),
  MIT, by the same author. The recorded traces under `fixtures/` were produced by it, and
  `tracewake/policy.py` re-implements its argument-constraint semantics so that replay decisions
  match the original guard exactly. `policies/v1.json` is its `capabilities.json`.

"Kafka" is a trademark of the Apache Software Foundation. TRACEWAKE is an independent project and
is not affiliated with or endorsed by AutoMQ or the Apache Software Foundation.
