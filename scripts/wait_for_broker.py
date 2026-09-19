"""Wait until a Kafka-compatible broker answers, then print what it says about itself.

    python scripts/wait_for_broker.py localhost:9092 --timeout 240
"""
from __future__ import annotations

import argparse
import sys
import time


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("bootstrap")
    ap.add_argument("--timeout", type=int, default=240)
    args = ap.parse_args()
    from kafka import KafkaAdminClient
    from kafka.errors import KafkaError

    deadline = time.monotonic() + args.timeout
    attempt = 0
    while True:
        attempt += 1
        try:
            admin = KafkaAdminClient(bootstrap_servers=args.bootstrap, request_timeout_ms=10000)
            info = admin.describe_cluster()
            version = admin._client.check_version() if hasattr(admin, "_client") else "?"
            admin.close()
            print(f"broker ready after {attempt} attempts: cluster {info.get('cluster_id')}, "
                  f"{len(info.get('brokers', []))} broker(s), protocol version {version}")
            return 0
        except (KafkaError, OSError, AssertionError) as e:  # noqa: PERF203
            if time.monotonic() > deadline:
                print(f"broker not reachable after {args.timeout}s: {e}", file=sys.stderr)
                return 1
            time.sleep(3)


if __name__ == "__main__":
    sys.exit(main())
