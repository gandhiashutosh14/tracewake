"""The log. Two implementations of one small interface.

``MemoryBus`` is an in-process log with partitions, keys and offsets, so every test and the
``tracewake demo`` command run without a broker. ``KafkaBus`` is the same interface on
kafka-python, which is how the CI proof runs against a real AutoMQ cluster. Both preserve the one
property TRACEWAKE depends on: messages with the same key are appended, and read back, in order.
"""
from __future__ import annotations

import time
import zlib
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Protocol


@dataclass(frozen=True)
class Message:
    topic: str
    partition: int
    offset: int
    key: Optional[bytes]
    value: bytes


class Bus(Protocol):
    def ensure_topic(self, topic: str, partitions: Optional[int] = None) -> None: ...
    def publish(self, topic: str, key: Optional[bytes], value: bytes) -> None: ...
    def flush(self) -> None: ...
    def consume(self, topic: str) -> List[Message]: ...
    def end_offsets(self, topic: str) -> Dict[int, int]: ...
    def describe(self) -> str: ...
    def close(self) -> None: ...


def partition_for(key: Optional[bytes], partitions: int, fallback: int = 0) -> int:
    if key is None:
        return fallback % partitions
    return zlib.crc32(key) % partitions


class MemoryBus:
    """An in-memory log: topics of partitions of append-only message lists."""

    def __init__(self, partitions: int = 3):
        self.default_partitions = partitions
        self._topics: Dict[str, List[List[Message]]] = {}
        self._round_robin = 0

    def ensure_topic(self, topic: str, partitions: Optional[int] = None) -> None:
        if topic not in self._topics:
            self._topics[topic] = [[] for _ in range(partitions or self.default_partitions)]

    def publish(self, topic: str, key: Optional[bytes], value: bytes) -> None:
        self.ensure_topic(topic)
        parts = self._topics[topic]
        p = partition_for(key, len(parts), self._round_robin)
        if key is None:
            self._round_robin += 1
        parts[p].append(Message(topic, p, len(parts[p]), key, value))

    def flush(self) -> None:
        return None

    def consume(self, topic: str) -> List[Message]:
        parts = self._topics.get(topic, [])
        out: List[Message] = []
        for p in parts:
            out.extend(p)
        return out

    def end_offsets(self, topic: str) -> Dict[int, int]:
        return {i: len(p) for i, p in enumerate(self._topics.get(topic, []))}

    def describe(self) -> str:
        return f"MemoryBus (in-process, {self.default_partitions} partitions per topic)"

    def close(self) -> None:
        return None


class KafkaBus:
    """The same interface on a Kafka-compatible broker (AutoMQ in CI) through kafka-python."""

    def __init__(self, bootstrap: str, partitions: int = 3, client_id: str = "tracewake",
                 request_timeout_ms: int = 30000, ready_timeout_s: float = 60.0):
        self.bootstrap = bootstrap
        self.default_partitions = partitions
        self.client_id = client_id
        self.request_timeout_ms = request_timeout_ms
        self.ready_timeout_s = ready_timeout_s
        self._producer = None
        self._pending: List[object] = []
        self._known: Dict[str, int] = {}

    # ------------------------------------------------------------------
    def _admin(self):
        from kafka import KafkaAdminClient
        return KafkaAdminClient(bootstrap_servers=self.bootstrap, client_id=self.client_id + "-admin",
                                request_timeout_ms=self.request_timeout_ms)

    def ensure_topic(self, topic: str, partitions: Optional[int] = None) -> None:
        """Create the topic if needed, then wait until every partition has a leader that serves offsets.

        A broker that keeps its logs on object storage (AutoMQ) can take a moment after creation
        before a partition accepts writes; publishing into that window loses batches. So the topic is
        not considered ready until the broker reports a leader for every partition and a consumer can
        read end offsets from all of them.
        """
        from kafka.admin import NewTopic
        from kafka.errors import TopicAlreadyExistsError
        wanted = partitions or self.default_partitions
        admin = self._admin()
        try:
            admin.create_topics([NewTopic(name=topic, num_partitions=wanted, replication_factor=1)])
        except TopicAlreadyExistsError:
            wanted = 0  # existing topic: accept however many partitions it has
        finally:
            admin.close()
        self._known[topic] = self._wait_ready(topic, wanted)

    def _wait_ready(self, topic: str, wanted: int) -> int:
        from kafka import TopicPartition
        from kafka.errors import KafkaError
        deadline = time.monotonic() + self.ready_timeout_s
        last = "no metadata yet"
        while time.monotonic() < deadline:
            admin = self._admin()
            try:
                desc = admin.describe_topics([topic])
            except KafkaError as e:  # noqa: PERF203
                desc, last = [], f"describe failed: {e}"
            finally:
                admin.close()
            parts = [p for t in desc if t.get("topic") == topic for p in t.get("partitions", [])]
            leaderless = [p["partition"] for p in parts if p.get("leader", -1) in (-1, None)]
            if parts and not leaderless and (wanted == 0 or len(parts) >= wanted):
                consumer = self._consumer()
                try:
                    tps = [TopicPartition(topic, p["partition"]) for p in parts]
                    consumer.end_offsets(tps)
                    return len(parts)
                except KafkaError as e:
                    last = f"end offsets failed: {e}"
                finally:
                    consumer.close()
            else:
                last = f"{len(parts)} partitions, leaderless: {leaderless}"
            time.sleep(0.25)
        raise TimeoutError(f"topic {topic} was not ready within {self.ready_timeout_s}s ({last})")

    def publish(self, topic: str, key: Optional[bytes], value: bytes) -> None:
        if self._producer is None:
            from kafka import KafkaProducer
            self._producer = KafkaProducer(bootstrap_servers=self.bootstrap, client_id=self.client_id,
                                           acks="all", retries=20, retry_backoff_ms=200, linger_ms=20,
                                           max_in_flight_requests_per_connection=1,
                                           request_timeout_ms=self.request_timeout_ms)
        self._pending.append(self._producer.send(topic, key=key, value=value))

    def flush(self) -> None:
        """Wait for every send to be acknowledged; raise on the first one the broker refused.

        kafka-python reports a failed send only through the future it returned, so a publisher
        that ignores those futures can lose messages silently. This one does not.
        """
        if self._producer is None:
            return
        self._producer.flush()
        pending, self._pending = self._pending, []
        for i, future in enumerate(pending):
            try:
                future.get(timeout=self.request_timeout_ms / 1000)
            except Exception as e:  # noqa: BLE001
                raise RuntimeError(f"send {i + 1} of {len(pending)} was not acknowledged: {type(e).__name__}: {e}") from e

    def _consumer(self):
        from kafka import KafkaConsumer
        return KafkaConsumer(bootstrap_servers=self.bootstrap, client_id=self.client_id + "-consumer",
                             group_id=None, enable_auto_commit=False, auto_offset_reset="earliest",
                             request_timeout_ms=self.request_timeout_ms)

    def consume(self, topic: str) -> List[Message]:
        """Every message currently in the topic, from offset 0, in (partition, offset) order."""
        from kafka import TopicPartition
        consumer = self._consumer()
        try:
            partitions = consumer.partitions_for_topic(topic)
            if not partitions:
                return []
            known = self._known.get(topic)
            if known and len(partitions) < known:
                raise RuntimeError(f"topic {topic} shows {len(partitions)} partitions but {known} were created")
            tps = [TopicPartition(topic, p) for p in sorted(partitions)]
            consumer.assign(tps)
            consumer.seek_to_beginning(*tps)
            ends = consumer.end_offsets(tps)
            out: List[Message] = []
            deadline = time.monotonic() + self.request_timeout_ms / 1000 * 4
            while any(consumer.position(tp) < ends[tp] for tp in tps):
                if time.monotonic() > deadline:
                    raise TimeoutError(f"consumer did not reach the end offsets of {topic} in time")
                for tp, records in consumer.poll(timeout_ms=1000).items():
                    for r in records:
                        out.append(Message(topic, tp.partition, r.offset, r.key, r.value))
            out.sort(key=lambda m: (m.partition, m.offset))
            return out
        finally:
            consumer.close()

    def end_offsets(self, topic: str) -> Dict[int, int]:
        from kafka import TopicPartition
        consumer = self._consumer()
        try:
            partitions = consumer.partitions_for_topic(topic) or set()
            tps = [TopicPartition(topic, p) for p in sorted(partitions)]
            return {tp.partition: off for tp, off in consumer.end_offsets(tps).items()} if tps else {}
        finally:
            consumer.close()

    def describe(self) -> str:
        return f"KafkaBus on {self.bootstrap} (kafka-python)"

    def close(self) -> None:
        if self._producer is not None:
            self._producer.flush()
            self._producer.close()
            self._producer = None


def publish_all(bus: Bus, topic: str, items: Iterable[tuple]) -> int:
    n = 0
    for key, value in items:
        bus.publish(topic, key, value)
        n += 1
    bus.flush()
    return n
