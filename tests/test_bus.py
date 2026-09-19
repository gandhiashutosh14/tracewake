from tracewake.bus import MemoryBus, partition_for


def test_same_key_stays_on_one_partition_in_order():
    bus = MemoryBus(partitions=3)
    for i in range(10):
        bus.publish("t", b"run-1", str(i).encode())
    msgs = [m for m in bus.consume("t") if m.key == b"run-1"]
    assert len({m.partition for m in msgs}) == 1
    assert [m.offset for m in msgs] == list(range(10))
    assert [m.value for m in msgs] == [str(i).encode() for i in range(10)]


def test_different_keys_spread_over_partitions():
    bus = MemoryBus(partitions=3)
    for i in range(60):
        bus.publish("t", f"run-{i}".encode(), b"x")
    assert len({m.partition for m in bus.consume("t")}) == 3
    assert partition_for(b"run-1", 3) == partition_for(b"run-1", 3)


def test_end_offsets_and_idempotent_topic_creation():
    bus = MemoryBus(partitions=2)
    bus.ensure_topic("t")
    bus.ensure_topic("t", partitions=5)  # ignored: the topic already exists
    assert bus.end_offsets("t") == {0: 0, 1: 0}
    bus.publish("t", b"a", b"1")
    assert sum(bus.end_offsets("t").values()) == 1


def test_unknown_topic_is_empty():
    assert MemoryBus().consume("nothing") == []
    assert MemoryBus().end_offsets("nothing") == {}


def test_messages_without_a_key_are_spread_round_robin():
    bus = MemoryBus(partitions=2)
    for _ in range(4):
        bus.publish("t", None, b"x")
    assert bus.end_offsets("t") == {0: 2, 1: 2}
