"""KafkaBus behaviour that can be checked without a broker: a refused send must not be silent."""
import pytest

from tracewake.bus import KafkaBus


class FakeFuture:
    def __init__(self, error=None):
        self.error = error

    def get(self, timeout=None):
        if self.error:
            raise self.error
        return "ok"


class FakeProducer:
    def __init__(self, errors):
        self.errors, self.sent, self.flushed = list(errors), [], 0

    def send(self, topic, key=None, value=None):
        self.sent.append((topic, key, value))
        return FakeFuture(self.errors.pop(0) if self.errors else None)

    def flush(self):
        self.flushed += 1

    def close(self):
        return None


def test_flush_raises_when_the_broker_refused_a_send():
    bus = KafkaBus("nowhere:9092")
    bus._producer = FakeProducer([None, RuntimeError("OutOfOrderSequenceException"), None])
    for i in range(3):
        bus.publish("t", b"k", str(i).encode())
    with pytest.raises(RuntimeError, match="send 2 of 3 was not acknowledged"):
        bus.flush()
    assert bus._producer.flushed == 1
    assert bus._pending == []  # nothing is silently carried over


def test_flush_passes_when_every_send_is_acknowledged():
    bus = KafkaBus("nowhere:9092")
    bus._producer = FakeProducer([])
    bus.publish("t", b"k", b"v")
    bus.publish("t", None, b"w")
    bus.flush()
    assert [s[2] for s in bus._producer.sent] == [b"v", b"w"]
    assert bus._pending == []


def test_flush_without_a_producer_is_a_no_op():
    KafkaBus("nowhere:9092").flush()
