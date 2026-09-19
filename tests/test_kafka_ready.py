"""The topic-readiness wait, driven by fake admin and consumer clients, for both metadata shapes."""
import pytest

from tracewake.bus import KafkaBus, _partition_view, _topic_name


class FakeAdmin:
    def __init__(self, descriptions):
        self.descriptions = list(descriptions)  # one per call; the last one repeats

    def describe_topics(self, topics):
        return self.descriptions.pop(0) if len(self.descriptions) > 1 else self.descriptions[0]

    def close(self):
        return None


class FakeConsumer:
    def __init__(self, fail_first=0):
        self.fail_first = fail_first
        self.calls = 0

    def end_offsets(self, tps):
        self.calls += 1
        if self.calls <= self.fail_first:
            from kafka.errors import KafkaError
            raise KafkaError("not yet")
        return {tp: 0 for tp in tps}

    def close(self):
        return None


def _bus(descriptions, consumer=None):
    bus = KafkaBus("nowhere:9092", ready_timeout_s=3)
    bus._admin = lambda: FakeAdmin(descriptions)
    bus._consumer = lambda: consumer or FakeConsumer()
    return bus


NEW_STYLE = [{"name": "t", "partitions": [{"partition_index": 0, "leader_id": 0}, {"partition_index": 1, "leader_id": 0},
                                          {"partition_index": 2, "leader_id": 0}]}]
OLD_STYLE = [{"topic": "t", "partitions": [{"partition": 0, "leader": 0}, {"partition": 1, "leader": 0},
                                           {"partition": 2, "leader": 0}]}]


def test_field_name_normalisation():
    assert _topic_name({"name": "a"}) == "a" and _topic_name({"topic": "b"}) == "b"
    assert _partition_view({"partition_index": 2, "leader_id": 0}) == {"partition": 2, "leader": 0}
    assert _partition_view({"partition": 1, "leader": -1}) == {"partition": 1, "leader": -1}


@pytest.mark.parametrize("desc", [NEW_STYLE, OLD_STYLE])
def test_ready_when_every_partition_has_a_leader(desc):
    assert _bus([desc])._wait_ready("t", 3) == 3


def test_waits_through_leaderless_and_partial_metadata():
    empty = [{"name": "t", "partitions": []}]
    leaderless = [{"name": "t", "partitions": [{"partition_index": 0, "leader_id": -1}] + NEW_STYLE[0]["partitions"][1:]}]
    bus = _bus([empty, leaderless, NEW_STYLE])
    assert bus._wait_ready("t", 3) == 3


def test_waits_until_end_offsets_can_be_served():
    consumer = FakeConsumer(fail_first=2)
    bus = _bus([NEW_STYLE], consumer)
    assert bus._wait_ready("t", 3) == 3
    assert consumer.calls == 3


def test_times_out_with_a_reason():
    bus = _bus([[{"name": "t", "partitions": [{"partition_index": 0, "leader_id": -1}]}]])
    bus.ready_timeout_s = 0.6
    with pytest.raises(TimeoutError, match="leaderless: \\[0\\]"):
        bus._wait_ready("t", 1)
