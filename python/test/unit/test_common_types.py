# Copyright 2025 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Tests of the ``confluent_kafka.common`` value types.

Java's tests translated: ``UuidTest`` (all), ``TopicPartitionTest``,
``TopicIdPartitionTest`` (all), ``PartitionInfoTest`` (all), ``KafkaMetricTest``.
Skipped, with the reason at the test's place: the Java-serialization
compatibility tests (a checked-in ``ObjectOutputStream`` blob has no Python
meaning), and ``KafkaMetricTest.testMeasurableValueReturnsZeroWhenNotMeasurable``
(``measurableValue`` is package-private, so not generated). Per public method:
a positional call is a ``TypeError`` and each ``java_forms`` rejection has the
exact message (CLAUDE.md, Python Binding Conventions, Tests and typing).
"""

from __future__ import annotations

import base64
import copy
import pickle
import threading
from typing import Any

import pytest

from confluent_kafka import IllegalArgumentError, IllegalStateError, NoSuchElementError, NullPointerError
from confluent_kafka._java import java_str
from confluent_kafka.common import (
    Cluster,
    KafkaMetric,
    Measurable,
    Metric,
    MetricConfig,
    MetricName,
    Node,
    PartitionInfo,
    TimestampType,
    TopicIdPartition,
    TopicPartition,
    Uuid,
)
from confluent_kafka.common.headers import _headers_to_string, _read_headers

# --------------------------------------------------------------------------- #
# Uuid: UuidTest
# --------------------------------------------------------------------------- #


def test_significant_bits() -> None:
    uid = Uuid(most_sig_bits=34, least_sig_bits=98)
    assert uid.most_significant_bits() == 34
    assert uid.least_significant_bits() == 98


def test_uuid_equality() -> None:
    id1 = Uuid(most_sig_bits=12, least_sig_bits=13)
    id2 = Uuid(most_sig_bits=12, least_sig_bits=13)
    id3 = Uuid(most_sig_bits=24, least_sig_bits=38)
    assert Uuid.ZERO_UUID == Uuid.ZERO_UUID
    assert id1 == id2
    assert id1 != id3
    assert hash(Uuid.ZERO_UUID) == hash(Uuid.ZERO_UUID)
    assert hash(id1) == hash(id2)
    assert hash(id1) != hash(id3)


def test_hash_code() -> None:
    assert hash(Uuid(most_sig_bits=16, least_sig_bits=7)) == 23
    assert hash(Uuid(most_sig_bits=1043, least_sig_bits=20075)) == 19064
    assert hash(Uuid(most_sig_bits=104312423523523, least_sig_bits=200732425676585)) == -2011255899


def test_string_conversion() -> None:
    uid = Uuid.random_uuid()
    assert Uuid.from_string(str=str(uid)) == uid
    assert Uuid.ZERO_UUID == Uuid.from_string(str=str(Uuid.ZERO_UUID))


@pytest.mark.parametrize("repetition", range(100))  # Java @RepeatedTest(100)
def test_random_uuid(repetition: int) -> None:
    random_id = Uuid.random_uuid()
    assert Uuid.ZERO_UUID != random_id
    assert Uuid.METADATA_TOPIC_ID != random_id
    assert not str(random_id).startswith("-")


def test_compare_uuids() -> None:
    id00 = Uuid(most_sig_bits=0, least_sig_bits=0)
    id01 = Uuid(most_sig_bits=0, least_sig_bits=1)
    id10 = Uuid(most_sig_bits=1, least_sig_bits=0)
    assert not id00 < id00 and not id00 > id00 and id00 <= id00
    assert id00 < id01 and id00 < id10
    assert id01 > id00 and id10 > id00
    assert id01 < id10 and id10 > id01
    # Java's longs are signed: a set high bit sorts first.
    assert Uuid(most_sig_bits=-1, least_sig_bits=0) < id00


def test_from_string_with_invalid_input() -> None:
    oversize = base64.urlsafe_b64encode(bytes(32)).rstrip(b"=").decode()
    with pytest.raises(IllegalArgumentError) as exc:
        Uuid.from_string(str=oversize)
    assert str(exc.value) == (
        f"Input string with prefix `{oversize[:24]}` is too long to be decoded as a base64 UUID")
    undersize = base64.urlsafe_b64encode(bytes(4)).rstrip(b"=").decode()
    with pytest.raises(IllegalArgumentError) as exc:
        Uuid.from_string(str=undersize)
    assert str(exc.value) == (
        f"Input string `{undersize}` decoded as 4 bytes, which is not equal to the expected "
        "16 bytes of a base64-encoded UUID")


@pytest.mark.parametrize("text,message", [
    ("AAAA!AAA", "Illegal base64 character 21"),
    ("A", "Last unit does not have enough valid bits"),
    ("AA=", "Input byte array has wrong 4-byte ending unit"),
    ("AAA=x", "Input byte array has incorrect ending byte at 4"),
])
def test_from_string_decodes_with_javas_base64_rules(text: str, message: str) -> None:
    with pytest.raises(IllegalArgumentError) as exc:
        Uuid.from_string(str=text)
    assert str(exc.value) == message


def test_to_array() -> None:
    assert Uuid.to_array(list=None) is None
    other = Uuid.from_string(str="UXyU9i5ARn6W00ON2taeWA")
    assert Uuid.to_array(list=[Uuid.ZERO_UUID, other]) == (Uuid.ZERO_UUID, other)


def test_to_list() -> None:
    assert Uuid.to_list(array=None) is None
    other = Uuid.from_string(str="UXyU9i5ARn6W00ON2taeWA")
    assert Uuid.to_list(array=(Uuid.ZERO_UUID, other)) == [Uuid.ZERO_UUID, other]


def test_uuid_constants() -> None:
    assert Uuid.ONE_UUID == Uuid(most_sig_bits=0, least_sig_bits=1)
    assert Uuid.METADATA_TOPIC_ID is Uuid.ONE_UUID
    assert Uuid.ZERO_UUID == Uuid(most_sig_bits=0, least_sig_bits=0)
    assert Uuid.RESERVED == frozenset({Uuid.ZERO_UUID, Uuid.ONE_UUID})
    assert isinstance(Uuid.RESERVED, frozenset)


def test_uuid_keyword_only() -> None:
    with pytest.raises(TypeError):
        Uuid(0, 0)  # type: ignore[call-arg]
    with pytest.raises(TypeError):
        Uuid.from_string("AAAAAAAAAAAAAAAAAAAAAA")  # type: ignore[call-arg]


# --------------------------------------------------------------------------- #
# TopicPartition: TopicPartitionTest
# --------------------------------------------------------------------------- #


def test_serialization_roundtrip() -> None:
    """TopicPartitionTest.testSerializationRoundtrip, with pickle for Java's
    ObjectOutputStream. (testTopiPartitionSerializationCompatibility reads a
    checked-in Java-serialized file: no Python meaning, not translated.)"""
    orig = TopicPartition(topic="mytopic", partition=5)
    clone = pickle.loads(pickle.dumps(orig))
    assert isinstance(clone, TopicPartition)
    assert clone.partition() == 5 and clone.topic() == "mytopic"
    assert copy.copy(orig) == orig


def test_topic_partition_value_semantics() -> None:
    a = TopicPartition(topic="t", partition=1)
    assert a == TopicPartition(topic="t", partition=1)
    assert hash(a) == hash(TopicPartition(topic="t", partition=1))
    assert a != TopicPartition(topic="t", partition=2)
    assert a != TopicPartition(topic="u", partition=1)
    assert {a: 1}[TopicPartition(topic="t", partition=1)] == 1
    assert str(a) == "t-1"
    assert str(TopicPartition(topic=None, partition=0)) == "null-0"  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        TopicPartition("t", 0)  # type: ignore[call-arg]


# --------------------------------------------------------------------------- #
# TopicIdPartition: TopicIdPartitionTest
# --------------------------------------------------------------------------- #

_TOPIC_ID0 = Uuid(most_sig_bits=-4883993789924556279, least_sig_bits=-5960309683534398572)
_TOPIC_NAME0 = "a_topic_name"
_PARTITION1 = 1
_TOPIC_PARTITION0 = TopicPartition(topic=_TOPIC_NAME0, partition=_PARTITION1)
_TIDP0 = TopicIdPartition(topic_id=_TOPIC_ID0, topic_partition=_TOPIC_PARTITION0)
_TIDP1 = TopicIdPartition(topic_id=_TOPIC_ID0, partition=_PARTITION1, topic=_TOPIC_NAME0)
# Java's `new TopicIdPartition(topicId0, partition1, null)`: `topic` defaults to
# UNSET, so `topic=None` is given and selects that constructor (CLAUDE.md,
# Python Binding Conventions, Signatures).
_NULL_TOPIC = TopicPartition(topic=None, partition=_PARTITION1)  # type: ignore[arg-type]
_TIDP_NULL0 = TopicIdPartition(topic_id=_TOPIC_ID0, partition=_PARTITION1, topic=None)
_TIDP_NULL1 = TopicIdPartition(topic_id=_TOPIC_ID0, topic_partition=_NULL_TOPIC)
_TOPIC_ID1 = Uuid(most_sig_bits=7759286116672424028, least_sig_bits=-5081215629859775948)
_TIDP2 = TopicIdPartition(topic_id=_TOPIC_ID1, partition=_PARTITION1, topic="another_topic_name")
_TIDP_NULL2 = TopicIdPartition(topic_id=_TOPIC_ID1, topic_partition=_NULL_TOPIC)


def test_topic_id_partition_equals() -> None:
    assert _TIDP0 == _TIDP1
    assert _TIDP1 == _TIDP0
    assert _TIDP_NULL0 == _TIDP_NULL1
    assert _TIDP0 != _TIDP2
    assert _TIDP2 != _TIDP0
    assert _TIDP0 != _TIDP_NULL0
    assert _TIDP_NULL0 != _TIDP_NULL2


def test_topic_id_partition_hash_code() -> None:
    assert hash(_TIDP0) == hash(_TIDP1)
    assert hash(_TIDP_NULL0) == hash(_TIDP_NULL1)
    assert hash(_TIDP0) != hash(_TIDP2)
    assert hash(_TIDP0) != hash(_TIDP_NULL0)
    assert hash(_TIDP_NULL0) != hash(_TIDP_NULL2)


def test_topic_id_partition_to_string() -> None:
    assert str(_TIDP0) == "vDiRhkpVQgmtSLnsAZx7lA:a_topic_name-1"
    assert str(_TIDP_NULL0) == "vDiRhkpVQgmtSLnsAZx7lA:null-1"


def test_topic_id_partition_forms() -> None:
    assert (_TIDP1.topic_id(), _TIDP1.topic(), _TIDP1.partition()) == (
        _TOPIC_ID0, _TOPIC_NAME0, _PARTITION1)
    assert _TIDP1.topic_partition() == _TOPIC_PARTITION0
    assert _TIDP_NULL0.topic() is None
    with pytest.raises(IllegalArgumentError) as exc:
        TopicIdPartition(topic_id=_TOPIC_ID0)  # type: ignore[call-overload]
    assert str(exc.value) == (
        "TopicIdPartition() takes one of (topic_id, topic_partition), "
        "(topic_id, partition, topic); got (topic_id)")
    with pytest.raises(IllegalArgumentError) as exc:
        TopicIdPartition(topic_id=_TOPIC_ID0, partition=0)  # type: ignore[call-overload]
    assert str(exc.value).endswith("got (topic_id, partition)")
    with pytest.raises(IllegalArgumentError):
        TopicIdPartition(topic_id=_TOPIC_ID0, topic_partition=_TOPIC_PARTITION0,  # type: ignore[call-overload]
                         partition=1, topic="t")
    with pytest.raises(NullPointerError) as npe:
        TopicIdPartition(topic_id=None, topic_partition=_TOPIC_PARTITION0)  # type: ignore[call-overload]
    assert str(npe.value) == "topicId can not be null"
    with pytest.raises(TypeError):
        TopicIdPartition(_TOPIC_ID0, _TOPIC_PARTITION0)  # type: ignore[call-overload]


# --------------------------------------------------------------------------- #
# PartitionInfo: PartitionInfoTest
# --------------------------------------------------------------------------- #


def test_partition_info_to_string() -> None:
    leader = Node(id=0, host="localhost", port=9092)
    r1 = Node(id=1, host="localhost", port=9093)
    r2 = Node(id=2, host="localhost", port=9094)
    info = PartitionInfo(topic="sample", partition=0, leader=leader, replicas=(leader, r1, r2),
                         in_sync_replicas=(leader, r1), offline_replicas=(r2,))
    assert str(info) == ("Partition(topic = sample, partition = 0, leader = 0, "
                         "replicas = [0,1,2], isr = [0,1], offlineReplicas = [2])")


def test_partition_info_short_form_and_equality() -> None:
    leader = Node(id=0, host="h", port=1)
    info = PartitionInfo(topic="t", partition=1, leader=None, replicas=(leader,),
                         in_sync_replicas=())
    # new Node[0]: the Java-given offline replicas.
    assert info.offline_replicas() == ()
    assert info.leader() is None
    assert "leader = none" in str(info)
    assert info == PartitionInfo(topic="t", partition=1, leader=None, replicas=(leader,),
                                 in_sync_replicas=(), offline_replicas=())
    assert hash(info) == hash(PartitionInfo(topic="t", partition=1, leader=None,
                                            replicas=(leader,), in_sync_replicas=()))


# --------------------------------------------------------------------------- #
# Node (no Java test)
# --------------------------------------------------------------------------- #


def test_node_constructors_and_accessors() -> None:
    n = Node(id=5, host="h", port=9092)
    assert (n.id(), n.id_string(), n.host(), n.port()) == (5, "5", "h", 9092)
    assert n.rack() is None and not n.has_rack() and not n.is_fenced()
    r = Node(id=5, host="h", port=9092, rack="r1")
    assert r.has_rack() and r.rack() == "r1"
    # Only Java's three constructors are accepted: (id, host, port, is_fenced)
    # is none of them, although (id, host, port) passes (null, false).
    with pytest.raises(IllegalArgumentError) as exc:
        Node(id=5, host="h", port=9092, is_fenced=True)  # type: ignore[call-overload]
    assert str(exc.value) == (
        "Node() takes one of (id, host, port), (id, host, port, rack), "
        "(id, host, port, rack, is_fenced); got (id, host, port, is_fenced)")
    # rack defaults to UNSET, so rack=None is given: the five-argument
    # constructor with a null rack.
    fenced = Node(id=5, host="h", port=9092, rack=None, is_fenced=True)
    assert fenced.is_fenced() and fenced.rack() is None and not fenced.has_rack()
    assert Node(id=5, host="h", port=9092, rack=None) == n
    assert str(r) == "h:9092 (id: 5 rack: r1 isFenced: false)"
    assert str(n) == "h:9092 (id: 5 rack: null isFenced: false)"
    assert n == Node(id=5, host="h", port=9092) and n != r
    assert hash(n) == hash(Node(id=5, host="h", port=9092))
    assert Node.no_node().is_empty()
    assert str(Node.no_node()) == ":-1 (id: -1 rack: null isFenced: false)"
    with pytest.raises(TypeError):
        Node(5, "h", 9092)  # type: ignore[call-arg]


# --------------------------------------------------------------------------- #
# MetricName, Metric, KafkaMetric: KafkaMetricTest
# --------------------------------------------------------------------------- #

_METRIC_NAME = MetricName(name="name", group="group", description="description", tags={})


def test_metric_name() -> None:
    m = MetricName(name="n", group="g", description="d", tags={"client-id": "c1"})
    assert (m.name(), m.group(), m.description(), m.tags()) == ("n", "g", "d", {"client-id": "c1"})
    # Equality and the hash exclude the description.
    same = MetricName(name="n", group="g", description="other", tags={"client-id": "c1"})
    assert m == same and hash(m) == hash(same)
    assert m != MetricName(name="n", group="g", description="d", tags={})
    assert str(m) == "MetricName [name=n, group=g, description=d, tags={client-id=c1}]"
    for field in ("name", "group", "description", "tags"):
        args: dict[str, Any] = {"name": "n", "group": "g", "description": "d", "tags": {}}
        args[field] = None
        with pytest.raises(NullPointerError):
            MetricName(**args)
    with pytest.raises(TypeError):
        MetricName("n", "g", "d", {})  # type: ignore[call-arg]


class _Measurable:
    def measure(self, config: object, now: int) -> float:
        return 0.0


class _Gauge:
    def __init__(self, value: object) -> None:
        self._value = value

    def value(self, config: object, now: int) -> object:
        return self._value


class _MockTime:
    def milliseconds(self) -> int:
        return 1000


def test_is_measurable() -> None:
    provider = _Measurable()
    metric = KafkaMetric(lock=object(), metric_name=_METRIC_NAME, value_provider=provider,
                         config=MetricConfig(), time=_MockTime())
    assert metric.is_measurable()
    assert metric.measurable() is provider
    assert metric.metric_value() == 0.0


def test_is_measurable_with_gauge_provider() -> None:
    metric = KafkaMetric(lock=object(), metric_name=_METRIC_NAME, value_provider=_Gauge(0.0),
                         config=MetricConfig(), time=_MockTime())
    assert not metric.is_measurable()
    with pytest.raises(IllegalStateError) as exc:
        metric.measurable()
    assert str(exc.value) == "Not a measurable: class test.unit.test_common_types._Gauge"


# KafkaMetricTest.testMeasurableValueReturnsZeroWhenNotMeasurable: Java's
# measurableValue(long) is package-private, so it is not generated.


def test_kafka_metric_accepts_non_measurable_non_gauge_provider() -> None:
    metric = KafkaMetric(lock=threading.Lock(), metric_name=_METRIC_NAME,
                         value_provider=_Gauge("metric value provider"),
                         config=MetricConfig(), time=_MockTime())
    assert metric.metric_value() == "metric value provider"
    assert metric.metric_name() is _METRIC_NAME
    assert isinstance(metric, Metric)


def test_constructor_with_null_provider() -> None:
    with pytest.raises(NullPointerError) as exc:
        KafkaMetric(lock=object(), metric_name=_METRIC_NAME, value_provider=None,
                    config=MetricConfig(), time=_MockTime())
    assert str(exc.value) == "valueProvider must not be null"


def test_kafka_metric_config_getter_and_setter() -> None:
    config = MetricConfig()
    metric = KafkaMetric(lock=object(), metric_name=_METRIC_NAME, value_provider=_Gauge(1),
                         config=config, time=_MockTime())
    assert metric.config() is config
    other = MetricConfig()
    assert metric.config(config=other) is None
    assert metric.config() is other


def test_placeholders_are_object() -> None:
    assert Cluster is object and MetricConfig is object and Measurable is object


# --------------------------------------------------------------------------- #
# TimestampType
# --------------------------------------------------------------------------- #


def test_timestamp_type() -> None:
    assert [t.value for t in TimestampType] == [-1, 0, 1]
    assert [str(t) for t in TimestampType] == ["NoTimestampType", "CreateTime", "LogAppendTime"]
    assert TimestampType.for_name(name="CreateTime") is TimestampType.CREATE_TIME
    with pytest.raises(NoSuchElementError) as exc:
        TimestampType.for_name(name="nope")
    assert str(exc.value) == "Invalid timestamp type nope"
    assert not hasattr(TimestampType.CREATE_TIME, "label")
    with pytest.raises(TypeError):
        TimestampType.for_name("CreateTime")  # type: ignore[call-arg]


# --------------------------------------------------------------------------- #
# Headers and Java's string conversion
# --------------------------------------------------------------------------- #


def test_read_headers_views_the_values_without_copying() -> None:
    raw = bytearray(b"v2")
    out = _read_headers([("k1", b"v1"), ("k2", raw), ("k3", None)])
    assert [(k, None if v is None else v.tobytes()) for k, v in out] == [
        ("k1", b"v1"), ("k2", b"v2"), ("k3", None)]
    assert all(v is None or isinstance(v, memoryview) for _, v in out)
    raw[0] = ord("V")  # a view, not a copy (CLAUDE.md §12)
    assert out[1][1] is not None and out[1][1].tobytes() == b"V2"
    assert _read_headers(None) == ()


def test_read_headers_rejects_a_bad_value_or_shape() -> None:
    with pytest.raises(TypeError):
        _read_headers([("k", "not-bytes")])  # type: ignore[list-item]
    with pytest.raises(TypeError):
        _read_headers([("k",)])  # type: ignore[list-item]


def test_read_headers_rejects_a_null_key_with_javas_message() -> None:
    with pytest.raises(NullPointerError) as exc:
        _read_headers([(None, b"v")])  # type: ignore[list-item]
    assert str(exc.value) == "Null header keys are not permitted"


def test_headers_to_string_is_record_headers_to_string() -> None:
    assert _headers_to_string(_read_headers([("k", b"\x01\xff"), ("n", None)])) == (
        "RecordHeaders(headers = [RecordHeader(key = k, value = [1, -1]), "
        "RecordHeader(key = n, value = null)], isReadOnly = false)")


@pytest.mark.parametrize("value,text", [
    (None, "null"), (True, "true"), (False, "false"), (5, "5"), ("s", "s"),
    (1.0, "1.0"), (0.001, "0.001"), (1e7, "1.0E7"), (1.5e-5, "1.5E-5"), (-2.5e20, "-2.5E20"),
    (float("nan"), "NaN"), (float("inf"), "Infinity"), (-0.0, "-0.0"),
    ([1, None], "[1, null]"), ({"a": 1}, "{a=1}"),
    (TopicPartition(topic="t", partition=0), "t-0"),
])
def test_java_str_is_string_value_of(value: object, text: str) -> None:
    assert java_str(value) == text
