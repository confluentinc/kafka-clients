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

"""Unit tests for ``confluent_kafka.common`` value types (P2).

Translates the Java tests for these classes where they exist:

- ``UuidTest`` — all cases except the two Java-serialization round-trip helpers
  (``testToArray`` / ``testToList`` cover ``Uuid.toArray``/``toList``, static
  array<->list helpers that are not part of the Python surface — skipped, noted
  below).
- ``TopicPartitionTest`` — its two tests are Java-``Serializable`` round-trips
  (``ObjectOutputStream`` / a checked-in serialized blob); not relevant to
  Python, which has no equivalent wire format. Replaced with value-type tests.
- ``TopicIdPartitionTest`` — ``testEquals`` / ``testHashCode`` / ``testToString``.
- ``PartitionInfoTest`` — ``testToString``.

No Java test exists for ``Node``, ``TimestampType``, ``MetricName`` or
``Headers``; behavioural tests are added for parity with the Java contract.
"""

from __future__ import annotations

import base64

import pytest

from confluent_kafka import IllegalArgumentError
from confluent_kafka.common import (
    MetricName,
    Node,
    PartitionInfo,
    TimestampType,
    TopicIdPartition,
    TopicPartition,
    Uuid,
    validate_written_headers,
)

# --------------------------------------------------------------------------- #
# Uuid — translated from UuidTest
# --------------------------------------------------------------------------- #


def test_significant_bits() -> None:
    uid = Uuid(most_significant_bits=34, least_significant_bits=98)
    assert uid.most_significant_bits() == 34
    assert uid.least_significant_bits() == 98


def test_uuid_equality() -> None:
    id1 = Uuid(most_significant_bits=12, least_significant_bits=13)
    id2 = Uuid(most_significant_bits=12, least_significant_bits=13)
    id3 = Uuid(most_significant_bits=24, least_significant_bits=38)

    assert Uuid.ZERO_UUID == Uuid.ZERO_UUID
    assert id1 == id2
    assert id1 != id3

    assert hash(Uuid.ZERO_UUID) == hash(Uuid.ZERO_UUID)
    assert hash(id1) == hash(id2)
    assert hash(id1) != hash(id3)


def test_hash_code() -> None:
    # Java testHashCode exact vectors.
    id1 = Uuid(most_significant_bits=16, least_significant_bits=7)
    id2 = Uuid(most_significant_bits=1043, least_significant_bits=20075)
    id3 = Uuid(most_significant_bits=104312423523523,
               least_significant_bits=200732425676585)
    assert hash(id1) == 23
    assert hash(id2) == 19064
    assert hash(id3) == -2011255899


def test_string_conversion() -> None:
    uid = Uuid.random_uuid()
    assert Uuid.from_string(s=str(uid)) == uid

    zero = str(Uuid.ZERO_UUID)
    assert Uuid.ZERO_UUID == Uuid.from_string(s=zero)


@pytest.mark.parametrize("_", range(100))  # Java @RepeatedTest(100)
def test_random_uuid(_: int) -> None:
    random_id = Uuid.random_uuid()
    assert Uuid.ZERO_UUID != random_id
    assert Uuid.METADATA_TOPIC_ID != random_id
    assert not str(random_id).startswith("-")


def test_compare_uuids() -> None:
    id00 = Uuid(most_significant_bits=0, least_significant_bits=0)
    id01 = Uuid(most_significant_bits=0, least_significant_bits=1)
    id10 = Uuid(most_significant_bits=1, least_significant_bits=0)
    assert not (id00 < id00) and not (id00 > id00)
    assert id00 < id01
    assert id00 < id10
    assert id01 > id00
    assert id10 > id00
    assert id01 < id10
    assert id10 > id01


def test_from_string_with_invalid_input() -> None:
    oversize = base64.urlsafe_b64encode(bytes(32)).rstrip(b"=").decode()
    with pytest.raises(IllegalArgumentError):
        Uuid.from_string(s=oversize)

    undersize = base64.urlsafe_b64encode(bytes(4)).rstrip(b"=").decode()
    with pytest.raises(IllegalArgumentError):
        Uuid.from_string(s=undersize)


def test_uuid_reserved_and_metadata() -> None:
    assert Uuid.METADATA_TOPIC_ID == Uuid.ONE_UUID
    assert Uuid.ZERO_UUID in Uuid.RESERVED
    assert Uuid.ONE_UUID in Uuid.RESERVED


# Skipped: UuidTest.testToArray / testToList exercise Uuid.toArray / toList,
# static Java array<->List helpers with no Python surface (lists/tuples are
# native), so there is nothing to translate.


def test_uuid_constructor_is_keyword_only() -> None:
    with pytest.raises(TypeError):
        Uuid(0, 0)  # type: ignore[misc, call-arg]


# --------------------------------------------------------------------------- #
# TopicPartition — value-type tests (Java serialization tests not relevant)
# --------------------------------------------------------------------------- #


def test_topic_partition_accessors_and_repr() -> None:
    tp = TopicPartition(topic="mytopic", partition=5)
    assert tp.topic() == "mytopic"
    assert tp.partition() == 5
    assert repr(tp) == "mytopic-5"


def test_topic_partition_equality_and_hash() -> None:
    a = TopicPartition(topic="t", partition=1)
    b = TopicPartition(topic="t", partition=1)
    c = TopicPartition(topic="t", partition=2)
    d = TopicPartition(topic="u", partition=1)
    assert a == b
    assert hash(a) == hash(b)
    assert a != c
    assert a != d
    # Hashable / usable as a dict key.
    assert {a: 1}[b] == 1


def test_topic_partition_keyword_only() -> None:
    with pytest.raises(TypeError):
        TopicPartition("t", 0)  # type: ignore[misc, call-arg]


# --------------------------------------------------------------------------- #
# TopicIdPartition — translated from TopicIdPartitionTest
# --------------------------------------------------------------------------- #

_TID0 = Uuid(most_significant_bits=-4883993789924556279,
             least_significant_bits=-5960309683534398572)
_NAME0 = "a_topic_name"
_PART1 = 1
_TP0 = TopicPartition(topic=_NAME0, partition=_PART1)
_TIDP0 = TopicIdPartition(topic_id=_TID0, topic_partition=_TP0)
_TIDP1 = TopicIdPartition(topic_id=_TID0, partition=_PART1, topic=_NAME0)
_TIDP_NULL0 = TopicIdPartition(topic_id=_TID0, partition=_PART1, topic=None)
_TIDP_NULL1 = TopicIdPartition(
    topic_id=_TID0, topic_partition=TopicPartition(topic=None, partition=_PART1),  # type: ignore[arg-type]
)
_TID1 = Uuid(most_significant_bits=7759286116672424028,
             least_significant_bits=-5081215629859775948)
_NAME1 = "another_topic_name"
_TIDP2 = TopicIdPartition(topic_id=_TID1, partition=_PART1, topic=_NAME1)
_TIDP_NULL2 = TopicIdPartition(
    topic_id=_TID1, topic_partition=TopicPartition(topic=None, partition=_PART1),  # type: ignore[arg-type]
)


def test_topic_id_partition_equals() -> None:
    assert _TIDP0 == _TIDP1
    assert _TIDP1 == _TIDP0
    assert _TIDP_NULL0 == _TIDP_NULL1

    assert _TIDP0 != _TIDP2
    assert _TIDP2 != _TIDP0
    assert _TIDP0 != _TIDP_NULL0
    assert _TIDP_NULL0 != _TIDP_NULL2


def test_topic_id_partition_hash_code() -> None:
    assert hash(_TIDP0) == hash((_TIDP0.topic_id(), _TIDP0.topic_partition()))
    assert hash(_TIDP0) == hash(_TIDP1)
    assert hash(_TIDP_NULL0) == hash(_TIDP_NULL1)
    assert hash(_TIDP0) != hash(_TIDP2)
    assert hash(_TIDP0) != hash(_TIDP_NULL0)
    assert hash(_TIDP_NULL0) != hash(_TIDP_NULL2)


def test_topic_id_partition_to_string() -> None:
    assert repr(_TIDP0) == "vDiRhkpVQgmtSLnsAZx7lA:a_topic_name-1"
    assert repr(_TIDP_NULL0) == "vDiRhkpVQgmtSLnsAZx7lA:None-1"


def test_topic_id_partition_accessors() -> None:
    assert _TIDP1.topic_id() == _TID0
    assert _TIDP1.partition() == _PART1
    assert _TIDP1.topic() == _NAME0
    assert _TIDP1.topic_partition() == _TP0


def test_topic_id_partition_requires_exactly_one_form() -> None:
    with pytest.raises(IllegalArgumentError) as exc:
        TopicIdPartition(topic_id=_TID0)  # neither form
    assert "takes exactly one of partition, topic_partition" in str(exc.value)

    with pytest.raises(IllegalArgumentError):
        # both forms
        TopicIdPartition(topic_id=_TID0, partition=1, topic_partition=_TP0)


def test_topic_id_partition_null_topic_id() -> None:
    with pytest.raises(TypeError):
        TopicIdPartition(topic_id=None, topic_partition=_TP0)  # type: ignore[arg-type]


# --------------------------------------------------------------------------- #
# Node
# --------------------------------------------------------------------------- #


def test_node_accessors_and_overloads() -> None:
    n = Node(id=1, host="h", port=9092)
    assert n.id() == 1
    assert n.id_string() == "1"
    assert n.host() == "h"
    assert n.port() == 9092
    assert n.has_rack() is False
    assert n.rack() is None
    assert n.is_fenced() is False
    assert n.is_empty() is False

    with_rack = Node(id=2, host="h2", port=9093, rack="r")
    assert with_rack.has_rack() is True
    assert with_rack.rack() == "r"

    fenced = Node(id=3, host="h3", port=9094, rack="r", is_fenced=True)
    assert fenced.is_fenced() is True


def test_node_no_node_is_empty() -> None:
    nn = Node.no_node()
    assert nn.id() == -1
    assert nn.host() == ""
    assert nn.port() == -1
    assert nn.is_empty() is True


def test_node_equality_hash_repr() -> None:
    a = Node(id=1, host="h", port=9092, rack="r")
    b = Node(id=1, host="h", port=9092, rack="r")
    c = Node(id=1, host="h", port=9092, rack="other")
    assert a == b
    assert hash(a) == hash(b)
    assert a != c
    assert repr(a) == "h:9092 (id: 1 rack: r isFenced: False)"


def test_node_keyword_only() -> None:
    with pytest.raises(TypeError):
        Node(1, "h", 9092)  # type: ignore[misc, call-arg]


# --------------------------------------------------------------------------- #
# PartitionInfo — translated from PartitionInfoTest
# --------------------------------------------------------------------------- #


def test_partition_info_to_string() -> None:
    leader = Node(id=0, host="localhost", port=9092)
    r1 = Node(id=1, host="localhost", port=9093)
    r2 = Node(id=2, host="localhost", port=9094)
    info = PartitionInfo(
        topic="sample", partition=0, leader=leader,
        replicas=(leader, r1, r2), in_sync_replicas=(leader, r1),
        offline_replicas=(r2,),
    )
    expected = ("Partition(topic = sample, partition = 0, leader = 0, "
                "replicas = [0,1,2], isr = [0,1], offlineReplicas = [2])")
    assert repr(info) == expected


def test_partition_info_offline_default_and_equality() -> None:
    leader = Node(id=0, host="localhost", port=9092)
    a = PartitionInfo(topic="t", partition=0, leader=leader,
                      replicas=(leader,), in_sync_replicas=(leader,))
    assert a.offline_replicas() == ()
    b = PartitionInfo(topic="t", partition=0, leader=leader,
                      replicas=(leader,), in_sync_replicas=(leader,))
    assert a == b
    assert hash(a) == hash(b)


def test_partition_info_null_leader() -> None:
    info = PartitionInfo(topic="t", partition=0, leader=None,
                         replicas=(), in_sync_replicas=())
    assert info.leader() is None
    assert "leader = none" in repr(info)


# --------------------------------------------------------------------------- #
# TimestampType
# --------------------------------------------------------------------------- #


def test_timestamp_type_values_and_labels() -> None:
    assert int(TimestampType.NO_TIMESTAMP_TYPE) == -1
    assert int(TimestampType.CREATE_TIME) == 0
    assert int(TimestampType.LOG_APPEND_TIME) == 1
    assert TimestampType.NO_TIMESTAMP_TYPE.id() == -1
    assert TimestampType.CREATE_TIME.label() == "CreateTime"
    assert str(TimestampType.LOG_APPEND_TIME) == "LogAppendTime"


def test_timestamp_type_for_name() -> None:
    assert TimestampType.for_name(name="CreateTime") is TimestampType.CREATE_TIME
    with pytest.raises(KeyError):
        TimestampType.for_name(name="nope")


# --------------------------------------------------------------------------- #
# MetricName
# --------------------------------------------------------------------------- #


def test_metric_name_accessors_and_equality() -> None:
    a = MetricName(name="n", group="g", description="d1", tags={"k": "v"})
    b = MetricName(name="n", group="g", description="d2", tags={"k": "v"})
    assert a.name() == "n"
    assert a.group() == "g"
    assert a.description() == "d1"
    assert a.tags() == {"k": "v"}
    # description excluded from equality/hash (Java).
    assert a == b
    assert hash(a) == hash(b)
    c = MetricName(name="n", group="g", description="d1", tags={"k": "other"})
    assert a != c


def test_metric_name_hashable_as_dict_key() -> None:
    a = MetricName(name="n", group="g", description="d", tags={})
    assert {a: 1}[MetricName(name="n", group="g", description="x", tags={})] == 1


# --------------------------------------------------------------------------- #
# Headers write-side validator
# --------------------------------------------------------------------------- #


def test_validate_written_headers_normalizes() -> None:
    out = validate_written_headers(
        [("k1", b"v1"), ("k2", bytearray(b"v2")), ("k3", None)]
    )
    assert out == (("k1", b"v1"), ("k2", b"v2"), ("k3", None))


def test_validate_written_headers_rejects_bad_value() -> None:
    with pytest.raises(IllegalArgumentError):
        validate_written_headers([("k", "not-bytes")])  # type: ignore[list-item]


def test_validate_written_headers_rejects_bad_shape() -> None:
    with pytest.raises(IllegalArgumentError):
        validate_written_headers([("k",)])  # type: ignore[list-item]
