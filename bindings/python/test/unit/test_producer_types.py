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

"""Tests of ``ProducerRecord`` and ``RecordMetadata``.

Java's tests translated: ``ProducerRecordTest`` and ``RecordMetadataTest``
(all), with the exact messages Java raises.
"""

from __future__ import annotations

import pytest

from confluent_kafka import IllegalArgumentError, NullPointerError
from confluent_kafka.common import TopicPartition
from confluent_kafka.producer import ProducerRecord, RecordMetadata

# --------------------------------------------------------------------------- #
# ProducerRecord: ProducerRecordTest
# --------------------------------------------------------------------------- #


def test_equals_and_hash_code() -> None:
    producer_record = ProducerRecord(topic="test", partition=1, key="key", value=1)
    assert producer_record == producer_record
    assert hash(producer_record) == hash(producer_record)
    equal_record = ProducerRecord(topic="test", partition=1, key="key", value=1)
    assert producer_record == equal_record
    assert hash(producer_record) == hash(equal_record)
    assert producer_record != ProducerRecord(topic="test-1", partition=1, key="key", value=1)
    assert producer_record != ProducerRecord(topic="test", partition=2, key="key", value=1)
    assert producer_record != ProducerRecord(topic="test", partition=1, key="key-1", value=1)
    assert producer_record != ProducerRecord(topic="test", partition=1, key="key", value=2)
    null_fields = ProducerRecord(topic="topic", partition=None, timestamp=None, key=None,
                                 value=None, headers=None)  # type: ignore[call-overload]
    assert null_fields == null_fields
    assert hash(null_fields) == hash(null_fields)


def test_invalid_records() -> None:
    with pytest.raises(IllegalArgumentError) as exc:
        ProducerRecord(topic=None, partition=0, key="key", value=1)  # type: ignore[call-overload]
    assert str(exc.value) == "Topic cannot be null."
    with pytest.raises(IllegalArgumentError) as exc:
        ProducerRecord(topic="test", partition=0, timestamp=-1, key="key", value=1)
    assert str(exc.value) == (
        "Invalid timestamp: -1. Timestamp should always be non-negative or null.")
    with pytest.raises(IllegalArgumentError) as exc:
        ProducerRecord(topic="test", partition=-1, key="key", value=1)
    assert str(exc.value) == (
        "Invalid partition: -1. Partition number should always be non-negative or null.")


def test_producer_record_forms_and_headers() -> None:
    # Every Java constructor delegates to the longest: any subset is a form.
    r = ProducerRecord(topic="t", value=b"v")
    assert (r.topic(), r.partition(), r.timestamp(), r.key(), r.value()) == (
        "t", None, None, None, b"v")
    assert r.headers() == ()
    raw = bytearray(b"h")
    tombstone = ProducerRecord(topic="t", key=b"k", value=None,
                               headers=[("a", raw), ("b", None)])
    assert tombstone.value() is None
    (a, va), (b, vb) = tombstone.headers()
    assert (a, b, vb) == ("a", "b", None)
    assert isinstance(va, memoryview) and va.tobytes() == b"h"
    raw[0] = ord("H")  # the header value is viewed, not copied
    assert va.tobytes() == b"H"
    assert str(ProducerRecord(topic="t", partition=1, key="k", value="v")) == (
        "ProducerRecord(topic=t, partition=1, headers=RecordHeaders(headers = [], "
        "isReadOnly = false), key=k, value=v, timestamp=null)")
    with pytest.raises(NullPointerError) as npe:
        ProducerRecord(topic="t", value=1, headers=[(None, b"x")])  # type: ignore[list-item]
    assert str(npe.value) == "Null header keys are not permitted"
    with pytest.raises(TypeError):
        ProducerRecord("t", None, None, None, 1)  # type: ignore[call-overload]


# --------------------------------------------------------------------------- #
# RecordMetadata: RecordMetadataTest
# --------------------------------------------------------------------------- #


def test_construction_with_missing_batch_index() -> None:
    tp = TopicPartition(topic="foo", partition=0)
    metadata = RecordMetadata(topic_partition=tp, base_offset=-1, batch_index=-1,
                              timestamp=2340234, serialized_key_size=3, serialized_value_size=5)
    assert metadata.topic() == tp.topic()
    assert metadata.partition() == tp.partition()
    assert metadata.timestamp() == 2340234
    assert not metadata.has_offset()
    assert metadata.offset() == -1
    assert metadata.serialized_key_size() == 3
    assert metadata.serialized_value_size() == 5


def test_construction_with_batch_index_offset() -> None:
    tp = TopicPartition(topic="foo", partition=0)
    metadata = RecordMetadata(topic_partition=tp, base_offset=15, batch_index=3,
                              timestamp=2340234, serialized_key_size=3, serialized_value_size=5)
    assert metadata.topic() == tp.topic()
    assert metadata.partition() == tp.partition()
    assert metadata.timestamp() == 2340234
    assert metadata.offset() == 18
    assert metadata.serialized_key_size() == 3
    assert metadata.serialized_value_size() == 5


def test_record_metadata_surface() -> None:
    tp = TopicPartition(topic="foo", partition=2)
    metadata = RecordMetadata(topic_partition=tp, base_offset=7, batch_index=0, timestamp=-1,
                              serialized_key_size=-1, serialized_value_size=-1)
    assert RecordMetadata.UNKNOWN_PARTITION == -1
    assert metadata.has_offset() and not metadata.has_timestamp()
    assert str(metadata) == "foo-2@7"
    with pytest.raises(TypeError):
        RecordMetadata(tp, 0, 0, 0, 0, 0)  # type: ignore[call-arg]
