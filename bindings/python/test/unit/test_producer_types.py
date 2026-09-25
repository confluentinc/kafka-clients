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

"""Unit tests for ``confluent_kafka.producer`` value types (P2).

Translates the Java tests:

- ``ProducerRecordTest`` — ``testEqualsAndHashCode`` / ``testInvalidRecords``.
- ``RecordMetadataTest`` — ``testConstructionWithMissingBatchIndex`` /
  ``testConstructionWithBatchIndexOffset``, plus ``has_offset()`` /
  ``has_timestamp()`` sentinel coverage.
"""

from __future__ import annotations

import pytest

from confluent_kafka import IllegalArgumentError
from confluent_kafka.common import TopicPartition
from confluent_kafka.producer import ProducerRecord, RecordMetadata

# --------------------------------------------------------------------------- #
# ProducerRecord — translated from ProducerRecordTest
# --------------------------------------------------------------------------- #


def test_producer_record_equals_and_hash_code() -> None:
    pr: ProducerRecord[str, int] = ProducerRecord(
        topic="test", partition=1, key="key", value=1)
    assert pr == pr
    assert hash(pr) == hash(pr)

    equal: ProducerRecord[str, int] = ProducerRecord(
        topic="test", partition=1, key="key", value=1)
    assert pr == equal
    assert hash(pr) == hash(equal)

    topic_mismatch: ProducerRecord[str, int] = ProducerRecord(
        topic="test-1", partition=1, key="key", value=1)
    assert pr != topic_mismatch

    partition_mismatch: ProducerRecord[str, int] = ProducerRecord(
        topic="test", partition=2, key="key", value=1)
    assert pr != partition_mismatch

    key_mismatch: ProducerRecord[str, int] = ProducerRecord(
        topic="test", partition=1, key="key-1", value=1)
    assert pr != key_mismatch

    value_mismatch: ProducerRecord[str, int] = ProducerRecord(
        topic="test", partition=1, key="key", value=2)
    assert pr != value_mismatch

    null_fields: ProducerRecord[object, object] = ProducerRecord(
        topic="topic", partition=None, timestamp=None, key=None, value=None)
    assert null_fields == null_fields
    assert hash(null_fields) == hash(null_fields)


def test_producer_record_invalid_records() -> None:
    with pytest.raises(IllegalArgumentError):
        ProducerRecord(topic=None, partition=0, key="key",  # type: ignore[arg-type]
                       value=1)
    with pytest.raises(IllegalArgumentError):
        ProducerRecord(topic="test", partition=0, timestamp=-1, key="key",
                       value=1)
    with pytest.raises(IllegalArgumentError):
        ProducerRecord(topic="test", partition=-1, key="key", value=1)


def test_producer_record_accessors_and_headers() -> None:
    pr: ProducerRecord[bytes, bytes] = ProducerRecord(
        topic="t", partition=3, timestamp=99, key=b"k", value=b"v",
        headers=[("h", b"hv")])
    assert pr.topic() == "t"
    assert pr.partition() == 3
    assert pr.timestamp() == 99
    assert pr.key() == b"k"
    assert pr.value() == b"v"
    assert pr.headers() == (("h", b"hv"),)


def test_producer_record_null_value_is_legal() -> None:
    # Java null-checks only the topic; a null value is a legal record.
    pr: ProducerRecord[str, str] = ProducerRecord(topic="t", value=None)
    assert pr.value() is None
    assert pr.partition() is None
    assert pr.timestamp() is None
    assert pr.key() is None


def test_producer_record_keyword_only() -> None:
    with pytest.raises(TypeError):
        ProducerRecord("t", 1, "k", 1)  # type: ignore[misc, call-arg]


# --------------------------------------------------------------------------- #
# RecordMetadata — translated from RecordMetadataTest
# --------------------------------------------------------------------------- #


def test_record_metadata_missing_batch_index() -> None:
    tp = TopicPartition(topic="foo", partition=0)
    md = RecordMetadata(topic_partition=tp, base_offset=-1, batch_index=-1,
                        timestamp=2340234, serialized_key_size=3,
                        serialized_value_size=5)
    assert md.topic() == "foo"
    assert md.partition() == 0
    assert md.timestamp() == 2340234
    assert md.has_offset() is False
    assert md.offset() == -1
    assert md.serialized_key_size() == 3
    assert md.serialized_value_size() == 5


def test_record_metadata_batch_index_offset() -> None:
    tp = TopicPartition(topic="foo", partition=0)
    base_offset = 15
    batch_index = 3
    md = RecordMetadata(topic_partition=tp, base_offset=base_offset,
                        batch_index=batch_index, timestamp=2340234,
                        serialized_key_size=3, serialized_value_size=5)
    assert md.topic() == "foo"
    assert md.partition() == 0
    assert md.timestamp() == 2340234
    assert md.offset() == base_offset + batch_index
    assert md.has_offset() is True
    assert md.serialized_key_size() == 3
    assert md.serialized_value_size() == 5


def test_record_metadata_has_timestamp_sentinel() -> None:
    tp = TopicPartition(topic="foo", partition=0)
    with_ts = RecordMetadata(topic_partition=tp, base_offset=0, batch_index=0,
                             timestamp=5, serialized_key_size=1,
                             serialized_value_size=1)
    assert with_ts.has_timestamp() is True
    no_ts = RecordMetadata(topic_partition=tp, base_offset=0, batch_index=0,
                           timestamp=-1, serialized_key_size=1,
                           serialized_value_size=1)
    assert no_ts.has_timestamp() is False
    assert no_ts.timestamp() == -1


def test_record_metadata_repr() -> None:
    tp = TopicPartition(topic="foo", partition=0)
    md = RecordMetadata(topic_partition=tp, base_offset=10, batch_index=0,
                        timestamp=1, serialized_key_size=1,
                        serialized_value_size=1)
    assert repr(md) == "foo-0@10"


def test_record_metadata_keyword_only() -> None:
    tp = TopicPartition(topic="foo", partition=0)
    with pytest.raises(TypeError):
        RecordMetadata(tp, 0, 0, 0, 0, 0)  # type: ignore[misc, call-arg]
