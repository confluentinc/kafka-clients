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

"""An ``int`` wider than its Java type (CLAUDE.md, Python Binding Conventions,
Types): it raises ``OverflowError`` where it reaches the FFI; the value types
and the mocks do not check; ``Uuid`` keeps the low 64 bits of each half as
Java's signed ``long``. Java has no test of its own: its compiler refuses the
value."""

from __future__ import annotations

import pytest

from confluent_kafka.common import TopicPartition, Uuid
from confluent_kafka.consumer import KafkaConsumer, MockConsumer, OffsetAndMetadata

_LONG_MAX = 2**63 - 1


def test_value_types_accept_a_wide_int() -> None:
    assert TopicPartition(topic="t", partition=2**40).partition() == 2**40
    assert OffsetAndMetadata(offset=2**70).offset() == 2**70


def test_the_mock_consumer_accepts_a_wide_offset() -> None:
    consumer: MockConsumer[bytes, bytes] = MockConsumer(offset_reset_strategy="earliest")
    partition = TopicPartition(topic="t", partition=0)
    consumer.assign(partitions=[partition])
    consumer.seek(partition=partition, offset=2**70)
    assert consumer.position(partition=partition) == 2**70


def test_the_real_consumer_raises_overflow_error_at_the_ffi() -> None:
    configs = {"bootstrap.servers": "localhost:1", "group.protocol": "consumer"}
    with KafkaConsumer(configs=configs) as consumer:
        partition = TopicPartition(topic="t", partition=0)
        with pytest.raises(OverflowError):
            consumer.assign(partitions=[TopicPartition(topic="t", partition=2**40)])
        consumer.assign(partitions=[partition])
        with pytest.raises(OverflowError):
            consumer.seek(partition=partition, offset=2**70)
        consumer.seek(partition=partition, offset=_LONG_MAX)


def test_uuid_keeps_the_low_64_bits_of_each_half_as_a_signed_long() -> None:
    uuid = Uuid(most_sig_bits=2**64, least_sig_bits=-1)
    assert uuid.most_significant_bits() == 0
    assert uuid.least_significant_bits() == -1
    unsigned = Uuid(most_sig_bits=2**64 - 1, least_sig_bits=2**63)
    assert unsigned.most_significant_bits() == -1
    assert unsigned.least_significant_bits() == -(2**63)
