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

"""Static-typing assertions for the P2 value types.

Run by ``mypy --strict`` (the module is in the ``confluent_kafka`` test tree and
also imported at runtime as a smoke check). ``typing.assert_type`` fails the
type check if the inferred type of an expression is not exactly the asserted
one, which is how the ``@overload`` stubs on ``TopicIdPartition``,
``ConsumerRecords.records`` and the generics inference on the record types are
machine-verified.
"""

from __future__ import annotations

from typing import assert_type

from confluent_kafka.common import TopicIdPartition, TopicPartition, Uuid
from confluent_kafka.common.serialization import (
    Deserializer,
    Serializer,
    bool_deserializer,
    bool_serializer,
    bytes_deserializer,
    bytes_serializer,
    float_deserializer,
    float_serializer,
    int_deserializer,
    int_serializer,
    json_deserializer,
    json_serializer,
    memoryview_deserializer,
    string_deserializer,
    string_serializer,
    uuid_deserializer,
    uuid_serializer,
)
from confluent_kafka.common.errors import KafkaError, TopicAuthorizationError
from confluent_kafka.consumer import (
    ConsumerRecord,
    ConsumerRecords,
    OffsetAndMetadata,
)
from confluent_kafka.producer import ProducerRecord, RecordMetadata


def _topic_id_partition_overloads() -> None:
    uid = Uuid.random_uuid()
    a = TopicIdPartition(topic_id=uid, partition=0, topic="t")
    assert_type(a, TopicIdPartition)
    b = TopicIdPartition(topic_id=uid,
                         topic_partition=TopicPartition(topic="t", partition=0))
    assert_type(b, TopicIdPartition)
    assert_type(a.topic_id(), Uuid)
    assert_type(a.topic_partition(), TopicPartition)


def _consumer_records_overloads() -> None:
    cr: ConsumerRecords[int, str] = ConsumerRecords.empty()
    by_partition = cr.records(partition=TopicPartition(topic="t", partition=0))
    assert_type(by_partition, "list[ConsumerRecord[int, str]]")
    by_topic = cr.records(topic="t")
    assert_type(by_topic, "list[ConsumerRecord[int, str]]")


def _record_generics_inference() -> None:
    # K/V are inferred from the constructor arguments.
    cr = ConsumerRecord(topic="t", partition=0, offset=0, key=1, value="v")
    assert_type(cr.key(), "int | None")
    assert_type(cr.value(), "str | None")

    pr = ProducerRecord(topic="t", key=b"k", value=b"v")
    assert_type(pr.key(), "bytes | None")
    assert_type(pr.value(), "bytes | None")


def _sentinel_return_types() -> None:
    md = RecordMetadata(
        topic_partition=TopicPartition(topic="t", partition=0),
        base_offset=0, batch_index=0, timestamp=0,
        serialized_key_size=0, serialized_value_size=0)
    # A long+hasX() pair is int, not int | None.
    assert_type(md.offset(), int)
    assert_type(md.has_offset(), bool)

    om = OffsetAndMetadata(offset=1)
    assert_type(om.leader_epoch(), "int | None")


def _serde_factory_types() -> None:
    # Each factory returns a typed callable, so K/V infer from it (D11).
    assert_type(bytes_serializer(), "Serializer[bytes]")
    assert_type(bytes_deserializer(), "Deserializer[bytes]")
    assert_type(memoryview_deserializer(), "Deserializer[memoryview]")
    assert_type(string_serializer(), "Serializer[str]")
    assert_type(string_deserializer(), "Deserializer[str]")
    assert_type(int_serializer(), "Serializer[int]")
    assert_type(int_deserializer(), "Deserializer[int]")
    assert_type(float_serializer(), "Serializer[float]")
    assert_type(float_deserializer(), "Deserializer[float]")
    assert_type(bool_serializer(), "Serializer[bool]")
    assert_type(bool_deserializer(), "Deserializer[bool]")
    assert_type(uuid_serializer(), "Serializer[Uuid]")
    assert_type(uuid_deserializer(), "Deserializer[Uuid]")

    # A serde is just a callable of the right shape — a bare function type-checks.
    def value_deser(topic: str, data: memoryview | None,
                    headers: object = None) -> str | None:
        return None if data is None else bytes(data).decode()

    d: Deserializer[str] = value_deser
    assert_type(d, "Deserializer[str]")


def _error_reexport_is_typed() -> None:
    # C19: importing a generated error class from the *package* path
    # (confluent_kafka.common.errors) must resolve to the class, not ``object``.
    # Without the generated ``common/errors/__init__.pyi`` the star re-export left
    # mypy inferring ``object`` here, which this assert_type would then fail.
    assert_type(TopicAuthorizationError, type[TopicAuthorizationError])
    err = TopicAuthorizationError("nope")
    assert_type(err, TopicAuthorizationError)
    # A KafkaError-typed slot accepts it (the re-export carries the base relation).
    base: KafkaError = err
    assert_type(base, KafkaError)


def test_error_reexport_derives_from_kafka_error() -> None:
    """Runtime side of C19: the package-path re-export is a real ``KafkaError``
    subclass (its static typing is enforced by ``_error_reexport_is_typed`` under
    ``mypy --strict``)."""
    assert issubclass(TopicAuthorizationError, KafkaError)
    assert isinstance(TopicAuthorizationError("x"), KafkaError)


def test_typing_module_imports() -> None:
    """Runtime smoke check that the typing module imports cleanly. The real
    assertions above are enforced statically by mypy --strict."""
    _topic_id_partition_overloads()
    _consumer_records_overloads()
    _record_generics_inference()
    _sentinel_return_types()
    _serde_factory_types()
    _error_reexport_is_typed()
