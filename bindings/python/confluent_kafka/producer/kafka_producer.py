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

"""``KafkaProducer``: Java's ``org.apache.kafka.clients.producer.KafkaProducer``.

Only Java's public constructors, every method being :class:`Producer`'s
(CLAUDE.md, Python Binding Conventions, Class family). Java's four
constructors, ``(Map configs)``, ``(Map configs, Serializer, Serializer)``,
``(Properties properties)`` and ``(Properties properties, Serializer,
Serializer)``, are one keyword-only ``__init__``: ``Map`` and ``Properties``
are both ``dict``, so the parameter is ``configs``; the shorter constructors
pass ``null`` serializers, so every combination is a Java overload and the
stubs only bind the type parameters (Signatures). A serializer defaults to the
config key, else ``bytes_serializer()`` *(deviation: Java requires one)*. The
constructor maps to ``kafka_producer_KafkaProducer_new``; the serializers run
in Python on the caller's thread.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar, TypeVar, overload

from .producer import Producer

if TYPE_CHECKING:
    from confluent_kafka.common.serialization import Serializer

__all__ = ["KafkaProducer"]

K = TypeVar("K")
V = TypeVar("V")


class KafkaProducer(Producer[K, V]):
    """A Kafka client that publishes records to the Kafka cluster.

    The producer is thread safe and sharing a single producer instance across
    threads will generally be faster than having multiple instances. It
    consists of a pool of buffer space that holds records that haven't yet been
    transmitted to the server, and a background task responsible for turning
    these records into requests and transmitting them to the cluster. Failure
    to close the producer after use will leak these resources.

    Java: ``org.apache.kafka.clients.producer.KafkaProducer<K, V>``.
    """

    NETWORK_THREAD_PREFIX: ClassVar[str] = "kafka-producer-network-thread"
    PRODUCER_METRIC_GROUP_NAME: ClassVar[str] = "producer-metrics"

    @overload
    def __init__(self: KafkaProducer[bytes, bytes], *, configs: dict[str, Any]) -> None: ...
    @overload
    def __init__(self: KafkaProducer[K, bytes], *, configs: dict[str, Any],
                 key_serializer: Serializer[K]) -> None: ...
    @overload
    def __init__(self: KafkaProducer[bytes, V], *, configs: dict[str, Any],
                 value_serializer: Serializer[V]) -> None: ...
    @overload
    def __init__(self, *, configs: dict[str, Any], key_serializer: Serializer[K],
                 value_serializer: Serializer[V]) -> None: ...

    def __init__(self, *, configs: dict[str, Any], key_serializer: Serializer[Any] | None = None,
                 value_serializer: Serializer[Any] | None = None) -> None:
        """A producer is instantiated by providing a set of key-value pairs as
        configuration, and a key and a value serializer. Valid configuration
        strings are documented at
        http://kafka.apache.org/documentation.html#producerconfigs. Values can
        be either strings or objects of the appropriate type (for example a
        numeric configuration would accept either the string "42" or the
        integer 42). The ``configure()`` method won't be called in the producer
        when the serializer is passed in directly.

        Note: after creating a ``KafkaProducer`` you must always ``close()`` it
        to avoid resource leaks.
        """
        Producer.__init__(self)
        self._start(configs, key_serializer, value_serializer)
