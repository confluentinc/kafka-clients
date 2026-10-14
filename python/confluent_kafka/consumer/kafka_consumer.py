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

"""``KafkaConsumer``: Java's ``org.apache.kafka.clients.consumer.KafkaConsumer``.

Only Java's public constructors, every method being :class:`Consumer`'s
(CLAUDE.md, Python Binding Conventions, Class family). Java's four
constructors, ``(Map configs)``, ``(Properties properties)``, ``(Properties
properties, Deserializer, Deserializer)`` and ``(Map configs, Deserializer,
Deserializer)``, are one keyword-only ``__init__``: ``Map`` and ``Properties``
are both ``dict``, so the parameter is ``configs``; the shorter constructors
pass ``null`` deserializers, so every combination is a Java overload and the
stubs only bind the type parameters (Signatures). A deserializer defaults to
the config key, else ``bytes_deserializer()`` *(deviation: Java requires one)*.
The constructor maps to ``kafka_consumer_KafkaConsumer_new``; the deserializers
run in Python on the caller's thread.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, TypeVar, overload

from .consumer import Consumer

if TYPE_CHECKING:
    from confluent_kafka.common.serialization import Deserializer

__all__ = ["KafkaConsumer"]

K = TypeVar("K")
V = TypeVar("V")


class KafkaConsumer(Consumer[K, V]):
    """A client that consumes records from a Kafka cluster.

    This client transparently handles the failure of Kafka brokers, and
    transparently adapts as topic partitions it fetches migrate within the
    cluster. It also interacts with the broker to allow groups of consumers to
    load balance consumption using consumer groups (``group.protocol=consumer``,
    KIP-848). The consumer is not thread-safe: a call while another thread is
    inside it raises ``ConcurrentModificationError``, ``wakeup()`` excepted.
    Failure to close the consumer after use will leak its resources.

    Java: ``org.apache.kafka.clients.consumer.KafkaConsumer<K, V>``.
    """

    @overload
    def __init__(self: KafkaConsumer[bytes, bytes], *, configs: dict[str, Any]) -> None: ...
    @overload
    def __init__(self: KafkaConsumer[K, bytes], *, configs: dict[str, Any],
                 key_deserializer: Deserializer[K]) -> None: ...
    @overload
    def __init__(self: KafkaConsumer[bytes, V], *, configs: dict[str, Any],
                 value_deserializer: Deserializer[V]) -> None: ...
    @overload
    def __init__(self, *, configs: dict[str, Any], key_deserializer: Deserializer[K],
                 value_deserializer: Deserializer[V]) -> None: ...

    def __init__(self, *, configs: dict[str, Any],
                 key_deserializer: Deserializer[Any] | None = None,
                 value_deserializer: Deserializer[Any] | None = None) -> None:
        """A consumer is instantiated by providing a set of key-value pairs as
        configuration, and a key and a value deserializer. Valid configuration
        strings are documented at
        http://kafka.apache.org/documentation.html#consumerconfigs. Values can
        be either strings or objects of the appropriate type (for example a
        numeric configuration would accept either the string "42" or the
        integer 42). The ``configure()`` method won't be called in the consumer
        when the deserializer is passed in directly. ``group.id`` is optional;
        the group APIs raise ``InvalidGroupIdError`` without it.

        Note: after creating a ``KafkaConsumer`` you must always ``close()`` it
        to avoid resource leaks.
        """
        Consumer.__init__(self)
        self._start(configs, key_deserializer, value_deserializer)
