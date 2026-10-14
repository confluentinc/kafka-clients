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

"""``AsyncKafkaConsumer``: the asyncio peer of :class:`KafkaConsumer`, with the
same constructor (CLAUDE.md, Python Binding Conventions, Class family); every
method is :class:`AsyncConsumer`'s."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, TypeVar, overload

from .async_consumer import AsyncConsumer

if TYPE_CHECKING:
    from confluent_kafka.common.serialization import Deserializer

__all__ = ["AsyncKafkaConsumer"]

K = TypeVar("K")
V = TypeVar("V")


class AsyncKafkaConsumer(AsyncConsumer[K, V]):
    """The asyncio peer of ``KafkaConsumer``: a client that consumes records
    from a Kafka cluster. See ``KafkaConsumer`` for the ``configs`` and the
    deserializers.

    Java: ``org.apache.kafka.clients.consumer.KafkaConsumer<K, V>``.
    """

    @overload
    def __init__(self: AsyncKafkaConsumer[bytes, bytes], *, configs: dict[str, Any]) -> None: ...
    @overload
    def __init__(self: AsyncKafkaConsumer[K, bytes], *, configs: dict[str, Any],
                 key_deserializer: Deserializer[K]) -> None: ...
    @overload
    def __init__(self: AsyncKafkaConsumer[bytes, V], *, configs: dict[str, Any],
                 value_deserializer: Deserializer[V]) -> None: ...
    @overload
    def __init__(self, *, configs: dict[str, Any], key_deserializer: Deserializer[K],
                 value_deserializer: Deserializer[V]) -> None: ...

    def __init__(self, *, configs: dict[str, Any],
                 key_deserializer: Deserializer[Any] | None = None,
                 value_deserializer: Deserializer[Any] | None = None) -> None:
        """See :meth:`KafkaConsumer.__init__`."""
        AsyncConsumer.__init__(self)
        self._start(configs, key_deserializer, value_deserializer)
