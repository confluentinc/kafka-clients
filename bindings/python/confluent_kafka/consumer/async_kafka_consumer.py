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

"""``AsyncKafkaConsumer`` — the real asyncio consumer.

Same constructor as ``KafkaConsumer``; every method inherited from
``AsyncConsumer`` (spec §6.2).
"""

from __future__ import annotations

from typing import Any, Generic, TypeVar

from confluent_kafka.common.serialization import Deserializer, bytes_deserializer

from ._config_resolve import resolve_consumer_construction
from .async_consumer import AsyncConsumer

K = TypeVar("K")
V = TypeVar("V")


class AsyncKafkaConsumer(AsyncConsumer[K, V], Generic[K, V]):
    """The real asyncio consumer connected to a Kafka cluster.

    Java: ``KafkaConsumer<K, V>`` (the async peer). See ``KafkaConsumer`` for the
    ``config`` contract.
    """

    __slots__ = ()

    def __init__(self, *, config: dict[str, Any],
                 key_deserializer: Deserializer[K] = bytes_deserializer(),  # type: ignore[assignment]
                 value_deserializer: Deserializer[V] = bytes_deserializer(),  # type: ignore[assignment]
                 ) -> None:
        handle, key_deser, value_deser = resolve_consumer_construction(
            config=config,
            key_deserializer=key_deserializer,
            value_deserializer=value_deserializer,
        )
        self._engine_init(
            handle=handle,
            key_deserializer=key_deser,
            value_deserializer=value_deser,
        )
