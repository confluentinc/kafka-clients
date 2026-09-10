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

"""``MockConsumer`` — the in-memory synchronous test consumer.

Translated from ``org.apache.kafka.clients.consumer.MockConsumer`` (Apache Kafka
4.3.1). Constructor plus Java's full public mock surface (the driver methods come
from ``_MockDriverMixin``; ``rebalance`` is here because it fires the listener
and so is synchronous on the sync mock).

The mock applies the configured deserializers on ``poll()`` (spec §6.2), so a
record added with ``add_record`` carries its **serialized** (``bytes``) key /
value — the one deviation from Java's ``MockConsumer<K,V>`` (whose ``addRecord``
takes the typed value), recorded in the clarifications file.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Generic, TypeVar, overload

import _confluentkafka as _lib  # type: ignore[import-not-found]

from confluent_kafka.common.serialization import Deserializer, bytes_deserializer
from confluent_kafka.common.topic_partition import TopicPartition

from ._conversions import tp_to_spec
from ._mock_driver import _MockDriverMixin
from .consumer import Consumer
from .offset_reset_strategy import OffsetResetStrategy

K = TypeVar("K")
V = TypeVar("V")


class MockConsumer(_MockDriverMixin, Consumer[K, V], Generic[K, V]):
    """The in-memory test consumer (Java ``MockConsumer<K, V>``)."""

    __slots__ = ()

    @overload
    def __init__(self, *, offset_reset_strategy: str,
                 key_deserializer: Deserializer[K] = ...,
                 value_deserializer: Deserializer[V] = ...,
                 ) -> None: ...
    @overload
    def __init__(self, *, offset_reset_strategy: OffsetResetStrategy,
                 key_deserializer: Deserializer[K] = ...,
                 value_deserializer: Deserializer[V] = ...,
                 ) -> None: ...

    def __init__(self, *, offset_reset_strategy: str | OffsetResetStrategy,
                 key_deserializer: Deserializer[K] = bytes_deserializer(),  # type: ignore[assignment]
                 value_deserializer: Deserializer[V] = bytes_deserializer(),  # type: ignore[assignment]
                 ) -> None:
        """Java: ``MockConsumer(String offsetResetStrategy)`` /
        ``@Deprecated MockConsumer(OffsetResetStrategy)`` (the value is the
        ``auto.offset.reset`` setting; the enum form is the deprecated
        overload, kept)."""
        reset = (str(offset_reset_strategy)
                 if isinstance(offset_reset_strategy, OffsetResetStrategy)
                 else offset_reset_strategy)
        handle = _lib.Consumer_MockConsumer_new(reset)
        self._engine_init(
            handle=handle,
            key_deserializer=key_deserializer,
            value_deserializer=value_deserializer,
        )

    def rebalance(self, *, partitions: Iterable[TopicPartition]) -> None:
        """Java ``rebalance(Collection)`` — simulate a rebalance, firing the
        listener callbacks on the caller's thread (§31)."""
        self._check_closed()
        spec = tp_to_spec(partitions)
        self._run_sync(
            lambda cb: _lib.MockConsumer_rebalance_async(self._h, spec, cb),
            self._resolve_void, self._free_void,
        )
