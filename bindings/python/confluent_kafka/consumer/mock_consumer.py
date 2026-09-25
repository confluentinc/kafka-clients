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

"""``MockConsumer``: Java's ``org.apache.kafka.clients.consumer.MockConsumer``.

Java's two public constructors, ``(@Deprecated OffsetResetStrategy
offsetResetStrategy)`` and ``(String offsetResetStrategy)``, share one name
with two types, so one parameter typed as their union with one stub per type,
the deprecated form last (Signatures); the deprecated one warns. The Java
mock's own methods follow (Class family) in Java's order; ``shouldRebalance()``
/ ``resetShouldRebalance()`` read and clear state only the dropped
``enforceRebalance()`` sets, and ``setClientInstanceId`` /
``injectTimeoutException`` / ``disableTelemetry`` / ``addedMetrics`` serve only
the methods not generated, so they are dropped too *(deviation)*.

The mock has no deserializers: ``add_record`` takes the ``ConsumerRecord[K,
V]`` the test builds, and ``poll()`` returns those very records, as Java's mock
does. Nothing binds ``K`` / ``V``, so the caller annotates the mock as Java
writes its type arguments: ``c: MockConsumer[str, str] =
MockConsumer(offset_reset_strategy="earliest")``.

A direct Python translation of Java's mock (``_mock_core``), not FFI-backed
*(deviation, see there)*.
"""

from __future__ import annotations

import warnings
from collections.abc import Iterable
from typing import TYPE_CHECKING, Generic, TypeVar, overload

from ._auto_offset_reset_strategy import AutoOffsetResetStrategy
from ._mock_core import MockConsumerCore
from .consumer import Consumer
from .offset_reset_strategy import OffsetResetStrategy

if TYPE_CHECKING:
    from confluent_kafka.common.topic_partition import TopicPartition

__all__ = ["MockConsumer"]

K = TypeVar("K")
V = TypeVar("V")

DEPRECATED_CONSTRUCTOR = ("MockConsumer(offset_reset_strategy: OffsetResetStrategy) is "
                          "deprecated. Since 4.0. Use MockConsumer(offset_reset_strategy: str) "
                          "instead.")


def reset_strategy_of(offset_reset_strategy: str | OffsetResetStrategy) -> AutoOffsetResetStrategy:
    """Java's two constructors: the enum form (deprecated, warns) is
    ``AutoOffsetResetStrategy.fromString(offsetResetStrategy.toString())``."""
    if isinstance(offset_reset_strategy, OffsetResetStrategy):
        warnings.warn(DEPRECATED_CONSTRUCTOR, DeprecationWarning, stacklevel=3)
        return AutoOffsetResetStrategy.from_string(str(offset_reset_strategy))
    return AutoOffsetResetStrategy.from_string(offset_reset_strategy)


class MockConsumer(MockConsumerCore[K, V], Consumer[K, V], Generic[K, V]):
    """A mock of the ``Consumer`` interface you can use for testing code that
    uses Kafka. This class is not thread-safe. However, you can use
    ``schedule_poll_task()`` to write multithreaded tests where a driver thread
    waits for ``poll()`` to be called by a background thread and then can
    safely perform operations during a callback.

    Java: ``org.apache.kafka.clients.consumer.MockConsumer<K, V>``.
    """

    @overload
    def __init__(self, *, offset_reset_strategy: str) -> None: ...
    @overload
    def __init__(self, *, offset_reset_strategy: OffsetResetStrategy) -> None: ...

    def __init__(self, *, offset_reset_strategy: str | OffsetResetStrategy) -> None:
        """A mock consumer is instantiated by providing the ``auto.offset.reset``
        value (``"earliest"``, ``"latest"``, ``"none"`` or
        ``"by_duration:<ISO-8601 duration>"``) as the input.

        Deprecated: the ``OffsetResetStrategy`` form. Since 4.0. Use the ``str``
        form instead.
        """
        strategy = reset_strategy_of(offset_reset_strategy)
        Consumer.__init__(self)
        self._init_mock(strategy)

    def rebalance(self, *, new_assignment: Iterable[TopicPartition]) -> None:
        """Simulate a rebalance event: the listener's ``on_partitions_revoked``
        runs with the partitions removed (if any), the assignment becomes
        ``new_assignment``, then ``on_partitions_assigned`` runs with the
        partitions added, on this thread; the buffered records are cleared."""
        self._c_rebalance(new_assignment)
