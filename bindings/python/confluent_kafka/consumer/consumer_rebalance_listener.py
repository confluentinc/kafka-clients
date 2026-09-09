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

"""``ConsumerRebalanceListener`` and the ``CommitCallback`` alias.

Translated from ``org.apache.kafka.clients.consumer.ConsumerRebalanceListener``
and ``OffsetCommitCallback`` (Apache Kafka 4.3.1).

``ConsumerRebalanceListener`` is a multi-method interface, so it stays a class
(rule 3.9); subclass it and override what you need. The methods are invoked
**positionally** — they are user-implemented callables (rule 3.2 exemption (a)),
so they take only ``self`` plus the partitions, exactly as Java's interface
does — and on the **caller's thread** during ``poll()`` / ``commit*`` /
``unsubscribe()`` / ``close()`` (consumer-threading.md §31). The rebalance does
not proceed until the callback returns.

On ``AsyncKafkaConsumer`` an override may be an ``async def``; the consumer
awaits it and the rebalance does not proceed until it completes (decision F,
spec §6.2). A plain ``def`` is accepted on either class.

``OffsetCommitCallback`` is a single-method callback interface, so it collapses
to a callable with the ``CommitCallback`` alias (rule 3.9).
"""

from __future__ import annotations

from collections.abc import Callable

from confluent_kafka.common.errors._base import KafkaError
from confluent_kafka.common.topic_partition import TopicPartition

from .offset_and_metadata import OffsetAndMetadata

__all__ = ["ConsumerRebalanceListener", "CommitCallback"]


class ConsumerRebalanceListener:
    """A callback interface the user implements to trigger custom actions when
    the set of partitions assigned to the consumer changes.

    Java: ``org.apache.kafka.clients.consumer.ConsumerRebalanceListener``.
    Subclass and override ``on_partitions_revoked`` / ``on_partitions_assigned``
    (and optionally ``on_partitions_lost``). The base implementations are no-ops,
    matching Java's interface having no behaviour of its own beyond the
    ``on_partitions_lost`` default.
    """

    def on_partitions_revoked(
        self, partitions: set[TopicPartition]
    ) -> None:
        """Called before partitions are revoked from the consumer (Java
        ``onPartitionsRevoked``). Override to commit offsets or flush state
        before the partitions are reassigned. The default is a no-op."""

    def on_partitions_assigned(
        self, partitions: set[TopicPartition]
    ) -> None:
        """Called after partitions are assigned to the consumer (Java
        ``onPartitionsAssigned``). The default is a no-op."""

    def on_partitions_lost(
        self, partitions: set[TopicPartition]
    ) -> None:
        """Called when partitions are lost without a clean revocation (Java
        ``onPartitionsLost``). Java's interface default delegates to
        ``onPartitionsRevoked``; this reproduces it."""
        self.on_partitions_revoked(partitions)


# Java: OffsetCommitCallback.onComplete(Map<TopicPartition, OffsetAndMetadata>,
# Exception). A single-method interface → a callable alias (rule 3.9). The
# second argument is ``None`` on success. Invoked positionally on the caller's
# thread inside commit_nowait()/poll()/... (consumer-threading.md §31).
CommitCallback = Callable[
    [dict[TopicPartition, OffsetAndMetadata] | None, KafkaError | None], None
]
