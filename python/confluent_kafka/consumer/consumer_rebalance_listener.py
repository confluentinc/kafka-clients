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

"""``ConsumerRebalanceListener``: Java's
``org.apache.kafka.clients.consumer.ConsumerRebalanceListener``.

A multi-method interface the user implements, so a class to subclass: the
abstract ``onPartitionsRevoked`` / ``onPartitionsAssigned`` become no-ops and the
``default`` ``onPartitionsLost`` keeps its body (CLAUDE.md, Python Binding
Conventions, Idiom translations). The methods are called positionally with the
partitions (a user-written callable, Signatures).

The listener runs on the caller's thread, inside the call that delivers it
(``poll()``, ``unsubscribe()``, ``close()``, ``MockConsumer.rebalance()``, …),
and the rebalance does not advance until it returns; it may call back into its
consumer (``commit()``, ``seek()``, ``position()``, …). On the async consumers a
method may be ``async def``: it is awaited on the event loop (Threads and
callbacks); its calls back into the consumer block the loop while they run (the
core's reentrant ``ConsumerHandle`` has no ``_async`` forms).
"""

from __future__ import annotations

from confluent_kafka.common.topic_partition import TopicPartition

__all__ = ["ConsumerRebalanceListener"]


class ConsumerRebalanceListener:
    """A callback interface that the user can implement to trigger custom
    actions when the set of partitions assigned to the consumer changes.

    This is applicable when the consumer is having Kafka auto-manage group
    membership. If the consumer directly assigns partitions, those partitions
    will never be reassigned and this callback is not applicable.

    Java: ``org.apache.kafka.clients.consumer.ConsumerRebalanceListener``.
    """

    def on_partitions_revoked(self, partitions: set[TopicPartition]) -> None:
        """A callback method the user can implement to provide handling of
        offset commits to a customized store. This method will be called during
        a rebalance operation when the consumer has to give up some partitions:
        if the consumer assignment changes, if the consumer is being closed, or
        if it is unsubscribing. It is recommended that offsets should be
        committed in this callback to either Kafka or a custom offset store to
        prevent duplicate data.

        This callback is always called before re-assigning the partitions. Under
        the consumer rebalance protocol it is called with the partitions to
        revoke iff the set is non-empty.

        A ``WakeupError`` or ``InterruptError`` raised from a nested call to the
        consumer propagates to the call in which this callback is being executed.
        """

    def on_partitions_assigned(self, partitions: set[TopicPartition]) -> None:
        """A callback method the user can implement to provide handling of
        customized offsets on completion of a successful partition
        re-assignment. This method will be called after the partition
        re-assignment completes (even if no new partitions were assigned to the
        consumer), and before the consumer starts fetching data, and only as the
        result of a ``poll()`` call.

        ``partitions`` are the partitions that have been added to the assignment
        as a result of the rebalance.

        A ``WakeupError`` or ``InterruptError`` raised from a nested call to the
        consumer propagates to the call in which this callback is being executed.
        """

    def on_partitions_lost(self, partitions: set[TopicPartition]) -> None:
        """A callback method you can implement to provide handling of cleaning
        up resources for partitions that have already been reassigned to other
        consumers. This method will not be called during normal execution; it is
        invoked when the consumer realized that it does not own these partitions
        any longer without a normal rebalance (for example, when its session
        timeout expired, or a fatal error indicated it is no longer part of the
        group).

        By default it will just trigger ``on_partitions_revoked``; override it
        to distinguish the handling of revoked from lost partitions. (Its
        result is returned, so an ``async def`` ``on_partitions_revoked`` is
        awaited by the async consumers.)
        """
        return self.on_partitions_revoked(partitions)
