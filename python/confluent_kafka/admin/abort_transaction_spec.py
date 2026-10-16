# Copyright 2026 Confluent Inc.
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

"""``AbortTransactionSpec``: Java's
``org.apache.kafka.clients.admin.AbortTransactionSpec``."""

from __future__ import annotations

from typing import TYPE_CHECKING

from confluent_kafka._java import java_str

if TYPE_CHECKING:
    from confluent_kafka.common import TopicPartition

__all__ = ["AbortTransactionSpec"]


class AbortTransactionSpec:
    """The transaction to abort: its partition, producer id and epoch, and
    coordinator epoch.

    Java: ``org.apache.kafka.clients.admin.AbortTransactionSpec``.
    """

    __slots__ = ("_topic_partition", "_producer_id", "_producer_epoch", "_coordinator_epoch")

    def __init__(self, *, topic_partition: TopicPartition, producer_id: int,
                 producer_epoch: int, coordinator_epoch: int) -> None:
        self._topic_partition = topic_partition
        self._producer_id = producer_id
        self._producer_epoch = producer_epoch
        self._coordinator_epoch = coordinator_epoch

    def topic_partition(self) -> TopicPartition:
        return self._topic_partition

    def producer_id(self) -> int:
        return self._producer_id

    def producer_epoch(self) -> int:
        return self._producer_epoch

    def coordinator_epoch(self) -> int:
        return self._coordinator_epoch

    def __eq__(self, other: object) -> bool:
        if self is other:
            return True
        if type(other) is not type(self):
            return NotImplemented
        assert isinstance(other, AbortTransactionSpec)
        return (self._producer_id == other._producer_id
                and self._producer_epoch == other._producer_epoch
                and self._coordinator_epoch == other._coordinator_epoch
                and self._topic_partition == other._topic_partition)

    def __hash__(self) -> int:
        return hash((self._topic_partition, self._producer_id, self._producer_epoch,
                     self._coordinator_epoch))

    def __str__(self) -> str:
        return ("AbortTransactionSpec(topicPartition=" + java_str(self._topic_partition)
                + ", producerId=" + str(self._producer_id)
                + ", producerEpoch=" + str(self._producer_epoch)
                + ", coordinatorEpoch=" + str(self._coordinator_epoch) + ")")
