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

"""``TransactionDescription``: Java's
``org.apache.kafka.clients.admin.TransactionDescription``.

Java's ``OptionalLong`` field is ``int | None`` (Types); ``__str__`` keeps
Java's ``OptionalLong[…]`` / ``OptionalLong.empty`` text.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import TYPE_CHECKING

from confluent_kafka._java import java_str

from ._values import optional_str

if TYPE_CHECKING:
    from confluent_kafka.common import TopicPartition

    from .transaction_state import TransactionState

__all__ = ["TransactionDescription"]


class TransactionDescription:
    """A description of a transaction.

    Java: ``org.apache.kafka.clients.admin.TransactionDescription``.
    """

    __slots__ = ("_coordinator_id", "_state", "_producer_id", "_producer_epoch",
                 "_transaction_timeout_ms", "_transaction_start_time_ms", "_topic_partitions")

    def __init__(self, *, coordinator_id: int, state: TransactionState, producer_id: int,
                 producer_epoch: int, transaction_timeout_ms: int,
                 transaction_start_time_ms: int | None,
                 topic_partitions: Iterable[TopicPartition]) -> None:
        self._coordinator_id = coordinator_id
        self._state = state
        self._producer_id = producer_id
        self._producer_epoch = producer_epoch
        self._transaction_timeout_ms = transaction_timeout_ms
        self._transaction_start_time_ms = transaction_start_time_ms
        # A None is stored, as Java's constructor stores it (Class family).
        self._topic_partitions: frozenset[TopicPartition] | None = (
            None if topic_partitions is None else frozenset(topic_partitions))

    def coordinator_id(self) -> int:
        return self._coordinator_id

    def state(self) -> TransactionState:
        return self._state

    def producer_id(self) -> int:
        return self._producer_id

    def producer_epoch(self) -> int:
        return self._producer_epoch

    def transaction_timeout_ms(self) -> int:
        return self._transaction_timeout_ms

    def transaction_start_time_ms(self) -> int | None:
        return self._transaction_start_time_ms

    def topic_partitions(self) -> set[TopicPartition]:
        return None if self._topic_partitions is None else set(self._topic_partitions)  # type: ignore[return-value]  # a None given, as Java returns it

    def __eq__(self, other: object) -> bool:
        if self is other:
            return True
        if type(other) is not type(self):
            return NotImplemented
        assert isinstance(other, TransactionDescription)
        return (self._coordinator_id == other._coordinator_id
                and self._producer_id == other._producer_id
                and self._producer_epoch == other._producer_epoch
                and self._transaction_timeout_ms == other._transaction_timeout_ms
                and self._state is other._state
                and self._transaction_start_time_ms == other._transaction_start_time_ms
                and self._topic_partitions == other._topic_partitions)

    def __hash__(self) -> int:
        return hash((self._coordinator_id, self._state, self._producer_id, self._producer_epoch,
                     self._transaction_timeout_ms, self._transaction_start_time_ms,
                     self._topic_partitions))

    def __str__(self) -> str:
        return ("TransactionDescription(coordinatorId=" + str(self._coordinator_id)
                + ", state=" + java_str(self._state)
                + ", producerId=" + str(self._producer_id)
                + ", producerEpoch=" + str(self._producer_epoch)
                + ", transactionTimeoutMs=" + str(self._transaction_timeout_ms)
                + ", transactionStartTimeMs="
                + optional_str(self._transaction_start_time_ms, "OptionalLong")
                + ", topicPartitions=" + java_str(self._topic_partitions) + ")")
