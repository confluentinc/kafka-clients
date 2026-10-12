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

"""``ProducerState``: Java's ``org.apache.kafka.clients.admin.ProducerState``.

Java's ``OptionalInt`` / ``OptionalLong`` fields are ``int | None`` (Types);
``__str__`` keeps Java's ``OptionalInt[3]`` / ``OptionalLong.empty`` text.
"""

from __future__ import annotations

from ._values import optional_str

__all__ = ["ProducerState"]


class ProducerState:
    """The state of an active producer of a partition.

    Java: ``org.apache.kafka.clients.admin.ProducerState``.
    """

    __slots__ = ("_producer_id", "_producer_epoch", "_last_sequence", "_last_timestamp",
                 "_coordinator_epoch", "_current_transaction_start_offset")

    def __init__(self, *, producer_id: int, producer_epoch: int, last_sequence: int,
                 last_timestamp: int, coordinator_epoch: int | None,
                 current_transaction_start_offset: int | None) -> None:
        self._producer_id = producer_id
        self._producer_epoch = producer_epoch
        self._last_sequence = last_sequence
        self._last_timestamp = last_timestamp
        self._coordinator_epoch = coordinator_epoch
        self._current_transaction_start_offset = current_transaction_start_offset

    def producer_id(self) -> int:
        return self._producer_id

    def producer_epoch(self) -> int:
        return self._producer_epoch

    def last_sequence(self) -> int:
        return self._last_sequence

    def last_timestamp(self) -> int:
        return self._last_timestamp

    def current_transaction_start_offset(self) -> int | None:
        return self._current_transaction_start_offset

    def coordinator_epoch(self) -> int | None:
        return self._coordinator_epoch

    def __eq__(self, other: object) -> bool:
        if self is other:
            return True
        if type(other) is not type(self):
            return NotImplemented
        assert isinstance(other, ProducerState)
        return (self._producer_id == other._producer_id
                and self._producer_epoch == other._producer_epoch
                and self._last_sequence == other._last_sequence
                and self._last_timestamp == other._last_timestamp
                and self._coordinator_epoch == other._coordinator_epoch
                and self._current_transaction_start_offset
                == other._current_transaction_start_offset)

    def __hash__(self) -> int:
        return hash((self._producer_id, self._producer_epoch, self._last_sequence,
                     self._last_timestamp, self._coordinator_epoch,
                     self._current_transaction_start_offset))

    def __str__(self) -> str:
        return ("ProducerState(producerId=" + str(self._producer_id)
                + ", producerEpoch=" + str(self._producer_epoch)
                + ", lastSequence=" + str(self._last_sequence)
                + ", lastTimestamp=" + str(self._last_timestamp)
                + ", coordinatorEpoch=" + optional_str(self._coordinator_epoch, "OptionalInt")
                + ", currentTransactionStartOffset="
                + optional_str(self._current_transaction_start_offset, "OptionalLong") + ")")
