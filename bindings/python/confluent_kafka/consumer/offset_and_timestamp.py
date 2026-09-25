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

"""``OffsetAndTimestamp`` — an offset with the timestamp it was found at.

Translated from ``org.apache.kafka.clients.consumer.OffsetAndTimestamp`` (Apache
Kafka 4.3.1). Java's two constructors collapse to one keyword-only constructor
(``leader_epoch=None``). Negative offset or timestamp raise
``IllegalArgumentException`` in Java — the binding raises
``IllegalArgumentError`` with the same messages.
"""

from __future__ import annotations

from confluent_kafka import IllegalArgumentError


class OffsetAndTimestamp:
    """An offset with the timestamp it was found at.

    Java: ``org.apache.kafka.clients.consumer.OffsetAndTimestamp``
    (``OffsetAndTimestamp(long offset, long timestamp,
    Optional<Integer> leaderEpoch)``).
    """

    __slots__ = ("_offset", "_timestamp", "_leader_epoch")

    def __init__(self, *, offset: int, timestamp: int,
                 leader_epoch: int | None = None) -> None:
        if offset < 0:
            raise IllegalArgumentError("Invalid negative offset")
        if timestamp < 0:
            raise IllegalArgumentError("Invalid negative timestamp")
        self._offset = offset
        self._timestamp = timestamp
        self._leader_epoch = leader_epoch

    def offset(self) -> int:
        return self._offset

    def timestamp(self) -> int:
        return self._timestamp

    def leader_epoch(self) -> int | None:
        # Unlike OffsetAndMetadata, Java returns the stored Optional verbatim
        # here (no null-or-negative filtering), and equals/hashCode use it as-is.
        return self._leader_epoch

    def __eq__(self, other: object) -> bool:
        if self is other:
            return True
        if not isinstance(other, OffsetAndTimestamp):
            return NotImplemented
        return (self._timestamp == other._timestamp
                and self._offset == other._offset
                and self._leader_epoch == other._leader_epoch)

    def __hash__(self) -> int:
        return hash((self._timestamp, self._offset, self._leader_epoch))

    def __repr__(self) -> str:
        return (f"(timestamp={self._timestamp}, "
                f"leaderEpoch={self._leader_epoch}, offset={self._offset})")
