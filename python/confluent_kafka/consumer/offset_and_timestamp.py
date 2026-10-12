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

"""``OffsetAndTimestamp``: Java's ``org.apache.kafka.clients.consumer.OffsetAndTimestamp``.

Java's two constructors are one keyword-only constructor: ``(offset,
timestamp)`` passes ``Optional.empty()`` as the leader epoch. Every combination
is one of them, so there is no ``java_forms`` check; ``leader_epoch`` defaults
to ``UNSET`` (CLAUDE.md, Python Binding Conventions, Signatures: one constructor
requires it and the other defaults it), and the constructor fills it.
"""

from __future__ import annotations

from confluent_kafka._args import UNSET
from confluent_kafka._java import java_str
from confluent_kafka.illegal_argument_error import IllegalArgumentError

__all__ = ["OffsetAndTimestamp"]


class OffsetAndTimestamp:
    """A container class for offset and timestamp.

    Java: ``org.apache.kafka.clients.consumer.OffsetAndTimestamp`` (``final``).
    """

    __slots__ = ("_timestamp", "_offset", "_leader_epoch")

    def __init__(self, *, offset: int, timestamp: int,
                 leader_epoch: int | None = UNSET) -> None:
        if leader_epoch is UNSET:
            leader_epoch = None  # (offset, timestamp) passes Optional.empty()
        if offset < 0:
            raise IllegalArgumentError(message="Invalid negative offset")
        if timestamp < 0:
            raise IllegalArgumentError(message="Invalid negative timestamp")
        self._offset = offset
        self._timestamp = timestamp
        self._leader_epoch = leader_epoch

    def timestamp(self) -> int:
        return self._timestamp

    def offset(self) -> int:
        return self._offset

    def leader_epoch(self) -> int | None:
        """Get the leader epoch corresponding to the offset that was found (if
        one exists). This can be provided to ``seek()`` to ensure that the log
        hasn't been truncated prior to fetching. ``None`` if it is not known."""
        return self._leader_epoch

    def __str__(self) -> str:
        return ("(timestamp=" + str(self._timestamp) + ", leaderEpoch="
                + java_str(self._leader_epoch) + ", offset=" + str(self._offset) + ")")

    def __eq__(self, other: object) -> bool:
        if self is other:
            return True
        if type(other) is not type(self):
            return NotImplemented
        assert isinstance(other, OffsetAndTimestamp)
        return (self._timestamp == other._timestamp and self._offset == other._offset
                and self._leader_epoch == other._leader_epoch)

    def __hash__(self) -> int:
        return hash((self._timestamp, self._offset, self._leader_epoch))
