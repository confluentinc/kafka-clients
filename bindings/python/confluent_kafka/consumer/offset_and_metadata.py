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

"""``OffsetAndMetadata`` — a committed offset with optional metadata.

Translated from ``org.apache.kafka.clients.consumer.OffsetAndMetadata`` (Apache
Kafka 4.3.1). Java's three constructors collapse to one keyword-only
constructor (``leader_epoch=None``, ``metadata=""``). A negative offset raises
``IllegalArgumentException`` in Java — the binding raises ``IllegalArgumentError``
with the same message.
"""

from __future__ import annotations

from confluent_kafka import IllegalArgumentError


class OffsetAndMetadata:
    """A committed offset with optional metadata.

    Java: ``org.apache.kafka.clients.consumer.OffsetAndMetadata``
    (``OffsetAndMetadata(long offset, Optional<Integer> leaderEpoch,
    String metadata)``).
    """

    __slots__ = ("_offset", "_metadata", "_leader_epoch")

    def __init__(self, *, offset: int, leader_epoch: int | None = None,
                 metadata: str = "") -> None:
        if offset < 0:
            raise IllegalArgumentError("Invalid negative offset")
        self._offset = offset
        # Java stores the leader epoch verbatim (may be negative); the accessor
        # filters null-or-negative to "absent".
        self._leader_epoch = leader_epoch
        # Java: the server converts null metadata to an empty string; store the
        # empty string to stay consistent. A None arg maps to "".
        self._metadata = metadata if metadata is not None else ""

    def offset(self) -> int:
        return self._offset

    def metadata(self) -> str:
        return self._metadata

    def leader_epoch(self) -> int | None:
        # Java: returns empty if leaderEpoch is null OR negative.
        if self._leader_epoch is None or self._leader_epoch < 0:
            return None
        return self._leader_epoch

    def __eq__(self, other: object) -> bool:
        if self is other:
            return True
        if not isinstance(other, OffsetAndMetadata):
            return NotImplemented
        # Java compares the FILTERED leader_epoch (leaderEpoch()), so a null and
        # a negative stored epoch are equal.
        return (self._offset == other._offset
                and self._metadata == other._metadata
                and self.leader_epoch() == other.leader_epoch())

    def __hash__(self) -> int:
        # Java: Objects.hash(offset, metadata, leaderEpoch()) — the filtered form.
        return hash((self._offset, self._metadata, self.leader_epoch()))

    def __repr__(self) -> str:
        return (f"OffsetAndMetadata{{offset={self._offset}, "
                f"leaderEpoch={self.leader_epoch()}, "
                f"metadata='{self._metadata}'}}")
