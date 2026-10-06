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

"""``OffsetAndMetadata``: Java's ``org.apache.kafka.clients.consumer.OffsetAndMetadata``.

Java's three constructors ``(offset, leaderEpoch, metadata)``,
``(offset, metadata)`` and ``(offset)`` are one keyword-only constructor matched
by ``java_forms`` (CLAUDE.md, Python Binding Conventions, Signatures):
``(offset, metadata)`` passes ``Optional.empty()`` as the leader epoch, and
``(offset)`` passes ``""`` as the metadata of ``(offset, metadata)``. So
``(offset, leader_epoch)`` without ``metadata`` is rejected, as in Java, and
the stubs are ``(offset, leader_epoch, metadata)`` and ``(offset, metadata="")``.
"""

from __future__ import annotations

from typing import overload

from confluent_kafka._args import UNSET, Form, java_forms
from confluent_kafka._java import java_str
from confluent_kafka.illegal_argument_error import IllegalArgumentError

__all__ = ["OffsetAndMetadata"]


class OffsetAndMetadata:
    """The Kafka offset commit API allows users to provide additional metadata
    (in the form of a string) when an offset is committed. This can be useful
    (for example) to store information about which node made the commit, what
    time the commit was made, etc.

    Java: ``org.apache.kafka.clients.consumer.OffsetAndMetadata``.
    """

    __slots__ = ("_offset", "_metadata", "_leader_epoch")

    @overload
    def __init__(self, *, offset: int, leader_epoch: int | None, metadata: str) -> None: ...
    @overload
    def __init__(self, *, offset: int, metadata: str = "") -> None: ...

    @java_forms(
        Form("offset", "leader_epoch", "metadata", defaults={"leader_epoch": None}),
        Form("offset", "metadata", defaults={"metadata": ""}),
        Form("offset"),
    )
    def __init__(self, *, offset: int, leader_epoch: int | None = None,
                 metadata: str = UNSET) -> None:
        """Construct a new ``OffsetAndMetadata`` object for committing through
        ``KafkaConsumer``: the offset to be committed, the optional leader
        epoch of the last consumed record, and non-null metadata (empty when
        not given)."""
        if offset < 0:
            raise IllegalArgumentError(message="Invalid negative offset")
        self._offset = offset
        # We use null to represent the absence of a leader epoch.
        self._leader_epoch = leader_epoch
        # The server converts null metadata to an empty string. So we store it
        # as an empty string as well on the client to be consistent.
        self._metadata = "" if metadata is None else metadata

    def offset(self) -> int:
        return self._offset

    def metadata(self) -> str:
        """Get the metadata of the previously consumed record: the metadata or
        empty string if no metadata."""
        return self._metadata

    def leader_epoch(self) -> int | None:
        """Get the leader epoch of the previously consumed record (if one is
        known). Log truncation is detected if there exists a leader epoch which
        is larger than this epoch and begins at an offset earlier than the
        committed offset. ``None`` if not known."""
        if self._leader_epoch is None or self._leader_epoch < 0:
            return None
        return self._leader_epoch

    def __eq__(self, other: object) -> bool:
        if self is other:
            return True
        if type(other) is not type(self):
            return NotImplemented
        assert isinstance(other, OffsetAndMetadata)
        return (self._offset == other._offset and self._metadata == other._metadata
                and self.leader_epoch() == other.leader_epoch())

    def __hash__(self) -> int:
        return hash((self._offset, self._metadata, self.leader_epoch()))

    def __str__(self) -> str:
        return ("OffsetAndMetadata{offset=" + str(self._offset) + ", leaderEpoch="
                + java_str(self.leader_epoch()) + ", metadata='" + self._metadata + "'}")
