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

"""``LogDirDescription``: Java's ``org.apache.kafka.clients.admin.LogDirDescription``.

Java's constructors ``(error, replicaInfos)`` and ``(error, replicaInfos,
totalBytes, usableBytes)`` are one keyword-only constructor matched by
``java_forms``: the shorter passes ``UNKNOWN_VOLUME_BYTES`` (``-1``) for both
sizes, which, like Java, are then empty (``None``).

Java declares no ``equals`` / ``hashCode``, so equality is identity.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, overload

from confluent_kafka._args import MISSING, Form, java_forms
from confluent_kafka._java import java_str

from ._values import optional_str

if TYPE_CHECKING:
    from confluent_kafka.common import TopicPartition
    from confluent_kafka.common.errors import ApiError

    from .replica_info import ReplicaInfo

__all__ = ["LogDirDescription"]

# Java's DescribeLogDirsResponse.UNKNOWN_VOLUME_BYTES.
_UNKNOWN_VOLUME_BYTES = -1


class LogDirDescription:
    """A description of a log directory on a particular broker.

    Java: ``org.apache.kafka.clients.admin.LogDirDescription``.
    """

    __slots__ = ("_replica_infos", "_error", "_total_bytes", "_usable_bytes", "_is_cordoned")

    @overload
    def __init__(self, *, error: ApiError | None,
                 replica_infos: Mapping[TopicPartition, ReplicaInfo]) -> None: ...
    @overload
    def __init__(self, *, error: ApiError | None,
                 replica_infos: Mapping[TopicPartition, ReplicaInfo], total_bytes: int,
                 usable_bytes: int) -> None: ...
    @overload
    def __init__(self, *, error: ApiError | None,
                 replica_infos: Mapping[TopicPartition, ReplicaInfo], total_bytes: int,
                 usable_bytes: int, is_cordoned: bool) -> None: ...

    @java_forms(Form("error", "replica_infos"),
                Form("error", "replica_infos", "total_bytes", "usable_bytes"),
                Form("error", "replica_infos", "total_bytes", "usable_bytes", "is_cordoned",
                     defaults={"total_bytes": _UNKNOWN_VOLUME_BYTES,
                               "usable_bytes": _UNKNOWN_VOLUME_BYTES,
                               "is_cordoned": False}))
    def __init__(self, *, error: ApiError | None,
                 replica_infos: Mapping[TopicPartition, ReplicaInfo],
                 total_bytes: int = MISSING, usable_bytes: int = MISSING,
                 is_cordoned: bool = MISSING) -> None:
        self._error = error
        self._replica_infos = dict(replica_infos)
        self._total_bytes = None if total_bytes == _UNKNOWN_VOLUME_BYTES else total_bytes
        self._usable_bytes = None if usable_bytes == _UNKNOWN_VOLUME_BYTES else usable_bytes
        self._is_cordoned = is_cordoned

    def error(self) -> ApiError | None:
        """Returns ``ApiError`` if the log directory is offline or an error
        occurred, otherwise returns ``None``.

        - ``KafkaStorageError`` - The log directory is offline.
        - ``UnknownServerError`` - The server experienced an unexpected error
          when processing the request.
        """
        return self._error

    def replica_infos(self) -> dict[TopicPartition, ReplicaInfo]:
        """A map from topic partition to replica information for that
        partition in this log directory."""
        return dict(self._replica_infos)

    def total_bytes(self) -> int | None:
        """The total size of the volume this log directory is on or ``None`` if
        the broker did not return a value. For volumes larger than
        ``Long.MAX_VALUE``, ``Long.MAX_VALUE`` is returned. This value does not
        include the size of data stored in remote storage."""
        return self._total_bytes

    def usable_bytes(self) -> int | None:
        """The usable size on the volume this log directory is on or ``None`` if
        the broker did not return a value. For usable sizes larger than
        ``Long.MAX_VALUE``, ``Long.MAX_VALUE`` is returned. This value does not
        include the size of data stored in remote storage."""
        return self._usable_bytes

    def is_cordoned(self) -> bool:
        """Whether this log directory is cordoned (KIP-1066): the broker does
        not place new replicas on it."""
        return self._is_cordoned

    def __str__(self) -> str:
        return ("LogDirDescription(replicaInfos=" + java_str(self._replica_infos)
                + ", error=" + java_str(self._error)
                + ", totalBytes=" + optional_str(self._total_bytes, "OptionalLong")
                + ", usableBytes=" + optional_str(self._usable_bytes, "OptionalLong")
                + ", isCordoned=" + java_str(self._is_cordoned) + ")")
