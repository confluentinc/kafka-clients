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

"""``ReplicaInfo``: Java's ``org.apache.kafka.clients.admin.ReplicaInfo``.

Java declares no ``equals`` / ``hashCode``, so equality is identity.
"""

from __future__ import annotations

from confluent_kafka._java import java_str

__all__ = ["ReplicaInfo"]


class ReplicaInfo:
    """A description of a replica on a particular broker.

    Java: ``org.apache.kafka.clients.admin.ReplicaInfo``.
    """

    __slots__ = ("_size", "_offset_lag", "_is_future")

    def __init__(self, *, size: int, offset_lag: int, is_future: bool) -> None:
        self._size = size
        self._offset_lag = offset_lag
        self._is_future = is_future

    def size(self) -> int:
        """The total size of the log segments in this replica in bytes. This
        value does not include the size of data stored in remote storage."""
        return self._size

    def offset_lag(self) -> int:
        """The lag of the log's LEO with respect to the partition's high
        watermark (if it is the current log for the partition) or the current
        replica's LEO (if it is the future log for the partition)."""
        return self._offset_lag

    def is_future(self) -> bool:
        """Whether this replica has been created by a AlterReplicaLogDirsRequest
        but not yet replaced the current replica on the broker: ``True`` if
        this log is created by AlterReplicaLogDirsRequest and will replace the
        current log of the replica at some time in the future."""
        return self._is_future

    def __str__(self) -> str:
        return ("ReplicaInfo(size=" + str(self._size) + ", offsetLag=" + str(self._offset_lag)
                + ", isFuture=" + java_str(self._is_future) + ")")
