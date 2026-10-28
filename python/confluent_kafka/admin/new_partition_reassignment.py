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

"""``NewPartitionReassignment``: Java's
``org.apache.kafka.clients.admin.NewPartitionReassignment``.

Java declares no ``equals`` / ``hashCode`` / ``toString``, so equality is
identity.
"""

from __future__ import annotations

from collections.abc import Sequence

from confluent_kafka.illegal_argument_error import IllegalArgumentError

__all__ = ["NewPartitionReassignment"]


class NewPartitionReassignment:
    """A new partition reassignment, which can be applied via the admin
    client's ``alter_partition_reassignments()``.

    Java: ``org.apache.kafka.clients.admin.NewPartitionReassignment``.
    """

    __slots__ = ("_target_replicas",)

    def __init__(self, *, target_replicas: Sequence[int]) -> None:
        """Raises ``IllegalArgumentError`` if no replicas are supplied."""
        if target_replicas is None or len(target_replicas) == 0:
            raise IllegalArgumentError(
                message="Cannot create a new partition reassignment without any replicas")
        self._target_replicas = tuple(target_replicas)
        if None in self._target_replicas:
            # Java's ``List.copyOf`` rejects a null element (Types).
            raise TypeError("target_replicas must not contain None")

    def target_replicas(self) -> list[int]:
        return list(self._target_replicas)
