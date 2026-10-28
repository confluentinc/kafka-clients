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

"""``PartitionReassignment``: Java's
``org.apache.kafka.clients.admin.PartitionReassignment``.

Java declares no ``equals`` / ``hashCode``, so equality is identity.
"""

from __future__ import annotations

from collections.abc import Sequence

from confluent_kafka._java import java_str

__all__ = ["PartitionReassignment"]


class PartitionReassignment:
    """A partition reassignment, which has been listed via the admin client's
    ``list_partition_reassignments()``.

    Java: ``org.apache.kafka.clients.admin.PartitionReassignment``.
    """

    __slots__ = ("_replicas", "_adding_replicas", "_removing_replicas")

    def __init__(self, *, replicas: Sequence[int], adding_replicas: Sequence[int],
                 removing_replicas: Sequence[int]) -> None:
        # Java's ``Collections.unmodifiableList`` dereferences each list, in
        # this order (Types).
        if replicas is None:
            raise TypeError("replicas must not be None")
        self._replicas = tuple(replicas)
        if adding_replicas is None:
            raise TypeError("adding_replicas must not be None")
        self._adding_replicas = tuple(adding_replicas)
        if removing_replicas is None:
            raise TypeError("removing_replicas must not be None")
        self._removing_replicas = tuple(removing_replicas)

    def replicas(self) -> list[int]:
        """The brokers which this partition currently resides on."""
        return list(self._replicas)

    def adding_replicas(self) -> list[int]:
        """The brokers that we are adding this partition to as part of a
        reassignment. A subset of replicas."""
        return list(self._adding_replicas)

    def removing_replicas(self) -> list[int]:
        """The brokers that we are removing this partition from as part of a
        reassignment. A subset of replicas."""
        return list(self._removing_replicas)

    def __str__(self) -> str:
        return ("PartitionReassignment(replicas=" + java_str(self._replicas)
                + ", addingReplicas=" + java_str(self._adding_replicas)
                + ", removingReplicas=" + java_str(self._removing_replicas) + ")")
