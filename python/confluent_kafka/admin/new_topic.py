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

"""``NewTopic``: Java's ``org.apache.kafka.clients.admin.NewTopic``.

Java's constructors ``(name, int numPartitions, short replicationFactor)``,
``(name, Optional<Integer> numPartitions, Optional<Short> replicationFactor)``
and ``(name, replicasAssignments)`` are one keyword-only constructor matched by
``java_forms`` (CLAUDE.md, Python Binding Conventions, Signatures): the first
two have the same names at the same positions and Python types ``int`` and
``int | None``, so they are one form whose ``None`` is Java's
``Optional.empty()`` (the broker's ``num.partitions`` /
``default.replication.factor``). ``num_partitions`` and ``replication_factor``
must be given together, ``None`` counting as given.

``configs`` is Java's setter and getter of one name: no argument reads, one
argument sets and returns the topic, an explicit ``None`` setting Java's
``null``. Java's package-private
``convertToCreatableTopic()`` is not generated.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import overload

from confluent_kafka._args import MISSING, Form, java_forms
from confluent_kafka._java import java_str

from ._values import frozen

__all__ = ["NewTopic"]

# Java's CreateTopicsRequest.NO_NUM_PARTITIONS / NO_REPLICATION_FACTOR.
_NO_NUM_PARTITIONS = -1
_NO_REPLICATION_FACTOR = -1


class NewTopic:
    """A new topic to be created via the admin client's ``create_topics()``.

    Java: ``org.apache.kafka.clients.admin.NewTopic``.
    """

    __slots__ = ("_name", "_num_partitions", "_replication_factor", "_replicas_assignments",
                 "_configs")

    @overload
    def __init__(self, *, name: str, num_partitions: int, replication_factor: int) -> None: ...
    @overload
    def __init__(self, *, name: str, num_partitions: int | None,
                 replication_factor: int | None) -> None: ...
    @overload
    def __init__(self, *, name: str,
                 replicas_assignments: Mapping[int, Sequence[int]]) -> None: ...

    @java_forms(Form("name", "num_partitions", "replication_factor"),
                Form("name", "replicas_assignments"))
    def __init__(self, *, name: str, num_partitions: int | None = MISSING,
                 replication_factor: int | None = MISSING,
                 replicas_assignments: Mapping[int, Sequence[int]] = MISSING) -> None:
        """A new topic with the specified replication factor and number of
        partitions, ``None`` defaulting them to the broker configurations for
        ``num.partitions`` and ``default.replication.factor`` respectively; or
        a new topic with the specified replica assignment configuration.

        ``replicas_assignments``: a map from partition id to replica ids (i.e.
        broker ids). Although not enforced, it is generally a good idea for all
        partitions to have the same number of replicas. The first replica will
        be treated as the preferred leader. A ``None`` raises ``TypeError``
        (Java's ``Collections.unmodifiableMap(null)``).
        """
        self._name = name
        self._configs: dict[str, str | None] | None = None
        if replicas_assignments is MISSING:
            self._num_partitions: int | None = num_partitions
            self._replication_factor: int | None = replication_factor
            self._replicas_assignments: dict[int, tuple[int, ...]] | None = None
        else:
            if replicas_assignments is None:
                raise TypeError("replicas_assignments must not be None")
            self._num_partitions = None
            self._replication_factor = None
            self._replicas_assignments = {
                k: tuple(v) for k, v in replicas_assignments.items()}

    def name(self) -> str:
        """The name of the topic to be created."""
        return self._name

    def num_partitions(self) -> int:
        """The number of partitions for the new topic or -1 if a replica
        assignment has been specified."""
        n = self._num_partitions
        return _NO_NUM_PARTITIONS if n is None else n

    def replication_factor(self) -> int:
        """The replication factor for the new topic or -1 if a replica
        assignment has been specified."""
        r = self._replication_factor
        return _NO_REPLICATION_FACTOR if r is None else r

    def replicas_assignments(self) -> dict[int, list[int]] | None:
        """A map from partition id to replica ids (i.e. broker ids) or ``None``
        if the number of partitions and replication factor have been specified
        instead."""
        a = self._replicas_assignments
        return None if a is None else {k: list(v) for k, v in a.items()}

    @overload
    def configs(self) -> dict[str, str | None] | None: ...
    @overload
    def configs(self, *, configs: Mapping[str, str | None] | None) -> NewTopic: ...

    def configs(self, *, configs: Mapping[str, str | None] | None = MISSING
                ) -> dict[str, str | None] | None | NewTopic:
        """With no argument: the configuration for the new topic or ``None`` if
        no configs ever specified.

        With ``configs``: set the configuration to use on the new topic and
        return this ``NewTopic``. Java stores the map as given, so a ``None``
        value is Java's ``null`` config value, which ``createTopics`` sends to
        the broker as a null value.
        """
        if configs is MISSING:
            c = self._configs
            return None if c is None else dict(c)
        self._configs = None if configs is None else dict(configs)
        return self

    def __str__(self) -> str:
        n, r = self._num_partitions, self._replication_factor
        return ("(name=" + java_str(self._name)
                + ", numPartitions=" + ("default" if n is None else str(n))
                + ", replicationFactor=" + ("default" if r is None else str(r))
                + ", replicasAssignments=" + java_str(self._replicas_assignments)
                + ", configs=" + java_str(self._configs) + ")")

    def __eq__(self, other: object) -> bool:
        if self is other:
            return True
        if type(other) is not type(self):
            return NotImplemented
        assert isinstance(other, NewTopic)
        return (self._name == other._name
                and self._num_partitions == other._num_partitions
                and self._replication_factor == other._replication_factor
                and self._replicas_assignments == other._replicas_assignments
                and self._configs == other._configs)

    def __hash__(self) -> int:
        return hash((self._name, self._num_partitions, self._replication_factor,
                     frozen(self._replicas_assignments), frozen(self._configs)))
