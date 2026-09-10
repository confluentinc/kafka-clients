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

"""``Node`` — information about a Kafka node.

Translated from ``org.apache.kafka.common.Node`` (Apache Kafka 4.3.1). Java's
three constructors ``(id, host, port)`` / ``(id, host, port, rack)`` /
``(id, host, port, rack, isFenced)`` collapse to one keyword-only constructor
with ``rack=None`` and ``is_fenced=False`` defaults (the shorter overloads'
delegation values). ``has_rack()`` pairs with ``rack()`` as a sentinel/predicate
pair, not an ``Optional`` (rule 3.8).
"""

from __future__ import annotations


class Node:
    """Information about a Kafka node.

    Java: ``org.apache.kafka.common.Node``.
    """

    __slots__ = ("_id", "_id_string", "_host", "_port", "_rack", "_is_fenced")

    def __init__(self, *, id: int, host: str, port: int,
                 rack: str | None = None, is_fenced: bool = False) -> None:
        self._id = id
        self._id_string = str(id)
        self._host = host
        self._port = port
        self._rack = rack
        self._is_fenced = is_fenced

    @staticmethod
    def no_node() -> Node:
        """Java ``noNode()`` — the ``(-1, "", -1)`` placeholder node."""
        return _NO_NODE

    def is_empty(self) -> bool:
        """Java ``isEmpty()`` — a placeholder node (from ``noNode()`` or an
        error response) has no usable host/port."""
        return self._host is None or self._host == "" or self._port < 0

    def id(self) -> int:
        return self._id

    def id_string(self) -> str:
        return self._id_string

    def host(self) -> str:
        return self._host

    def port(self) -> int:
        return self._port

    def has_rack(self) -> bool:
        """Java ``hasRack()`` — true iff this node has a defined rack."""
        return self._rack is not None

    def rack(self) -> str | None:
        return self._rack

    def is_fenced(self) -> bool:
        return self._is_fenced

    def __eq__(self, other: object) -> bool:
        if self is other:
            return True
        if not isinstance(other, Node):
            return NotImplemented
        return (self._id == other._id
                and self._port == other._port
                and self._host == other._host
                and self._rack == other._rack
                and self._is_fenced == other._is_fenced)

    def __hash__(self) -> int:
        return hash((self._id, self._host, self._port, self._rack,
                     self._is_fenced))

    def __repr__(self) -> str:
        # Java toString: host + ":" + port + " (id: " + idString + " rack: " ...
        return (f"{self._host}:{self._port} (id: {self._id_string} "
                f"rack: {self._rack} isFenced: {self._is_fenced})")


_NO_NODE = Node(id=-1, host="", port=-1)
