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

"""``Node``: Java's ``org.apache.kafka.common.Node``.

Java's three constructors ``(id, host, port)``, ``(id, host, port, rack)`` and
``(id, host, port, rack, isFenced)`` are one keyword-only constructor: the
shorter ones pass ``null`` and ``false`` to the longest (CLAUDE.md, Python
Binding Conventions, Signatures), so every combination is a Java form.
"""

from __future__ import annotations

from confluent_kafka._java import java_str

__all__ = ["Node"]


class Node:
    """Information about a Kafka node.

    Java: ``org.apache.kafka.common.Node``.
    """

    __slots__ = ("_id", "_id_string", "_host", "_port", "_rack", "_is_fenced")

    def __init__(self, *, id: int, host: str, port: int,  # noqa: A002 - Java's name
                 rack: str | None = None, is_fenced: bool = False) -> None:
        self._id = id
        self._id_string = str(id)
        self._host = host
        self._port = port
        self._rack = rack
        self._is_fenced = is_fenced

    @staticmethod
    def no_node() -> Node:
        return _NO_NODE

    def is_empty(self) -> bool:
        """Check whether this node is empty, which may be the case if
        ``no_node()`` is used as a placeholder in a response payload with an
        error."""
        return self._host is None or self._host == "" or self._port < 0

    def id(self) -> int:
        """The node id of this node."""
        return self._id

    def id_string(self) -> str:
        """String representation of the node id. Typically the integer id is
        used to serialize over the wire, the string representation is used as
        an identifier with ``NetworkClient`` code."""
        return self._id_string

    def host(self) -> str:
        """The host name for this node."""
        return self._host

    def port(self) -> int:
        """The port for this node."""
        return self._port

    def has_rack(self) -> bool:
        """True if this node has a defined rack."""
        return self._rack is not None

    def rack(self) -> str | None:
        """The rack for this node."""
        return self._rack

    def is_fenced(self) -> bool:
        """Returns whether this node is fenced. This applies to broker nodes
        only. For controller quorum nodes, this field is not relevant and is
        defined to be ``False``."""
        return self._is_fenced

    def __hash__(self) -> int:
        return hash((self._host, self._id, self._port, self._rack, self._is_fenced))

    def __eq__(self, other: object) -> bool:
        if self is other:
            return True
        if type(other) is not type(self):
            return NotImplemented
        assert isinstance(other, Node)
        return (self._id == other._id
                and self._port == other._port
                and self._host == other._host
                and self._rack == other._rack
                and self._is_fenced == other._is_fenced)

    def __str__(self) -> str:
        return (java_str(self._host) + ":" + str(self._port) + " (id: " + self._id_string
                + " rack: " + java_str(self._rack) + " isFenced: "
                + java_str(self._is_fenced) + ")")


_NO_NODE = Node(id=-1, host="", port=-1)
