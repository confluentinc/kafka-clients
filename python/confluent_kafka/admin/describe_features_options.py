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

"""``DescribeFeaturesOptions``: Java's ``org.apache.kafka.clients.admin.DescribeFeaturesOptions``.

Java builds it with setters, so the constructor also takes one keyword per setter
field (CLAUDE.md, Python Binding Conventions, Signatures). ``nodeId`` starts as
``OptionalInt.empty()``, which has no value of the setter's type (``int``), so the stub
writes ``node_id: int = ...``. The setter stores ``OptionalInt.of(nodeId)``, which is
the ``int`` itself here, as ``OptionalInt`` is ``int | None`` (Types).
"""

from __future__ import annotations

from typing import Any

from confluent_kafka._args import MISSING
from confluent_kafka.admin.abstract_options import AbstractOptions

__all__ = ["DescribeFeaturesOptions"]


class DescribeFeaturesOptions(AbstractOptions):
    """Options for ``Admin.describe_features()``.

    Java: ``org.apache.kafka.clients.admin.DescribeFeaturesOptions``.
    """

    __slots__ = ("_node_id",)

    def __init__(self, *, timeout_ms: int | None = MISSING, node_id: int = MISSING) -> None:
        super().__init__()
        self._node_id: int | None = None  # OptionalInt.empty()
        if timeout_ms is not MISSING:
            self.timeout_ms(timeout_ms=timeout_ms)
        if node_id is not MISSING:
            self.node_id(node_id=node_id)

    def node_id(self, *, node_id: int = MISSING) -> Any:
        """With ``node_id``: set the node id to which the request should be sent, and
        return this object.

        Without: the node id to which the request should be sent. If the node id is
        empty (``None``), the request will be sent to the arbitrary controller/broker.
        """
        if node_id is MISSING:
            return self._node_id
        if node_id is None:  # a int: Java cannot pass null
            raise TypeError("node_id must not be None")
        self._node_id = node_id  # OptionalInt.of(nodeId)
        self._record("node_id", self._node_id)
        return self
