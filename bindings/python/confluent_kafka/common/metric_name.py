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

"""``MetricName`` — the key of the ``metrics()`` map.

Translated from ``org.apache.kafka.common.MetricName`` (Apache Kafka 4.3.1).
Immutable, hashable; equality over ``(name, group, tags)`` — Java excludes
``description`` from ``equals``/``hashCode``.
"""

from __future__ import annotations

from collections.abc import Mapping


class MetricName:
    """The key of the ``metrics()`` map.

    Java: ``org.apache.kafka.common.MetricName``
    (``MetricName(String name, String group, String description,
    Map<String, String> tags)``).
    """

    __slots__ = ("_name", "_group", "_description", "_tags")

    def __init__(self, *, name: str, group: str, description: str,
                 tags: Mapping[str, str]) -> None:
        # Java: Objects.requireNonNull on every argument.
        if name is None or group is None or description is None or tags is None:
            raise TypeError("MetricName arguments must not be null")
        self._name = name
        self._group = group
        self._description = description
        self._tags = dict(tags)

    def name(self) -> str:
        return self._name

    def group(self) -> str:
        return self._group

    def description(self) -> str:
        return self._description

    def tags(self) -> dict[str, str]:
        return dict(self._tags)

    def __eq__(self, other: object) -> bool:
        if self is other:
            return True
        if not isinstance(other, MetricName):
            return NotImplemented
        # Java: group, name and tags — description is not compared.
        return (self._group == other._group
                and self._name == other._name
                and self._tags == other._tags)

    def __hash__(self) -> int:
        # Java: over group, name, tags (description excluded).
        return hash((self._group, self._name,
                     tuple(sorted(self._tags.items()))))

    def __repr__(self) -> str:
        return (f"MetricName [name={self._name}, group={self._group}, "
                f"description={self._description}, tags={self._tags}]")
