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

"""``MetricName``: Java's ``org.apache.kafka.common.MetricName``, the key of the
``metrics()`` map."""

from __future__ import annotations

from collections.abc import Mapping

from confluent_kafka._java import java_str
from confluent_kafka.null_pointer_error import NullPointerError

__all__ = ["MetricName"]


class MetricName:
    """The ``MetricName`` class encapsulates a metric's name, logical group and
    its related attributes.

    - ``name``: the name of the metric;
    - ``group``: logical group name of the metrics to which this metric belongs;
    - ``description``: a human-readable description to include in the metric;
    - ``tags``: additional key/value attributes of the metric.

    ``group`` and ``tags`` can be used to create unique metric names while
    reporting in JMX or any custom reporting. Equality and the hash are over
    ``group``, ``name`` and ``tags``.

    Java: ``org.apache.kafka.common.MetricName`` (``final``).
    """

    __slots__ = ("_name", "_group", "_description", "_tags")

    def __init__(self, *, name: str, group: str, description: str,
                 tags: Mapping[str, str]) -> None:
        # Java: Objects.requireNonNull on each argument, without a message.
        for value in (name, group, description, tags):
            if value is None:
                raise NullPointerError()
        self._name = name
        self._group = group
        self._description = description
        self._tags = dict(tags)

    def name(self) -> str:
        return self._name

    def group(self) -> str:
        return self._group

    def tags(self) -> dict[str, str]:
        return dict(self._tags)

    def description(self) -> str:
        return self._description

    def __hash__(self) -> int:
        return hash((self._group, self._name, frozenset(self._tags.items())))

    def __eq__(self, other: object) -> bool:
        if self is other:
            return True
        if type(other) is not type(self):
            return NotImplemented
        assert isinstance(other, MetricName)
        return (self._group == other._group and self._name == other._name
                and self._tags == other._tags)

    def __str__(self) -> str:
        return ("MetricName [name=" + self._name + ", group=" + self._group
                + ", description=" + self._description + ", tags="
                + java_str(self._tags) + "]")
