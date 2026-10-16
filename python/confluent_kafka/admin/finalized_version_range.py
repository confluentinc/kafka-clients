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

"""``FinalizedVersionRange``: Java's
``org.apache.kafka.clients.admin.FinalizedVersionRange``."""

from __future__ import annotations

from confluent_kafka.illegal_argument_error import IllegalArgumentError

__all__ = ["FinalizedVersionRange"]


class FinalizedVersionRange:
    """Represents a range of version levels supported by every broker in a
    cluster for some feature.

    Java: ``org.apache.kafka.clients.admin.FinalizedVersionRange``.
    """

    __slots__ = ("_min_version_level", "_max_version_level")

    def __init__(self, *, min_version_level: int, max_version_level: int) -> None:
        """Raises an error unless the following condition is met:
        ``min_version_level >= 1`` and ``max_version_level >= 1`` and
        ``max_version_level >= min_version_level``.

        Raises ``IllegalArgumentError`` when the condition described above is
        not met.
        """
        if (min_version_level < 0 or max_version_level < 0
                or max_version_level < min_version_level):
            raise IllegalArgumentError(message=(
                "Expected minVersionLevel >= 0, maxVersionLevel >= 0 and"
                " maxVersionLevel >= minVersionLevel, but received"
                f" minVersionLevel: {min_version_level}, maxVersionLevel: {max_version_level}"))
        self._min_version_level = min_version_level
        self._max_version_level = max_version_level

    def min_version_level(self) -> int:
        return self._min_version_level

    def max_version_level(self) -> int:
        return self._max_version_level

    def __eq__(self, other: object) -> bool:
        if self is other:
            return True
        if not isinstance(other, FinalizedVersionRange):
            return NotImplemented
        return (self._min_version_level == other._min_version_level
                and self._max_version_level == other._max_version_level)

    def __hash__(self) -> int:
        return hash((self._min_version_level, self._max_version_level))

    def __str__(self) -> str:
        return (f"FinalizedVersionRange[min_version_level:{self._min_version_level}, "
                f"max_version_level:{self._max_version_level}]")
