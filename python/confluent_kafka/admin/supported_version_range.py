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

"""``SupportedVersionRange``: Java's
``org.apache.kafka.clients.admin.SupportedVersionRange``."""

from __future__ import annotations

from confluent_kafka.illegal_argument_error import IllegalArgumentError

__all__ = ["SupportedVersionRange"]


class SupportedVersionRange:
    """Represents a range of versions that a particular broker supports for
    some feature.

    Java: ``org.apache.kafka.clients.admin.SupportedVersionRange``.
    """

    __slots__ = ("_min_version", "_max_version")

    def __init__(self, *, min_version: int, max_version: int) -> None:
        """Raises an error unless the following conditions are met:
        ``0 <= min_version <= max_version``.

        Raises ``IllegalArgumentError`` when the condition described above is
        not met.
        """
        if min_version < 0 or max_version < 0 or max_version < min_version:
            raise IllegalArgumentError(message=(
                f"Expected 0 <= minVersion <= maxVersion but received "
                f"minVersion:{min_version}, maxVersion:{max_version}."))
        self._min_version = min_version
        self._max_version = max_version

    def min_version(self) -> int:
        return self._min_version

    def max_version(self) -> int:
        return self._max_version

    def __eq__(self, other: object) -> bool:
        if self is other:
            return True
        if type(other) is not type(self):
            return NotImplemented
        assert isinstance(other, SupportedVersionRange)
        return self._min_version == other._min_version and self._max_version == other._max_version

    def __hash__(self) -> int:
        return hash((self._min_version, self._max_version))

    def __str__(self) -> str:
        return (f"SupportedVersionRange[min_version:{self._min_version}, "
                f"max_version:{self._max_version}]")
