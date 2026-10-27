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

"""``FeatureUpdate``: Java's ``org.apache.kafka.clients.admin.FeatureUpdate``.

``UpgradeType`` is an ``enum.IntEnum`` valued by Java's one integer field,
``code``.
"""

from __future__ import annotations

from enum import IntEnum

from confluent_kafka._java import java_str
from confluent_kafka.illegal_argument_error import IllegalArgumentError

__all__ = ["FeatureUpdate"]


class FeatureUpdate:
    """Encapsulates details about an update to a finalized feature.

    Java: ``org.apache.kafka.clients.admin.FeatureUpdate``.
    """

    class UpgradeType(IntEnum):
        """The kind of upgrade to perform.

        Java: ``org.apache.kafka.clients.admin.FeatureUpdate.UpgradeType``.
        """

        UNKNOWN = 0
        UPGRADE = 1
        SAFE_DOWNGRADE = 2
        UNSAFE_DOWNGRADE = 3

        def code(self) -> int:
            return self.value

        @staticmethod
        def from_code(*, code: int) -> FeatureUpdate.UpgradeType:
            if code == 1:
                return FeatureUpdate.UpgradeType.UPGRADE
            elif code == 2:
                return FeatureUpdate.UpgradeType.SAFE_DOWNGRADE
            elif code == 3:
                return FeatureUpdate.UpgradeType.UNSAFE_DOWNGRADE
            else:
                return FeatureUpdate.UpgradeType.UNKNOWN

        def __str__(self) -> str:
            return self.name

    __slots__ = ("_max_version_level", "_upgrade_type")

    def __init__(self, *, max_version_level: int, upgrade_type: FeatureUpdate.UpgradeType) -> None:
        """``max_version_level``: the new maximum version level for the
        finalized feature. A value of zero is special and indicates that the
        update is intended to delete the finalized feature, and should be
        accompanied by setting the upgrade type to safe or unsafe.

        ``upgrade_type``: indicate what kind of upgrade should be performed in
        this operation: ``UPGRADE`` upgrades the feature level;
        ``SAFE_DOWNGRADE`` permits only downgrades which do not result in
        metadata loss; ``UNSAFE_DOWNGRADE`` permits any downgrade, including
        those which may result in metadata loss.
        """
        if max_version_level == 0:
            if upgrade_type is None:
                raise TypeError("upgrade_type must not be None")
            if upgrade_type is FeatureUpdate.UpgradeType.UPGRADE:
                raise IllegalArgumentError(message=(
                    "The upgradeType flag should be set to SAFE_DOWNGRADE or UNSAFE_DOWNGRADE "
                    f"when the provided maxVersionLevel:{max_version_level} is < 1."))
        if max_version_level < 0:
            raise IllegalArgumentError(message="Cannot specify a negative version level.")
        self._max_version_level = max_version_level
        self._upgrade_type = upgrade_type

    def max_version_level(self) -> int:
        return self._max_version_level

    def upgrade_type(self) -> FeatureUpdate.UpgradeType:
        return self._upgrade_type

    def __eq__(self, other: object) -> bool:
        if self is other:
            return True
        if not isinstance(other, FeatureUpdate):
            return NotImplemented
        if self._max_version_level != other._max_version_level:
            return False
        if self._upgrade_type is None:
            # Java's this.upgradeType.equals(...) dereferences it (Types); its
            # hashCode is null-safe.
            raise TypeError("upgrade_type must not be None")
        return self._upgrade_type == other._upgrade_type

    def __hash__(self) -> int:
        return hash((self._max_version_level, self._upgrade_type))

    def __str__(self) -> str:
        return (f"FeatureUpdate{{maxVersionLevel:{self._max_version_level}, "
                f"upgradeType:{java_str(self._upgrade_type)}}}")
