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

"""``FeatureMetadata``: Java's ``org.apache.kafka.clients.admin.FeatureMetadata``.

Java's constructor is package-private, so ``FeatureMetadata()`` raises
``TypeError`` naming the method that returns it, and the client builds it
through ``_new``. Java's ``Optional<Long>`` epoch is
``int | None`` (Types).
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any

from confluent_kafka._java import java_str

from ._values import frozen

if TYPE_CHECKING:
    from .finalized_version_range import FinalizedVersionRange
    from .supported_version_range import SupportedVersionRange

__all__ = ["FeatureMetadata"]


def _map_to_string(feature_versions_map: Mapping[str, object]) -> str:
    return "{" + ", ".join(
        "(" + java_str(k) + " -> " + java_str(v) + ")"
        for k, v in feature_versions_map.items()) + "}"


class FeatureMetadata:
    """Encapsulates details about finalized as well as supported features.
    This is particularly useful to hold the result returned by the admin
    client's ``describe_features()``.

    Java: ``org.apache.kafka.clients.admin.FeatureMetadata``.
    """

    __slots__ = ("_finalized_features", "_finalized_features_epoch", "_supported_features")

    _finalized_features: dict[str, FinalizedVersionRange]
    _finalized_features_epoch: int | None
    _supported_features: dict[str, SupportedVersionRange]

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        raise TypeError(
            "FeatureMetadata cannot be constructed directly; use "
            "DescribeFeaturesResult.feature_metadata()")

    @classmethod
    def _new(cls, *, finalized_features: Mapping[str, FinalizedVersionRange],
             finalized_features_epoch: int | None,
             supported_features: Mapping[str, SupportedVersionRange]) -> FeatureMetadata:
        """Java's package-private ``FeatureMetadata(Map, Optional<Long>,
        Map)``."""
        metadata = cls.__new__(cls)
        metadata._finalized_features = dict(finalized_features)
        metadata._finalized_features_epoch = finalized_features_epoch
        metadata._supported_features = dict(supported_features)
        return metadata

    def finalized_features(self) -> dict[str, FinalizedVersionRange]:
        """Returns a map of finalized feature versions. Each entry in the map
        contains a key being a feature name and the value being a range of
        version levels supported by every broker in the cluster."""
        return dict(self._finalized_features)

    def finalized_features_epoch(self) -> int | None:
        """The epoch for the finalized features. If the returned value is
        ``None``, it means the finalized features are absent/unavailable."""
        return self._finalized_features_epoch

    def supported_features(self) -> dict[str, SupportedVersionRange]:
        """Returns a map of supported feature versions. Each entry in the map
        contains a key being a feature name and the value being a range of
        versions supported by a particular broker in the cluster."""
        return dict(self._supported_features)

    def __eq__(self, other: object) -> bool:
        if self is other:
            return True
        if not isinstance(other, FeatureMetadata):
            return NotImplemented
        return (self._finalized_features == other._finalized_features
                and self._finalized_features_epoch == other._finalized_features_epoch
                and self._supported_features == other._supported_features)

    def __hash__(self) -> int:
        return hash((frozen(self._finalized_features), self._finalized_features_epoch,
                     frozen(self._supported_features)))

    def __str__(self) -> str:
        epoch = self._finalized_features_epoch
        return ("FeatureMetadata{finalizedFeatures:" + _map_to_string(self._finalized_features)
                + ", finalizedFeaturesEpoch:" + ("<none>" if epoch is None else str(epoch))
                + ", supportedFeatures:" + _map_to_string(self._supported_features) + "}")
