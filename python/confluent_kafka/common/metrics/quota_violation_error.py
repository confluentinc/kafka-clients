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

"""``QuotaViolationError``: Java's ``org.apache.kafka.common.metrics.QuotaViolationException``.

GENERATED, DO NOT EDIT. Produced from the Java source by
``cargo xtask generate-error-codes`` (CLAUDE.md, Python Binding Conventions,
Errors) and validated for staleness by ``cargo xtask check-generated``.
"""

from __future__ import annotations

from typing import ClassVar, TYPE_CHECKING

from confluent_kafka import _throwable
from confluent_kafka.common.kafka_error import KafkaError

if TYPE_CHECKING:
    from confluent_kafka.common.kafka_metric import KafkaMetric

__all__ = ["QuotaViolationError"]


class QuotaViolationError(KafkaError):
    """Thrown when a sensor records a value that causes a metric to go outside the
    bounds configured as its quota

    Java: ``org.apache.kafka.common.metrics.QuotaViolationException``.
    """

    __module__ = "confluent_kafka.common.metrics"

    _ffi_id: ClassVar[int] = -26  # kafka_common_ErrorCode_QUOTA_VIOLATION

    def __init__(
        self,
        *,
        metric: KafkaMetric,
        value: float,
        bound: float,
    ) -> None:
        _throwable.init(self, None, None)
        self._metric = metric
        self._value = value
        self._bound = bound
        self._java_kwargs = _throwable.kwargs(metric=metric, value=value, bound=bound)

    def metric(self) -> KafkaMetric:
        """Java's ``metric()``."""
        return self._metric

    def value(self) -> float:
        """Java's ``value()``."""
        return self._value

    def bound(self) -> float:
        """Java's ``bound()``."""
        return self._bound
