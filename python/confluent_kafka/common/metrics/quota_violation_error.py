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

from typing import Any, ClassVar, TYPE_CHECKING

from confluent_kafka import _throwable
from confluent_kafka._java import java_str
from confluent_kafka.common.kafka_error import KafkaError

if TYPE_CHECKING:
    from confluent_kafka.common.metrics.kafka_metric import KafkaMetric

__all__ = ["QuotaViolationError"]


class QuotaViolationError(KafkaError):
    """Thrown when a sensor records a value that causes a metric to go outside the
    bounds configured as its quota

    Java: ``org.apache.kafka.common.metrics.QuotaViolationException``.
    """

    __module__ = "confluent_kafka.common.metrics"

    _ffi_id: ClassVar[int] = -26  # kafka_common_Error_QUOTA_VIOLATION

    def __init__(
        self,
        *,
        metric: KafkaMetric,
        value: float,
        bound: float,
    ) -> None:
        _throwable.init(self, None, None)
        self._metric = metric
        self._value = float(value)
        self._bound = float(bound)
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

    def _java_to_string(self) -> str | None:
        """Java's ``toString()`` override, with the Python class path
        (``_throwable.to_string`` prefers it); ``None`` without the payload."""
        if not (hasattr(self, "_metric") and hasattr(self, "_value") and hasattr(self, "_bound")):
            return None
        return (type(self).__module__ + "." + type(self).__qualname__ + ": '" + java_str(_throwable.non_null(self._metric, "metric").metric_name()) + "' violated quota. Actual: " + java_str(self._value) + ", Threshold: " + java_str(self._bound))

    def __str__(self) -> str:
        """Java's ``toString()`` override's text after the class name (Errors)."""
        if not (hasattr(self, "_metric") and hasattr(self, "_value") and hasattr(self, "_bound")):
            return _throwable.message_text(self)
        return ("'" + java_str(_throwable.non_null(self._metric, "metric").metric_name()) + "' violated quota. Actual: " + java_str(self._value) + ", Threshold: " + java_str(self._bound))

    def __reduce__(self) -> str | tuple[Any, ...]:
        """Refused, as Java's serialization of ``QuotaViolationException``
        throws ``NotSerializableException``: ``metric`` (``KafkaMetric``) is
        not ``Serializable`` (CLAUDE.md, Python Binding Conventions,
        Errors)."""
        raise TypeError(
            "QuotaViolationError cannot be pickled or deep-copied, as Java's "
            "QuotaViolationException cannot be serialized")

    def __copy__(self) -> QuotaViolationError:
        """``copy.copy``: a rebuild from the same constructor arguments, which it
        shares, with the cause, the notes and the attributes a caller added."""
        return _throwable.shallow_copy(self)
