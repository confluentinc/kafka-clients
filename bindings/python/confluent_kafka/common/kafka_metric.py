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

"""``KafkaMetric``: Java's ``org.apache.kafka.common.metrics.KafkaMetric``.

A class (CLAUDE.md, Python Binding Conventions, Types), in
``confluent_kafka.common``, the nearest module above Java's
``common.metrics`` package. It reaches the surface through
``QuotaViolationError.metric()``.

Java's constructor takes a ``MetricValueProvider`` and a ``Time``, types the
rules map to nothing: here they are any object with Java's methods under their
snake_case names (``value(config, now)``, ``milliseconds()``), the way a mock
uses the ``Partitioner`` and ``Cluster`` it is given. ``MetricConfig`` and
``Measurable`` are the placeholder aliases of ``object`` *(deviation)*; a value
provider is measurable when it has Java's ``measure(config, now)``.
"""

from __future__ import annotations

import threading
import time as _time
from typing import TYPE_CHECKING, Any, overload

from confluent_kafka.illegal_state_error import IllegalStateError
from confluent_kafka.null_pointer_error import NullPointerError

if TYPE_CHECKING:
    from .measurable import Measurable
    from .metric_config import MetricConfig
    from .metric_name import MetricName

__all__ = ["KafkaMetric"]

_NOT_GIVEN: Any = object()


class KafkaMetric:
    """A metric, backed by a value provider.

    Java: ``org.apache.kafka.common.metrics.KafkaMetric`` (``final``,
    ``implements Metric``).
    """

    __slots__ = ("_metric_name", "_lock", "_time", "_metric_value_provider", "_config")

    def __init__(self, *, lock: object, metric_name: MetricName,
                 value_provider: object, config: MetricConfig,
                 time: object) -> None:
        """Create a metric to monitor an object that implements
        ``MetricValueProvider``: ``lock`` guards its reads (a context manager
        such as ``threading.Lock``; anything else is not locked), ``time``
        gives ``milliseconds()``."""
        self._metric_name = metric_name
        self._lock = lock
        # Objects.requireNonNull(valueProvider, "valueProvider must not be null")
        if value_provider is None:
            raise NullPointerError(message="valueProvider must not be null")
        self._metric_value_provider = value_provider
        self._config = config
        self._time = time

    @overload
    def config(self) -> MetricConfig: ...
    @overload
    def config(self, *, config: MetricConfig) -> None: ...

    def config(self, *, config: Any = _NOT_GIVEN) -> Any:
        """Java's ``config()`` (the configuration of this metric) and
        ``config(MetricConfig)`` (set it). "This is supposed to be used by
        server only." """
        if config is _NOT_GIVEN:
            return self._config
        with self._locked():
            self._config = config
        return None

    def metric_name(self) -> MetricName:
        """The name of this metric."""
        return self._metric_name

    def metric_value(self) -> Any:
        """The metric value, via the provider's ``value(config, now)``."""
        now = self._time.milliseconds()  # type: ignore[attr-defined]
        with self._locked():
            return self._metric_value_provider.value(self._config, now)  # type: ignore[attr-defined]

    def is_measurable(self) -> bool:
        """Whether the value provider is a ``Measurable``."""
        return callable(getattr(self._metric_value_provider, "measure", None))

    def measurable(self) -> Measurable:
        """The value provider, which should be a ``Measurable``; raises
        ``IllegalStateError`` if it is not."""
        if self.is_measurable():
            return self._metric_value_provider
        cls = type(self._metric_value_provider)
        # Java: "Not a measurable: " + this.metricValueProvider.getClass()
        raise IllegalStateError(
            message=f"Not a measurable: class {cls.__module__}.{cls.__qualname__}")

    def _locked(self) -> Any:
        lock = self._lock
        if hasattr(lock, "__enter__") and hasattr(lock, "__exit__"):
            return lock
        return _NoLock()

    @classmethod
    def _snapshot(cls, *, name: str, group: str, value: Any) -> KafkaMetric:
        """A metric the core reported by name and value (the metric of a
        ``QuotaViolationError``): a constant provider, no configuration."""
        from .metric_name import MetricName

        return cls(lock=threading.Lock(),
                   metric_name=MetricName(name=name, group=group, description="", tags={}),
                   value_provider=_Constant(value), config=None, time=_SystemTime())


class _NoLock:
    __slots__ = ()

    def __enter__(self) -> None:
        return None

    def __exit__(self, *exc: object) -> None:
        return None


class _Constant:
    """A ``MetricValueProvider`` returning one value."""

    __slots__ = ("_value",)

    def __init__(self, value: Any) -> None:
        self._value = value

    def value(self, config: object, now: int) -> Any:
        return self._value


class _SystemTime:
    """Java's ``Time.SYSTEM``: ``milliseconds()`` of the wall clock."""

    __slots__ = ()

    def milliseconds(self) -> int:
        return int(_time.time() * 1000)
