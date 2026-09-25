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

"""``Metric`` and ``KafkaMetric`` — the value type of the ``metrics()`` map.

``Metric`` is translated from the Java interface ``org.apache.kafka.common.
Metric`` and modelled as a ``Protocol``. ``KafkaMetric`` is Java's concrete
``org.apache.kafka.common.metrics.KafkaMetric``, the value the clients hand out
and the parameter type of ``register_metric_for_subscription`` (KIP-1076).

``config()`` and ``measurable()`` return the Java types ``MetricConfig`` and
``Measurable``, which the spec (§5.1) defers to the metrics/plugin design pass;
until then they are typed as opaque ``object`` (see the P2 clarifications
entry). ``metric_value()`` is Java's ``Object`` (``Any``).
"""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable

from .metric_name import MetricName


@runtime_checkable
class Metric(Protocol):
    """The value of the ``metrics()`` map.

    Java interface: ``org.apache.kafka.common.Metric`` (``metricName()``,
    ``metricValue()``). ``metric_value()`` is a float for measurable metrics and
    an arbitrary object for non-measurable gauges — Java's ``Object``.
    """

    def metric_name(self) -> MetricName: ...
    def metric_value(self) -> Any: ...


class KafkaMetric(Metric, Protocol):
    """The concrete ``Metric`` the clients hand out (KIP-1076 registration
    parameter type).

    Java final class: ``org.apache.kafka.common.metrics.KafkaMetric``
    (``config()``, ``metricName()``, ``metricValue()``, ``isMeasurable()``,
    ``measurable()``). Not user-constructed: instances come from ``metrics()``.
    Modelled as a ``Protocol`` because the concrete instance is produced by the
    core; ``config()`` / ``measurable()`` return types are defined in the
    metrics design pass and are typed as opaque ``object`` for now.
    """

    def metric_name(self) -> MetricName: ...
    def metric_value(self) -> Any: ...
    def is_measurable(self) -> bool: ...
    def config(self) -> object: ...       # Java MetricConfig — metrics pass
    def measurable(self) -> object: ...   # Java Measurable — metrics pass
