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

"""``confluent_kafka.common.metrics``: Java's ``org.apache.kafka.common.metrics``.

``KafkaMetric``, the placeholders ``MetricConfig``, ``Measurable``,
``MetricValueProvider`` and ``Gauge``, and the errors of the Java package
(``QuotaViolationError``). One file per class; import the classes from
this module, never from the files (CLAUDE.md, Python Binding Conventions, Modules).
"""

from __future__ import annotations

from .gauge import Gauge as Gauge
from .kafka_metric import KafkaMetric as KafkaMetric
from .measurable import Measurable as Measurable
from .metric_config import MetricConfig as MetricConfig
from .metric_value_provider import MetricValueProvider as MetricValueProvider

__all__ = [
    "Gauge",
    "KafkaMetric",
    "Measurable",
    "MetricConfig",
    "MetricValueProvider",
]

# BEGIN GENERATED ERRORS (cargo xtask generate-error-codes; do not edit)
from .quota_violation_error import QuotaViolationError as QuotaViolationError
__all__ += [
    "QuotaViolationError",
]
# END GENERATED ERRORS
