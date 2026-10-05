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

"""``Metric``: Java's ``org.apache.kafka.common.Metric``.

An interface the client only hands out (the values of ``metrics()``), so a
``typing.Protocol`` (CLAUDE.md, Python Binding Conventions, Types).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

if TYPE_CHECKING:
    from .metric_name import MetricName

__all__ = ["Metric"]


@runtime_checkable
class Metric(Protocol):
    """A metric tracked for monitoring purposes.

    Java: ``org.apache.kafka.common.Metric``.
    """

    def metric_name(self) -> MetricName:
        """A name for this metric."""
        ...

    def metric_value(self) -> Any:
        """The value of the metric, which may be measurable or a non-measurable
        gauge."""
        ...
