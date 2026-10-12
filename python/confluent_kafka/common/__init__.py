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

"""``confluent_kafka.common``: Java's ``org.apache.kafka.common``.

The common value types, plus the types of ``common.*`` subpackages that are not
a module of their own (CLAUDE.md, Python Binding Conventions, Modules):
``TimestampType`` (``common.record``), ``Headers`` (``common.header``),
``KafkaMetric``, ``MetricConfig`` and ``Measurable`` (``common.metrics``). The
errors of ``org.apache.kafka.common`` (``KafkaError``, ``InvalidRecordError``)
live here too; the other error packages are the modules ``common.errors``,
``common.requests``, ``common.network``, ``common.metrics`` and
``common.protocol.types``.
"""

from __future__ import annotations

from .cluster import Cluster as Cluster
from .headers import Headers as Headers
from .kafka_metric import KafkaMetric as KafkaMetric
from .measurable import Measurable as Measurable
from .metric import Metric as Metric
from .metric_config import MetricConfig as MetricConfig
from .metric_name import MetricName as MetricName
from .node import Node as Node
from .partition_info import PartitionInfo as PartitionInfo
from .timestamp_type import TimestampType as TimestampType
from .topic_id_partition import TopicIdPartition as TopicIdPartition
from .topic_partition import TopicPartition as TopicPartition
from .uuid import Uuid as Uuid

__all__ = [
    "Cluster",
    "Headers",
    "KafkaMetric",
    "Measurable",
    "Metric",
    "MetricConfig",
    "MetricName",
    "Node",
    "PartitionInfo",
    "TimestampType",
    "TopicIdPartition",
    "TopicPartition",
    "Uuid",
]

# BEGIN GENERATED ERRORS (cargo xtask generate-error-codes; do not edit)
from .invalid_record_error import InvalidRecordError as InvalidRecordError
from .kafka_error import KafkaError as KafkaError
__all__ += [
    "InvalidRecordError",
    "KafkaError",
]
# END GENERATED ERRORS
