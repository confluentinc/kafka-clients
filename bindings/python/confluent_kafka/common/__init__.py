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

"""``confluent_kafka.common`` — mirror of ``org.apache.kafka.common``.

Common value types (``TopicPartition``, ``TopicIdPartition``, ``Node``,
``PartitionInfo``, ``Uuid``, ``MetricName``, ``Metric``, ``KafkaMetric``,
``TimestampType``, ``Headers``) live here (P2). The sub-packages
``common.errors``, ``common.config`` and ``common.serialization`` hold the
error hierarchy and the serialization surface.
"""

from __future__ import annotations

from .headers import Headers
from .metric import KafkaMetric, Metric
from .metric_name import MetricName
from .node import Node
from .partition_info import PartitionInfo
from .timestamp_type import TimestampType
from .topic_id_partition import TopicIdPartition
from .topic_partition import TopicPartition
from .uuid import Uuid

__all__ = [
    "Headers",
    "KafkaMetric",
    "Metric",
    "MetricName",
    "Node",
    "PartitionInfo",
    "TimestampType",
    "TopicIdPartition",
    "TopicPartition",
    "Uuid",
]
