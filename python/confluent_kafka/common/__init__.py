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

The value types of ``org.apache.kafka.common`` (``TopicPartition``, ``Uuid``,
``Node``, ``KafkaFuture``, ...) and its errors (``KafkaError``,
``InvalidRecordError``). Every ``common.*`` subpackage is a module of its own,
holding the types of that Java package (CLAUDE.md, Python Binding Conventions,
Modules): ``common.acl``, ``common.config``, ``common.errors``, ``common.header``,
``common.metrics``, ``common.network``, ``common.protocol.types``, ``common.quota``,
``common.record``, ``common.requests``, ``common.resource``,
``common.security.auth``, ``common.security.token.delegation`` and
``common.serialization``.
"""

from __future__ import annotations

from .classic_group_state import ClassicGroupState as ClassicGroupState
from .cluster import Cluster as Cluster
from .election_type import ElectionType as ElectionType
from .group_state import GroupState as GroupState
from .group_type import GroupType as GroupType
from .isolation_level import IsolationLevel as IsolationLevel
from .kafka_future import AsyncKafkaFuture as AsyncKafkaFuture
from .kafka_future import KafkaFuture as KafkaFuture
from .metric import Metric as Metric
from .metric_name import MetricName as MetricName
from .node import Node as Node
from .partition_info import PartitionInfo as PartitionInfo
from .topic_collection import TopicCollection as TopicCollection
from .topic_id_partition import TopicIdPartition as TopicIdPartition
from .topic_partition import TopicPartition as TopicPartition
from .topic_partition_info import TopicPartitionInfo as TopicPartitionInfo
from .topic_partition_replica import TopicPartitionReplica as TopicPartitionReplica
from .uuid import Uuid as Uuid

__all__ = [
    "AsyncKafkaFuture",
    "ClassicGroupState",
    "Cluster",
    "ElectionType",
    "GroupState",
    "GroupType",
    "IsolationLevel",
    "KafkaFuture",
    "Metric",
    "MetricName",
    "Node",
    "PartitionInfo",
    "TopicCollection",
    "TopicIdPartition",
    "TopicPartition",
    "TopicPartitionInfo",
    "TopicPartitionReplica",
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
