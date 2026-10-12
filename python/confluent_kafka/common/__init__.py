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
``KafkaMetric``, ``MetricConfig`` and ``Measurable`` (``common.metrics``),
the ACL types of ``common.acl`` (``AclBinding``, ``AclOperation``, ...), the
resource types of ``common.resource`` (``ResourcePattern``, ``ResourceType``,
...), the quota types of ``common.quota`` (``ClientQuotaEntity``, ...),
``KafkaPrincipal`` (``common.security.auth``) and ``DelegationToken`` /
``TokenInformation`` (``common.security.token.delegation``). The
errors of ``org.apache.kafka.common`` (``KafkaError``, ``InvalidRecordError``)
live here too; the other error packages are the modules ``common.errors``,
``common.requests``, ``common.network``, ``common.metrics`` and
``common.protocol.types``.
"""

from __future__ import annotations

from .access_control_entry import AccessControlEntry as AccessControlEntry
from .access_control_entry_filter import AccessControlEntryFilter as AccessControlEntryFilter
from .acl_binding import AclBinding as AclBinding
from .acl_binding_filter import AclBindingFilter as AclBindingFilter
from .acl_operation import AclOperation as AclOperation
from .acl_permission_type import AclPermissionType as AclPermissionType
from .classic_group_state import ClassicGroupState as ClassicGroupState
from .client_quota_alteration import ClientQuotaAlteration as ClientQuotaAlteration
from .client_quota_entity import ClientQuotaEntity as ClientQuotaEntity
from .client_quota_filter import ClientQuotaFilter as ClientQuotaFilter
from .client_quota_filter_component import ClientQuotaFilterComponent as ClientQuotaFilterComponent
from .cluster import Cluster as Cluster
from .delegation_token import DelegationToken as DelegationToken
from .election_type import ElectionType as ElectionType
from .group_state import GroupState as GroupState
from .group_type import GroupType as GroupType
from .headers import Headers as Headers
from .isolation_level import IsolationLevel as IsolationLevel
from .kafka_metric import KafkaMetric as KafkaMetric
from .kafka_principal import KafkaPrincipal as KafkaPrincipal
from .measurable import Measurable as Measurable
from .metric import Metric as Metric
from .metric_config import MetricConfig as MetricConfig
from .metric_name import MetricName as MetricName
from .node import Node as Node
from .partition_info import PartitionInfo as PartitionInfo
from .pattern_type import PatternType as PatternType
from .resource_pattern import ResourcePattern as ResourcePattern
from .resource_pattern_filter import ResourcePatternFilter as ResourcePatternFilter
from .resource_type import ResourceType as ResourceType
from .timestamp_type import TimestampType as TimestampType
from .token_information import TokenInformation as TokenInformation
from .topic_collection import TopicCollection as TopicCollection
from .topic_id_partition import TopicIdPartition as TopicIdPartition
from .topic_partition import TopicPartition as TopicPartition
from .topic_partition_info import TopicPartitionInfo as TopicPartitionInfo
from .topic_partition_replica import TopicPartitionReplica as TopicPartitionReplica
from .uuid import Uuid as Uuid

__all__ = [
    "AccessControlEntry",
    "AccessControlEntryFilter",
    "AclBinding",
    "AclBindingFilter",
    "AclOperation",
    "AclPermissionType",
    "ClassicGroupState",
    "ClientQuotaAlteration",
    "ClientQuotaEntity",
    "ClientQuotaFilter",
    "ClientQuotaFilterComponent",
    "Cluster",
    "DelegationToken",
    "ElectionType",
    "GroupState",
    "GroupType",
    "Headers",
    "IsolationLevel",
    "KafkaMetric",
    "KafkaPrincipal",
    "Measurable",
    "Metric",
    "MetricConfig",
    "MetricName",
    "Node",
    "PartitionInfo",
    "PatternType",
    "ResourcePattern",
    "ResourcePatternFilter",
    "ResourceType",
    "TimestampType",
    "TokenInformation",
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
