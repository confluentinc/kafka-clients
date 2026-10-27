# Copyright 2026 Confluent Inc.
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

"""The state and logic of Java's ``MockAdminClient``, shared by ``MockAdminClient``
and its asyncio peer (private).

``MockAdminClient`` is a direct Python translation of its Java source
(``clients/src/test/java/org/apache/kafka/clients/admin/MockAdminClient.java``), not
an FFI client (CLAUDE.md, Python Binding Conventions, Implementation over the FFI).
This mixin holds the Java mock's fields, its own public methods (plain
``def`` on both classes, as none waits) and the bodies of the ``Admin`` methods,
which each class wraps with its own ``def`` / ``async def``; ``_kind`` (the client
kind marker of ``_futures``) decides which futures the results hold. Java's
``synchronized`` methods take one re-entrant lock per instance (``_lock``).
The nested ``Builder``'s setters live in :class:`MockAdminClientBuilderCore`, which
each class's ``Builder`` extends with its own ``build()``.

The Java internals it uses are private here: ``KafkaFutureImpl`` is a future of the
client's kind over ``_KafkaFutureImpl`` (completed through it, ``_complete``),
``DescribeLogDirsResponse.INVALID_OFFSET_LAG`` / ``UNKNOWN_VOLUME_BYTES`` are module
constants, the private nested ``TopicMetadata`` is :class:`_TopicMetadata`,
``CoordinatorKey`` and ``ConsumerProtocol.PROTOCOL_TYPE`` are ``_internals``'
``_CoordinatorKey`` and ``_ConsumerProtocol``, ``System.currentTimeMillis()`` is
:func:`_current_time_millis`. JDK
behaviour the mock's bodies rely on is kept: ``List.get`` out of range raises
the root ``IndexOutOfBoundsError`` with Java's ``IndexOutOfBoundsException`` message,
``List.subList`` checks its range as Java does, ``Integer.parseInt`` raises ``IllegalArgumentError``
(``NumberFormatException``), and a ``null`` the mock itself looks up and dereferences
(Java's ``NullPointerException``) raises ``IllegalStateError`` naming the state the
mock lacks, or ``TypeError`` for an option left ``None``. Java's ``UnsupportedOperationException``
is the root ``UnsupportedOperationError`` (Types), with Java's messages, typo included
.

Left out with what they serve (Class family, Dropped): ``setMockMetrics`` (state of
``metrics()``), ``disableTelemetry``, ``injectTimeoutException``,
``advanceTimeOnClientInstanceId``, ``setClientInstanceId`` (state of
``clientInstanceId(Duration)``) and ``addedMetrics`` (state of
``registerMetricForSubscription`` / ``unregisterMetricFromSubscription``), whose base
methods are not generated (the "Not generated" list of ``Admin``'s module docstring),
with their fields.
"""

from __future__ import annotations

import sys
import threading
import time
from collections.abc import Iterable, Mapping, Sequence
from typing import TYPE_CHECKING, Any, ClassVar, TypeVar, overload

from confluent_kafka._args import MISSING, reject_str
from confluent_kafka.admin._conversions import element, non_null
from confluent_kafka.illegal_state_error import IllegalStateError
from confluent_kafka.index_out_of_bounds_error import IndexOutOfBoundsError
from confluent_kafka._java import java_str, parse_int
from confluent_kafka.admin._futures import Async, Sync
from confluent_kafka.common.kafka_future import _KafkaFutureImpl
from confluent_kafka.illegal_argument_error import IllegalArgumentError

if TYPE_CHECKING:
    if sys.version_info >= (3, 11):
        from typing import Self
    else:
        from typing_extensions import Self

    from confluent_kafka.admin.abort_transaction_result import AbortTransactionResult
    from confluent_kafka.admin.abort_transaction_spec import AbortTransactionSpec
    from confluent_kafka.admin.alter_client_quotas_result import AlterClientQuotasResult
    from confluent_kafka.admin.alter_config_op import AlterConfigOp
    from confluent_kafka.admin.alter_configs_result import AlterConfigsResult
    from confluent_kafka.admin.alter_consumer_group_offsets_result import (
        AlterConsumerGroupOffsetsResult,
    )
    from confluent_kafka.admin.alter_user_scram_credentials_result import (
        AlterUserScramCredentialsResult,
    )
    from confluent_kafka.admin.create_acls_result import CreateAclsResult
    from confluent_kafka.admin.create_delegation_token_options import (
        CreateDelegationTokenOptions,
    )
    from confluent_kafka.admin.create_delegation_token_result import CreateDelegationTokenResult
    from confluent_kafka.admin.delete_acls_result import DeleteAclsResult
    from confluent_kafka.admin.delete_consumer_group_offsets_result import (
        DeleteConsumerGroupOffsetsResult,
    )
    from confluent_kafka.admin.delete_consumer_groups_result import DeleteConsumerGroupsResult
    from confluent_kafka.admin.describe_acls_result import DescribeAclsResult
    from confluent_kafka.admin.describe_classic_groups_result import DescribeClassicGroupsResult
    from confluent_kafka.admin.describe_client_quotas_result import DescribeClientQuotasResult
    from confluent_kafka.admin.describe_consumer_groups_result import DescribeConsumerGroupsResult
    from confluent_kafka.admin.describe_delegation_token_options import (
        DescribeDelegationTokenOptions,
    )
    from confluent_kafka.admin.describe_delegation_token_result import (
        DescribeDelegationTokenResult,
    )
    from confluent_kafka.admin.describe_features_result import DescribeFeaturesResult
    from confluent_kafka.admin.describe_producers_result import DescribeProducersResult
    from confluent_kafka.admin.describe_transactions_result import DescribeTransactionsResult
    from confluent_kafka.admin.describe_user_scram_credentials_result import (
        DescribeUserScramCredentialsResult,
    )
    from confluent_kafka.admin.expire_delegation_token_options import (
        ExpireDelegationTokenOptions,
    )
    from confluent_kafka.admin.expire_delegation_token_result import ExpireDelegationTokenResult
    from confluent_kafka.admin.feature_update import FeatureUpdate
    from confluent_kafka.admin.fence_producers_result import FenceProducersResult
    from confluent_kafka.admin.list_consumer_group_offsets_result import (
        ListConsumerGroupOffsetsResult,
    )
    from confluent_kafka.admin.list_consumer_group_offsets_spec import (
        ListConsumerGroupOffsetsSpec,
    )
    from confluent_kafka.admin.list_groups_result import ListGroupsResult
    from confluent_kafka.admin.list_transactions_result import ListTransactionsResult
    from confluent_kafka.admin.remove_members_from_consumer_group_options import (
        RemoveMembersFromConsumerGroupOptions,
    )
    from confluent_kafka.admin.remove_members_from_consumer_group_result import (
        RemoveMembersFromConsumerGroupResult,
    )
    from confluent_kafka.admin.renew_delegation_token_options import RenewDelegationTokenOptions
    from confluent_kafka.admin.renew_delegation_token_result import RenewDelegationTokenResult
    from confluent_kafka.admin.terminate_transaction_result import TerminateTransactionResult
    from confluent_kafka.admin.update_features_options import UpdateFeaturesOptions
    from confluent_kafka.admin.update_features_result import UpdateFeaturesResult
    from confluent_kafka.admin.user_scram_credential_alteration import (
        UserScramCredentialAlteration,
    )
    from confluent_kafka.admin.alter_partition_reassignments_result import (
        AlterPartitionReassignmentsResult,
    )
    from confluent_kafka.admin.alter_replica_log_dirs_result import AlterReplicaLogDirsResult
    from confluent_kafka.admin.config import Config
    from confluent_kafka.admin.create_partitions_result import CreatePartitionsResult
    from confluent_kafka.admin.create_topics_result import CreateTopicsResult
    from confluent_kafka.admin.delete_records_result import DeleteRecordsResult
    from confluent_kafka.admin.delete_topics_result import DeleteTopicsResult
    from confluent_kafka.admin.describe_cluster_result import DescribeClusterResult
    from confluent_kafka.admin.describe_configs_result import DescribeConfigsResult
    from confluent_kafka.admin.describe_log_dirs_result import DescribeLogDirsResult
    from confluent_kafka.admin.describe_replica_log_dirs_result import DescribeReplicaLogDirsResult
    from confluent_kafka.admin.describe_topics_result import DescribeTopicsResult
    from confluent_kafka.admin.elect_leaders_result import ElectLeadersResult
    from confluent_kafka.admin.list_config_resources_result import ListConfigResourcesResult
    from confluent_kafka.admin.list_offsets_result import ListOffsetsResult
    from confluent_kafka.admin.list_partition_reassignments_result import (
        ListPartitionReassignmentsResult,
    )
    from confluent_kafka.admin.list_topics_result import ListTopicsResult
    from confluent_kafka.admin.new_partition_reassignment import NewPartitionReassignment
    from confluent_kafka.admin.new_partitions import NewPartitions
    from confluent_kafka.admin.new_topic import NewTopic
    from confluent_kafka.admin.offset_spec import OffsetSpec
    from confluent_kafka.admin.partition_reassignment import PartitionReassignment
    from confluent_kafka.admin.records_to_delete import RecordsToDelete
    from confluent_kafka.admin.topic_description import TopicDescription
    from confluent_kafka.admin.unregister_controller_result import UnregisterControllerResult
    from confluent_kafka.common.acl import AclBinding, AclBindingFilter
    from confluent_kafka.common.quota import ClientQuotaAlteration, ClientQuotaFilter
    from confluent_kafka.common.security.token.delegation import DelegationToken
    from confluent_kafka.common import (
        ElectionType,
        Node,
        TopicCollection,
        TopicPartition,
        TopicPartitionInfo,
        TopicPartitionReplica,
        Uuid,
    )
    from confluent_kafka.common.security.auth import KafkaPrincipal
    from confluent_kafka.common.config import ConfigResource
    from confluent_kafka.common.kafka_future import AsyncKafkaFuture, KafkaFuture
    from confluent_kafka.consumer import OffsetAndMetadata

__all__ = ["DEFAULT_CLUSTER_ID", "DEFAULT_LOG_DIRS", "MockAdminClientBuilderCore",
           "MockAdminClientCore"]

_T = TypeVar("_T")


def _complete(future: KafkaFuture[_T] | AsyncKafkaFuture[_T], value: _T) -> bool:
    """Java's ``KafkaFutureImpl.complete(value)`` on a future :meth:`_future` made:
    ``False`` when it was already done."""
    impl = future._impl
    assert impl is not None
    return impl.complete(value)


def _complete_with_error(future: KafkaFuture[Any] | AsyncKafkaFuture[Any],
                         error: BaseException) -> bool:
    """Java's ``KafkaFutureImpl.completeExceptionally(error)`` on a future
    :meth:`_future` made: ``False`` when it was already done."""
    impl = future._impl
    assert impl is not None
    return impl.complete_with_error(error)

#: ``MockAdminClient.DEFAULT_CLUSTER_ID``.
DEFAULT_CLUSTER_ID = "I4ZmrWqfT2e-upky_4fdPA"

#: ``MockAdminClient.DEFAULT_LOG_DIRS``: Java's ``Collections.singletonList``, an
#: immutable list, so a tuple.
DEFAULT_LOG_DIRS: tuple[str, ...] = ("/tmp/kafka-logs",)

#: ``DescribeLogDirsResponse.INVALID_OFFSET_LAG``.
_INVALID_OFFSET_LAG = -1
#: ``DescribeLogDirsResponse.UNKNOWN_VOLUME_BYTES``.
_UNKNOWN_VOLUME_BYTES = -1

#: Java's message for the methods its mock leaves unimplemented.
_NOT_IMPLEMENTED = "Not implemented yet"
#: The same, with the typo of some of the Java mock's methods.
_NOT_IMPLEMENT = "Not implement yet"


class _CopiesList(list[_T]):
    """A list built by ``Collections.nCopies`` or ``Collections.singletonList``, whose
    ``get`` reports an index out of range as ``"Index: i, Size: n"`` (not
    ``ArrayList``'s text); see :func:`_get`."""

    __slots__ = ()


def _get(items: Sequence[_T], index: int) -> _T:
    """Java's ``List.get(index)``: ``IndexOutOfBoundsError`` with the ``IndexOutOfBoundsException``
    message of the Java list behind ``items`` for an index out of range, a negative one
    included: ``Collections.nCopies`` / ``singletonList``'s (``"Index: -1, Size: 1"``)
    for a :class:`_CopiesList`, else ``ArrayList``'s (``"Index -1 out of bounds for
    length 1"``)."""
    if not 0 <= index < len(items):
        if isinstance(items, _CopiesList):
            raise IndexOutOfBoundsError(message=f"Index: {index}, Size: {len(items)}")
        raise IndexOutOfBoundsError(message=f"Index {index} out of bounds for length {len(items)}")
    return items[index]


def _illegal_capacity(capacity: int) -> None:
    """Java's ``new ArrayList<>(capacity)``: ``IllegalArgumentException("Illegal
    Capacity: -2")`` for a negative capacity."""
    if capacity < 0:
        raise IllegalArgumentError(message=f"Illegal Capacity: {capacity}")


def _sub_list(items: list[_T], from_index: int, to_index: int) -> list[_T]:
    """Java's ``List.subList(fromIndex, toIndex)`` with its range check
    (``AbstractList.subListRangeCheck``); a copy, as nothing else holds the list."""
    if from_index < 0:
        raise IndexOutOfBoundsError(message=f"fromIndex = {from_index}")
    if to_index > len(items):
        raise IndexOutOfBoundsError(message=f"toIndex = {to_index}")
    if from_index > to_index:
        raise IllegalArgumentError(message=f"fromIndex({from_index}) > toIndex({to_index})")
    return items[from_index:to_index]


def _current_time_millis() -> int:
    """Java's ``System.currentTimeMillis()``."""
    return time.time_ns() // 1_000_000


def _unsupported(message: str | None = None) -> Exception:
    """Java's ``UnsupportedOperationException``: ``UnsupportedOperationError``
    (Types); without a message, Java's no-argument constructor."""
    from confluent_kafka.unsupported_operation_error import UnsupportedOperationError

    if message is None:
        return UnsupportedOperationError()
    return UnsupportedOperationError(message=message)


def _unsupported_version(message: str) -> Exception:
    """Java's ``UnsupportedVersionException``, which ``unregisterController``
    fails its future with when the mock does not use a raft controller."""
    from confluent_kafka.common.errors.unsupported_version_error import (
        UnsupportedVersionError,
    )

    return UnsupportedVersionError(message=message)


def _timeout() -> Exception:
    """Java's ``new TimeoutException()`` (Kafka's, ``common.errors``)."""
    from confluent_kafka.common.errors.timeout_error import TimeoutError as KafkaTimeoutError

    return KafkaTimeoutError()


def _config(entries: Mapping[str, str | None] | None, resource: object = None) -> Config:
    """The private ``toConfigObject(Map)``: a ``Config`` of one ``ConfigEntry(name,
    value)`` per entry; it dereferences its map, so Java throws
    ``NullPointerException`` for ``null``: here ``None`` raises ``IllegalStateError``
    naming the ``resource`` the mock holds no configs for (the private
    ``config(NewTopic)`` checks for ``null`` first and passes an empty map)."""
    from confluent_kafka.admin.config import Config
    from confluent_kafka.admin.config_entry import ConfigEntry

    if entries is None:
        raise IllegalStateError(
            message="MockAdminClient has no configs for "
                    + ("the resource" if resource is None else java_str(resource)))
    return Config(entries=[ConfigEntry(name=k, value=v) for k, v in entries.items()])


class _TopicMetadata:
    """Java's private ``MockAdminClient.TopicMetadata``."""

    __slots__ = ("topic_id", "is_internal_topic", "partitions", "partition_log_dirs",
                 "configs", "fetches_remaining_until_visible", "marked_for_deletion")

    def __init__(self, topic_id: Uuid, is_internal_topic: bool,
                 partitions: list[TopicPartitionInfo], partition_log_dirs: list[str],
                 configs: Mapping[str, str | None] | None) -> None:
        self.topic_id = topic_id
        self.is_internal_topic = is_internal_topic
        self.partitions = partitions
        self.partition_log_dirs = partition_log_dirs
        # Java's `Map<String, String>` holds a config set to null as given.
        self.configs: Mapping[str, str | None] = configs if configs is not None else {}
        self.marked_for_deletion = False
        self.fetches_remaining_until_visible = 0




class MockAdminClientBuilderCore:
    """The fields and setters of Java's ``MockAdminClient.Builder``; each client's
    ``Builder`` adds ``build()``.

    The keyword constructor runs Java's ``Builder()`` (which calls
    ``numBrokers(1)``), then calls the setter of each keyword given, ``None`` included,
    in dependency order: ``brokers``, ``controller``, ``broker_log_dirs`` (``brokers``
    rewrites ``brokerLogDirs`` through ``numBrokers``, and ``controller(int)`` reads
    ``brokers``), then the others in the keywords' order. Each keyword is named after
    Java's field: ``brokers`` calls ``brokers(...)``, the setter named like the field
    (``num_brokers(...)``, which also writes it, stays a method only), and
    ``controller`` takes the index ``controller(int index)`` takes.

    The ``TYPE_CHECKING`` signature is the stub: it shows Java's starting values, or
    ``...`` where the field has none of the keyword's type (``brokers`` and
    ``broker_log_dirs``, set by ``numBrokers(1)``; ``controller``,
    ``default_partitions`` and ``default_replication_factor``, ``null``; the maps,
    ``Collections.emptyMap()``); the run-time defaults are ``MISSING`` (Signatures).
    """

    def _init_builder(self, *, cluster_id: str = MISSING, brokers: Sequence[Node] = MISSING,
                 controller: int = MISSING, broker_log_dirs: Sequence[Sequence[str]] = MISSING,
                 default_partitions: int = MISSING, using_raft_controller: bool = MISSING,
                 default_replication_factor: int = MISSING,
                 feature_levels: Mapping[str, int] = MISSING,
                 min_supported_feature_levels: Mapping[str, int] = MISSING,
                 max_supported_feature_levels: Mapping[str, int] = MISSING,
                 default_group_configs: Mapping[str, str] = MISSING) -> None:
        """The keyword constructor's body (the run-time ``__init__``)."""
        self._cluster_id: str = DEFAULT_CLUSTER_ID
        self._brokers: list[Node] = []
        self._controller: Node | None = None
        self._broker_log_dirs: list[Sequence[str]] = []
        self._default_partitions: int | None = None
        self._using_raft_controller = False
        self._default_replication_factor: int | None = None
        self._feature_levels: Mapping[str, int] = {}
        self._min_supported_feature_levels: Mapping[str, int] = {}
        self._max_supported_feature_levels: Mapping[str, int] = {}
        self._default_group_configs: Mapping[str, str] = {}
        self.num_brokers(num_brokers=1)
        # Dependency order, then the keywords' order.
        if brokers is not MISSING:
            self.brokers(brokers=brokers)
        if controller is not MISSING:
            self.controller(index=controller)
        if broker_log_dirs is not MISSING:
            self.broker_log_dirs(broker_log_dirs=broker_log_dirs)
        if cluster_id is not MISSING:
            self.cluster_id(cluster_id=cluster_id)
        if default_partitions is not MISSING:
            self.default_partitions(num_partitions=default_partitions)
        if using_raft_controller is not MISSING:
            self.using_raft_controller(using_raft_controller=using_raft_controller)
        if default_replication_factor is not MISSING:
            self.default_replication_factor(default_replication_factor=default_replication_factor)
        if feature_levels is not MISSING:
            self.feature_levels(feature_levels=feature_levels)
        if min_supported_feature_levels is not MISSING:
            self.min_supported_feature_levels(
                min_supported_feature_levels=min_supported_feature_levels)
        if max_supported_feature_levels is not MISSING:
            self.max_supported_feature_levels(
                max_supported_feature_levels=max_supported_feature_levels)
        if default_group_configs is not MISSING:
            self.default_group_configs(default_group_configs=default_group_configs)

    if TYPE_CHECKING:
        def __init__(self, *, cluster_id: str = "I4ZmrWqfT2e-upky_4fdPA",
                     brokers: Sequence[Node] = ..., controller: int = ...,
                     broker_log_dirs: Sequence[Sequence[str]] = ...,
                     default_partitions: int = ..., using_raft_controller: bool = False,
                     default_replication_factor: int = ...,
                     feature_levels: Mapping[str, int] = ...,
                     min_supported_feature_levels: Mapping[str, int] = ...,
                     max_supported_feature_levels: Mapping[str, int] = ...,
                     default_group_configs: Mapping[str, str] = ...) -> None: ...
    else:
        __init__ = _init_builder

    def cluster_id(self, *, cluster_id: str) -> Self:
        self._cluster_id = cluster_id
        return self

    def brokers(self, *, brokers: Sequence[Node]) -> Self:
        """Java's ``numBrokers(brokers.size())`` on the current list (so
        ``brokerLogDirs`` keeps that length), then the given brokers replace it. The
        list is copied *(deviation: Java keeps the caller's list and may later grow it
        in place)*. A ``None`` raises ``TypeError`` (Java's ``brokers.size()``)."""
        non_null(brokers, "brokers")
        self.num_brokers(num_brokers=len(brokers))
        self._brokers = list(brokers)
        return self

    def num_brokers(self, *, num_brokers: int) -> Self:
        from confluent_kafka.common import Node

        if len(self._brokers) >= num_brokers:
            self._brokers = _sub_list(self._brokers, 0, num_brokers)
            self._broker_log_dirs = _sub_list(self._broker_log_dirs, 0, num_brokers)
        else:
            for id_ in range(len(self._brokers), num_brokers):
                self._brokers.append(Node(id=id_, host="localhost", port=1000 + id_))
                self._broker_log_dirs.append(DEFAULT_LOG_DIRS)
        return self

    def controller(self, *, index: int) -> Self:
        """Java's ``brokers.get(index)``: ``IndexOutOfBoundsError`` for an index out of range."""
        self._controller = _get(self._brokers, index)
        return self

    def broker_log_dirs(self, *, broker_log_dirs: Sequence[Sequence[str]]) -> Self:
        reject_str("broker_log_dirs", broker_log_dirs, element="lists of str")
        dirs = list(broker_log_dirs)  # read once: a one-shot iterable keeps its items
        for broker_dirs in dirs:
            reject_str("broker_log_dirs", broker_dirs)
        self._broker_log_dirs = dirs
        return self

    def default_replication_factor(self, *, default_replication_factor: int) -> Self:
        self._default_replication_factor = default_replication_factor
        return self

    def using_raft_controller(self, *, using_raft_controller: bool) -> Self:
        self._using_raft_controller = using_raft_controller
        return self

    def default_partitions(self, *, num_partitions: int) -> Self:
        self._default_partitions = num_partitions
        return self

    def feature_levels(self, *, feature_levels: Mapping[str, int]) -> Self:
        self._feature_levels = feature_levels
        return self

    def min_supported_feature_levels(self, *,
                                     min_supported_feature_levels: Mapping[str, int]) -> Self:
        self._min_supported_feature_levels = min_supported_feature_levels
        return self

    def max_supported_feature_levels(self, *,
                                     max_supported_feature_levels: Mapping[str, int]) -> Self:
        self._max_supported_feature_levels = max_supported_feature_levels
        return self

    def default_group_configs(self, *, default_group_configs: Mapping[str, str]) -> Self:
        self._default_group_configs = default_group_configs
        return self

    def _build_args(self) -> dict[str, Any]:
        """The arguments ``build()`` passes to the private constructor."""
        return {
            "brokers": self._brokers,
            "controller": (_get(self._brokers, 0) if self._controller is None
                           else self._controller),
            "cluster_id": self._cluster_id,
            "default_partitions": (self._default_partitions
                                   if self._default_partitions is not None else 1),
            "default_replication_factor": (
                _short_value(self._default_replication_factor)
                if self._default_replication_factor is not None
                else min(len(self._brokers), 3)),
            "broker_log_dirs": self._broker_log_dirs,
            "using_raft_controller": self._using_raft_controller,
            "feature_levels": self._feature_levels,
            "min_supported_feature_levels": self._min_supported_feature_levels,
            "max_supported_feature_levels": self._max_supported_feature_levels,
            "default_group_configs": self._default_group_configs,
        }


def _short_value(value: int) -> int:
    """Java's ``Integer.shortValue()``: the low 16 bits, signed."""
    value &= 0xFFFF
    return value - 0x10000 if value >= 0x8000 else value


class MockAdminClientCore:
    """Every field and method of Java's ``MockAdminClient`` (see the module doc)."""

    DEFAULT_CLUSTER_ID: ClassVar[str] = DEFAULT_CLUSTER_ID
    DEFAULT_LOG_DIRS: ClassVar[tuple[str, ...]] = DEFAULT_LOG_DIRS

    #: The client kind of the futures this mock's results hold; each class sets it.
    _kind: ClassVar[type[Sync] | type[Async]]

    def _init_mock(self, *, brokers: Sequence[Node], controller: Node, cluster_id: str,
                   default_partitions: int, default_replication_factor: int,
                   broker_log_dirs: Sequence[Sequence[str]], using_raft_controller: bool,
                   feature_levels: Mapping[str, int],
                   min_supported_feature_levels: Mapping[str, int],
                   max_supported_feature_levels: Mapping[str, int],
                   default_group_configs: Mapping[str, str]) -> None:
        """The body of Java's private eleven-argument constructor, after its field
        initializers."""
        self._lock = threading.RLock()
        self._all_topics: dict[str, _TopicMetadata] = {}
        self._topic_ids: dict[str, Uuid] = {}
        self._topic_names: dict[Uuid, str] = {}
        self._reassignments: dict[TopicPartition, NewPartitionReassignment] = {}
        self._replica_moves: dict[TopicPartitionReplica,
                                  DescribeReplicaLogDirsResult.ReplicaLogDirInfo] = {}
        self._timeout_next_requests = 0
        self._all_tokens: list[DelegationToken] = []

        # Java keeps the given lists; a _CopiesList keeps its Java list's messages.
        self._brokers: list[Node] = (brokers if isinstance(brokers, _CopiesList)
                                     else list(brokers))
        self.controller(controller=controller)
        self._cluster_id = cluster_id
        self._default_partitions = default_partitions
        self._default_replication_factor = default_replication_factor
        self._broker_log_dirs: list[Sequence[str]] = (
            broker_log_dirs if isinstance(broker_log_dirs, _CopiesList)
            else list(broker_log_dirs))
        self._broker_configs: list[dict[str, str | None]] = []
        self._client_metrics_configs: dict[str, dict[str, str | None]] = {}
        self._group_configs: dict[str, dict[str, str | None]] = {}
        non_null(default_group_configs, "default_group_configs")  # new HashMap<>(null)
        self._default_group_configs = dict(default_group_configs)
        for _ in range(len(self._brokers)):
            self._broker_configs.append(
                {"default.replication.factor": java_str(default_replication_factor)})
        self._beginning_offsets: dict[TopicPartition, int] = {}
        self._end_offsets: dict[TopicPartition, int] = {}
        self._committed_offsets: dict[TopicPartition, int] = {}
        self._using_raft_controller = using_raft_controller
        self._feature_levels = dict(feature_levels)
        self._min_supported_feature_levels = dict(min_supported_feature_levels)
        self._max_supported_feature_levels = dict(max_supported_feature_levels)

    def _init_two(self, brokers: Sequence[Node] | None, controller: Node | None) -> None:
        """Java's public constructors: ``()`` passes ``(singletonList(Node.noNode()),
        Node.noNode())`` to ``(brokers, controller)``, which passes the defaults; a
        ``None`` ``brokers`` raises ``TypeError`` (Java's ``brokers.size()``), and a
        ``None`` ``controller`` is refused by ``controller()``, as in Java."""
        from confluent_kafka.common import Node

        if brokers is MISSING:  # java_forms: both or neither
            brokers = _CopiesList([Node.no_node()])  # Collections.singletonList
            controller = Node.no_node()
        non_null(brokers, "brokers")  # brokers.size()
        assert brokers is not None
        self._init_mock(
            brokers=brokers,
            controller=controller,  # type: ignore[arg-type]  # None: controller() refuses it
            cluster_id=DEFAULT_CLUSTER_ID,
            default_partitions=1, default_replication_factor=len(brokers),
            broker_log_dirs=_CopiesList([DEFAULT_LOG_DIRS] * len(brokers)),  # nCopies
            using_raft_controller=False,
            feature_levels={}, min_supported_feature_levels={},
            max_supported_feature_levels={}, default_group_configs={})

    # ---- The Java mock's own public methods, in Java's order -------------------

    def controller(self, *, controller: Node) -> None:
        """Set the controller node; it must be one of the brokers."""
        with self._lock:
            if controller not in self._brokers:
                raise IllegalArgumentError(
                    message="The controller node must be in the list of brokers")
            self._controller = controller

    @overload
    def add_topic(self, *, internal: bool, name: str, partitions: Sequence[TopicPartitionInfo],
                  configs: Mapping[str, str]) -> None: ...
    @overload
    def add_topic(self, *, internal: bool, name: str, partitions: Sequence[TopicPartitionInfo],
                  configs: Mapping[str, str], uses_topic_id: bool) -> None: ...

    def add_topic(self, *, internal: bool, name: str, partitions: Sequence[TopicPartitionInfo],
                  configs: Mapping[str, str], uses_topic_id: bool = MISSING) -> None:
        """Add a topic with the given partitions and configs; ``uses_topic_id``
        (left out, Java's four-argument overload passes ``true``) gives it a random
        topic id, else ``Uuid.ZERO_UUID``."""
        from confluent_kafka.common import Uuid

        if uses_topic_id is MISSING:
            uses_topic_id = True
        with self._lock:
            if name in self._all_topics:
                raise IllegalArgumentError(message=f"Topic {name} was already added.")
            for partition in partitions:
                if partition.leader() not in self._brokers:
                    raise IllegalArgumentError(message="Leader broker unknown")
                if not all(n in self._brokers for n in partition.replicas()):
                    raise IllegalArgumentError(message="Unknown brokers in replica list")
                if not all(n in self._brokers for n in partition.isr()):
                    raise IllegalArgumentError(message="Unknown brokers in isr list")
            log_dirs: list[str] = []
            for partition in partitions:
                leader = partition.leader()
                if leader is not None:
                    log_dirs.append(_get(_get(self._broker_log_dirs, leader.id()), 0))
            if uses_topic_id:
                topic_id = Uuid.random_uuid()
                self._topic_ids[name] = topic_id
                self._topic_names[topic_id] = name
            else:
                topic_id = Uuid.ZERO_UUID
            self._all_topics[name] = _TopicMetadata(topic_id, internal, list(partitions),
                                                    log_dirs, configs)

    def mark_topic_for_deletion(self, *, name: str) -> None:
        """Mark a topic as being deleted: ``describe_topics`` and ``describe_configs``
        no longer find it."""
        with self._lock:
            if name not in self._all_topics:
                raise IllegalArgumentError(message=f"Topic {name} did not exist.")
            self._all_topics[name].marked_for_deletion = True

    def timeout_next_request(self, *, number_of_request: int) -> None:
        """Fail every future of the next ``number_of_request`` calls of the methods
        that honour it with ``TimeoutError``."""
        with self._lock:
            self._timeout_next_requests = number_of_request

    def _handle_describe_topics_using_ids(
            self, topic_ids: Iterable[Uuid]) -> dict[Uuid, KafkaFuture[TopicDescription] | AsyncKafkaFuture[TopicDescription]]:
        """The body of Java's public ``handleDescribeTopicsUsingIds``, which each class
        wraps with its own future type."""
        from confluent_kafka.admin.topic_description import TopicDescription
        from confluent_kafka.common.errors.unknown_topic_id_error import UnknownTopicIdError

        with self._lock:
            topic_descriptions: dict[Uuid, KafkaFuture[TopicDescription] | AsyncKafkaFuture[TopicDescription]] = {}
            if self._timeout_next_requests > 0:
                for requested_topic_id in topic_ids:
                    topic_descriptions[requested_topic_id] = self._failed(_timeout())
                self._timeout_next_requests -= 1
                return topic_descriptions
            for requested_topic_id in topic_ids:
                for topic_name, metadata in self._all_topics.items():
                    topic_id = self._topic_ids.get(topic_name)
                    if (topic_id is not None and topic_id == requested_topic_id
                            and not metadata.marked_for_deletion):
                        if metadata.fetches_remaining_until_visible > 0:
                            metadata.fetches_remaining_until_visible -= 1
                        else:
                            topic_descriptions[requested_topic_id] = self._done(TopicDescription(
                                name=topic_name, internal=metadata.is_internal_topic,
                                partitions=metadata.partitions, authorized_operations=(),
                                topic_id=topic_id))
                            break
                if requested_topic_id not in topic_descriptions:
                    topic_descriptions[requested_topic_id] = self._failed(UnknownTopicIdError(
                        message="Topic id" + java_str(requested_topic_id) + " not found."))
            return topic_descriptions

    def update_beginning_offsets(self, *, new_offsets: Mapping[TopicPartition, int]) -> None:
        """Set the offsets ``list_offsets`` gives for ``OffsetSpec.earliest()``."""
        with self._lock:
            self._beginning_offsets.update(new_offsets)

    def update_end_offsets(self, *, new_offsets: Mapping[TopicPartition, int]) -> None:
        """Set the offsets ``list_offsets`` gives for every spec but the earliest."""
        with self._lock:
            self._end_offsets.update(new_offsets)

    def update_consumer_group_offsets(self, *,
                                      new_offsets: Mapping[TopicPartition, int]) -> None:
        """Set the committed offsets ``list_consumer_group_offsets`` gives."""
        with self._lock:
            self._committed_offsets.update(new_offsets)

    def set_fetches_remaining_until_visible(self, *, topic_name: str,
                                            fetches_remaining_until_visible: int) -> None:
        """Hide a topic from the next ``fetches_remaining_until_visible`` listings and
        descriptions."""
        with self._lock:
            metadata = self._all_topics.get(topic_name)
            if metadata is None:
                raise RuntimeError("No such topic as " + topic_name)
            metadata.fetches_remaining_until_visible = fetches_remaining_until_visible

    def brokers(self) -> list[Node]:
        """A copy of the brokers."""
        with self._lock:
            return list(self._brokers)

    def broker(self, *, index: int) -> Node:
        """The broker at ``index`` (``IndexOutOfBoundsError`` when out of range)."""
        with self._lock:
            return _get(self._brokers, index)

    # ---- Futures (Java's KafkaFutureImpl) ---------------------------------------

    def _future(self) -> KafkaFuture[Any] | AsyncKafkaFuture[Any]:
        """``new KafkaFutureImpl<>()`` of this client's kind: a future over a
        ``_KafkaFutureImpl``, completed through it (``_complete``)."""
        return self._kind._FUTURE._of_impl(_KafkaFutureImpl())  # type: ignore[no-any-return]

    def _done(self, value: _T) -> KafkaFuture[_T] | AsyncKafkaFuture[_T]:
        future: KafkaFuture[_T] | AsyncKafkaFuture[_T] = self._future()
        _complete(future, value)
        return future

    def _failed(self, error: BaseException) -> KafkaFuture[Any] | AsyncKafkaFuture[Any]:
        future = self._future()
        _complete_with_error(future, error)
        return future

    # ---- The Admin methods' bodies, in Admin's declaration order --------------

    def _create_topics(self, new_topics: Iterable[NewTopic]) -> CreateTopicsResult[Any]:
        from confluent_kafka.admin.create_topics_result import (
            CreateTopicsResult,
            _TopicMetadataAndConfig,
        )
        from confluent_kafka.common import TopicPartitionInfo, Uuid
        from confluent_kafka.common.errors.invalid_replication_factor_error import (
            InvalidReplicationFactorError,
        )
        from confluent_kafka.common.errors.topic_exists_error import TopicExistsError

        with self._lock:
            create_topic_result: dict[str, KafkaFuture[_TopicMetadataAndConfig] | AsyncKafkaFuture[_TopicMetadataAndConfig]] = {}
            non_null(new_topics, "new_topics")  # for (NewTopic newTopic : newTopics)
            if self._timeout_next_requests > 0:
                for new_topic in new_topics:
                    create_topic_result[element(new_topic, "new_topics").name()] = (
                        self._failed(_timeout()))
                self._timeout_next_requests -= 1
                return CreateTopicsResult._new(futures=create_topic_result, kind=self._kind)
            for new_topic in new_topics:
                topic_name = element(new_topic, "new_topics").name()
                if topic_name in self._all_topics:
                    create_topic_result[topic_name] = self._failed(
                        TopicExistsError(message=f"Topic {topic_name} exists already."))
                    continue
                replication_factor = new_topic.replication_factor()
                if replication_factor == -1:
                    replication_factor = self._default_replication_factor
                if replication_factor > len(self._brokers):
                    create_topic_result[topic_name] = self._failed(InvalidReplicationFactorError(
                        message=f"Replication factor: {new_topic.replication_factor()} is "
                                f"larger than brokers: {len(self._brokers)}"))
                    continue
                _illegal_capacity(replication_factor)  # new ArrayList<>(replicationFactor)
                replicas = [_get(self._brokers, i) for i in range(replication_factor)]
                number_of_partitions = new_topic.num_partitions()
                if number_of_partitions == -1:
                    number_of_partitions = self._default_partitions
                _illegal_capacity(number_of_partitions)  # new ArrayList<>(numberOfPartitions)
                partitions: list[TopicPartitionInfo] = []
                # Partitions start off on the first log directory of each broker, for now.
                log_dirs: list[str] = []
                for i in range(number_of_partitions):
                    partitions.append(TopicPartitionInfo(
                        partition=i, leader=_get(self._brokers, 0), replicas=replicas, isr=(),
                        elr=(), last_known_elr=()))
                    leader = partitions[i].leader()
                    assert leader is not None
                    log_dirs.append(_get(_get(self._broker_log_dirs, leader.id()), 0))
                topic_id = Uuid.random_uuid()
                self._topic_ids[topic_name] = topic_id
                self._topic_names[topic_id] = topic_name
                configs = new_topic.configs()
                self._all_topics[topic_name] = _TopicMetadata(topic_id, False, partitions,
                                                              log_dirs, configs)
                create_topic_result[topic_name] = self._done(_TopicMetadataAndConfig(
                    topic_id=topic_id, num_partitions=number_of_partitions,
                    replication_factor=replication_factor,
                    config=_config(configs if configs is not None else {})))
            return CreateTopicsResult._new(futures=create_topic_result, kind=self._kind)

    def _delete_topics(self, topics: Iterable[str] | TopicCollection) -> DeleteTopicsResult[Any]:
        from confluent_kafka.admin.delete_topics_result import DeleteTopicsResult
        from confluent_kafka.common import TopicCollection

        if not isinstance(topics, TopicCollection):
            # Admin.deleteTopics(Collection<String>, options):
            # deleteTopics(TopicCollection.ofTopicNames(topics), options).
            topics = TopicCollection.of_topic_names(topics=topics)
        with self._lock:
            if isinstance(topics, TopicCollection.TopicIdCollection):
                return DeleteTopicsResult._of_topic_ids(
                    topic_id_futures=dict(self._handle_delete_topics_using_ids(
                        topics.topic_ids())), kind=self._kind)
            if isinstance(topics, TopicCollection.TopicNameCollection):
                return DeleteTopicsResult._of_topic_names(
                    name_futures=dict(self._handle_delete_topics_using_names(
                        topics.topic_names())), kind=self._kind)
            raise IllegalArgumentError(
                message="The TopicCollection provided did not match any supported classes "
                        "for deleteTopics.")

    def _handle_delete_topics_using_names(
            self, topic_name_collection: Iterable[str]) -> dict[str, KafkaFuture[None] | AsyncKafkaFuture[None]]:
        from confluent_kafka.common.errors.unknown_topic_or_partition_error import (
            UnknownTopicOrPartitionError,
        )

        delete_topics_result: dict[str, KafkaFuture[None] | AsyncKafkaFuture[None]] = {}
        topic_names: list[Any] = list(topic_name_collection)
        if self._timeout_next_requests > 0:
            for topic_name in topic_names:
                delete_topics_result[topic_name] = self._failed(_timeout())
            self._timeout_next_requests -= 1
            return delete_topics_result
        for topic_name in topic_names:
            if self._all_topics.pop(topic_name, None) is None:
                future = self._failed(UnknownTopicOrPartitionError(
                    message=f"Topic {topic_name} does not exist."))
            else:
                # Java removes the id from its local list of names, so the field
                # topicNames keeps the stale id -> name entry.
                removed_id = self._topic_ids.pop(topic_name, None)
                if removed_id in topic_names:
                    topic_names.remove(removed_id)
                future = self._done(None)
            delete_topics_result[topic_name] = future
        return delete_topics_result

    def _handle_delete_topics_using_ids(
            self, topic_id_collection: Iterable[Uuid]) -> dict[Uuid, KafkaFuture[None] | AsyncKafkaFuture[None]]:
        from confluent_kafka.common.errors.unknown_topic_or_partition_error import (
            UnknownTopicOrPartitionError,
        )

        delete_topics_result: dict[Uuid, KafkaFuture[None] | AsyncKafkaFuture[None]] = {}
        topic_ids: list[Any] = list(topic_id_collection)
        if self._timeout_next_requests > 0:
            for topic_id in topic_ids:
                delete_topics_result[topic_id] = self._failed(_timeout())
            self._timeout_next_requests -= 1
            return delete_topics_result
        for topic_id in topic_ids:
            name = self._topic_names.pop(topic_id, None)
            if name is None or self._all_topics.pop(name, None) is None:
                future = self._failed(UnknownTopicOrPartitionError(
                    message=f"Topic {java_str(topic_id)} does not exist."))
            else:
                # Java removes the name from its local list of ids, so the field
                # topicIds keeps the stale name -> id entry.
                if name in topic_ids:
                    topic_ids.remove(name)
                future = self._done(None)
            delete_topics_result[topic_id] = future
        return delete_topics_result

    def _list_topics(self) -> ListTopicsResult[Any]:
        from confluent_kafka.admin.list_topics_result import ListTopicsResult
        from confluent_kafka.admin.topic_listing import TopicListing

        with self._lock:
            topic_listings: dict[str, TopicListing] = {}
            if self._timeout_next_requests > 0:
                self._timeout_next_requests -= 1
                return ListTopicsResult._new(future=self._failed(_timeout()), kind=self._kind)
            for topic_name, metadata in self._all_topics.items():
                if metadata.fetches_remaining_until_visible > 0:
                    metadata.fetches_remaining_until_visible -= 1
                else:
                    topic_listings[topic_name] = TopicListing(
                        name=topic_name, topic_id=metadata.topic_id,
                        internal=metadata.is_internal_topic)
            return ListTopicsResult._new(future=self._done(topic_listings), kind=self._kind)

    def _describe_topics(self, topic_names: Iterable[str] | None,
                         topics: TopicCollection | None) -> DescribeTopicsResult[Any]:
        """``topic_names`` or ``topics``, the other ``MISSING`` (``java_forms``)."""
        if topics is MISSING:
            reject_str("topic_names", topic_names)
        from confluent_kafka.admin.describe_topics_result import DescribeTopicsResult
        from confluent_kafka.common import TopicCollection

        if topics is MISSING:
            # Admin.describeTopics(Collection<String>, options):
            # describeTopics(TopicCollection.ofTopicNames(topicNames), options), whose
            # new ArrayList<>(null) throws NullPointerException.
            non_null(topic_names, "topic_names")
            assert topic_names is not None
            topics = TopicCollection.of_topic_names(topics=topic_names)
        with self._lock:
            if isinstance(topics, TopicCollection.TopicIdCollection):
                return DescribeTopicsResult._of_topic_ids(
                    topic_id_futures=dict(self._handle_describe_topics_using_ids(
                        topics.topic_ids())), kind=self._kind)
            if isinstance(topics, TopicCollection.TopicNameCollection):
                return DescribeTopicsResult._of_topic_names(
                    name_futures=dict(self._handle_describe_topics_by_names(
                        topics.topic_names())), kind=self._kind)
            raise IllegalArgumentError(
                message="The TopicCollection provided did not match any supported classes "
                        "for describeTopics.")

    def _handle_describe_topics_by_names(
            self, topic_names: Iterable[str]) -> dict[str, KafkaFuture[TopicDescription] | AsyncKafkaFuture[TopicDescription]]:
        from confluent_kafka.admin.topic_description import TopicDescription
        from confluent_kafka.common import Uuid
        from confluent_kafka.common.errors.unknown_topic_or_partition_error import (
            UnknownTopicOrPartitionError,
        )

        topic_descriptions: dict[str, KafkaFuture[TopicDescription] | AsyncKafkaFuture[TopicDescription]] = {}
        if self._timeout_next_requests > 0:
            for requested_topic in topic_names:
                topic_descriptions[requested_topic] = self._failed(_timeout())
            self._timeout_next_requests -= 1
            return topic_descriptions
        for requested_topic in topic_names:
            for topic_name, metadata in self._all_topics.items():
                topic_id = self._topic_ids.get(topic_name, Uuid.ZERO_UUID)
                if topic_name == requested_topic and not metadata.marked_for_deletion:
                    if metadata.fetches_remaining_until_visible > 0:
                        metadata.fetches_remaining_until_visible -= 1
                    else:
                        topic_descriptions[topic_name] = self._done(TopicDescription(
                            name=topic_name, internal=metadata.is_internal_topic,
                            partitions=metadata.partitions, authorized_operations=(),
                            topic_id=topic_id))
                        break
            if requested_topic not in topic_descriptions:
                topic_descriptions[requested_topic] = self._failed(UnknownTopicOrPartitionError(
                    message="Topic " + java_str(requested_topic) + " not found."))
        return topic_descriptions

    def _describe_cluster(self) -> DescribeClusterResult[Any]:
        from confluent_kafka.admin.describe_cluster_result import DescribeClusterResult

        with self._lock:
            if self._timeout_next_requests > 0:
                nodes = self._failed(_timeout())
                controller = self._failed(_timeout())
                broker_id = self._failed(_timeout())
                authorized_operations = self._failed(_timeout())
                self._timeout_next_requests -= 1
            else:
                # Java completes with its live broker list.
                nodes = self._done(self._brokers)
                controller = self._done(self._controller)
                broker_id = self._done(self._cluster_id)
                authorized_operations = self._done(set())
            return DescribeClusterResult._new(
                nodes=nodes, controller=controller, cluster_id=broker_id,
                authorized_operations=authorized_operations, kind=self._kind)

    def _describe_acls(self, filter: AclBindingFilter  # noqa: A002 - Java's name
                       ) -> DescribeAclsResult[Any]:
        with self._lock:
            raise _unsupported(_NOT_IMPLEMENTED)

    def _create_acls(self, acls: Iterable[AclBinding]) -> CreateAclsResult[Any]:
        with self._lock:
            raise _unsupported(_NOT_IMPLEMENTED)

    def _delete_acls(self, filters: Iterable[AclBindingFilter]) -> DeleteAclsResult[Any]:
        with self._lock:
            raise _unsupported(_NOT_IMPLEMENTED)

    def _describe_configs(self, resources: Iterable[ConfigResource]) -> DescribeConfigsResult[Any]:
        from confluent_kafka.admin.describe_configs_result import DescribeConfigsResult

        with self._lock:
            non_null(resources, "resources")  # for (ConfigResource resource : resources)
            if self._timeout_next_requests > 0:
                configs: dict[ConfigResource, KafkaFuture[Config] | AsyncKafkaFuture[Config]] = {}
                for requested_resource in resources:
                    configs[requested_resource] = self._failed(_timeout())
                self._timeout_next_requests -= 1
                return DescribeConfigsResult._new(futures=configs, kind=self._kind)
            results: dict[ConfigResource, KafkaFuture[Config] | AsyncKafkaFuture[Config]] = {}
            for resource in resources:
                future = self._future()
                results[resource] = future
                try:
                    _complete(future, self._get_resource_description(resource))
                except Exception as e:  # noqa: BLE001 - Java's catch (Throwable)
                    _complete_with_error(future, e)
            return DescribeConfigsResult._new(futures=results, kind=self._kind)

    def _get_resource_description(self, resource: ConfigResource) -> Config:
        from confluent_kafka.common.config import ConfigResource
        from confluent_kafka.common.errors.invalid_request_error import InvalidRequestError
        from confluent_kafka.common.errors.unknown_topic_or_partition_error import (
            UnknownTopicOrPartitionError,
        )

        with self._lock:
            # resource.type() on a null element: the NullPointerException fails its
            # future (Java's catch (Throwable) in describeConfigs).
            resource_type = element(resource, "resources").type()
            if resource_type is ConfigResource.Type.BROKER:
                broker_id = parse_int(resource.name())
                if broker_id >= len(self._broker_configs):
                    raise InvalidRequestError(
                        message="Broker " + resource.name() + " not found.")
                return _config(_get(self._broker_configs, broker_id), resource)
            if resource_type is ConfigResource.Type.TOPIC:
                metadata = self._all_topics.get(resource.name())
                if metadata is not None and not metadata.marked_for_deletion:
                    if metadata.fetches_remaining_until_visible > 0:
                        metadata.fetches_remaining_until_visible = max(
                            0, metadata.fetches_remaining_until_visible - 1)
                    else:
                        return _config(metadata.configs, resource)
                raise UnknownTopicOrPartitionError(
                    message="Resource " + java_str(resource) + " not found.")
            if resource_type is ConfigResource.Type.CLIENT_METRICS:
                resource_name = resource.name()
                if not resource_name:
                    raise InvalidRequestError(message="Empty resource name")
                return _config(self._client_metrics_configs.get(resource_name), resource)
            if resource_type is ConfigResource.Type.GROUP:
                resource_name = resource.name()
                if not resource_name:
                    raise InvalidRequestError(message="Empty resource name")
                group_config = self._group_configs.get(resource_name, {})
                for key, value in self._default_group_configs.items():
                    # Java's putIfAbsent, which also replaces a key set to null.
                    if group_config.get(key) is None:
                        group_config[key] = value
                return _config(group_config, resource)
            raise _unsupported(_NOT_IMPLEMENTED)

    def _incremental_alter_configs(
            self, configs: Mapping[ConfigResource, Iterable[AlterConfigOp]]
    ) -> AlterConfigsResult[Any]:
        from confluent_kafka.admin.alter_configs_result import AlterConfigsResult

        with self._lock:
            futures: dict[ConfigResource, KafkaFuture[None] | AsyncKafkaFuture[None]] = {}
            non_null(configs, "configs")  # configs.entrySet()
            for resource, ops in configs.items():
                future = self._future()
                futures[resource] = future
                error = self._handle_incremental_resource_alteration(resource, ops)
                if error is None:
                    _complete(future, None)
                else:
                    _complete_with_error(future, error)
            return AlterConfigsResult._new(futures=futures, kind=self._kind)

    @staticmethod
    def _apply_ops(config: Mapping[str, str | None],
                   ops: Iterable[AlterConfigOp]) -> dict[str, str | None] | Exception:
        """The ``SET`` / ``DELETE`` loop each resource type repeats: the new map, or
        Java's ``InvalidRequestException`` for another op type."""
        from confluent_kafka.admin.alter_config_op import AlterConfigOp
        from confluent_kafka.common.errors.invalid_request_error import InvalidRequestError

        new_map = dict(config)
        for op in element(ops, "configs"):  # for (AlterConfigOp op : ops)
            if element(op, "configs").op_type() is AlterConfigOp.OpType.SET:
                value = op.config_entry().value()
                new_map[op.config_entry().name()] = value
            elif op.op_type() is AlterConfigOp.OpType.DELETE:
                new_map.pop(op.config_entry().name(), None)
            else:
                return InvalidRequestError(message="Unsupported op type " + java_str(op.op_type()))
        return new_map

    def _handle_incremental_resource_alteration(
            self, resource: ConfigResource, ops: Iterable[AlterConfigOp]) -> Exception | None:
        from confluent_kafka.common.config import ConfigResource
        from confluent_kafka.common.errors.invalid_request_error import InvalidRequestError
        from confluent_kafka.common.errors.unknown_topic_or_partition_error import (
            UnknownTopicOrPartitionError,
        )

        with self._lock:
            resource_type = element(resource, "configs").type()  # uncaught in Java
            if resource_type is ConfigResource.Type.BROKER:
                try:
                    broker_id = parse_int(resource.name())
                except IllegalArgumentError as e:  # Java's NumberFormatException
                    return e
                if broker_id >= len(self._broker_configs):
                    return InvalidRequestError(message="no such broker as " + java_str(broker_id))
                new_map = self._apply_ops(_get(self._broker_configs, broker_id), ops)
                if isinstance(new_map, Exception):
                    return new_map
                self._broker_configs[broker_id] = new_map
                return None
            if resource_type is ConfigResource.Type.TOPIC:
                metadata = self._all_topics.get(resource.name())
                if metadata is None:
                    return UnknownTopicOrPartitionError(
                        message="No such topic as " + resource.name())
                new_map = self._apply_ops(metadata.configs, ops)
                if isinstance(new_map, Exception):
                    return new_map
                metadata.configs = new_map
                return None
            if resource_type in (ConfigResource.Type.CLIENT_METRICS, ConfigResource.Type.GROUP):
                stored = (self._client_metrics_configs
                          if resource_type is ConfigResource.Type.CLIENT_METRICS
                          else self._group_configs)
                resource_name = resource.name()
                if not resource_name:
                    return InvalidRequestError(message="Empty resource name")
                if resource_name not in stored:
                    stored[resource_name] = {}
                new_map = self._apply_ops(stored[resource_name], ops)
                if isinstance(new_map, Exception):
                    return new_map
                stored[resource_name] = new_map
                return None
            return _unsupported()

    def _alter_replica_log_dirs(
            self, replica_assignment: Mapping[TopicPartitionReplica, str]
            ) -> AlterReplicaLogDirsResult[Any]:
        from confluent_kafka.admin.alter_replica_log_dirs_result import AlterReplicaLogDirsResult
        from confluent_kafka.admin.describe_replica_log_dirs_result import (
            DescribeReplicaLogDirsResult,
        )
        from confluent_kafka.common.errors.kafka_storage_error import KafkaStorageError
        from confluent_kafka.common.errors.replica_not_available_error import (
            ReplicaNotAvailableError,
        )

        with self._lock:
            results: dict[TopicPartitionReplica, KafkaFuture[None] | AsyncKafkaFuture[None]] = {}
            non_null(replica_assignment, "replica_assignment")  # replicaAssignment.entrySet()
            for replica, new_log_dir in replica_assignment.items():
                future = self._future()
                results[replica] = future
                dirs = _get(self._broker_log_dirs,
                            element(replica, "replica_assignment").broker_id())
                if dirs is None:
                    _complete_with_error(future, ReplicaNotAvailableError(
                        message="Can't find " + java_str(replica)))
                elif new_log_dir not in dirs:
                    _complete_with_error(future, KafkaStorageError(
                        message="Log directory " + java_str(new_log_dir) + " is offline"))
                else:
                    metadata = self._all_topics.get(replica.topic())
                    if metadata is None or len(metadata.partitions) <= replica.partition():
                        _complete_with_error(future, ReplicaNotAvailableError(
                            message="Can't find " + java_str(replica)))
                    else:
                        current_log_dir = _get(metadata.partition_log_dirs, replica.partition())
                        self._replica_moves[replica] = (
                            DescribeReplicaLogDirsResult.ReplicaLogDirInfo._new(
                                current_replica_log_dir=current_log_dir,
                                current_replica_offset_lag=0,
                                future_replica_log_dir=new_log_dir,
                                future_replica_offset_lag=0))
                        _complete(future, None)
            return AlterReplicaLogDirsResult._new(futures=results, kind=self._kind)

    def _describe_log_dirs(self, brokers: Iterable[int]) -> DescribeLogDirsResult[Any]:
        from confluent_kafka.admin.describe_log_dirs_result import DescribeLogDirsResult
        from confluent_kafka.admin.log_dir_description import LogDirDescription
        from confluent_kafka.admin.replica_info import ReplicaInfo
        from confluent_kafka.common import TopicPartition

        with self._lock:
            unwrapped_results: dict[int, dict[str, LogDirDescription]] = {}
            non_null(brokers, "brokers")  # for (Integer broker : brokers)
            for broker in brokers:
                unwrapped_results.setdefault(broker, {})
            for topic_name, metadata in self._all_topics.items():
                # For tests, we make the assumption that there will always be only 1 entry.
                partition_log_dirs = metadata.partition_log_dirs
                for topic_partition_info in metadata.partitions:
                    for node in topic_partition_info.replicas():
                        log_dir_description_map = unwrapped_results.get(node.id())
                        if log_dir_description_map is None:  # Java dereferences it: NullPointerException
                            raise IllegalStateError(
                                message=f"MockAdminClient has no log dirs for broker {node.id()}")
                        log_dir = _get(partition_log_dirs, 0)
                        log_dir_description = log_dir_description_map.get(log_dir)
                        if log_dir_description is None:
                            log_dir_description = LogDirDescription(error=None, replica_infos={})
                        replica_infos = dict(log_dir_description.replica_infos())
                        replica_infos[TopicPartition(
                            topic=topic_name, partition=topic_partition_info.partition())] = (
                            ReplicaInfo(size=0, offset_lag=0, is_future=False))
                        total_bytes = log_dir_description.total_bytes()
                        usable_bytes = log_dir_description.usable_bytes()
                        log_dir_description_map[log_dir] = LogDirDescription(
                            error=log_dir_description.error(), replica_infos=replica_infos,
                            total_bytes=(_UNKNOWN_VOLUME_BYTES if total_bytes is None
                                         else total_bytes),
                            usable_bytes=(_UNKNOWN_VOLUME_BYTES if usable_bytes is None
                                          else usable_bytes))
            results: dict[int, KafkaFuture[dict[str, LogDirDescription]] | AsyncKafkaFuture[dict[str, LogDirDescription]]] = {}
            for broker_id, descriptions in unwrapped_results.items():
                results[broker_id] = self._done(descriptions)
            return DescribeLogDirsResult._new(futures=results, kind=self._kind)

    def _describe_replica_log_dirs(
            self, replicas: Iterable[TopicPartitionReplica]) -> DescribeReplicaLogDirsResult[Any]:
        from confluent_kafka.admin.describe_replica_log_dirs_result import (
            DescribeReplicaLogDirsResult,
        )

        info_class = DescribeReplicaLogDirsResult.ReplicaLogDirInfo
        with self._lock:
            results: dict[TopicPartitionReplica,
                          KafkaFuture[DescribeReplicaLogDirsResult.ReplicaLogDirInfo] | AsyncKafkaFuture[DescribeReplicaLogDirsResult.ReplicaLogDirInfo]] = {}
            non_null(replicas, "replicas")  # for (TopicPartitionReplica replica : replicas)
            for replica in replicas:
                if self._all_topics.get(element(replica, "replicas").topic()) is not None:
                    future = self._future()
                    results[replica] = future
                    current_log_dir = self._current_log_dir(replica)
                    if current_log_dir is None:
                        _complete(future, info_class._new(
                            current_replica_log_dir=None,
                            current_replica_offset_lag=_INVALID_OFFSET_LAG,
                            future_replica_log_dir=None,
                            future_replica_offset_lag=_INVALID_OFFSET_LAG))
                    else:
                        info = self._replica_moves.get(replica)
                        if info is None:
                            info = info_class._new(
                                current_replica_log_dir=current_log_dir,
                                current_replica_offset_lag=0, future_replica_log_dir=None,
                                future_replica_offset_lag=0)
                        _complete(future, info)
            return DescribeReplicaLogDirsResult._new(futures=results, kind=self._kind)

    def _current_log_dir(self, replica: TopicPartitionReplica) -> str | None:
        with self._lock:
            metadata = self._all_topics.get(replica.topic())
            if metadata is None:
                return None
            if len(metadata.partition_log_dirs) <= replica.partition():
                return None
            return _get(metadata.partition_log_dirs, replica.partition())

    def _create_partitions(self, new_partitions: Mapping[str, NewPartitions]
    ) -> CreatePartitionsResult[Any]:
        with self._lock:
            raise _unsupported(_NOT_IMPLEMENTED)

    def _delete_records(self, records_to_delete: Mapping[TopicPartition, RecordsToDelete]
    ) -> DeleteRecordsResult[Any]:
        from confluent_kafka.admin.delete_records_result import DeleteRecordsResult

        with self._lock:
            non_null(records_to_delete, "records_to_delete")  # recordsToDelete.isEmpty()
            if not records_to_delete:
                return DeleteRecordsResult._new(futures={}, kind=self._kind)
            raise _unsupported(_NOT_IMPLEMENTED)

    def _create_delegation_token(self, options: CreateDelegationTokenOptions
                                 ) -> CreateDelegationTokenResult[Any]:
        """A token owned by the first renewer, every renewer of the ``User`` type;
        ``options.renewers().get(0)`` on no renewer raises ``IndexOutOfBoundsError`` with the
        message of the options' default ``LinkedList``, ``"Index: 0, Size: 0"``.

        *(deviation: when the user sets an empty renewers list, Java's
        ``IndexOutOfBoundsException`` text depends on the list's class — ``ArrayList``
        or ``List.of``: ``"Index 0 out of bounds for length 0"``, ``emptyList``:
        ``"Index: 0"`` — while here it is always ``LinkedList``'s
        ``"Index: 0, Size: 0"``, because ``CreateDelegationTokenOptions`` copies the
        list.)*
        """
        from confluent_kafka.admin.create_delegation_token_result import (
            CreateDelegationTokenResult,
        )
        from confluent_kafka.common.security.token.delegation import (
            DelegationToken,
            TokenInformation,
        )
        from confluent_kafka.common.security.auth import KafkaPrincipal
        from confluent_kafka.common import Uuid
        from confluent_kafka.common.errors.invalid_principal_type_error import (
            InvalidPrincipalTypeError,
        )

        with self._lock:
            future: KafkaFuture[DelegationToken] | AsyncKafkaFuture[DelegationToken] = self._future()
            renewers: list[KafkaPrincipal] | None = options.renewers()
            if renewers is None:  # for (KafkaPrincipal renewer : options.renewers())
                raise TypeError("CreateDelegationTokenOptions.renewers must not be None")
            for renewer in renewers:
                if renewer.principal_type() != KafkaPrincipal.USER_TYPE:
                    _complete_with_error(future, InvalidPrincipalTypeError(message=""))
                    return CreateDelegationTokenResult._new(delegation_token=future,
                                                            kind=self._kind)
            token_id = java_str(Uuid.random_uuid())
            if not renewers:  # LinkedList.get(0)
                raise IndexOutOfBoundsError(message="Index: 0, Size: 0")
            token_info = TokenInformation(
                token_id=token_id, owner=renewers[0], renewers=renewers,
                issue_timestamp=_current_time_millis(),
                max_timestamp=options.max_lifetime_ms(), expiry_timestamp=-1)
            token = DelegationToken(token_information=token_info, hmac=token_id.encode())
            self._all_tokens.append(token)
            _complete(future, token)
            return CreateDelegationTokenResult._new(delegation_token=future, kind=self._kind)

    def _renew_delegation_token(self, hmac: bytes, options: RenewDelegationTokenOptions
                                ) -> RenewDelegationTokenResult[Any]:
        from confluent_kafka.admin.renew_delegation_token_result import (
            RenewDelegationTokenResult,
        )
        from confluent_kafka.common.errors.delegation_token_not_found_error import (
            DelegationTokenNotFoundError,
        )

        with self._lock:
            future: KafkaFuture[int] | AsyncKafkaFuture[int] = self._future()
            token_found = False
            expiry_timestamp: int = options.renew_time_period_ms()
            for token in self._all_tokens:
                if token.hmac() == hmac:  # Arrays.equals
                    token.token_info().set_expiry_timestamp(expiry_timestamp=expiry_timestamp)
                    token_found = True
            if token_found:
                _complete(future, expiry_timestamp)
            else:
                _complete_with_error(future, DelegationTokenNotFoundError(message=""))
            return RenewDelegationTokenResult._new(expiry_timestamp=future, kind=self._kind)

    def _expire_delegation_token(self, hmac: bytes, options: ExpireDelegationTokenOptions
                                 ) -> ExpireDelegationTokenResult[Any]:
        from confluent_kafka.admin.expire_delegation_token_result import (
            ExpireDelegationTokenResult,
        )
        from confluent_kafka.common.errors.delegation_token_not_found_error import (
            DelegationTokenNotFoundError,
        )

        with self._lock:
            future: KafkaFuture[int] | AsyncKafkaFuture[int] = self._future()
            expiry_timestamp: int = options.expiry_time_period_ms()
            tokens_to_remove: list[DelegationToken] = []
            token_found = False
            for token in self._all_tokens:
                if token.hmac() == hmac:  # Arrays.equals
                    if expiry_timestamp == -1 or expiry_timestamp < _current_time_millis():
                        tokens_to_remove.append(token)
                    token_found = True
            if token_found:
                # List.removeAll, in place: describe_delegation_token hands out this list.
                self._all_tokens[:] = [t for t in self._all_tokens if t not in tokens_to_remove]
                _complete(future, expiry_timestamp)
            else:
                _complete_with_error(future, DelegationTokenNotFoundError(message=""))
            return ExpireDelegationTokenResult._new(expiry_timestamp=future, kind=self._kind)

    def _describe_delegation_token(self, options: DescribeDelegationTokenOptions
                                   ) -> DescribeDelegationTokenResult[Any]:
        """Every token for an empty ``owners`` (the mock's own list, as Java's), else
        the tokens of those owners; Java's ``options.owners().isEmpty()`` on the
        default ``null`` owners raises ``TypeError`` (Java's ``NullPointerException``)."""
        from confluent_kafka.admin.describe_delegation_token_result import (
            DescribeDelegationTokenResult,
        )

        with self._lock:
            future: KafkaFuture[list[DelegationToken]] | AsyncKafkaFuture[list[DelegationToken]] = self._future()
            owners: list[KafkaPrincipal] | None = options.owners()
            if owners is None:  # Java's options.owners().isEmpty(): NullPointerException
                raise TypeError("DescribeDelegationTokenOptions.owners must not be None")
            if not owners:
                _complete(future, self._all_tokens)
            else:
                tokens_result: list[DelegationToken] = []
                for token in self._all_tokens:
                    if token.token_info().owner() in owners:
                        tokens_result.append(token)
                _complete(future, tokens_result)
            return DescribeDelegationTokenResult._new(delegation_tokens=future, kind=self._kind)

    def _describe_consumer_groups(self, group_ids: Iterable[str]
                                  ) -> DescribeConsumerGroupsResult[Any]:
        reject_str("group_ids", group_ids)
        with self._lock:
            raise _unsupported(_NOT_IMPLEMENTED)

    def _list_consumer_group_offsets(
            self, group_specs: Mapping[str, ListConsumerGroupOffsetsSpec]
            ) -> ListConsumerGroupOffsetsResult[Any]:
        """The committed offsets of the one group (all for an empty spec, else those
        of its partitions), keyed by ``CoordinatorKey.byGroupId``; Java's filter
        dereferences the spec's default ``null`` partitions once an offset is stored,
        so that raises ``TypeError`` (Java's ``NullPointerException``)."""
        from confluent_kafka.admin._internals import _CoordinatorKey
        from confluent_kafka.admin.list_consumer_group_offsets_result import (
            ListConsumerGroupOffsetsResult,
        )
        from confluent_kafka.consumer import OffsetAndMetadata

        with self._lock:
            # ignoring the groups and assume one test would only work on one group only
            non_null(group_specs, "group_specs")  # groupSpecs.size()
            if len(group_specs) != 1:
                raise _unsupported(_NOT_IMPLEMENTED)
            group = next(iter(group_specs))
            topic_partitions = element(group_specs[group], "group_specs").topic_partitions()
            offsets: dict[TopicPartition, OffsetAndMetadata | None] = {}
            for tp, offset in self._committed_offsets.items():
                if topic_partitions is None:  # topicPartitions.isEmpty(): NullPointerException
                    raise TypeError("ListConsumerGroupOffsetsSpec.topic_partitions must not be None")
                if not topic_partitions or tp in topic_partitions:
                    offsets[tp] = OffsetAndMetadata(offset=offset)
            return ListConsumerGroupOffsetsResult._new(
                futures={_CoordinatorKey.by_group_id(group_id=group): self._done(offsets)},
                kind=self._kind)

    def _delete_consumer_groups(self, group_ids: Iterable[str]
                                ) -> DeleteConsumerGroupsResult[Any]:
        reject_str("group_ids", group_ids)
        with self._lock:
            raise _unsupported(_NOT_IMPLEMENTED)

    def _delete_consumer_group_offsets(self, group_id: str,
                                       partitions: Iterable[TopicPartition]
                                       ) -> DeleteConsumerGroupOffsetsResult[Any]:
        with self._lock:
            raise _unsupported(_NOT_IMPLEMENTED)

    def _list_groups(self) -> ListGroupsResult[Any]:
        """One stable consumer group of the ``consumer`` protocol per group config."""
        from confluent_kafka.admin._internals import _ConsumerProtocol
        from confluent_kafka.admin.group_listing import GroupListing
        from confluent_kafka.admin.list_groups_result import ListGroupsResult
        from confluent_kafka.common import GroupState, GroupType

        with self._lock:
            future: KafkaFuture[Iterable[object]] | AsyncKafkaFuture[Iterable[object]] = self._done([
                GroupListing(group_id=g, type=GroupType.CONSUMER,
                             protocol=_ConsumerProtocol.PROTOCOL_TYPE,
                             group_state=GroupState.STABLE)
                for g in self._group_configs])
            return ListGroupsResult._new(future=future, kind=self._kind)

    def _elect_leaders(self, election_type: ElectionType,
                       partitions: Iterable[TopicPartition] | None) -> ElectLeadersResult[Any]:
        with self._lock:
            raise _unsupported(_NOT_IMPLEMENTED)

    def _alter_partition_reassignments(
            self, reassignments: Mapping[TopicPartition, NewPartitionReassignment | None]
            ) -> AlterPartitionReassignmentsResult[Any]:
        from confluent_kafka.admin.alter_partition_reassignments_result import (
            AlterPartitionReassignmentsResult,
        )
        from confluent_kafka.common.errors.unknown_topic_or_partition_error import (
            UnknownTopicOrPartitionError,
        )

        with self._lock:
            futures: dict[TopicPartition, KafkaFuture[None] | AsyncKafkaFuture[None]] = {}
            non_null(reassignments, "reassignments")  # newReassignments.entrySet()
            for partition, new_reassignment in reassignments.items():
                future = self._future()
                futures[partition] = future
                if partition is None:  # partition.topic(); a None value is an empty Optional
                    raise TypeError("reassignments must not contain a None key")
                metadata = self._all_topics.get(partition.topic())
                if (partition.partition() < 0 or metadata is None
                        or len(metadata.partitions) <= partition.partition()):
                    _complete_with_error(future, UnknownTopicOrPartitionError())
                elif new_reassignment is not None:
                    self._reassignments[partition] = new_reassignment
                    _complete(future, None)
                else:
                    self._reassignments.pop(partition, None)
                    _complete(future, None)
            return AlterPartitionReassignmentsResult._new(futures=futures, kind=self._kind)

    def _list_partition_reassignments(
            self, partitions: Iterable[TopicPartition] | None
            ) -> ListPartitionReassignmentsResult[Any]:
        from confluent_kafka.admin.list_partition_reassignments_result import (
            ListPartitionReassignmentsResult,
        )

        with self._lock:
            result: dict[TopicPartition, PartitionReassignment] = {}
            for partition in (list(self._reassignments) if partitions is None else partitions):
                reassignment = self._find_partition_reassignment(partition)
                if reassignment is not None:
                    result[partition] = reassignment
            return ListPartitionReassignmentsResult._new(
                reassignments=self._done(result), kind=self._kind)

    def _find_partition_reassignment(self, partition: TopicPartition
                                     ) -> PartitionReassignment | None:
        from confluent_kafka.admin.partition_reassignment import PartitionReassignment

        with self._lock:
            reassignment = self._reassignments.get(partition)
            if reassignment is None:
                return None
            metadata = self._all_topics.get(partition.topic())
            if metadata is None:
                raise RuntimeError(
                    "Internal MockAdminClient logic error: found reassignment for "
                    + java_str(partition) + ", but no TopicMetadata")
            info: TopicPartitionInfo | None = _get(metadata.partitions, partition.partition())
            if info is None:
                raise RuntimeError(
                    "Internal MockAdminClient logic error: found reassignment for "
                    + java_str(partition) + ", but no TopicPartitionInfo")
            target_replicas = reassignment.target_replicas()
            replicas: list[int] = []
            removing_replicas: list[int] = []
            adding_replicas = list(target_replicas)
            for node in info.replicas():
                replicas.append(node.id())
                if node.id() not in target_replicas:
                    removing_replicas.append(node.id())
                if node.id() in adding_replicas:
                    adding_replicas.remove(node.id())
            return PartitionReassignment(replicas=replicas, adding_replicas=adding_replicas,
                                         removing_replicas=removing_replicas)

    def _remove_members_from_consumer_group(
            self, group_id: str, options: RemoveMembersFromConsumerGroupOptions
            ) -> RemoveMembersFromConsumerGroupResult[Any]:
        with self._lock:
            raise _unsupported(_NOT_IMPLEMENTED)

    def _alter_consumer_group_offsets(self, group_id: str,
                                      offsets: Mapping[TopicPartition, OffsetAndMetadata]
                                      ) -> AlterConsumerGroupOffsetsResult[Any]:
        with self._lock:
            raise _unsupported(_NOT_IMPLEMENT)

    def _list_offsets(self, topic_partition_offsets: Mapping[TopicPartition, OffsetSpec]
                      ) -> ListOffsetsResult[Any]:
        from confluent_kafka.admin.list_offsets_result import ListOffsetsResult
        from confluent_kafka.admin.offset_spec import OffsetSpec

        info_class = ListOffsetsResult.ListOffsetsResultInfo
        with self._lock:
            futures: dict[TopicPartition, KafkaFuture[ListOffsetsResult.ListOffsetsResultInfo] | AsyncKafkaFuture[ListOffsetsResult.ListOffsetsResultInfo]] = {}
            non_null(topic_partition_offsets, "topic_partition_offsets")  # .entrySet()
            for tp, spec in topic_partition_offsets.items():
                future = self._future()
                if isinstance(spec, OffsetSpec.TimestampSpec):
                    raise _unsupported(_NOT_IMPLEMENT)
                offsets = (self._beginning_offsets if isinstance(spec, OffsetSpec.EarliestSpec)
                           else self._end_offsets)
                offset = offsets.get(tp)
                if offset is None:  # Java unboxes the missing Long: NullPointerException
                    which = "beginning" if offsets is self._beginning_offsets else "end"
                    raise IllegalStateError(
                        message=f"MockAdminClient has no {which} offset for {java_str(tp)}")
                _complete(future, info_class(offset=offset, timestamp=-1, leader_epoch=None))
                futures[tp] = future
            return ListOffsetsResult._new(futures=futures, kind=self._kind)

    def _describe_client_quotas(self, filter: ClientQuotaFilter  # noqa: A002 - Java's name
                                ) -> DescribeClientQuotasResult[Any]:
        """Not synchronized in Java."""
        raise _unsupported(_NOT_IMPLEMENT)

    def _alter_client_quotas(self, entries: Iterable[ClientQuotaAlteration]
                             ) -> AlterClientQuotasResult[Any]:
        """Not synchronized in Java."""
        raise _unsupported(_NOT_IMPLEMENT)

    def _describe_user_scram_credentials(self, users: Sequence[str] | None
                                         ) -> DescribeUserScramCredentialsResult[Any]:
        """Not synchronized in Java."""
        if users is not None:
            reject_str("users", users)
        raise _unsupported(_NOT_IMPLEMENTED)

    def _alter_user_scram_credentials(self, alterations: Sequence[UserScramCredentialAlteration]
                                      ) -> AlterUserScramCredentialsResult[Any]:
        """Not synchronized in Java."""
        raise _unsupported(_NOT_IMPLEMENTED)

    def _describe_features(self) -> DescribeFeaturesResult[Any]:
        """Not synchronized in Java. Each finalized level as a ``[level, level]``
        range with its supported range, epoch 123; a feature level with no
        supported min or max raises ``IllegalStateError`` (Java unboxes the
        missing ``Short``: ``NullPointerException``)."""
        from confluent_kafka.admin.describe_features_result import DescribeFeaturesResult
        from confluent_kafka.admin.feature_metadata import FeatureMetadata
        from confluent_kafka.admin.finalized_version_range import FinalizedVersionRange
        from confluent_kafka.admin.supported_version_range import SupportedVersionRange

        finalized_features: dict[str, FinalizedVersionRange] = {}
        supported_features: dict[str, SupportedVersionRange] = {}
        for feature, level in list(self._feature_levels.items()):
            finalized_features[feature] = FinalizedVersionRange(min_version_level=level,
                                                                max_version_level=level)
            min_version = self._min_supported_feature_levels.get(feature)
            max_version = self._max_supported_feature_levels.get(feature)
            if min_version is None or max_version is None:  # Java unboxes them: NullPointerException
                raise IllegalStateError(
                    message=f"MockAdminClient has no supported version range for feature {feature}")
            supported_features[feature] = SupportedVersionRange(min_version=min_version,
                                                                max_version=max_version)
        return DescribeFeaturesResult._new(
            future=self._done(FeatureMetadata._new(
                finalized_features=finalized_features, finalized_features_epoch=123,
                supported_features=supported_features)),
            kind=self._kind)

    def _update_features(self, feature_updates: Mapping[str, FeatureUpdate],
                         options: UpdateFeaturesOptions | None) -> UpdateFeaturesResult[Any]:
        """Not synchronized in Java. Checks the updates in order; the first that
        fails fails every future, with ``InvalidRequestError``; else each level is
        set (unless ``options.validate_only()``, which Java reads only then, per
        update: a ``None`` ``options`` raises ``TypeError`` there)."""
        from confluent_kafka.admin.feature_update import FeatureUpdate
        from confluent_kafka.admin.update_features_result import UpdateFeaturesResult
        from confluent_kafka.common.errors.invalid_request_error import InvalidRequestError
        from confluent_kafka.common.errors.invalid_update_version_error import (
            InvalidUpdateVersionError,
        )

        upgrade_type = FeatureUpdate.UpgradeType
        error: BaseException | None = None
        non_null(feature_updates, "feature_updates")  # featureUpdates.entrySet()
        for feature, update in feature_updates.items():
            cur = self._feature_levels.get(feature, 0)
            next_ = element(update, "feature_updates").max_version_level()
            min_ = self._min_supported_feature_levels.get(feature, 0)
            max_ = self._max_supported_feature_levels.get(feature, 0)
            try:
                kind = update.upgrade_type()
                if kind is upgrade_type.UNKNOWN:
                    raise InvalidRequestError(message="Invalid upgrade type.")
                if kind is upgrade_type.UPGRADE:
                    if cur > next_:
                        raise InvalidUpdateVersionError(message="Can't upgrade to lower version.")
                elif kind is upgrade_type.SAFE_DOWNGRADE:
                    if cur < next_:
                        raise InvalidUpdateVersionError(
                            message="Can't downgrade to newer version.")
                elif kind is upgrade_type.UNSAFE_DOWNGRADE:
                    if cur < next_:
                        raise InvalidUpdateVersionError(
                            message="Can't downgrade to newer version.")
                    while next_ != cur:
                        # Simulate a scenario where all the even feature levels unsafe to
                        # downgrade from.
                        if cur % 2 == 0:
                            if update.upgrade_type() is upgrade_type.SAFE_DOWNGRADE:
                                raise InvalidUpdateVersionError(
                                    message="Unable to perform a safe downgrade.")
                        cur -= 1
                if next_ < min_:
                    raise InvalidUpdateVersionError(message=f"Can't downgrade below {min_}")
                if next_ > max_:
                    raise InvalidUpdateVersionError(message=f"Can't upgrade above {max_}")
            except Exception as e:  # noqa: BLE001 - Java's catch (Exception)
                error = InvalidRequestError(
                    message=f"Invalid update version {next_} for feature {feature}. {e}")
                break
        results: dict[str, KafkaFuture[None] | AsyncKafkaFuture[None]] = {}
        for feature, update in feature_updates.items():
            future: KafkaFuture[None] | AsyncKafkaFuture[None] = self._future()
            if error is None:
                _complete(future, None)
                non_null(options, "options")  # options.validateOnly()
                assert options is not None
                if not options.validate_only():
                    self._feature_levels[feature] = update.max_version_level()
            else:
                _complete_with_error(future, error)
            results[feature] = future
        return UpdateFeaturesResult._new(futures=results, kind=self._kind)

    def _unregister_controller(self, controller_id: int) -> UnregisterControllerResult[Any]:
        """Java's ``unregisterController``: a completed future when the mock uses a
        raft controller, else one failed with ``UnsupportedVersionError("")``. Not
        synchronized in Java."""
        from confluent_kafka.admin.unregister_controller_result import (
            UnregisterControllerResult,
        )

        if self._using_raft_controller:
            return UnregisterControllerResult._new(
                future=self._kind._FUTURE.completed_future(value=None), kind=self._kind)
        return UnregisterControllerResult._new(future=self._failed(_unsupported_version("")),
                                               kind=self._kind)

    def _describe_producers(self, partitions: Iterable[TopicPartition]
                            ) -> DescribeProducersResult[Any]:
        """Not synchronized in Java."""
        raise _unsupported(_NOT_IMPLEMENTED)

    def _describe_transactions(self, transactional_ids: Iterable[str]
                               ) -> DescribeTransactionsResult[Any]:
        """Not synchronized in Java."""
        reject_str("transactional_ids", transactional_ids)
        raise _unsupported(_NOT_IMPLEMENTED)

    def _abort_transaction(self, spec: AbortTransactionSpec) -> AbortTransactionResult[Any]:
        """Not synchronized in Java."""
        raise _unsupported(_NOT_IMPLEMENTED)

    def _list_transactions(self) -> ListTransactionsResult[Any]:
        """Not synchronized in Java."""
        raise _unsupported(_NOT_IMPLEMENTED)

    def _fence_producers(self, transactional_ids: Iterable[str]) -> FenceProducersResult[Any]:
        """Not synchronized in Java."""
        reject_str("transactional_ids", transactional_ids)
        raise _unsupported(_NOT_IMPLEMENTED)

    def _list_config_resources(self, config_resource_types: Iterable[ConfigResource.Type]
                               ) -> ListConfigResourcesResult[Any]:
        """Not synchronized in Java."""
        from confluent_kafka.admin.list_config_resources_result import ListConfigResourcesResult
        from confluent_kafka.common.config import ConfigResource

        non_null(config_resource_types, "config_resource_types")  # configResourceTypes.isEmpty()
        types = set(config_resource_types)
        resource_type = ConfigResource.Type
        config_resources: set[ConfigResource] = set()
        if not types or resource_type.TOPIC in types:
            for name in list(self._all_topics):
                config_resources.add(ConfigResource(type=resource_type.TOPIC, name=name))
        if not types or resource_type.BROKER in types:
            for i in range(len(self._brokers)):
                config_resources.add(ConfigResource(type=resource_type.BROKER, name=str(i)))
        if not types or resource_type.BROKER_LOGGER in types:
            for i in range(len(self._brokers)):
                config_resources.add(
                    ConfigResource(type=resource_type.BROKER_LOGGER, name=str(i)))
        if not types or resource_type.CLIENT_METRICS in types:
            for name in list(self._client_metrics_configs):
                config_resources.add(
                    ConfigResource(type=resource_type.CLIENT_METRICS, name=name))
        if not types or resource_type.GROUP in types:
            for name in list(self._group_configs):
                config_resources.add(ConfigResource(type=resource_type.GROUP, name=name))
        # Java completes the Collection<ConfigResource> with this HashSet, whose order is
        # unspecified; the result's future holds a list, as Java's client builds one.
        return ListConfigResourcesResult._new(future=self._done(list(config_resources)),
                                              kind=self._kind)

    def _describe_classic_groups(self, group_ids: Iterable[str]
                                 ) -> DescribeClassicGroupsResult[Any]:
        reject_str("group_ids", group_ids)
        with self._lock:
            raise _unsupported(_NOT_IMPLEMENTED)

    def _force_terminate_transaction(self, transactional_id: str
                                     ) -> TerminateTransactionResult[Any]:
        """Not synchronized in Java."""
        raise _unsupported(_NOT_IMPLEMENTED)

    def _close(self) -> None:
        """Java's ``close(Duration)``: an empty ``synchronized`` body."""
        with self._lock:
            pass
